package com.howard.lsm;

import com.howard.lsm.cache.ShardedCache;
import com.howard.lsm.config.LSMConfig;
import com.howard.lsm.core.*;
import com.howard.lsm.serialization.BinaryEncoder;
import com.howard.lsm.storage.LevelManager;
import com.howard.lsm.transaction.TransactionManager;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LSM-Tree KV存储引擎主要实现类
 *
 * 该类提供了完整的LSM-Tree存储引擎功能，包括：
 * 1. 内存表(MemTable)管理
 * 2. 排序字符串表(SSTable)管理
 * 3. 写前日志(WAL)支持
 * 4. 分片缓存系统
 * 5. 布隆过滤器优化
 * 6. 多级压缩策略
 * 7. ACID事务支持
 *
 * 修复内容：
 * 1. 修复了锁的错误使用问题
 * 2. 完善了缓存一致性策略
 * 3. 增强了错误处理机制
 */
public class LSMTree implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LSMTree.class);

    private final LSMConfig config;
    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();

    // 核心组件
    private MemTable activeMemTable;
    private final WriteAheadLog wal;
    private final LevelManager levelManager;
    private final CompactionManager compactionManager;
    private final ShardedCache<String, byte[]> cache;
    /**
     * -- GETTER --
     *  获取事务管理器
     */
    @Getter
    private final TransactionManager transactionManager;

    // 系统状态
    private volatile boolean closed = false;

    public LSMTree(LSMConfig config) throws IOException {
        this.config = config;

        // 初始化数据目录
        initializeDirectories();

        // 初始化核心组件
        this.wal = new WriteAheadLog(config);
        this.activeMemTable = new MemTable(config);
        this.levelManager = new LevelManager(config);
        this.compactionManager = new CompactionManager(config, levelManager);
        this.cache = new ShardedCache<>(config.getCacheShardCount());
        this.transactionManager = new TransactionManager(this);

        // 启动后台任务
        compactionManager.start();

        // 执行恢复过程
        recover();

        logger.info("LSM-Tree storage engine initialized successfully");
    }

    /**
     * 存储键值对
     *
     * 修复要点：
     * 1. 正确使用写锁（获取写锁，释放写锁）
     * 2. 在put操作成功后立即更新缓存，保证缓存一致性
     * 3. 增强错误处理，确保异常情况下锁能正确释放
     */
    public void put(String key, byte[] value) throws IOException {
        if (closed) {
            throw new IllegalStateException("Storage engine is closed");
        }

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null, use delete() for deletion");
        }

        globalLock.writeLock().lock();
        try {
            logger.debug("Putting key: {}, value length: {}", key, value.length);

            // 写入WAL确保持久性
            wal.append(key, value);

            // 写入活跃内存表
            activeMemTable.put(key, value);

            // 立即更新缓存，确保后续读取能获得最新值
            cache.put(key, value);

            logger.debug("Successfully put key: {}", key);

            // 检查是否需要刷新内存表
            if (activeMemTable.shouldFlush()) {
                flushMemTable();
            }

        } catch (IOException e) {
            logger.error("Failed to put key: {}", key, e);
            throw e;
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * 获取键对应的值
     *
     * 优化要点：
     * 1. 保持原有的查找顺序：缓存 -> 内存表 -> 磁盘
     * 2. 增强日志记录，便于调试
     * 3. 改进缓存策略，只在确实找到值时才更新缓存
     */
    public byte[] get(String key) throws IOException {
        if (closed) {
            throw new IllegalStateException("Storage engine is closed");
        }

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        globalLock.readLock().lock();
        try {
            logger.debug("Getting key: {}", key);

            // 1. 检查缓存
            byte[] cachedValue = cache.get(key);
            if (cachedValue != null) {
                logger.debug("Cache hit for key: {}", key);
                return cachedValue;
            }

            // 2. 检查活跃内存表
            byte[] value = activeMemTable.get(key);
            if (value != null) {
                logger.debug("Found key in active memtable: {}", key);
                // 将从内存表读取的值加入缓存
                cache.put(key, value);
                return value;
            }

            // 3. 检查磁盘上的SSTable (从最新到最旧)
            value = levelManager.get(key);
            if (value != null) {
                logger.debug("Found key in SSTable: {}", key);
                // 将从磁盘读取的值加入缓存
                cache.put(key, value);
                return value;
            }

            logger.debug("Key not found: {}", key);
            return null;

        } catch (IOException e) {
            logger.error("Failed to get key: {}", key, e);
            throw e;
        } finally {
            globalLock.readLock().unlock();
        }
    }

    /**
     * 删除键
     *
     * 修复要点：
     * 1. 使用写锁而不是读锁，因为删除是写操作
     * 2. 确保从缓存中移除键，保证缓存一致性
     * 3. 增强错误处理和日志记录
     */
    public void delete(String key) throws IOException {
        if (closed) {
            throw new IllegalStateException("Storage engine is closed");
        }

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        // 修复：使用写锁而不是读锁
        globalLock.writeLock().lock();
        try {
            logger.debug("Deleting key: {}", key);

            // LSM-Tree使用墓碑标记删除
            wal.append(key, null);
            activeMemTable.delete(key);

            // 从缓存中移除，确保缓存一致性
            cache.remove(key);

            logger.debug("Successfully deleted key: {}", key);

        } catch (IOException e) {
            logger.error("Failed to delete key: {}", key, e);
            throw e;
        } finally {
            // 修复：释放写锁
            globalLock.writeLock().unlock();
        }
    }

    /**
     * 手动触发压缩
     */
    public void compact() throws IOException {
        if (closed) {
            throw new IllegalStateException("Storage engine is closed");
        }

        logger.info("Manually triggering compaction");
        compactionManager.triggerCompaction();
    }

    /**
     * 刷新内存表到磁盘
     *
     * 优化要点：
     * 1. 确保在刷新过程中正确处理缓存
     * 2. 增强错误处理，确保资源正确释放
     * 3. 提供更详细的日志信息
     */
    private void flushMemTable() throws IOException {
        // 注意：这个方法在调用时已经持有写锁
        try {
            logger.info("Flushing memtable to disk, current size: {} bytes",
                    activeMemTable.getSize());

            // 创建新的内存表
            MemTable oldMemTable = activeMemTable;
            activeMemTable = new MemTable(config);

            // 将旧内存表转换为SSTable
            SSTable newSSTable = oldMemTable.flushToSSTable();
            levelManager.addSSTable(newSSTable, 0);

            // 清理WAL
            wal.markFlushed(oldMemTable.getMaxSequenceNumber());

            logger.info("Memtable flushed successfully, created SSTable: {}",
                    newSSTable.getFilePath());

        } catch (IOException e) {
            logger.error("Failed to flush memtable", e);
            throw e;
        }
    }

    /**
     * 恢复过程
     *
     * 增强要点：
     * 1. 在恢复过程中清空缓存，确保数据一致性
     * 2. 提供更详细的恢复进度信息
     * 3. 增强错误处理
     */
    private void recover() throws IOException {
        logger.info("Starting recovery process");

        try {
            // 清空缓存，确保恢复后的数据一致性
            cache.clear();
            // 从WAL恢复
            logger.info("Recovering from WAL...");
            wal.recover(activeMemTable);

            // 从磁盘加载SSTable
            logger.info("Loading existing SSTables...");
            levelManager.loadExistingSSTables();

            logger.info("Recovery process completed successfully");

        } catch (IOException e) {
            logger.error("Recovery process failed", e);
            throw e;
        }
    }

    /**
     * 初始化目录结构
     */
    private void initializeDirectories() throws IOException {
        Path dataDir = Paths.get(config.getDataDirectory());
        Path walDir = Paths.get(config.getWalDirectory());

        try {
            Files.createDirectories(dataDir);
            Files.createDirectories(walDir);
            logger.info("Initialized directories - Data: {}, WAL: {}", dataDir, walDir);
        } catch (IOException e) {
            logger.error("Failed to initialize directories", e);
            throw e;
        }
    }

    /**
     * 获取存储引擎的统计信息
     *
     * 新增方法：提供系统运行状态的可见性
     */
    public String getStats() {
        globalLock.readLock().lock();
        try {
            StringBuilder stats = new StringBuilder();
            stats.append("LSM-Tree Storage Engine Statistics:\n");
            stats.append("- Active MemTable Size: ").append(activeMemTable.getSize()).append(" bytes\n");
            stats.append("- Active MemTable Entries: ").append(activeMemTable.getMaxSequenceNumber()).append("\n");
            stats.append("- Cache Shard Count: ").append(cache.getShardCount()).append("\n");
            stats.append("- Engine Status: ").append(closed ? "CLOSED" : "RUNNING").append("\n");

            // 添加压缩统计信息
            var compactionStats = compactionManager.getStats();
            stats.append("- Total Compactions: ").append(compactionStats.getTotalCompactions()).append("\n");
            stats.append("- Total Bytes Compacted: ").append(compactionStats.getTotalBytesCompacted()).append("\n");

            return stats.toString();
        } finally {
            globalLock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        globalLock.writeLock().lock();
        try {
            logger.info("Closing LSM-Tree storage engine");
            closed = true;

            // 停止后台任务
            compactionManager.stop();

            // 刷新内存表
            if (activeMemTable.getSize() > 0) {
                flushMemTable();
            }

            // 关闭WAL
            wal.close();

            logger.info("LSM-Tree storage engine closed successfully");

        } catch (IOException e) {
            logger.error("Error during close", e);
            throw e;
        } finally {
            globalLock.writeLock().unlock();
        }
    }
}