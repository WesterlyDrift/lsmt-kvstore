package com.howard.lsm.core;

import com.howard.lsm.config.LSMConfig;
import com.howard.lsm.storage.LevelManager;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 压缩管理器
 *
 * 负责LSM-Tree的后台压缩任务：
 * 1. Level-based压缩策略：将上层的小文件合并到下层
 * 2. 减少文件数量：降低查询时需要检查的文件数
 * 3. 清理删除标记：在合并过程中清理已删除的键
 * 4. 优化存储空间：减少重复数据和碎片
 *
 * 压缩策略：
 * - Level 0: 直接从MemTable刷新，文件间可能有重叠
 * - Level 1+: 文件间无重叠，每层大小按倍数增长
 */
@Slf4j
public class CompactionManager {
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);

    private final LSMConfig config;
    private final LevelManager levelManager;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 压缩统计
    private final AtomicLong totalCompactions = new AtomicLong(0);
    private final AtomicLong totalBytesCompacted = new AtomicLong(0);

    public CompactionManager(LSMConfig config, LevelManager levelManager) {
        this.config = config;
        this.levelManager = levelManager;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "LSM-Compaction-Thread");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动压缩管理器
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            // 定期检查是否需要压缩
            scheduler.scheduleWithFixedDelay(this::performCompactionCheck,
                    10, 30, TimeUnit.SECONDS);
            logger.info("Compaction manager started");
        }
    }

    /**
     * 停止压缩管理器
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Compaction manager stopped");
        }
    }

    /**
     * 手动触发压缩
     */
    public void triggerCompaction() throws IOException {
        if (!running.get()) {
            throw new IllegalStateException("Compaction manager is not running");
        }

        scheduler.submit(this::performCompaction);
    }

    /**
     * 执行压缩检查
     */
    private void performCompactionCheck() {
        try {
            if (shouldPerformCompaction()) {
                performCompaction();
            }
        } catch (Exception e) {
            logger.error("Error during compaction check", e);
        }
    }

    /**
     * 判断是否需要执行压缩
     */
    private boolean shouldPerformCompaction() {
        // 检查各层级是否需要压缩
        for (int level = 0; level < config.getMaxLevel(); level++) {
            if (levelManager.needsCompaction(level)) {
                logger.debug("Level {} needs compaction", level);
                return true;
            }
        }
        return false;
    }

    /**
     * 执行压缩操作
     */
    private void performCompaction() {
        try {
            long startTime = System.currentTimeMillis();

            // 从Level 0开始检查每一层
            for (int level = 0; level < config.getMaxLevel() - 1; level++) {
                if (levelManager.needsCompaction(level)) {
                    compactLevel(level);
                    break; // 一次只压缩一层，避免资源争用
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.debug("Compaction completed in {}ms", duration);

        } catch (Exception e) {
            logger.error("Error during compaction", e);
        }
    }

    /**
     * 压缩指定层级
     */
    private void compactLevel(int level) throws IOException {
        logger.info("Starting compaction for level {}", level);

        // 1. 选择需要压缩的文件
        List<SSTable> sourceTables = levelManager.selectCompactionCandidates(level);
        if (sourceTables.isEmpty()) {
            return;
        }

        // 2. 如果是Level 0，需要特殊处理（因为文件间可能有重叠）
        List<SSTable> targetTables;
        if (level == 0) {
            targetTables = compactLevel0(sourceTables);
        } else {
            targetTables = compactLevelN(level, sourceTables);
        }

        // 3. 用新文件替换旧文件
        levelManager.replaceFiles(level, sourceTables, level + 1, targetTables);

        // 4. 删除旧文件
        for (SSTable oldTable : sourceTables) {
            oldTable.getFilePath().toFile().delete();
        }

        // 5. 更新统计信息
        totalCompactions.getAndIncrement();
        totalBytesCompacted.getAndAdd(
                sourceTables.stream()
                        .mapToLong(SSTable::getFileSize)
                        .sum());

        logger.info("Compaction completed for level {}, {} files merged into {} files",
                level, sourceTables.size(), targetTables.size());
    }

    /**
     * 压缩Level 0（文件间可能重叠）
     */
    private List<SSTable> compactLevel0(List<SSTable> sourceTables) throws IOException {
        // Level 0的压缩需要考虑键范围重叠
        // 这里简化实现，实际中需要复杂的合并逻辑
        return mergeSSTablesWithOverlap(sourceTables);
    }

    /**
     * 压缩Level N（N > 0，文件间无重叠）
     */
    private List<SSTable> compactLevelN(int level, List<SSTable> sourceTables) throws IOException {
        // Level N的压缩相对简单，因为文件间无重叠
        return mergeSSTablesWithoutOverlap(sourceTables);
    }

    /**
     * 合并有重叠的SSTable
     */
    private List<SSTable> mergeSSTablesWithOverlap(List<SSTable> tables) throws IOException {
        // 实现复杂的合并逻辑
        // 这里提供简化版本
        logger.debug("Merging {} SSTables with potential overlap", tables.size());

        // 使用优先队列合并多个有序序列
        PriorityQueue<SSTableIterator> heap = new PriorityQueue<>();

        // 初始化迭代器
        for (SSTable table : tables) {
            SSTableIterator iterator = new SSTableIterator(table);
            if (iterator.hasNext()) {
                heap.offer(iterator);
            }
        }

        // 执行归并操作
        return performMerge(heap);
    }

    /**
     * 合并无重叠的SSTable
     */
    private List<SSTable> mergeSSTablesWithoutOverlap(List<SSTable> tables) throws IOException {
        // 实现简单的合并逻辑
        logger.debug("Merging {} SSTables without overlap", tables.size());

        // 由于无重叠，可以直接按顺序合并
        return performSimpleMerge(tables);
    }

    /**
     * 执行归并操作
     */
    private List<SSTable> performMerge(PriorityQueue<SSTableIterator> heap) throws IOException {
        // 这里实现具体的归并逻辑
        // 返回合并后的SSTable列表
        return List.of(); // 简化实现
    }

    /**
     * 执行简单合并
     */
    private List<SSTable> performSimpleMerge(List<SSTable> tables) throws IOException {
        // 这里实现简单的文件合并逻辑
        return List.of(); // 简化实现
    }

    /**
     * SSTable迭代器，用于归并排序
     */
    @Getter
    private static class SSTableIterator implements Comparable<SSTableIterator> {
        private final SSTable table;
        private String currentKey;
        private byte[] currentValue;

        public SSTableIterator(SSTable table) {
            this.table = table;
            // 初始化第一个键值对
        }

        public boolean hasNext() {
            return currentKey != null;
        }

        public void next() {
            // 移动到下一个键值对
        }

        @Override
        public int compareTo(SSTableIterator other) {
            return this.currentKey.compareTo(other.currentKey);
        }

    }

    /**
     * 获取压缩统计信息
     */
    public CompactionStats getStats() {
        return new CompactionStats(totalCompactions.get(), totalBytesCompacted.get());
    }

    /**
     * 压缩统计信息
     */
    @Getter
    public static class CompactionStats {
        private final long totalCompactions;
        private final long totalBytesCompacted;

        public CompactionStats(long totalCompactions, long totalBytesCompacted) {
            this.totalCompactions = totalCompactions;
            this.totalBytesCompacted = totalBytesCompacted;
        }

    }
}