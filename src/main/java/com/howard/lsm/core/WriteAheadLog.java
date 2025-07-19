package com.howard.lsm.core;

import com.howard.lsm.config.LSMConfig;
import com.howard.lsm.serialization.BinaryEncoder;
import com.howard.lsm.serialization.BinaryDecoder;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 写前日志实现
 *
 * 提供故障恢复机制，确保数据持久性：
 * 1. 所有写操作先写入WAL
 * 2. 支持崩溃后恢复
 * 3. 支持日志截断和清理
 */
public class WriteAheadLog implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);
    private static final int HEADER_SIZE = 8; // CRC(4) + Length(4)
    private static final int MAX_ENTRY_SIZE = 10 * 1024 * 1024; // 10MB，防止读取过大的条目

    private final LSMConfig config;
    private final Path walPath;
    private final FileChannel channel;
    private final ByteBuffer buffer;
    private final ReentrantLock writeLock;
    private final BinaryEncoder encoder;
    private final BinaryDecoder decoder;

    @Getter
    private long lastFlushedSequence = 0;
    private volatile boolean closed = false;

    public WriteAheadLog(LSMConfig config) throws IOException {
        this.config = config;
        this.walPath = Paths.get(config.getWalDirectory(), "wal.log");
        this.channel = FileChannel.open(walPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        this.buffer = ByteBuffer.allocate(8192);
        this.writeLock = new ReentrantLock();
        this.encoder = new BinaryEncoder();
        this.decoder = new BinaryDecoder();

        logger.info("WAL initialized at: {}", walPath);
    }

    /**
     * 写入日志条目
     */
    public void append(String key, byte[] value) throws IOException {
        if (closed) {
            throw new IllegalStateException("WAL is closed");
        }

        writeLock.lock();
        try {
            // 编码日志条目
            byte[] entry = encoder.encodeLogEntry(key, value, System.currentTimeMillis());

            // 准备写入缓冲区
            buffer.clear();
            buffer.putInt(calculateCRC(entry));
            buffer.putInt(entry.length);
            buffer.put(entry);
            buffer.flip();

            // 写入文件
            channel.write(buffer);

            // 根据配置决定是否立即同步
            if (config.isWalSyncImmediate()) {
                channel.force(true);
            }

            logger.debug("WAL entry written: key={}, valueLength={}",
                    key, value != null ? value.length : 0);

        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 增强的WAL恢复机制
     *
     * 这个方法现在能够处理各种错误情况：
     * 1. 文件损坏 - 跳过损坏的条目，继续处理后续数据
     * 2. 不完整的条目 - 检测并停止在不完整的数据处
     * 3. 格式错误 - 验证数据格式，忽略无效条目
     */
    public void recover(MemTable memTable) throws IOException {
        if (!walPath.toFile().exists()) {
            logger.info("WAL file does not exist, skipping recovery");
            return;
        }

        long fileSize = walPath.toFile().length();
        if (fileSize == 0) {
            logger.info("WAL file is empty, skipping recovery");
            return;
        }

        logger.info("Starting WAL recovery from file: {} (size: {} bytes)", walPath, fileSize);

        int recoveredEntries = 0;
        int corruptedEntries = 0;

        try (FileChannel readChannel = FileChannel.open(walPath, StandardOpenOption.READ)) {
            ByteBuffer readBuffer = ByteBuffer.allocate(8192);
            long position = 0;

            while (position < fileSize) {
                // 尝试读取条目头部
                EntryHeader header = readEntryHeader(readChannel, readBuffer, position);
                if (header == null) {
                    // 文件结束或遇到不完整的头部
                    logger.warn("Reached end of valid data at position: {}", position);
                    break;
                }

                // 验证条目长度的合理性
                if (header.length <= 0 || header.length > MAX_ENTRY_SIZE) {
                    logger.warn("Invalid entry length: {} at position: {}, stopping recovery",
                            header.length, position);
                    break;
                }

                // 检查是否有足够的数据来读取完整的条目
                if (position + HEADER_SIZE + header.length > fileSize) {
                    logger.warn("Incomplete entry at position: {}, expected {} bytes but only {} available",
                            position, HEADER_SIZE + header.length, fileSize - position);
                    break;
                }

                // 读取条目数据
                byte[] entryData = readEntryData(readChannel, readBuffer, header.length);
                if (entryData == null) {
                    logger.warn("Failed to read entry data at position: {}", position);
                    break;
                }

                // 验证校验和
                int actualCRC = calculateCRC(entryData);
                if (actualCRC != header.crc) {
                    logger.warn("CRC mismatch at position: {}, expected: {}, actual: {}, skipping entry",
                            position, header.crc, actualCRC);
                    corruptedEntries++;
                    position += HEADER_SIZE + header.length;
                    continue;
                }

                // 解码并恢复条目
                try {
                    var logEntry = decoder.decodeLogEntry(entryData);
                    if (logEntry.getValue() == null) {
                        memTable.delete(logEntry.getKey());
                    } else {
                        memTable.put(logEntry.getKey(), logEntry.getValue());
                    }
                    recoveredEntries++;

                    logger.debug("Recovered entry: key={}, deleted={}",
                            logEntry.getKey(), logEntry.isDeleted());

                } catch (Exception e) {
                    logger.warn("Failed to decode entry at position: {}, error: {}",
                            position, e.getMessage());
                    corruptedEntries++;
                }

                position += HEADER_SIZE + header.length;
            }

        } catch (IOException e) {
            logger.error("I/O error during WAL recovery", e);
            throw e;
        }

        logger.info("WAL recovery completed: {} entries recovered, {} corrupted entries skipped",
                recoveredEntries, corruptedEntries);

        // 如果有损坏的条目，建议重建WAL
        if (corruptedEntries > 0) {
            logger.warn("Found {} corrupted entries, consider rebuilding WAL", corruptedEntries);
        }
    }

    /**
     * 读取条目头部
     */
    private EntryHeader readEntryHeader(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        buffer.clear();
        buffer.limit(HEADER_SIZE);

        channel.position(position);
        int bytesRead = channel.read(buffer);

        if (bytesRead < HEADER_SIZE) {
            return null; // 文件结束或数据不完整
        }

        buffer.flip();
        int crc = buffer.getInt();
        int length = buffer.getInt();

        return new EntryHeader(crc, length);
    }

    /**
     * 读取条目数据
     */
    private byte[] readEntryData(FileChannel channel, ByteBuffer buffer, int length) throws IOException {
        byte[] data = new byte[length];
        ByteBuffer dataBuffer = ByteBuffer.wrap(data);

        int totalBytesRead = 0;
        while (totalBytesRead < length) {
            int bytesRead = channel.read(dataBuffer);
            if (bytesRead == -1) {
                return null; // 文件结束
            }
            totalBytesRead += bytesRead;
        }

        return data;
    }

    /**
     * 标记已刷新的序列号
     */
    public void markFlushed(long sequenceNumber) throws IOException {
        this.lastFlushedSequence = sequenceNumber;

        // 如果配置允许，可以截断WAL
        if (config.isWalTruncateEnabled()) {
            truncateWAL();
        }
    }

    /**
     * 安全地截断WAL文件
     */
    private void truncateWAL() throws IOException {
        writeLock.lock();
        try {
            // 强制同步当前数据
            channel.force(true);

            // 创建新的WAL文件
            channel.truncate(0);

            logger.info("WAL truncated");
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 计算CRC32校验码
     */
    private int calculateCRC(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        writeLock.lock();
        try {
            closed = true;

            if (channel.isOpen()) {
                // 确保所有数据都写入磁盘
                channel.force(true);
                channel.close();
                logger.info("WAL closed successfully");
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 条目头部数据结构
     */
    private static class EntryHeader {
        final int crc;
        final int length;

        EntryHeader(int crc, int length) {
            this.crc = crc;
            this.length = length;
        }
    }

    /**
     * 修复损坏的WAL文件
     *
     * 这个方法可以用于手动修复损坏的WAL文件
     */
    public void repairWAL() throws IOException {
        logger.info("Starting WAL repair...");

        // 备份原文件
        Path backupPath = Paths.get(walPath.toString() + ".backup");
        java.nio.file.Files.copy(walPath, backupPath,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        // 重新创建WAL文件
        truncateWAL();

        logger.info("WAL repaired, backup saved to: {}", backupPath);
    }
}