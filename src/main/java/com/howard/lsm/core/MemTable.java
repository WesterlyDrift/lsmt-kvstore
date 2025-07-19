package com.howard.lsm.core;

import com.howard.lsm.config.LSMConfig;
import com.howard.lsm.serialization.BinaryEncoder;
import com.howard.lsm.storage.BloomFilter;
import com.howard.lsm.storage.Block;
import com.howard.lsm.storage.BlockBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 内存表实现
 *
 * 使用ConcurrentSkipListMap作为底层数据结构，提供：
 * 1. 线程安全的并发访问
 * 2. 有序的键值存储
 * 3. O(log n)的查找、插入、删除操作
 */
public class MemTable {
    private static final byte[] TOMBSTONE = new byte[0];

    private final ConcurrentSkipListMap<String, byte[]> data;
    private final AtomicLong size;
    private final AtomicLong sequenceNumber;
    private final long maxSize;
    private final LSMConfig config;

    public MemTable(LSMConfig config) {
        this.config = config;
        this.data = new ConcurrentSkipListMap<>();
        this.size = new AtomicLong(0);
        this.sequenceNumber = new AtomicLong(0);
        this.maxSize = config.getMemTableSize();
    }

    /**
     * 插入键值对
     */
    public void put(String key, byte[] value) {
        byte[] oldValue = data.put(key, value);
        long newSequence = sequenceNumber.incrementAndGet();

        // 更新大小统计
        if (oldValue == null) {
            size.addAndGet(key.length() + value.length);
        } else {
            size.addAndGet(value.length - oldValue.length);
        }
    }

    /**
     * 获取键对应的值
     */
    public byte[] get(String key) {
        byte[] value = data.get(key);
        if (value == TOMBSTONE) {
            return null; // 已删除
        }
        return value;
    }

    /**
     * 删除键（使用墓碑标记）
     */
    public void delete(String key) {
        byte[] oldValue = data.put(key, TOMBSTONE);
        sequenceNumber.incrementAndGet();

        if (oldValue != null && oldValue != TOMBSTONE) {
            size.addAndGet(-oldValue.length);
        }
    }

    /**
     * 检查是否应该刷新到磁盘
     */
    public boolean shouldFlush() {
        return size.get() >= maxSize;
    }

    /**
     * 获取当前大小
     */
    public long getSize() {
        return size.get();
    }

    /**
     * 获取最大序列号
     */
    public long getMaxSequenceNumber() {
        return sequenceNumber.get();
    }

    /**
     * 刷新到SSTable
     */
    public SSTable flushToSSTable() throws IOException {
        String filename = generateSSTableFilename();
        Path path = Paths.get(config.getDataDirectory(), filename);

        // 创建块构建器
        BlockBuilder blockBuilder = new BlockBuilder(config.getBlockSize());
        BloomFilter bloomFilter = new BloomFilter(data.size(), config.getBloomFilterFPP());

        // 写入所有数据
        BinaryEncoder encoder = new BinaryEncoder();
        for (var entry : data.entrySet()) {
            String key = entry.getKey();
            byte[] value = entry.getValue();

            // 跳过墓碑（在刷新时清理）
            if (value == TOMBSTONE) {
                continue;
            }

            // 添加到布隆过滤器
            bloomFilter.add(key);

            // 编码键值对
            byte[] encodedEntry = encoder.encode(key, value);
            blockBuilder.add(key, encodedEntry);
        }

        // 构建SSTable
        SSTable sstable = new SSTable(path, blockBuilder.build(), bloomFilter);
        return sstable;
    }

    /**
     * 生成SSTable文件名
     */
    private String generateSSTableFilename() {
        return String.format("sstable_%d_%d.dat",
                System.currentTimeMillis(),
                sequenceNumber.get());
    }
}