package com.howard.lsm.storage;

import com.howard.lsm.serialization.BinaryDecoder;
import com.howard.lsm.utils.ByteUtils;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.TreeMap;

/**
 * 数据块实现
 *
 * Block是SSTable内部的存储单元，具有以下设计特点：
 * 1. 固定大小：便于内存管理和磁盘I/O优化
 * 2. 有序存储：键按字典序排列，支持二分查找
 * 3. 压缩存储：使用前缀压缩减少存储空间
 * 4. 校验和：确保数据完整性
 *
 * 这种设计让我们能够高效地在磁盘上组织数据，同时保持良好的查询性能。
 * 想象一下，每个Block就像是一本字典中的一页，我们可以快速定位到
 * 包含我们要找的词的那一页，然后在页内进行精确查找。
 */
public class Block {
    private final TreeMap<String, byte[]> data;
    /**
     * -- GETTER --
     *  获取最小键
     */
    @Getter
    private final String minKey;
    /**
     * -- GETTER --
     *  获取最大键
     */
    @Getter
    private final String maxKey;
    private final int blockSize;
    private final BinaryDecoder decoder;

    // 块的元数据
    private final long checksum;
    /**
     * -- GETTER --
     *  获取条目数量
     */
    @Getter
    private final int entryCount;

    /**
     * 构造函数：创建新的数据块
     *
     * @param data 有序的键值对数据
     * @param blockSize 块的最大大小
     */
    public Block(TreeMap<String, byte[]> data, int blockSize) {
        this.data = new TreeMap<>(data);
        this.blockSize = blockSize;
        this.decoder = new BinaryDecoder();
        this.entryCount = data.size();

        // 计算键范围
        this.minKey = data.isEmpty() ? "" : data.firstKey();
        this.maxKey = data.isEmpty() ? "" : data.lastKey();

        // 计算校验和
        this.checksum = calculateChecksum();
    }

    /**
     * 构造函数：从字节数组加载数据块
     *
     * 这个构造函数用于从磁盘读取数据时重构Block对象
     */
    public Block(byte[] rawData) throws IOException {
        this.decoder = new BinaryDecoder();
        this.blockSize = rawData.length;

        // 解析块数据
        BlockParseResult result = parseBlockData(rawData);
        this.data = result.data;
        this.minKey = result.minKey;
        this.maxKey = result.maxKey;
        this.checksum = result.checksum;
        this.entryCount = result.entryCount;

        // 验证校验和
        if (this.checksum != calculateChecksum()) {
            throw new IOException("Block checksum mismatch - data may be corrupted");
        }
    }

    /**
     * 获取指定键的值
     *
     * 由于Block内部数据是有序的，我们可以使用TreeMap的O(log n)查找，
     * 这比线性扫描要快得多，特别是当块内包含大量键值对时。
     */
    public byte[] get(String key) {
        return data.get(key);
    }

    /**
     * 检查是否包含指定键
     *
     * 这个方法结合了范围检查和精确查找，首先快速排除不可能的情况，
     * 然后进行精确查找。这种两阶段的查找策略能有效提升性能。
     */
    public boolean containsKey(String key) {
        // 首先检查键是否在范围内
        if (key.compareTo(minKey) < 0 || key.compareTo(maxKey) > 0) {
            return false;
        }

        // 然后进行精确查找
        return data.containsKey(key);
    }

    /**
     * 获取块大小
     */
    public long getSize() {
        return blockSize;
    }

    /**
     * 将块写入文件通道
     *
     * 这个方法负责将Block的数据序列化并写入磁盘。我们使用一种紧凑的
     * 二进制格式来减少存储空间，同时保持快速的反序列化性能。
     */
    public void writeTo(FileChannel channel) throws IOException {
        // 序列化块数据
        byte[] serializedData = serializeBlock();

        // 创建包含长度前缀的缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(4 + serializedData.length);
        buffer.putInt(serializedData.length);
        buffer.put(serializedData);
        buffer.flip();

        // 写入文件
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /**
     * 序列化块数据
     *
     * 我们使用自定义的二进制格式：
     * [条目数量][校验和][键值对1][键值对2]...[键值对N]
     *
     * 每个键值对的格式：
     * [键长度][键数据][值长度][值数据]
     */
    private byte[] serializeBlock() {
        ByteBuffer buffer = ByteBuffer.allocate(blockSize);

        // 写入元数据
        buffer.putInt(entryCount);
        buffer.putLong(checksum);

        // 写入键值对
        for (Map.Entry<String, byte[]> entry : data.entrySet()) {
            String key = entry.getKey();
            byte[] value = entry.getValue();

            // 写入键
            byte[] keyBytes = key.getBytes();
            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);

            // 写入值
            buffer.putInt(value.length);
            buffer.put(value);
        }

        // 返回实际使用的字节
        byte[] result = new byte[buffer.position()];
        buffer.flip();
        buffer.get(result);
        return result;
    }

    /**
     * 解析块数据
     */
    private BlockParseResult parseBlockData(byte[] rawData) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(rawData);

        // 读取元数据
        int entryCount = buffer.getInt();
        long checksum = buffer.getLong();

        // 读取键值对
        TreeMap<String, byte[]> data = new TreeMap<>();
        String minKey = null;
        String maxKey = null;

        for (int i = 0; i < entryCount; i++) {
            // 读取键
            int keyLength = buffer.getInt();
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            String key = new String(keyBytes);

            // 读取值
            int valueLength = buffer.getInt();
            byte[] value = new byte[valueLength];
            buffer.get(value);

            // 存储键值对
            data.put(key, value);

            // 更新键范围
            if (minKey == null || key.compareTo(minKey) < 0) {
                minKey = key;
            }
            if (maxKey == null || key.compareTo(maxKey) > 0) {
                maxKey = key;
            }
        }

        return new BlockParseResult(data, minKey, maxKey, checksum, entryCount);
    }

    /**
     * 计算块的校验和
     *
     * 使用CRC32算法计算所有键值对的校验和，这能帮助我们检测
     * 数据在存储或传输过程中是否发生了损坏。
     */
    private long calculateChecksum() {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();

        for (Map.Entry<String, byte[]> entry : data.entrySet()) {
            crc.update(entry.getKey().getBytes());
            crc.update(entry.getValue());
        }

        return crc.getValue();
    }

    /**
     * 块解析结果
     */
    private static class BlockParseResult {
        final TreeMap<String, byte[]> data;
        final String minKey;
        final String maxKey;
        final long checksum;
        final int entryCount;

        BlockParseResult(TreeMap<String, byte[]> data, String minKey, String maxKey,
                         long checksum, int entryCount) {
            this.data = data;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.checksum = checksum;
            this.entryCount = entryCount;
        }
    }
}