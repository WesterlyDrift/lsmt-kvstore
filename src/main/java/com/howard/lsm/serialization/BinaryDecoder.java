package com.howard.lsm.serialization;

import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 二进制解码器
 *
 * BinaryDecoder是BinaryEncoder的"孪生兄弟"，负责将二进制数据
 * 重新转换为内存中的数据结构。它必须精确地理解编码器创建的格式，
 * 就像两个人使用同一本密码本进行通信。
 *
 * 解码器的设计原则：
 * 1. 容错性：能够处理格式版本变化和损坏的数据
 * 2. 性能：最小化内存分配和数据复制
 * 3. 安全性：验证数据完整性，防止恶意输入
 * 4. 向后兼容：支持旧版本格式的数据
 *
 * 这种编码/解码对的设计让我们的存储引擎能够可靠地在内存和磁盘之间
 * 转换数据，同时保持高性能和数据完整性。
 */
public class BinaryDecoder {

    // 版本兼容性映射
    private static final byte SUPPORTED_MIN_VERSION = 1;
    private static final byte SUPPORTED_MAX_VERSION = 1;

    // 数据标记常量
    private static final byte NULL_MARKER = 0x00;
    private static final byte DATA_MARKER = 0x01;
    private static final byte DELETED_MARKER = 0x02;

    /**
     * 解码键值对
     *
     * 这个方法执行编码的逆操作，从字节数组中重建键值对。
     * 解码过程中我们需要小心处理各种边界情况和错误条件。
     *
     * @param data 编码的字节数组
     * @return 解码后的键值对
     * @throws IOException 如果数据格式不正确或已损坏
     */
    public KeyValuePair decode(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            throw new IOException("Cannot decode null or empty data");
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            // 读取并验证版本
            byte version = dis.readByte();
            validateVersion(version);

            // 读取数据标记
            byte marker = dis.readByte();
            boolean isDeleted = (marker == DELETED_MARKER);

            // 解码键
            int keyLength = dis.readInt();
            if (keyLength < 0 || keyLength > 10240) { // 10KB限制
                throw new IOException("Invalid key length: " + keyLength);
            }

            byte[] keyBytes = new byte[keyLength];
            dis.readFully(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            // 解码值
            byte[] value = null;
            int valueLength = dis.readInt();

            if (!isDeleted && valueLength > 0) {
                if (valueLength > 1024 * 1024) { // 1MB限制
                    throw new IOException("Value too large: " + valueLength);
                }
                value = new byte[valueLength];
                dis.readFully(value);
            }

            // 验证校验和
            int expectedChecksum = dis.readInt();
            int actualChecksum = calculateChecksum(data, data.length - 4);

            if (expectedChecksum != actualChecksum) {
                throw new IOException("Checksum mismatch - data may be corrupted");
            }

            return new KeyValuePair(key, value, isDeleted);

        } catch (IOException e) {
            throw new IOException("Failed to decode key-value pair", e);
        }
    }

    /**
     * 解码WAL日志条目
     *
     * WAL条目包含额外的元数据，如时间戳和序列号。
     * 这些信息在恢复过程中用于确定操作的顺序和时效性。
     *
     * @param data 编码的字节数组
     * @return 解码后的日志条目
     */
    public LogEntry decodeLogEntry(byte[] data) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            // 读取版本和类型
            byte version = dis.readByte();
            validateVersion(version);

            byte marker = dis.readByte();
            boolean isDeleted = (marker == DELETED_MARKER);

            // 读取时间戳和序列号
            long timestamp = dis.readLong();
            long sequenceNumber = dis.readLong();

            // 解码键值对
            int keyLength = dis.readInt();
            byte[] keyBytes = new byte[keyLength];
            dis.readFully(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            byte[] value = null;
            int valueLength = dis.readInt();
            if (!isDeleted && valueLength > 0) {
                value = new byte[valueLength];
                dis.readFully(value);
            }

            // 验证校验和
            int expectedChecksum = dis.readInt();
            int actualChecksum = calculateChecksum(data, data.length - 4);

            if (expectedChecksum != actualChecksum) {
                throw new IOException("WAL entry checksum mismatch");
            }

            return new LogEntry(key, value, timestamp, sequenceNumber, isDeleted);
        }
    }

    /**
     * 解码块索引
     *
     * 块索引的解码需要重建索引数据结构，包括所有的键、偏移量和大小信息。
     * 这个索引将用于快速定位数据块中的特定数据。
     */
    public BlockIndex decodeBlockIndex(byte[] data) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            byte version = dis.readByte();
            validateVersion(version);

            int entryCount = dis.readInt();
            if (entryCount < 0 || entryCount > 100000) { // 合理的限制
                throw new IOException("Invalid index entry count: " + entryCount);
            }

            BlockIndex index = new BlockIndex();

            for (int i = 0; i < entryCount; i++) {
                // 读取键
                int keyLength = dis.readInt();
                byte[] keyBytes = new byte[keyLength];
                dis.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                // 读取偏移量和大小
                long offset = dis.readLong();
                int size = dis.readInt();

                index.addEntry(key, offset, size);
            }

            return index;
        }
    }

    /**
     * 解码布隆过滤器
     *
     * 重建布隆过滤器需要恢复其内部状态，包括位数组和哈希函数配置。
     * 解码后的过滤器必须与原始过滤器在功能上完全相同。
     */
    public BloomFilterData decodeBloomFilter(byte[] data) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            byte version = dis.readByte();
            validateVersion(version);

            // 读取过滤器参数
            int bitSetSize = dis.readInt();
            int numHashFunctions = dis.readInt();

            // 读取位数组
            int bitArrayLength = dis.readInt();
            byte[] bitArray = new byte[bitArrayLength];
            dis.readFully(bitArray);

            return new BloomFilterData(bitSetSize, numHashFunctions, bitArray);
        }
    }

    /**
     * 验证版本兼容性
     *
     * 这个方法确保我们能够处理数据的格式版本。如果遇到不支持的版本，
     * 我们需要优雅地失败，而不是产生错误的结果。
     */
    private void validateVersion(byte version) throws IOException {
        if (version < SUPPORTED_MIN_VERSION || version > SUPPORTED_MAX_VERSION) {
            throw new IOException(String.format(
                    "Unsupported format version: %d (supported: %d-%d)",
                    version, SUPPORTED_MIN_VERSION, SUPPORTED_MAX_VERSION));
        }
    }

    /**
     * 计算校验和
     *
     * 这个方法必须与编码器使用完全相同的算法，
     * 这样才能正确验证数据完整性。
     */
    private int calculateChecksum(byte[] data, int length) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data, 0, length);
        return (int) crc.getValue();
    }

    /**
     * 键值对数据结构
     */
    @Getter
    public static class KeyValuePair {
        private final String key;
        private final byte[] value;
        private final boolean deleted;

        public KeyValuePair(String key, byte[] value, boolean deleted) {
            this.key = key;
            this.value = value;
            this.deleted = deleted;
        }

    }

    /**
     * WAL日志条目数据结构
     */
    @Getter
    public static class LogEntry {
        private final String key;
        private final byte[] value;
        private final long timestamp;
        private final long sequenceNumber;
        private final boolean deleted;

        public LogEntry(String key, byte[] value, long timestamp,
                        long sequenceNumber, boolean deleted) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.sequenceNumber = sequenceNumber;
            this.deleted = deleted;
        }

    }

    /**
     * 布隆过滤器数据结构
     */
    @Getter
    public static class BloomFilterData {
        private final int bitSetSize;
        private final int numHashFunctions;
        private final byte[] bitArray;

        public BloomFilterData(int bitSetSize, int numHashFunctions, byte[] bitArray) {
            this.bitSetSize = bitSetSize;
            this.numHashFunctions = numHashFunctions;
            this.bitArray = bitArray;
        }

    }

    /**
     * 块索引数据结构
     */
    @Getter
    public static class BlockIndex {
        private final java.util.List<Entry> entries;

        public BlockIndex() {
            this.entries = new java.util.ArrayList<>();
        }

        public void addEntry(String key, long offset, int size) {
            entries.add(new Entry(key, offset, size));
        }

        @Getter
        public static class Entry {
            private final String key;
            private final long offset;
            private final int size;

            public Entry(String key, long offset, int size) {
                this.key = key;
                this.offset = offset;
                this.size = size;
            }

        }
    }
}