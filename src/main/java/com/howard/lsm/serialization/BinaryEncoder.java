package com.howard.lsm.serialization;

import com.howard.lsm.storage.BloomFilter;
import lombok.Getter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 二进制编码器
 *
 * BinaryEncoder负责将内存中的数据结构转换为紧凑的二进制格式。
 * 这就像是一个"翻译官"，将人类可读的数据转换为机器高效处理的格式。
 *
 * 为什么需要自定义的二进制格式？
 * 1. 性能：避免文本格式的解析开销
 * 2. 空间效率：二进制格式通常比文本格式更紧凑
 * 3. 跨平台：标准化的字节序保证在不同系统间的兼容性
 * 4. 完整性：内置校验机制确保数据不被损坏
 *
 * 我们的编码格式设计原则：
 * - 版本化：支持格式演进
 * - 自描述：包含必要的元数据
 * - 紧凑：最小化存储空间
 * - 快速：优化序列化和反序列化性能
 */
public class BinaryEncoder {

    // 版本信息，用于格式演进
    private static final byte FORMAT_VERSION = 1;

    // 特殊标记
    private static final byte NULL_MARKER = 0x00;
    private static final byte DATA_MARKER = 0x01;
    private static final byte DELETED_MARKER = 0x02;

    /**
     * 编码键值对
     *
     * 将一个键值对编码为二进制格式。这是存储引擎中最频繁的操作之一，
     * 因此我们需要确保它既正确又高效。
     *
     * 编码格式：
     * [版本][标记][键长度][键数据][值长度][值数据][校验和]
     *
     * @param key 键（不能为null）
     * @param value 值（可以为null，表示删除）
     * @return 编码后的字节数组
     */
    public byte[] encode(String key, byte[] value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            // 写入版本信息
            dos.writeByte(FORMAT_VERSION);

            // 写入数据标记
            if (value == null) {
                dos.writeByte(DELETED_MARKER);
            } else {
                dos.writeByte(DATA_MARKER);
            }

            // 编码键
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(keyBytes.length);
            dos.write(keyBytes);

            // 编码值
            if (value != null) {
                dos.writeInt(value.length);
                dos.write(value);
            } else {
                dos.writeInt(0); // 删除标记的值长度为0
            }

            // 计算并写入校验和
            byte[] dataWithoutChecksum = baos.toByteArray();
            int checksum = calculateChecksum(dataWithoutChecksum);
            dos.writeInt(checksum);

            return baos.toByteArray();
        }
    }

    /**
     * 编码WAL日志条目
     *
     * WAL条目比普通键值对需要更多的元数据，包括时间戳、序列号等。
     * 这些信息对于崩溃恢复和数据一致性至关重要。
     *
     * WAL条目格式：
     * [版本][条目类型][时间戳][序列号][键长度][键数据][值长度][值数据][校验和]
     *
     * @param key 键
     * @param value 值
     * @param timestamp 时间戳
     * @return 编码后的字节数组
     */
    public byte[] encodeLogEntry(String key, byte[] value, long timestamp) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            // 写入版本和类型
            dos.writeByte(FORMAT_VERSION);
            dos.writeByte(value == null ? DELETED_MARKER : DATA_MARKER);

            // 写入时间戳
            dos.writeLong(timestamp);

            // 写入序列号（这里简化为时间戳）
            dos.writeLong(timestamp);

            // 编码键值对
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(keyBytes.length);
            dos.write(keyBytes);

            if (value != null) {
                dos.writeInt(value.length);
                dos.write(value);
            } else {
                dos.writeInt(0);
            }

            // 计算并写入校验和
            byte[] dataWithoutChecksum = baos.toByteArray();
            int checksum = calculateChecksum(dataWithoutChecksum);
            dos.writeInt(checksum);

            return baos.toByteArray();
        }
    }

    /**
     * 编码块索引
     *
     * 块索引帮助我们快速定位数据块中的特定键。这就像书籍的目录，
     * 让我们能够直接跳转到包含所需信息的页面。
     *
     * 索引格式：
     * [版本][索引条目数][条目1][条目2]...[条目N]
     *
     * 每个索引条目：
     * [键长度][键数据][块偏移量][块大小]
     */
    public byte[] encodeBlockIndex(BlockIndex index) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeByte(FORMAT_VERSION);
            dos.writeInt(index.getEntryCount());

            for (BlockIndex.Entry entry : index.getEntries()) {
                // 编码键
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);

                // 编码偏移量和大小
                dos.writeLong(entry.getOffset());
                dos.writeInt(entry.getSize());
            }

            return baos.toByteArray();
        }
    }

    /**
     * 编码布隆过滤器
     *
     * 布隆过滤器的序列化需要保存位数组和哈希函数配置，
     * 这样反序列化时能够重建完全相同的过滤器。
     */
    public byte[] encodeBloomFilter(BloomFilter bloomFilter) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeByte(FORMAT_VERSION);

            // 写入布隆过滤器参数
            dos.writeInt(bloomFilter.getBitSetSize());
            dos.writeInt(bloomFilter.getNumHashFunctions());

            // 写入位数组
            byte[] bitArray = bloomFilter.getByteArray();
            dos.writeInt(bitArray.length);
            dos.write(bitArray);

            return baos.toByteArray();
        }
    }

    /**
     * 计算CRC32校验和
     *
     * 校验和是数据完整性的保证。它就像是数据的"指纹"，
     * 任何微小的变化都会导致完全不同的校验和。
     */
    private int calculateChecksum(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    /**
     * 块索引数据结构
     *
     * 这是一个辅助类，用于组织块索引信息
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

        public int getEntryCount() {
            return entries.size();
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