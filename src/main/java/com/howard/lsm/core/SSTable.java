package com.howard.lsm.core;

import com.howard.lsm.storage.Block;
import com.howard.lsm.storage.BloomFilter;
import com.howard.lsm.serialization.BinaryDecoder;
import com.howard.lsm.serialization.BinaryEncoder;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 排序字符串表(SSTable)实现
 *
 * SSTable是LSM-Tree的核心存储结构，具有以下特性：
 * 1. 不可变性：一旦创建就不再修改
 * 2. 有序性：键按字典序排列
 * 3. 分块存储：数据分块存储便于查找
 * 4. 布隆过滤器：快速判断键是否存在
 *
 * 文件结构：
 * [数据块1][数据块2]...[数据块N][索引块][布隆过滤器][元数据]
 */
public class SSTable {
    /**
     * -- GETTER --
     *  获取文件路径
     */
    @Getter
    private final Path filePath;
    private final List<Block> blocks;
    private final BloomFilter bloomFilter;
    /**
     * -- GETTER --
     *  获取文件大小
     */
    @Getter
    private final long fileSize;
    @Getter
    private final long minKey;
    @Getter
    private final long maxKey;
    /**
     * -- GETTER --
     *  获取级别
     */
    @Getter
    private final int level;

    // 文件通道，用于读取数据
    private FileChannel fileChannel;
    private final BinaryDecoder decoder;
    private final BinaryEncoder encoder;

    /**
     * 构造函数：创建新的SSTable
     */
    public SSTable(Path filePath, List<Block> blocks, BloomFilter bloomFilter) throws IOException {
        this.filePath = filePath;
        this.blocks = new ArrayList<>(blocks);
        this.bloomFilter = bloomFilter;
        this.decoder = new BinaryDecoder();
        this.encoder = new BinaryEncoder();
        this.level = 0; // 新创建的SSTable默认在Level 0

        // 计算文件大小和键范围
        this.fileSize = calculateFileSize();
        this.minKey = calculateMinKey();
        this.maxKey = calculateMaxKey();

        // 将数据写入磁盘
        writeToFile();
    }

    /**
     * 构造函数：从现有文件加载SSTable
     */
    public SSTable(Path filePath, int level) throws IOException {
        this.filePath = filePath;
        this.level = level;
        this.decoder = new BinaryDecoder();
        this.encoder = new BinaryEncoder();
        this.blocks = new ArrayList<>();

        // 从文件读取数据和布隆过滤器
        loadFromFile();

        // 重新计算属性
        this.fileSize = filePath.toFile().length();
        this.minKey = calculateMinKey();
        this.maxKey = calculateMaxKey();
    }

    /**
     * 查找指定键的值
     */
    public byte[] get(String key) throws IOException {
        // 1. 检查布隆过滤器
        if (!bloomFilter.mightContain(key)) {
            return null; // 键肯定不存在
        }

        // 2. 使用二分查找定位块
        Block targetBlock = findBlock(key);
        if (targetBlock == null) {
            return null;
        }

        // 3. 在块内查找键
        return targetBlock.get(key);
    }

    /**
     * 检查键是否可能存在
     */
    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }

    /**
     * 获取键范围
     */
    public boolean keyInRange(String key) {
        return key.compareTo(String.valueOf(minKey)) >= 0 &&
                key.compareTo(String.valueOf(maxKey)) <= 0;
    }

    /**
     * 使用二分查找定位包含指定键的块
     */
    private Block findBlock(String key) {
        int left = 0, right = blocks.size() - 1;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            Block block = blocks.get(mid);

            if (block.containsKey(key)) {
                return block;
            } else if (block.getMaxKey().compareTo(key) < 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return null;
    }

    /**
     * 写入文件
     */
    private void writeToFile() throws IOException {
        try (FileChannel channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            // 写入数据块
            for (Block block : blocks) {
                block.writeTo(channel);
            }

            // 写入布隆过滤器并获取长度
            int bloomLength = writeBloomFilter(channel);

            // 写入元数据
            writeMetadata(channel, bloomLength);
        }
    }

    /**
     * 从文件加载
     */
    private void loadFromFile() throws IOException {
        this.fileChannel = FileChannel.open(filePath, StandardOpenOption.READ);
        long size = fileChannel.size();

        // 读取元数据
        ByteBuffer metaBuf = ByteBuffer.allocate(8);
        fileChannel.position(size - 8);
        fileChannel.read(metaBuf);
        metaBuf.flip();
        int blockCount = metaBuf.getInt();
        int bloomLength = metaBuf.getInt();

        long bloomStart = size - 8L - bloomLength;

        // 读取数据块
        fileChannel.position(0);
        for (int i = 0; i < blockCount; i++) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            fileChannel.read(lenBuf);
            lenBuf.flip();
            int blockSize = lenBuf.getInt();

            ByteBuffer dataBuf = ByteBuffer.allocate(blockSize);
            fileChannel.read(dataBuf);
            dataBuf.flip();
            byte[] blockBytes = new byte[blockSize];
            dataBuf.get(blockBytes);
            blocks.add(new Block(blockBytes));
        }

        // 读取布隆过滤器
        ByteBuffer bloomBuf = ByteBuffer.allocate(bloomLength);
        fileChannel.position(bloomStart);
        fileChannel.read(bloomBuf);
        bloomBuf.flip();
        byte[] bloomBytes = new byte[bloomLength];
        bloomBuf.get(bloomBytes);
        BinaryDecoder.BloomFilterData bfData = decoder.decodeBloomFilter(bloomBytes);
        this.bloomFilter = new BloomFilter(bfData.getBitSetSize(), bfData.getNumHashFunctions(), bfData.getBitArray());
    }

    /**
     * 写入布隆过滤器
     */
    private int writeBloomFilter(FileChannel channel) throws IOException {
        byte[] data = encoder.encodeBloomFilter(bloomFilter);
        ByteBuffer buf = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        return data.length;
    }

    /**
     * 写入元数据
     */
    private void writeMetadata(FileChannel channel, int bloomLength) throws IOException {
        ByteBuffer meta = ByteBuffer.allocate(8);
        meta.putInt(blocks.size());
        meta.putInt(bloomLength);
        meta.flip();
        while (meta.hasRemaining()) {
            channel.write(meta);
        }
    }

    /**
     * 计算文件大小
     */
    private long calculateFileSize() {
        return blocks.stream().mapToLong(Block::getSize).sum();
    }


    /**
     * 计算最小键
     */
    private long calculateMinKey() {
        return blocks.isEmpty() ? 0 :
                blocks.get(0).getMinKey().hashCode();
    }

    /**
     * 计算最大键
     */
    private long calculateMaxKey() {
        return blocks.isEmpty() ? 0 :
                blocks.get(blocks.size() - 1).getMaxKey().hashCode();
    }

    /**
     * 扫描SSTable中的所有键值对
     */
    public java.util.List<java.util.Map.Entry<String, byte[]>> scanAll() {
        java.util.List<java.util.Map.Entry<String, byte[]>> all = new java.util.ArrayList<>();
        for (Block b : blocks) {
            all.addAll(b.entrySet());
        }
        return all;
    }

    /**
     * 关闭SSTable
     */
    public void close() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.close();
        }
    }
}