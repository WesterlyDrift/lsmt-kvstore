package com.howard.lsm.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * 块构建器
 *
 * BlockBuilder是构建数据块的工具类，它扮演着"建筑师"的角色。
 * 想象我们正在建造一栋房子（Block），BlockBuilder就是我们的工具箱，
 * 帮助我们按照最优的方式组装材料（键值对）。
 *
 * 核心设计理念：
 * 1. 增量构建：逐步添加键值对，直到达到块大小限制
 * 2. 自动分割：当数据量超过限制时，自动创建新块
 * 3. 内存优化：避免不必要的数据复制和内存分配
 * 4. 顺序保证：确保生成的块内数据保持有序
 */
public class BlockBuilder {
    private final int maxBlockSize;
    private final List<Block> completedBlocks;

    // 当前正在构建的块
    private TreeMap<String, byte[]> currentData;
    private int currentSize;

    // 统计信息
    private int totalEntries = 0;
    private long totalDataSize = 0;

    /**
     * 构造函数
     *
     * @param maxBlockSize 每个块的最大大小（字节）
     */
    public BlockBuilder(int maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
        this.completedBlocks = new ArrayList<>();
        this.currentData = new TreeMap<>();
        this.currentSize = 0;
    }

    /**
     * 添加键值对
     *
     * 这个方法是BlockBuilder的核心。它不仅仅是简单地添加数据，
     * 还会智能地管理块的大小和分割。就像一个经验丰富的包装工，
     * 知道什么时候该换一个新箱子。
     *
     * @param key 键
     * @param value 值
     */
    public void add(String key, byte[] value) {
        // 计算这个键值对需要的空间
        int entrySize = calculateEntrySize(key, value);

        // 检查是否需要创建新块
        if (currentSize + entrySize > maxBlockSize && !currentData.isEmpty()) {
            finishCurrentBlock();
            startNewBlock();
        }

        // 添加到当前块
        currentData.put(key, value);
        currentSize += entrySize;
        totalEntries++;
        totalDataSize += entrySize;
    }

    /**
     * 完成构建并返回所有块
     *
     * 这个方法类似于"交付成品"。它会确保所有未完成的工作
     * 都被妥善处理，然后返回完整的块列表。
     */
    public List<Block> build() {
        // 完成最后一个块（如果有数据的话）
        if (!currentData.isEmpty()) {
            finishCurrentBlock();
        }

        // 返回所有已完成的块
        List<Block> result = new ArrayList<>(completedBlocks);

        // 重置构建器状态，为下次使用做准备
        reset();

        return result;
    }

    /**
     * 获取当前统计信息
     *
     * 这些统计信息对于性能调优和系统监控非常有用。
     * 通过观察这些指标，我们可以了解数据分布特征，
     * 并据此调整块大小等参数。
     */
    public BuilderStats getStats() {
        return new BuilderStats(
                completedBlocks.size(),
                totalEntries,
                totalDataSize,
                completedBlocks.isEmpty() ? 0 : totalDataSize / completedBlocks.size()
        );
    }

    /**
     * 估算键值对所需的存储空间
     *
     * 这个方法考虑了序列化后的实际空间需求，包括：
     * - 键的长度前缀（4字节）
     * - 键的实际内容
     * - 值的长度前缀（4字节）
     * - 值的实际内容
     *
     * 准确的空间估算对于有效的块分割至关重要。
     */
    private int calculateEntrySize(String key, byte[] value) {
        return 4 + key.getBytes().length + 4 + value.length;
    }

    /**
     * 完成当前块的构建
     *
     * 这个方法将当前正在构建的数据转换为一个完整的Block对象。
     * 这个过程包括最终的数据整理和优化。
     */
    private void finishCurrentBlock() {
        if (currentData.isEmpty()) {
            return;
        }

        // 创建新的块
        Block block = new Block(currentData, currentSize);
        completedBlocks.add(block);

        // 记录块的统计信息
        logBlockStats(block);
    }

    /**
     * 开始构建新块
     *
     * 重置构建器的状态，为构建下一个块做准备。
     * 这就像清理工作台，准备开始下一个项目。
     */
    private void startNewBlock() {
        currentData = new TreeMap<>();
        currentSize = 0;
    }

    /**
     * 重置构建器状态
     */
    private void reset() {
        completedBlocks.clear();
        currentData.clear();
        currentSize = 0;
        totalEntries = 0;
        totalDataSize = 0;
    }

    /**
     * 记录块的统计信息
     */
    private void logBlockStats(Block block) {
        // 这里可以添加日志记录或监控指标
        // 例如记录块的大小分布、键的数量等
    }

    /**
     * 构建器统计信息
     */
    public static class BuilderStats {
        private final int blockCount;
        private final int totalEntries;
        private final long totalDataSize;
        private final long averageBlockSize;

        public BuilderStats(int blockCount, int totalEntries,
                            long totalDataSize, long averageBlockSize) {
            this.blockCount = blockCount;
            this.totalEntries = totalEntries;
            this.totalDataSize = totalDataSize;
            this.averageBlockSize = averageBlockSize;
        }

        public int getBlockCount() { return blockCount; }
        public int getTotalEntries() { return totalEntries; }
        public long getTotalDataSize() { return totalDataSize; }
        public long getAverageBlockSize() { return averageBlockSize; }

        @Override
        public String toString() {
            return String.format(
                    "BuilderStats{blocks=%d, entries=%d, totalSize=%d, avgBlockSize=%d}",
                    blockCount, totalEntries, totalDataSize, averageBlockSize
            );
        }
    }
}