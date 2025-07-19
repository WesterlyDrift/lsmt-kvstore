package com.howard.lsm.storage;

import lombok.Getter;

import java.util.BitSet;

/**
 * 布隆过滤器实现
 *
 * 用于快速判断键是否可能存在于SSTable中：
 * 1. 支持可配置的误判率
 * 2. 使用多个哈希函数
 * 3. 优化内存使用
 */
@Getter
public class BloomFilter {
    private final BitSet bitSet;
    private final int numHashFunctions;
    private final int bitSetSize;

    public BloomFilter(int expectedEntries, double falsePositiveRate) {
        this.bitSetSize = calculateOptimalSize(expectedEntries, falsePositiveRate);
        this.numHashFunctions = calculateOptimalHashFunctions(bitSetSize, expectedEntries);
        this.bitSet = new BitSet(bitSetSize);
    }

    /**
     * 添加键到过滤器
     */
    public void add(String key) {
        byte[] keyBytes = key.getBytes();
        for (int i = 0; i < numHashFunctions; i++) {
            int hash = hash(keyBytes, i);
            bitSet.set(Math.abs(hash % bitSetSize));
        }
    }

    /**
     * 检查键是否可能存在
     */
    public boolean mightContain(String key) {
        byte[] keyBytes = key.getBytes();
        for (int i = 0; i < numHashFunctions; i++) {
            int hash = hash(keyBytes, i);
            if (!bitSet.get(Math.abs(hash % bitSetSize))) {
                return false;
            }
        }
        return true;
    }

    public byte[] getByteArray() {
        return bitSet.toByteArray();
    }

    /**
     * 计算最优比特集大小
     */
    private int calculateOptimalSize(int expectedEntries, double falsePositiveRate) {
        return (int) (-expectedEntries * Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2)));
    }

    /**
     * 计算最优哈希函数数量
     */
    private int calculateOptimalHashFunctions(int bitSetSize, int expectedEntries) {
        return Math.max(1, (int) Math.round((double) bitSetSize / expectedEntries * Math.log(2)));
    }

    /**
     * 哈希函数实现（使用双重哈希）
     */
    private int hash(byte[] data, int i) {
        int hash1 = murmurHash3(data, 0);
        int hash2 = murmurHash3(data, hash1);
        return hash1 + i * hash2;
    }

    /**
     * MurmurHash3实现
     */
    private int murmurHash3(byte[] data, int seed) {
        int h = seed;
        for (byte datum : data) {
            h ^= datum;
            h *= 0x5bd1e995;
            h ^= h >>> 15;
        }
        return h;
    }
}