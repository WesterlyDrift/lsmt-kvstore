package com.howard.lsm.config;

import lombok.Data;
/**
 * LSM-Tree配置类
 *
 * 集中管理所有配置参数
 */
@Data
public class LSMConfig {
    // 存储配置
    private String dataDirectory = "/tmp/lsm-data";
    private String walDirectory = "/tmp/lsm-wal";

    // 内存表配置
    private long memTableSize = 64 * 1024 * 1024; // 64MB

    // 块配置
    private int blockSize = 4096; // 4KB

    // 布隆过滤器配置
    private double bloomFilterFPP = 0.01; // 1%误判率

    // 缓存配置
    private int cacheShardCount = 16;

    // WAL配置
    private boolean walSyncImmediate = false;
    private boolean walTruncateEnabled = true;

    // 压缩配置
    private int maxLevel = 7;
    private int levelMultiplier = 10;

    private int level0FileThreshold = 4;
    private long level1MaxSize = 10 * 1024 * 1024;

    // 构造函数
    public LSMConfig() {}

}