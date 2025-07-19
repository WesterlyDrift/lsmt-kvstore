package com.howard.lsm.storage;

import com.howard.lsm.config.LSMConfig;
import com.howard.lsm.core.SSTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 层级管理器
 *
 * LevelManager是LSM-Tree的"大脑"，负责管理整个多层存储结构。
 * 想象它是一个智能的图书管理员，知道每本书（SSTable）应该放在
 * 哪个书架（Level）上，以及何时需要重新整理书架。
 *
 * 层级设计哲学：
 * - Level 0: "临时存放区" - 直接从内存刷新，文件可能重叠
 * - Level 1+: "正式书架" - 经过整理，文件间无重叠，按键范围排序
 * - 每层容量递增：Level i 的容量是 Level i-1 的 N 倍（通常N=10）
 *
 * 这种设计确保了查询性能和存储效率的平衡。
 */
public class LevelManager {
    private static final Logger logger = LoggerFactory.getLogger(LevelManager.class);

    private final LSMConfig config;
    private final Map<Integer, List<SSTable>> levels;
    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();

    // 每层的大小限制（字节）
    private final long[] levelSizeLimits;

    public LevelManager(LSMConfig config) {
        this.config = config;
        this.levels = new ConcurrentHashMap<>();
        this.levelSizeLimits = calculateLevelSizeLimits();

        // 初始化所有层级
        for (int i = 0; i < config.getMaxLevel(); i++) {
            levels.put(i, new ArrayList<>());
        }
    }

    /**
     * 添加SSTable到指定层级
     *
     * 这个方法是数据流入存储引擎的关键入口。它不仅要正确地
     * 放置新的SSTable，还要维护层级的有序性和完整性。
     *
     * @param sstable 要添加的SSTable
     * @param level 目标层级
     */
    public void addSSTable(SSTable sstable, int level) throws IOException {
        if (level >= config.getMaxLevel()) {
            throw new IllegalArgumentException("Level " + level + " exceeds maximum");
        }

        globalLock.writeLock().lock();
        try {
            List<SSTable> levelTables = levels.get(level);

            if (level == 0) {
                // Level 0 允许重叠，直接添加
                levelTables.add(sstable);
                logger.debug("Added SSTable to level 0: {}", sstable.getFilePath());
            } else {
                // Level 1+ 需要保持有序且无重叠
                insertSortedSSTable(levelTables, sstable);
                logger.debug("Added SSTable to level {}: {}", level, sstable.getFilePath());
            }

            // 记录层级状态
            logLevelStats(level);

        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * 查找键对应的值
     *
     * 这是查询路径的核心实现。它体现了LSM-Tree的查询策略：
     * 从最新的数据开始查找（Level 0），逐层向下搜索，
     * 直到找到目标键或确认键不存在。
     */
    public byte[] get(String key) throws IOException {
        globalLock.readLock().lock();
        try {
            // 从Level 0开始查找（最新数据）
            for (int level = 0; level < config.getMaxLevel(); level++) {
                List<SSTable> levelTables = levels.get(level);

                if (level == 0) {
                    // Level 0 需要检查所有文件（因为可能重叠）
                    byte[] result = searchInLevel0(levelTables, key);
                    if (result != null) {
                        return result;
                    }
                } else {
                    // Level 1+ 可以使用二分查找优化
                    byte[] result = searchInLevelN(levelTables, key);
                    if (result != null) {
                        return result;
                    }
                }
            }

            return null; // 键不存在

        } finally {
            globalLock.readLock().unlock();
        }
    }

    /**
     * 检查指定层级是否需要压缩
     *
     * 这个方法实现了压缩触发的决策逻辑。它就像一个"空间管理顾问"，
     * 告诉我们什么时候某个书架太拥挤了，需要重新整理。
     */
    public boolean needsCompaction(int level) {
        globalLock.readLock().lock();
        try {
            List<SSTable> levelTables = levels.get(level);
            if (levelTables == null || levelTables.isEmpty()) {
                return false;
            }

            if (level == 0) {
                // Level 0 基于文件数量判断
                return levelTables.size() >= config.getLevel0FileThreshold();
            } else {
                // Level 1+ 基于总大小判断
                long totalSize = levelTables.stream()
                        .mapToLong(SSTable::getFileSize)
                        .sum();
                return totalSize > levelSizeLimits[level];
            }

        } finally {
            globalLock.readLock().unlock();
        }
    }

    /**
     * 选择压缩候选文件
     *
     * 这个方法实现了压缩文件的选择策略。不同的层级有不同的选择逻辑：
     * - Level 0: 选择所有文件（因为可能重叠）
     * - Level 1+: 选择大小最大的文件，以及与其重叠的下层文件
     */
    public List<SSTable> selectCompactionCandidates(int level) {
        globalLock.readLock().lock();
        try {
            List<SSTable> levelTables = levels.get(level);
            if (levelTables == null || levelTables.isEmpty()) {
                return Collections.emptyList();
            }

            if (level == 0) {
                // Level 0 选择所有文件
                return new ArrayList<>(levelTables);
            } else {
                // Level 1+ 选择一个文件进行压缩
                return selectSingleFileForCompaction(levelTables);
            }

        } finally {
            globalLock.readLock().unlock();
        }
    }

    /**
     * 替换文件
     *
     * 这是压缩操作的最后一步，用新生成的文件替换旧文件。
     * 这个操作必须是原子的，确保在任何时候数据都是一致的。
     */
    public void replaceFiles(int sourceLevel, List<SSTable> oldFiles,
                             int targetLevel, List<SSTable> newFiles) throws IOException {
        globalLock.writeLock().lock();
        try {
            // 从源层级移除旧文件
            List<SSTable> sourceTables = levels.get(sourceLevel);
            sourceTables.removeAll(oldFiles);

            // 向目标层级添加新文件
            for (SSTable newFile : newFiles) {
                addSSTable(newFile, targetLevel);
            }

            logger.info("Replaced {} files in level {} with {} files in level {}",
                    oldFiles.size(), sourceLevel, newFiles.size(), targetLevel);

        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * 加载现有的SSTable文件
     *
     * 系统启动时，需要从磁盘加载现有的SSTable文件。
     * 这个方法扫描数据目录，重建内存中的层级结构。
     */
    public void loadExistingSSTables() throws IOException {
        Path dataDir = Paths.get(config.getDataDirectory());
        if (!Files.exists(dataDir)) {
            return;
        }

        globalLock.writeLock().lock();
        try {
            // 扫描每个层级目录
            for (int level = 0; level < config.getMaxLevel(); level++) {
                Path levelDir = dataDir.resolve("level_" + level);
                if (!Files.exists(levelDir)) {
                    continue;
                }

                loadSSTables(level, levelDir);
            }

            logger.info("Loaded existing SSTables from disk");

        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * 在Level 0中搜索
     */
    private byte[] searchInLevel0(List<SSTable> tables, String key) throws IOException {
        // Level 0 文件可能重叠，需要按时间戳倒序查找
        // 这里简化为直接查找所有文件
        for (int i = tables.size() - 1; i >= 0; i--) {
            SSTable table = tables.get(i);
            if (table.mightContain(key)) {
                byte[] result = table.get(key);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    /**
     * 在Level N中搜索（N > 0）
     */
    private byte[] searchInLevelN(List<SSTable> tables, String key) throws IOException {
        // Level 1+ 文件无重叠，可以用二分查找
        int left = 0, right = tables.size() - 1;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            SSTable table = tables.get(mid);

            if (table.keyInRange(key)) {
                return table.get(key);
            } else if (table.getMaxKey() < 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return null;
    }

    /**
     * 有序插入SSTable
     */
    private void insertSortedSSTable(List<SSTable> tables, SSTable newTable) {
        // 保持层级内SSTable按键范围有序
        int insertPos = 0;
        for (int i = 0; i < tables.size(); i++) {
            if (newTable.getMinKey() < tables.get(i).getMinKey()) {
                insertPos = i;
                break;
            }
            insertPos = i + 1;
        }
        tables.add(insertPos, newTable);
    }

    /**
     * 为压缩选择单个文件
     */
    private List<SSTable> selectSingleFileForCompaction(List<SSTable> tables) {
        // 选择最大的文件
        SSTable largest = tables.stream()
                .max(Comparator.comparing(SSTable::getFileSize))
                .orElse(null);

        return largest != null ? List.of(largest) : Collections.emptyList();
    }

    /**
     * 计算各层级的大小限制
     */
    private long[] calculateLevelSizeLimits() {
        long[] limits = new long[config.getMaxLevel()];
        long baseSize = config.getLevel1MaxSize();

        for (int i = 0; i < limits.length; i++) {
            if (i == 0) {
                limits[i] = Long.MAX_VALUE; // Level 0 无大小限制
            } else {
                limits[i] = baseSize * (long) Math.pow(config.getLevelMultiplier(), i - 1);
            }
        }

        return limits;
    }

    /**
     * 加载指定层级的SSTable
     */
    private void loadSSTables(int level, Path levelDir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(levelDir, "*.dat")) {
            for (Path file : stream) {
                try {
                    SSTable sstable = new SSTable(file, level);
                    levels.get(level).add(sstable);
                    logger.debug("Loaded SSTable: {}", file);
                } catch (IOException e) {
                    logger.warn("Failed to load SSTable: {}", file, e);
                }
            }
        }
    }

    /**
     * 记录层级统计信息
     */
    private void logLevelStats(int level) {
        List<SSTable> tables = levels.get(level);
        long totalSize = tables.stream().mapToLong(SSTable::getFileSize).sum();

        logger.debug("Level {} stats: {} files, {} bytes",
                level, tables.size(), totalSize);
    }
}