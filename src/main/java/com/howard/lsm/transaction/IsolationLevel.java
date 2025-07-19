package com.howard.lsm.transaction;

/**
 * 事务隔离级别定义
 *
 * 隔离级别是数据库事务理论中的核心概念，它定义了并发事务之间
 * 可以看到彼此修改的程度。想象多个人同时在编辑同一份文档，
 * 隔离级别决定了他们能在何种程度上看到彼此的修改。
 *
 * 我们的LSM-Tree存储引擎支持以下隔离级别：
 * 1. READ_UNCOMMITTED：最低级别，可能读到未提交的数据
 * 2. READ_COMMITTED：只能读到已提交的数据
 * 3. REPEATABLE_READ：在事务期间读取结果保持一致
 * 4. SNAPSHOT_ISOLATION：基于快照的隔离（我们的默认实现）
 * 5. SERIALIZABLE：最高级别，事务串行执行的效果
 *
 * 隔离级别的选择涉及性能与一致性的权衡。级别越高，一致性越强，
 * 但并发性能可能越低。
 */
public enum IsolationLevel {

    /**
     * 读未提交
     *
     * 最低的隔离级别，事务可以读取其他未提交事务的修改。
     * 这种级别下可能出现脏读、不可重复读和幻读问题。
     *
     * 优点：最高的并发性能
     * 缺点：数据一致性最弱
     *
     * 适用场景：对数据一致性要求不高，但对性能要求极高的场景
     */
    READ_UNCOMMITTED(0, "Read Uncommitted",
            "允许读取未提交的数据修改，可能出现脏读"),

    /**
     * 读已提交
     *
     * 事务只能读取已经提交的数据修改。这避免了脏读问题，
     * 但仍可能出现不可重复读和幻读。
     *
     * 优点：避免脏读，保证基本的数据完整性
     * 缺点：仍可能出现不可重复读和幻读
     *
     * 适用场景：大多数Web应用的默认隔离级别
     */
    READ_COMMITTED(1, "Read Committed",
            "只读取已提交的数据，避免脏读但可能出现不可重复读"),

    /**
     * 可重复读
     *
     * 保证在同一事务中多次读取同一数据的结果一致。
     * 避免了脏读和不可重复读，但仍可能出现幻读。
     *
     * 优点：保证读取的一致性
     * 缺点：可能出现幻读，并发性能有所下降
     *
     * 适用场景：需要在事务期间保持数据一致性的应用
     */
    REPEATABLE_READ(2, "Repeatable Read",
            "保证事务期间读取结果一致，避免脏读和不可重复读"),

    /**
     * 快照隔离
     *
     * 基于多版本并发控制（MVCC）的隔离级别。每个事务看到的是
     * 事务开始时的数据快照，避免了大多数并发问题。
     *
     * 优点：良好的并发性能和一致性平衡
     * 缺点：可能出现写倾斜等异常
     *
     * 适用场景：现代数据库系统的推荐隔离级别
     */
    SNAPSHOT_ISOLATION(3, "Snapshot Isolation",
            "基于快照的隔离，提供良好的并发性能和一致性"),

    /**
     * 可串行化
     *
     * 最高的隔离级别，保证事务的执行结果与串行执行相同。
     * 完全避免了所有并发异常。
     *
     * 优点：最强的一致性保证
     * 缺点：并发性能最低
     *
     * 适用场景：对数据一致性要求极高的金融等关键应用
     */
    SERIALIZABLE(4, "Serializable",
            "最高隔离级别，保证串行化执行效果");

    private final int level;
    private final String displayName;
    private final String description;

    IsolationLevel(int level, String displayName, String description) {
        this.level = level;
        this.displayName = displayName;
        this.description = description;
    }

    /**
     * 获取隔离级别的数值表示
     *
     * 数值越大表示隔离程度越高。这个设计让我们可以方便地
     * 比较不同隔离级别的强弱。
     */
    public int getLevel() {
        return level;
    }

    /**
     * 获取显示名称
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * 获取详细描述
     */
    public String getDescription() {
        return description;
    }

    /**
     * 检查是否允许脏读
     *
     * 只有READ_UNCOMMITTED级别允许脏读
     */
    public boolean allowsDirtyReads() {
        return this == READ_UNCOMMITTED;
    }

    /**
     * 检查是否允许不可重复读
     *
     * READ_UNCOMMITTED和READ_COMMITTED允许不可重复读
     */
    public boolean allowsNonRepeatableReads() {
        return this.level <= READ_COMMITTED.level;
    }

    /**
     * 检查是否允许幻读
     *
     * 除了SNAPSHOT_ISOLATION和SERIALIZABLE，其他级别都可能出现幻读
     */
    public boolean allowsPhantomReads() {
        return this.level < SNAPSHOT_ISOLATION.level;
    }

    /**
     * 检查是否需要快照隔离
     *
     * SNAPSHOT_ISOLATION和SERIALIZABLE需要快照支持
     */
    public boolean requiresSnapshot() {
        return this.level >= SNAPSHOT_ISOLATION.level;
    }

    /**
     * 比较隔离级别强弱
     *
     * @param other 另一个隔离级别
     * @return 如果当前级别更严格则返回true
     */
    public boolean isStricterThan(IsolationLevel other) {
        return this.level > other.level;
    }

    /**
     * 获取推荐的隔离级别
     *
     * 对于大多数应用，SNAPSHOT_ISOLATION提供了最好的
     * 性能和一致性平衡。
     */
    public static IsolationLevel getRecommended() {
        return SNAPSHOT_ISOLATION;
    }

    @Override
    public String toString() {
        return String.format("%s (Level %d): %s", displayName, level, description);
    }
}