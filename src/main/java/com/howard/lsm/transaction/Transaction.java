package com.howard.lsm.transaction;

import lombok.Getter;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * 事务实现
 *
 * 支持快照隔离级别的事务：
 * 1. 读集验证
 * 2. 写集应用
 * 3. 锁管理
 */
public class Transaction {
    @Getter
    private final long id;
    private final TransactionManager manager;
    @Getter
    private final long startTimestamp;

    // 事务状态
    private final Map<String, byte[]> readSet;
    private final Map<String, byte[]> writeSet;
    private final Set<String> deleteSet;
    private final Set<String> lockedKeys;

    private volatile boolean active = true;

    public Transaction(long id, TransactionManager manager) {
        this.id = id;
        this.manager = manager;
        this.startTimestamp = System.currentTimeMillis();
        this.readSet = new HashMap<>();
        this.writeSet = new HashMap<>();
        this.deleteSet = new HashSet<>();
        this.lockedKeys = new HashSet<>();
    }

    /**
     * 事务内读取
     */
    public byte[] get(String key) throws IOException {
        if (!active) {
            throw new IllegalStateException("Transaction is not active");
        }

        // 检查写集
        if (writeSet.containsKey(key)) {
            return writeSet.get(key);
        }

        // 检查删除集
        if (deleteSet.contains(key)) {
            return null;
        }

        // 从存储引擎读取
        byte[] value = manager.getLSMTree().get(key);

        // 添加到读集
        readSet.put(key, value);

        return value;
    }

    /**
     * 事务内写入
     */
    public void put(String key, byte[] value) throws IOException {
        if (!active) {
            throw new IllegalStateException("Transaction is not active");
        }

        // 获取写锁
        acquireWriteLock(key);

        // 添加到写集
        writeSet.put(key, value);
        deleteSet.remove(key);
    }

    /**
     * 事务内删除
     */
    public void delete(String key) throws IOException {
        if (!active) {
            throw new IllegalStateException("Transaction is not active");
        }

        // 获取写锁
        acquireWriteLock(key);

        // 添加到删除集
        deleteSet.add(key);
        writeSet.remove(key);
    }

    /**
     * 提交事务
     */
    public void commit() throws IOException {
        if (!active) {
            throw new IllegalStateException("Transaction is not active");
        }

        manager.commitTransaction(this);
        active = false;
    }

    /**
     * 回滚事务
     */
    public void rollback() {
        if (!active) {
            return;
        }

        manager.rollbackTransaction(this);
        active = false;
    }

    /**
     * 验证读集
     */
    public void validateReadSet() throws IOException {
        for (Map.Entry<String, byte[]> entry : readSet.entrySet()) {
            String key = entry.getKey();
            byte[] expectedValue = entry.getValue();
            byte[] currentValue = manager.getLSMTree().get(key);

            if (!java.util.Arrays.equals(expectedValue, currentValue)) {
                throw new IOException("Read set validation failed for key: " + key);
            }
        }
    }

    /**
     * 应用写集
     */
    public void applyWriteSet() throws IOException {
        // 应用写入
        for (Map.Entry<String, byte[]> entry : writeSet.entrySet()) {
            manager.getLSMTree().put(entry.getKey(), entry.getValue());
        }

        // 应用删除
        for (String key : deleteSet) {
            manager.getLSMTree().delete(key);
        }
    }

    /**
     * 获取写锁
     */
    private void acquireWriteLock(String key) {
        ReadWriteLock lock = manager.getKeyLock(key);
        lock.writeLock().lock();
        lockedKeys.add(key);
    }

    /**
     * 释放所有锁
     */
    public void releaseAllLocks() {
        for (String key : lockedKeys) {
            ReadWriteLock lock = manager.getKeyLock(key);
            lock.writeLock().unlock();
        }
        lockedKeys.clear();
    }

}