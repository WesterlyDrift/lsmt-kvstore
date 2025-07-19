package com.howard.lsm.transaction;

import com.howard.lsm.LSMTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 事务管理器
 *
 * 提供ACID事务支持：
 * 1. 原子性：所有操作要么全部成功，要么全部失败
 * 2. 一致性：事务前后数据保持一致
 * 3. 隔离性：支持快照隔离级别
 * 4. 持久性：提交的事务保证持久化
 */
public class TransactionManager {
    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    private final LSMTree lsmTree;
    private final AtomicLong transactionIdGenerator;
    private final ConcurrentHashMap<Long, Transaction> activeTransactions;
    private final ConcurrentHashMap<String, ReadWriteLock> keyLocks;

    public TransactionManager(LSMTree lsmTree) {
        this.lsmTree = lsmTree;
        this.transactionIdGenerator = new AtomicLong(0);
        this.activeTransactions = new ConcurrentHashMap<>();
        this.keyLocks = new ConcurrentHashMap<>();
    }

    /**
     * 开始新事务
     */
    public Transaction beginTransaction() {
        long txId = transactionIdGenerator.incrementAndGet();
        Transaction transaction = new Transaction(txId, this);
        activeTransactions.put(txId, transaction);

        logger.debug("Transaction {} started", txId);
        return transaction;
    }

    /**
     * 提交事务
     */
    public void commitTransaction(Transaction transaction) throws IOException {
        long txId = transaction.getId();

        try {
            // 验证读集
            transaction.validateReadSet();

            // 应用写集
            transaction.applyWriteSet();

            // 从活跃事务中移除
            activeTransactions.remove(txId);

            logger.debug("Transaction {} committed successfully", txId);

        } catch (Exception e) {
            rollbackTransaction(transaction);
            throw new IOException("Transaction commit failed", e);
        }
    }

    /**
     * 回滚事务
     */
    public void rollbackTransaction(Transaction transaction) {
        long txId = transaction.getId();

        try {
            // 释放所有锁
            transaction.releaseAllLocks();

            // 从活跃事务中移除
            activeTransactions.remove(txId);

            logger.debug("Transaction {} rolled back", txId);

        } catch (Exception e) {
            logger.error("Error during transaction rollback", e);
        }
    }

    /**
     * 获取键锁
     */
    public ReadWriteLock getKeyLock(String key) {
        return keyLocks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
    }

    /**
     * 获取LSM Tree引用
     */
    public LSMTree getLSMTree() {
        return lsmTree;
    }
}