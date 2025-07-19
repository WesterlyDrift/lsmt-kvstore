package com.howard.lsm.cache;

import lombok.Getter;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ShardedCache<K, V> {
    private final CacheShard<K, V>[] shards;
    @Getter
    private final int shardCount;
    private final int shardMask;

    @SuppressWarnings("unchecked")
    public ShardedCache(int shardCount) {
        this.shardCount = shardCount;
        this.shardMask = shardCount - 1;
        this.shards = new CacheShard[shardCount];

        for(int i = 0; i < shardCount; i++) {
            shards[i] = new CacheShard<>();
        }
    }

    public V get(K key) {
        return getShard(key).get(key);
    }

    public void put(K key, V value) {
        getShard(key).put(key, value);
    }

    public void remove(K key) {
        getShard(key).remove(key);
    }

    public void clear() {
        for(int i = 0; i < shardCount; i++) {
            shards[i].clear();
        }
    }

    private CacheShard<K, V> getShard(K key) {
        int hash = key.hashCode();
        int shardIndex = hash & shardMask;
        return shards[shardIndex];
    }

    private static class CacheShard<K, V> {
        private final LRUCache<K, V> cache;
        private final ReadWriteLock lock;

        public CacheShard() {
            this.cache = new LRUCache<>(1000); // Default capacity, can be parameterized
            this.lock = new ReentrantReadWriteLock();
        }

        public V get(K key) {
            lock.readLock().lock();
            try {
                return cache.get(key);
            } finally {
                lock.readLock().unlock();
            }
        }

        public void put(K key, V value) {
            lock.writeLock().lock();
            try {
                cache.put(key, value);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void remove(K key) {
            lock.writeLock().lock();
            try {
                cache.remove(key);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void clear() {
            lock.writeLock().lock();
            try {
                cache.clear();
            } finally {
                lock.writeLock().unlock();
            }
        }
    }
}
