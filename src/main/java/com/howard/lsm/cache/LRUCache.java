package com.howard.lsm.cache;

import java.util.HashMap;
import java.util.Map;

public class LRUCache<K, V> {

    private static class Node<K, V> {
        K key;
        V value;
        Node<K, V> prev;
        Node<K, V> next;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private final int capacity;
    private final Map<K, Node<K, V>> cache;

    private final Node<K, V> head;
    private final Node<K, V> tail;

    public LRUCache(int capacity) {
        if(capacity <= 0) {
            throw new IllegalArgumentException("LRUCache capacity must be greater than 0!");
        }

        this.capacity = capacity;
        this.cache = new HashMap<>(capacity);

        this.head = new Node<>(null, null);
        this.tail = new Node<>(null, null);
        head.next = tail;
        tail.prev = head;
    }

    public V get(K key) {
        if(key == null) {
            throw new IllegalArgumentException("LRU cache key cannot be null");
        } else if(cache.containsKey(key)) {
            Node<K, V> node = cache.get(key);
            moveToHead(node);
            return node.value;
        } else {
            return null;
        }

    }

    public void remove(K key) {
        Node<K, V> node = cache.remove(key);
        if(node != null) {
            removeNode(node);
            cache.remove(key);
        }
    }

    public void put(K key, V value) {
        Node<K, V> existingNode = cache.get(key);
        if(existingNode != null) {
            existingNode.value = value;
            moveToHead(existingNode);
        } else {
            if(cache.size() >= capacity) {
                Node<K, V> victim = tail.prev;
                removeNode(victim);
                cache.remove(victim.key);
            }

            Node<K, V> newNode = new Node<>(key, value);
            addToHead(newNode);
            cache.put(key, newNode);
        }

    }

    public int size() {
        return cache.size();
    }

    private void moveToHead(Node<K, V> node) {
        removeNode(node);
        addToHead(node);
    }

    private void removeNode(Node<K, V> node) {
        node.next.prev = node.prev;
        node.prev.next = node.next;
    }

    private void addToHead(Node<K, V> node) {
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
        node.prev = head;
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    public void clear() {
        cache.clear();
        head.next = tail;
        tail.prev = head;
    }


}
