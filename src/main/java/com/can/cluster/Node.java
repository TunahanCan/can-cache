package com.can.cluster;

import java.time.Duration;

public interface Node<K,V> {
    void set(K key, V value, Duration ttl);
    V get(K key);
    boolean delete(K key);
    String id();
}