package com.can.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

final class CacheSegment<K>
{
    private final ReentrantLock lock = new ReentrantLock();
    private final int capacity;
    private final LinkedHashMap<K, CacheValue> map =
            new LinkedHashMap<>(16, 0.75f, true);

    CacheSegment(int capacity) { this.capacity = capacity; }

    CacheValue get(K key) {
        lock.lock();
        try { return map.get(key); } finally { lock.unlock(); }
    }
    void put(K key, CacheValue v) {
        lock.lock();
        try {
            map.put(key, v);
            while (map.size() > capacity) {
                Map.Entry<K, CacheValue> eldest = map.entrySet().iterator().next();
                map.remove(eldest.getKey());
            }
        } finally { lock.unlock(); }
    }
    CacheValue remove(K key) {
        lock.lock(); try { return map.remove(key); } finally { lock.unlock(); }
    }
    int size() {
        lock.lock(); try { return map.size(); } finally { lock.unlock(); }
    }
}
