package com.can.core;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Önbellek kapasitesini parçalara ayırarak eşzamanlı erişimi azaltan segment
 * yapısıdır. Her segment LRU erişim sırası izleyen bir {@link LinkedHashMap}
 * ve seçilen tahliye politikasını kullanarak anahtarların kabul edilmesi ya da
 * silinmesini kontrol eder.
 */
final class CacheSegment<K>
{
    private final ReentrantLock lock = new ReentrantLock();
    private final int capacity;
    private final LinkedHashMap<K, CacheValue> map =
            new LinkedHashMap<>(16, 0.75f, true);
    private final EvictionPolicy<K> policy;

    CacheSegment(int capacity, EvictionPolicy<K> policy)
    {
        this.capacity = capacity;
        this.policy = Objects.requireNonNull(policy);
    }

    CacheValue get(K key) {
        lock.lock();
        try {
            CacheValue v = map.get(key);
            if (v != null) policy.recordAccess(key);
            return v;
        }
        finally { lock.unlock(); }
    }
    boolean put(K key, CacheValue v) {
        return putInternal(key, v, false);
    }

    boolean putForce(K key, CacheValue v) {
        return putInternal(key, v, true);
    }

    private boolean putInternal(K key, CacheValue v, boolean force) {
        lock.lock();
        try {
            CacheValue existing = map.get(key);
            policy.recordAccess(key);
            if (existing != null) {
                map.put(key, v);
                return true;
            }

            if (!force) {
                EvictionPolicy.AdmissionDecision<K> decision = policy.admit(key, map, capacity);
                if (!decision.shouldAdmit()) {
                    return false;
                }
                K victim = decision.evictKey();
                if (victim != null) {
                    if (map.remove(victim) != null) policy.onRemove(victim);
                }
            } else if (map.size() >= capacity) {
                Iterator<Map.Entry<K, CacheValue>> it = map.entrySet().iterator();
                if (it.hasNext()) {
                    K victim = it.next().getKey();
                    it.remove();
                    policy.onRemove(victim);
                }
            }

            map.put(key, v);
            while (map.size() > capacity) {
                Iterator<Map.Entry<K, CacheValue>> it = map.entrySet().iterator();
                if (!it.hasNext()) break;
                Map.Entry<K, CacheValue> eldest = it.next();
                it.remove();
                policy.onRemove(eldest.getKey());
            }
            return true;
        } finally { lock.unlock(); }
    }
    CacheValue remove(K key) {
        lock.lock();
        try {
            CacheValue removed = map.remove(key);
            if (removed != null) policy.onRemove(key);
            return removed;
        }
        finally { lock.unlock(); }
    }

    boolean removeIfMatches(K key, long expireAtMillis) {
        lock.lock();
        try {
            CacheValue existing = map.get(key);
            if (existing == null || existing.expireAtMillis != expireAtMillis) {
                return false;
            }
            map.remove(key);
            policy.onRemove(key);
            return true;
        } finally {
            lock.unlock();
        }
    }
    int size() {
        lock.lock(); try { return map.size(); } finally { lock.unlock(); }
    }

    void forEach(BiConsumer<K, CacheValue> consumer) {
        Map<K, CacheValue> snapshot;
        lock.lock();
        try {
            snapshot = new LinkedHashMap<>(map);
        } finally {
            lock.unlock();
        }
        snapshot.forEach(consumer);
    }
}
