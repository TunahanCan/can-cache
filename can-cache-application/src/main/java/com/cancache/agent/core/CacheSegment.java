package com.cancache.agent.core;

import com.cancache.agent.core.model.CacheValue;
import com.cancache.agent.core.model.CasDecision;
import com.cancache.agent.core.model.CasResult;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

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
    private final CacheEngine.RemovalListener<K> removalListener;

    CacheSegment(int capacity, EvictionPolicy<K> policy, CacheEngine.RemovalListener<K> removalListener)
    {
        this.capacity = capacity;
        this.policy = Objects.requireNonNull(policy);
        this.removalListener = removalListener;
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
        List<K> removedKeys = new ArrayList<>(1);
        boolean stored = true;
        lock.lock();
        try {
            CacheValue existing = map.get(key);
            policy.recordAccess(key);
            if (existing != null) {
                map.put(key, v);
            } else if (!force) {
                EvictionPolicy.AdmissionDecision<K> decision = policy.admit(key, map, capacity);
                if (!decision.shouldAdmit()) {
                    stored = false;
                } else {
                    K victim = decision.evictKey();
                    if (map.size() >= capacity && victim == null) {
                        stored = false;
                    } else {
                        if (victim != null) {
                            CacheValue removed = map.remove(victim);
                            if (removed == null && map.size() >= capacity) {
                                stored = false;
                            } else if (removed != null) {
                                policy.onRemove(victim);
                                removedKeys.add(victim);
                            }
                        }
                        if (stored) {
                            map.put(key, v);
                        }
                    }
                }
            } else {
                while (map.size() >= capacity) {
                    Iterator<Map.Entry<K, CacheValue>> it = map.entrySet().iterator();
                    if (!it.hasNext()) {
                        break;
                    }
                    K victim = it.next().getKey();
                    it.remove();
                    policy.onRemove(victim);
                    removedKeys.add(victim);
                }
                map.put(key, v);
            }
        } finally {
            lock.unlock();
        }
        notifyRemovals(removedKeys);
        return stored;
    }

    CacheValue remove(K key) {
        CacheValue removed;
        lock.lock();
        try {
            removed = map.remove(key);
            if (removed != null) {
                policy.onRemove(key);
            }
        } finally {
            lock.unlock();
        }
        if (removed != null) {
            notifyRemoval(key);
        }
        return removed;
    }

    boolean removeIfMatches(K key, long expireAtMillis) {
        boolean removed = false;
        lock.lock();
        try {
            CacheValue existing = map.get(key);
            if (existing == null || existing.expireAtMillis() != expireAtMillis) {
                return false;
            }
            map.remove(key);
            policy.onRemove(key);
            removed = true;
        } finally {
            lock.unlock();
        }
        if (removed) {
            notifyRemoval(key);
        }
        return removed;
    }

    boolean removeIfSame(K key, CacheValue expected) {
        boolean removed = false;
        lock.lock();
        try {
            if (map.get(key) != expected) {
                return false;
            }
            map.remove(key);
            policy.onRemove(key);
            removed = true;
        } finally {
            lock.unlock();
        }
        if (removed) {
            notifyRemoval(key);
        }
        return removed;
    }

    CasResult compareAndSwap(K key, java.util.function.Function<CacheValue, CasDecision> decisionFn) {
        K removedKey = null;
        CasResult result;
        lock.lock();
        try {
            CacheValue existing = map.get(key);
            CasDecision decision = decisionFn.apply(existing);
            if (decision == null) {
                return new CasResult(false, null);
            }
            if (existing != null && decision.recordAccess()) {
                policy.recordAccess(key);
            }
            if (decision.removeExisting() && existing != null) {
                if (map.remove(key) != null) {
                    policy.onRemove(key);
                    if (decision.notifyRemoval()) {
                        removedKey = key;
                    }
                }
            }
            if (decision.success() && decision.newValue() != null) {
                map.put(key, decision.newValue());
            }
            result = new CasResult(decision.success(), decision.newValue());
        } finally {
            lock.unlock();
        }
        if (removedKey != null) {
            notifyRemoval(removedKey);
        }
        return result;
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

    private void notifyRemoval(K key) {
        if (removalListener != null) {
            removalListener.onRemoval(key);
        }
    }

    private void notifyRemovals(Iterable<K> keys) {
        for (K key : keys) {
            notifyRemoval(key);
        }
    }

    void clear() {
        List<K> removedKeys;
        lock.lock();
        try {
            if (map.isEmpty()) {
                return;
            }
            removedKeys = new ArrayList<>(map.keySet());
            for (K key : map.keySet()) {
                policy.onRemove(key);
            }
            map.clear();
        } finally {
            lock.unlock();
        }
        notifyRemovals(removedKeys);
    }
}
