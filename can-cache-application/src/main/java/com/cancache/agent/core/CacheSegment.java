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
    private final long maximumWeight;
    private final LinkedHashMap<K, CacheValue> map =
            new LinkedHashMap<>(16, 0.75f, true);
    private final EvictionPolicy<K> policy;
    private final CacheEngine.RemovalListener<K> removalListener;
    private long currentWeight;

    CacheSegment(int capacity, EvictionPolicy<K> policy, CacheEngine.RemovalListener<K> removalListener)
    {
        this(capacity, Long.MAX_VALUE, policy, removalListener);
    }

    CacheSegment(int capacity,
                 long maximumWeight,
                 EvictionPolicy<K> policy,
                 CacheEngine.RemovalListener<K> removalListener)
    {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be at least 1");
        }
        if (maximumWeight < 1L) {
            throw new IllegalArgumentException("maximumWeight must be at least 1");
        }
        this.capacity = capacity;
        this.maximumWeight = maximumWeight;
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
            long valueWeight = v.sizeBytes();
            if (valueWeight > maximumWeight) {
                stored = false;
            } else if (existing != null) {
                makeRoomForReplacement(key, existing, valueWeight, removedKeys);
                long previousWeight = existing.sizeBytes();
                map.put(key, v);
                currentWeight = currentWeight - previousWeight + valueWeight;
            } else {
                boolean evictionRequired = requiresEviction(valueWeight);
                EvictionPolicy.AdmissionDecision<K> decision = force
                        ? EvictionPolicy.AdmissionDecision.admit()
                        : policy.admit(key, map, evictionRequired);
                if (!decision.shouldAdmit()) {
                    stored = false;
                } else {
                    K victim = decision.evictKey();
                    if (victim != null && !removeVictim(victim, removedKeys) && evictionRequired) {
                        stored = false;
                    }
                    while (stored && requiresEviction(valueWeight)) {
                        K eldest = eldestKeyExcluding(null);
                        if (eldest == null || !removeVictim(eldest, removedKeys)) {
                            stored = false;
                            break;
                        }
                    }
                    if (stored) {
                        map.put(key, v);
                        currentWeight += valueWeight;
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        notifyRemovals(removedKeys);
        return stored;
    }

    private void makeRoomForReplacement(K key,
                                        CacheValue existing,
                                        long replacementWeight,
                                        List<K> removedKeys) {
        long weightWithoutExisting = currentWeight - existing.sizeBytes();
        while (wouldExceedWeight(weightWithoutExisting, replacementWeight)) {
            K victim = eldestKeyExcluding(key);
            if (victim == null || !removeVictim(victim, removedKeys)) {
                throw new IllegalStateException("Unable to satisfy segment weight bound");
            }
            weightWithoutExisting = currentWeight - existing.sizeBytes();
        }
    }

    private boolean requiresEviction(long candidateWeight) {
        return map.size() >= capacity || wouldExceedWeight(currentWeight, candidateWeight);
    }

    private boolean wouldExceedWeight(long baseWeight, long addedWeight) {
        return addedWeight > maximumWeight - baseWeight;
    }

    private K eldestKeyExcluding(K excluded) {
        Iterator<K> iterator = map.keySet().iterator();
        while (iterator.hasNext()) {
            K candidate = iterator.next();
            if (!Objects.equals(candidate, excluded)) {
                return candidate;
            }
        }
        return null;
    }

    private boolean removeVictim(K key, List<K> removedKeys) {
        CacheValue removed = map.remove(key);
        if (removed == null) {
            return false;
        }
        currentWeight -= removed.sizeBytes();
        policy.onRemove(key);
        removedKeys.add(key);
        return true;
    }

    CacheValue remove(K key) {
        CacheValue removed;
        lock.lock();
        try {
            removed = map.remove(key);
            if (removed != null) {
                currentWeight -= removed.sizeBytes();
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
            CacheValue removedValue = map.remove(key);
            currentWeight -= removedValue.sizeBytes();
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
            CacheValue removedValue = map.remove(key);
            currentWeight -= removedValue.sizeBytes();
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
        List<K> removedKeys = new ArrayList<>(1);
        CasResult result;
        lock.lock();
        try {
            CacheValue existing = map.get(key);
            CasDecision decision = decisionFn.apply(existing);
            if (decision == null) {
                return new CasResult(false, null);
            }
            CacheValue candidate = decision.newValue();
            if (decision.success() && candidate != null && candidate.sizeBytes() > maximumWeight) {
                return new CasResult(false, null);
            }
            if (existing != null && decision.recordAccess()) {
                policy.recordAccess(key);
            }
            if (decision.removeExisting() && existing != null) {
                CacheValue removed = map.remove(key);
                if (removed != null) {
                    currentWeight -= removed.sizeBytes();
                    policy.onRemove(key);
                    if (decision.notifyRemoval()) {
                        removedKeys.add(key);
                    }
                }
            }
            if (decision.success() && candidate != null) {
                CacheValue current = map.get(key);
                if (current != null) {
                    makeRoomForReplacement(key, current, candidate.sizeBytes(), removedKeys);
                    map.put(key, candidate);
                    currentWeight = currentWeight - current.sizeBytes() + candidate.sizeBytes();
                } else {
                    while (requiresEviction(candidate.sizeBytes())) {
                        K victim = eldestKeyExcluding(null);
                        if (victim == null || !removeVictim(victim, removedKeys)) {
                            return new CasResult(false, null);
                        }
                    }
                    map.put(key, candidate);
                    currentWeight += candidate.sizeBytes();
                }
            }
            result = new CasResult(decision.success(), candidate);
        } finally {
            lock.unlock();
        }
        notifyRemovals(removedKeys);
        return result;
    }
    int size() {
        lock.lock(); try { return map.size(); } finally { lock.unlock(); }
    }

    long weight() {
        lock.lock(); try { return currentWeight; } finally { lock.unlock(); }
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
            currentWeight = 0L;
        } finally {
            lock.unlock();
        }
        notifyRemovals(removedKeys);
    }
}
