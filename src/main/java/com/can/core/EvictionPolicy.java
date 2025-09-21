package com.can.core;

import java.util.LinkedHashMap;

/**
 * Abstraction that encapsulates admission and eviction behaviour for a cache segment.
 * Implementations may track access history or frequency statistics to decide whether
 * an entry should be admitted and which victim, if any, should be evicted.
 */
interface EvictionPolicy<K>
{
    /** Records that the given key has been accessed (hit or set). */
    void recordAccess(K key);

    /**
     * Called when a new key is about to be inserted into the segment.
     *
     * @param key       the candidate key that should be considered for admission
     * @param map       current storage map for the segment (access-order)
     * @param capacity  maximum number of entries allowed in the segment
     * @return decision describing whether the key should be admitted and an optional victim
     */
    AdmissionDecision<K> admit(K key, LinkedHashMap<K, CacheValue> map, int capacity);

    /** Signals that the given key has been removed from the segment. */
    void onRemove(K key);

    /** Result returned from {@link #admit(Object, LinkedHashMap, int)}. */
    final class AdmissionDecision<K>
    {
        private static final AdmissionDecision<?> REJECT = new AdmissionDecision<>(false, null);
        private static final AdmissionDecision<?> ADMIT_NO_EVICTION = new AdmissionDecision<>(true, null);

        private final boolean admit;
        private final K evictKey;

        private AdmissionDecision(boolean admit, K evictKey) {
            this.admit = admit;
            this.evictKey = evictKey;
        }

        public boolean shouldAdmit(){ return admit; }
        public K evictKey(){ return evictKey; }

        @SuppressWarnings("unchecked")
        static <K> AdmissionDecision<K> reject(){ return (AdmissionDecision<K>) REJECT; }
        @SuppressWarnings("unchecked")
        static <K> AdmissionDecision<K> admit(){ return (AdmissionDecision<K>) ADMIT_NO_EVICTION; }
        static <K> AdmissionDecision<K> admit(K evictKey){ return new AdmissionDecision<>(true, evictKey); }
    }
}

