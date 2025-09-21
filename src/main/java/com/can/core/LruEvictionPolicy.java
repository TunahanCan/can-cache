package com.can.core;

import java.util.LinkedHashMap;

/**
 * Klasik son kullanılan ilk çıkar yaklaşımını uygulayan basit tahliye politikasıdır.
 * Segment kapasitesi dolduğunda en eski erişilen girdiyi kurban seçer.
 */
final class LruEvictionPolicy<K> implements EvictionPolicy<K>
{
    @Override
    public void recordAccess(K key){}

    @Override
    public AdmissionDecision<K> admit(K key, LinkedHashMap<K, CacheValue> map, int capacity)
    {
        if (map.size() < capacity) return AdmissionDecision.admit();
        if (map.isEmpty()) return AdmissionDecision.admit();
        K eldest = map.entrySet().iterator().next().getKey();
        return AdmissionDecision.admit(eldest);
    }

    @Override
    public void onRemove(K key){}
}

