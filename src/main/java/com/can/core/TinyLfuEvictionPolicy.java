package com.can.core;

import java.util.LinkedHashMap;

final class TinyLfuEvictionPolicy<K> implements EvictionPolicy<K>
{
    private static final int[] SEEDS = {0x7f4a7c15, 0x9e3779b9, 0xc2b2ae35, 0x165667b1};

    private final FrequencySketch sketch;
    private final int[] samples;
    private int index;
    private int count;

    TinyLfuEvictionPolicy(int capacity)
    {
        this.samples = new int[determineSampleSize(capacity)];
        this.sketch = new FrequencySketch(samples.length == 0 ? 16 : samples.length);
    }

    private static int determineSampleSize(int capacity)
    {
        int base = Math.max(1, capacity);
        int size = base * 16;
        size = Math.max(size, 256);
        size = Math.min(size, 1 << 16);
        return size;
    }

    @Override
    public void recordAccess(K key)
    {
        int hash = spread(key.hashCode());
        if (samples.length == 0)
        {
            sketch.increment(hash);
            return;
        }
        if (count >= samples.length)
        {
            int oldHash = samples[index];
            sketch.decrement(oldHash);
        }
        else
        {
            count++;
        }
        samples[index] = hash;
        index++;
        if (index == samples.length) index = 0;
        sketch.increment(hash);
    }

    @Override
    public AdmissionDecision<K> admit(K key, LinkedHashMap<K, CacheValue> map, int capacity)
    {
        if (map.size() < capacity) return AdmissionDecision.admit();
        if (map.isEmpty()) return AdmissionDecision.admit();
        K victimKey = map.entrySet().iterator().next().getKey();
        int candidateFreq = sketch.estimate(spread(key.hashCode()));
        int victimFreq = sketch.estimate(spread(victimKey.hashCode()));
        if (candidateFreq > victimFreq)
        {
            return AdmissionDecision.admit(victimKey);
        }
        return AdmissionDecision.reject();
    }

    @Override public void onRemove(K key){}

    private static int spread(int hash)
    {
        hash ^= (hash >>> 16);
        hash *= 0x7feb352d;
        hash ^= (hash >>> 15);
        hash *= 0x846ca68b;
        hash ^= (hash >>> 16);
        return hash;
    }

    private static final class FrequencySketch
    {
        private final int[] table;
        private final int mask;

        FrequencySketch(int size)
        {
            int length = 1;
            while (length < size) length <<= 1;
            this.table = new int[length];
            this.mask = length - 1;
        }

        void increment(int hash)
        {
            for (int seed : SEEDS)
            {
                int idx = (hash ^ seed) & mask;
                if (table[idx] < 255) table[idx]++;
            }
        }

        void decrement(int hash)
        {
            for (int seed : SEEDS)
            {
                int idx = (hash ^ seed) & mask;
                if (table[idx] > 0) table[idx]--;
            }
        }

        int estimate(int hash)
        {
            int min = Integer.MAX_VALUE;
            for (int seed : SEEDS)
            {
                int idx = (hash ^ seed) & mask;
                int val = table[idx];
                if (val < min) min = val;
            }
            return min == Integer.MAX_VALUE ? 0 : min;
        }
    }
}

