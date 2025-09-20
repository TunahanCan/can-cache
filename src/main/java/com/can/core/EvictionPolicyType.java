package com.can.core;

import java.util.Locale;

public enum EvictionPolicyType
{
    LRU {
        @Override
        public <K> EvictionPolicy<K> create(int capacity)
        {
            return new LruEvictionPolicy<>();
        }
    },
    TINY_LFU {
        @Override
        public <K> EvictionPolicy<K> create(int capacity)
        {
            return new TinyLfuEvictionPolicy<>(capacity);
        }
    };

    public abstract <K> EvictionPolicy<K> create(int capacity);

    public static EvictionPolicyType fromConfig(String value)
    {
        if (value == null || value.isBlank()) return LRU;
        String normalized = value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        try {
            return EvictionPolicyType.valueOf(normalized);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unknown eviction policy: " + value, ex);
        }
    }
}

