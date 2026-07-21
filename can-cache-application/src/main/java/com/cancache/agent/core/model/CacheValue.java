package com.cancache.agent.core.model;

/**
 * Önbellekte tutulan bayt değerini ve varsa son kullanma zamanını kapsülleyen
 * basit taşıyıcı sınıftır.
 *
 * @param expireAtMillis <=0: no TTL
 */
public record CacheValue(byte[] value, long expireAtMillis)
{
    public CacheValue
    {
        value = java.util.Objects.requireNonNull(value, "value").clone();
    }

    @Override
    public byte[] value()
    {
        return value.clone();
    }

    public int sizeBytes()
    {
        return value.length;
    }

    public boolean expired(long now)
    {
        return expireAtMillis > 0 && now >= expireAtMillis;
    }
}
