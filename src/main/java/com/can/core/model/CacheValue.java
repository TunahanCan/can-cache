package com.can.core.model;

/**
 * Önbellekte tutulan bayt değerini ve varsa son kullanma zamanını kapsülleyen
 * basit taşıyıcı sınıftır.
 *
 * @param expireAtMillis <=0: no TTL
 */
public record CacheValue(byte[] value, long expireAtMillis)
{
    public boolean expired(long now)
    {
        return expireAtMillis > 0 && now >= expireAtMillis;
    }
}
