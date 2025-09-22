package com.can.core;

/**
 * Önbellekte tutulan bayt değerini ve varsa son kullanma zamanını kapsülleyen
 * basit taşıyıcı sınıftır.
 *
 * @param expireAtMillis <=0: no TTL
 */
record CacheValue(byte[] value, long expireAtMillis) {
    boolean expired(long now) {
        return expireAtMillis > 0 && now >= expireAtMillis;
    }
}
