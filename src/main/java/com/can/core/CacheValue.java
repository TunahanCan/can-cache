package com.can.core;

final class CacheValue
{
    final byte[] value;
    final long expireAtMillis; // <=0: no TTL

    CacheValue(byte[] value, long expireAtMillis) {
        this.value = value;
        this.expireAtMillis = expireAtMillis;
    }
    boolean expired(long now) { return expireAtMillis > 0 && now >= expireAtMillis; }
}