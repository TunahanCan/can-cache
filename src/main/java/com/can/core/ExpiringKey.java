package com.can.core;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Zamanı geldiğinde ilgili segmentten düşürülecek anahtarları temsil eden ve
 * {@link java.util.concurrent.DelayQueue} içinde kullanılan kayıt türüdür.
 */
record ExpiringKey(Object key, int segmentIndex, long expireAtMillis) implements Delayed
{
    @Override
    public long getDelay(TimeUnit unit) {
        long d = expireAtMillis - System.currentTimeMillis();
        return unit.convert(d, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        long d = getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
        return d == 0 ? 0 : (d < 0 ? -1 : 1);
    }
}
