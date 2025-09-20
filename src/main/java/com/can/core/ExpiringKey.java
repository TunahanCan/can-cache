package com.can.core;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

final class ExpiringKey implements Delayed
{
    final Object key;
    final int segmentIndex;
    final long expireAtMillis;

    ExpiringKey(Object key, int segmentIndex, long expireAtMillis) {
        this.key = key; this.segmentIndex = segmentIndex; this.expireAtMillis = expireAtMillis;
    }
    @Override public long getDelay(TimeUnit unit) {
        long d = expireAtMillis - System.currentTimeMillis();
        return unit.convert(d, TimeUnit.MILLISECONDS);
    }
    @Override public int compareTo(Delayed o) {
        long d = getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
        return d == 0 ? 0 : (d < 0 ? -1 : 1);
    }
}
