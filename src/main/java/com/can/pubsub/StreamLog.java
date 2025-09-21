package com.can.pubsub;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Yayınlanan mesajları sıralı bir tamponda tutan ve istenilen sayıda kaydı
 * okuma imkânı veren basit stream yapısıdır. Bellek içi log yaklaşımı sayesinde
 * son gelen olayları hızlıca tüketmek mümkündür.
 */
public final class StreamLog
{
    private final Deque<byte[]> log = new ArrayDeque<>();
    private final int maxEntries;

    public StreamLog(int maxEntries) { this.maxEntries = Math.max(1, maxEntries); }

    public synchronized long xadd(byte[] record) {
        if (log.size() >= maxEntries) log.removeFirst();
        log.addLast(record);
        return System.nanoTime();
    }

    public synchronized List<byte[]> xrange(int count) {
        var out = new ArrayList<byte[]>(Math.min(count, log.size()));
        int i=0; for (var b : log) { if (i++>=count) break; out.add(b); }
        return out;
    }
}
