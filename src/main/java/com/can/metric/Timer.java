package com.can.metric;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

/**
 * Süre ölçümlerini toplayan ve ortalama, yüzdelik dilim gibi istatistikler üreten
 * metrik zamanlayıcısıdır. Sanal reservoir kullanarak p50/p95 değerlerini
 * kestirir ve toplam çağrı sayısı ile minimum/maksimum süreleri saklar.
 */
public final class Timer
{
    private final String name;
    private final LongAdder count = new LongAdder();
    private final LongAdder totalNs = new LongAdder();
    private volatile long minNs = Long.MAX_VALUE;
    private volatile long maxNs = Long.MIN_VALUE;

    private final int reservoirSize;
    private final long[] reservoir;
    private volatile int idx = 0;

    public Timer(String name) { this(name, 1024); }
    public Timer(String name, int reservoirSize) {
        this.name = name;
        this.reservoirSize = Math.max(128, reservoirSize);
        this.reservoir = new long[this.reservoirSize];
    }

    public void record(long durationNs)
    {
        count.increment();
        totalNs.add(durationNs);
        if (durationNs < minNs) minNs = durationNs;
        if (durationNs > maxNs) maxNs = durationNs;
        int i = (idx = (idx + 1) % reservoirSize);
        reservoir[i] = durationNs;
    }

    public Sample snapshot() {
        long c = count.sum();
        long t = totalNs.sum();
        double avg = c == 0 ? 0.0 : (double) t / c;
        long min = (minNs == Long.MAX_VALUE) ? 0 : minNs;
        long max = (maxNs == Long.MIN_VALUE) ? 0 : maxNs;

        long[] copy = Arrays.copyOf(reservoir, reservoir.length);
        Arrays.sort(copy);
        long p50 = copy[(int)(0.50 * (copy.length - 1))];
        long p95 = copy[(int)(0.95 * (copy.length - 1))];

        return new Sample(name, c, t, avg, min, max, p50, p95);
    }

    /**
     * Anlık metrik değerlerini temsil eden immutable taşıyıcıdır.
     */
    public record Sample(String name, long count, long totalNs, double avgNs,
                         long minNs, long maxNs, long p50Ns, long p95Ns) {}
}