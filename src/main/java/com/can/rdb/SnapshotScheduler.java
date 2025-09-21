package com.can.rdb;

import com.can.core.CacheEngine;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SnapshotScheduler<K, V> implements AutoCloseable {

    private final CacheEngine<K, V> engine;
    private final SnapshotFile<K, V> snapshotFile;
    private final long intervalSeconds;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public SnapshotScheduler(CacheEngine<K, V> engine, SnapshotFile<K, V> snapshotFile, long intervalSeconds) {
        this.engine = engine;
        this.snapshotFile = snapshotFile;
        this.intervalSeconds = intervalSeconds;
        this.executor = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        executor.execute(this::safeSnapshot);
        if (intervalSeconds > 0) {
            executor.scheduleWithFixedDelay(this::safeSnapshot, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        }
    }

    private void safeSnapshot() {
        try {
            snapshotFile.write(engine);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
