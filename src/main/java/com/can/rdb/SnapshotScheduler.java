package com.can.rdb;

import com.can.config.AppProperties;
import com.can.core.CacheEngine;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Belirlenen aralıklarla {@link CacheEngine} üzerindeki veriyi güvenli bir şekilde
 * diske yazmak için arka planda çalışan zamanlayıcıdır. Sanal thread tabanlı
 * planlayıcıyı kullanarak ilk başlangıçta ve devamında periyodik olarak
 * {@link SnapshotFile#write(CacheEngine)} çağrısını gerçekleştirir ve hata
 * durumlarını loglayarak sistemin ayakta kalmasını sağlar.
 */
@Startup
@Singleton
public class SnapshotScheduler<K, V> implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(SnapshotScheduler.class);

    private final CacheEngine<K, V> engine;
    private final SnapshotFile<K, V> snapshotFile;
    private final long intervalSeconds;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private ScheduledExecutorService executor;

    @Inject
    public SnapshotScheduler(CacheEngine<K, V> engine, SnapshotFile<K, V> snapshotFile, AppProperties properties) {
        this(engine, snapshotFile, properties.rdb().snapshotIntervalSeconds());
    }

    public SnapshotScheduler(CacheEngine<K, V> engine, SnapshotFile<K, V> snapshotFile, long intervalSeconds) {
        this.engine = engine;
        this.snapshotFile = snapshotFile;
        this.intervalSeconds = intervalSeconds;
    }

    @PostConstruct
    void init() {
        start();
    }

    public synchronized void start() {
        if (started.get()) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
        started.set(true);
        executor.execute(this::safeSnapshot);
        if (intervalSeconds > 0) {
            executor.scheduleWithFixedDelay(this::safeSnapshot, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        }
    }

    public boolean isRunning() {
        return started.get();
    }

    private void safeSnapshot() {
        try {
            snapshotFile.write(engine);
        } catch (Throwable t) {
            LOG.error("Failed to persist snapshot", t);
        }
    }

    @PreDestroy
    void shutdown() {
        close();
    }

    @Override
    public synchronized void close() {
        if (!started.getAndSet(false)) {
            return;
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }
}
