package com.can.rdb;

import com.can.config.AppProperties;
import com.can.core.CacheEngine;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import io.quarkus.runtime.Startup;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Belirlenen aralıklarla {@link CacheEngine} üzerindeki veriyi güvenli bir şekilde
 * diske yazmak için arka planda çalışan basit bir zamanlayıcıdır. İlk başlangıçta
 * ve devamında periyodik olarak {@link SnapshotFile#write(CacheEngine)} çağrısını
 * gerçekleştirir ve hata durumlarını loglayarak sistemin ayakta kalmasını sağlar.
 */
@Startup
@Singleton
public class SnapshotScheduler implements AutoCloseable
{

    private static final Logger LOG = Logger.getLogger(SnapshotScheduler.class);

    private final CacheEngine<String, String> engine;
    private final SnapshotFile<String, String> snapshotFile;
    private final long intervalSeconds;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledTask;
    private boolean running;

    @Inject
    public SnapshotScheduler(CacheEngine<String, String> engine,
                             SnapshotFile<String, String> snapshotFile,
                             AppProperties properties) {
        this(engine, snapshotFile, properties.rdb().snapshotIntervalSeconds());
    }

    public SnapshotScheduler(CacheEngine<String, String> engine,
                             SnapshotFile<String, String> snapshotFile,
                             long intervalSeconds) {
        this.engine = engine;
        this.snapshotFile = snapshotFile;
        this.intervalSeconds = intervalSeconds;
        this.scheduler = intervalSeconds > 0
                ? Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread thread = new Thread(r, "snapshot-writer");
                    thread.setDaemon(true);
                    return thread;
                })
                : null;
    }

    @PostConstruct
    void init() {
        start();
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        safeSnapshot();
        if (intervalSeconds > 0 && scheduler != null) {
            scheduledTask = scheduler.scheduleAtFixedRate(this::safeSnapshot,
                    intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        }
    }

    public boolean isRunning() {
        return running;
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
    public synchronized void close()
    {
        if (!running) {
            return;
        }
        running = false;
        if (scheduledTask != null) {
            scheduledTask.cancel(false);
            scheduledTask = null;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
