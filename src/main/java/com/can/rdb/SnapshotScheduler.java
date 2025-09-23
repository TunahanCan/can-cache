package com.can.rdb;

import com.can.config.AppProperties;
import com.can.core.CacheEngine;
import io.quarkus.runtime.Startup;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

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
public class SnapshotScheduler implements AutoCloseable
{

    private static final Logger LOG = Logger.getLogger(SnapshotScheduler.class);

    private final CacheEngine<String, String> engine;
    private final SnapshotFile<String, String> snapshotFile;
    private final long intervalSeconds;
    private final Vertx vertx;
    private final WorkerExecutor workerExecutor;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private long periodicTimerId = -1L;

    @Inject
    public SnapshotScheduler(CacheEngine<String, String> engine,
                             SnapshotFile<String, String> snapshotFile,
                             AppProperties properties,
                             Vertx vertx,
                             WorkerExecutor workerExecutor) {
        this(engine, snapshotFile, properties.rdb().snapshotIntervalSeconds(), vertx, workerExecutor);
    }

    public SnapshotScheduler(CacheEngine<String, String> engine,
                             SnapshotFile<String, String> snapshotFile,
                             long intervalSeconds,
                             Vertx vertx,
                             WorkerExecutor workerExecutor) {
        this.engine = engine;
        this.snapshotFile = snapshotFile;
        this.intervalSeconds = intervalSeconds;
        this.vertx = vertx;
        this.workerExecutor = workerExecutor;
    }

    @PostConstruct
    void init() {
        start();
    }

    public synchronized void start() {
        if (started.get()) {
            return;
        }
        started.set(true);
        workerExecutor.executeBlocking(promise -> {
            safeSnapshot();
            promise.complete();
        });
        if (intervalSeconds > 0) {
            long delay = TimeUnit.SECONDS.toMillis(intervalSeconds);
            periodicTimerId = vertx.setPeriodic(delay, id ->
                    workerExecutor.executeBlocking(promise -> {
                        safeSnapshot();
                        promise.complete();
                    })
            );
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
        if (periodicTimerId >= 0L) {
            vertx.cancelTimer(periodicTimerId);
            periodicTimerId = -1L;
        }
    }
}
