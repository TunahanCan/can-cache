package com.can.metric;

import com.can.config.AppProperties;
import io.quarkus.runtime.Startup;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Metrik kayıt defterindeki sayaç ve zamanlayıcıları belirli aralıklarla konsola
 * raporlayan yardımcı servistir. Sanal thread'ler üzerinden çalışan zamanlayıcı
 * görevleri yönetir ve kapatıldığında kaynakları serbest bırakır.
 */
@Startup
@Singleton
public class MetricsReporter implements AutoCloseable
{
    private final MetricsRegistry registry;
    private final long intervalSeconds;
    private final Vertx vertx;
    private final WorkerExecutor workerExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private long timerId = -1L;

    @Inject
    public MetricsReporter(MetricsRegistry registry, AppProperties properties, Vertx vertx, WorkerExecutor workerExecutor) {
        this(registry, properties.metrics().reportIntervalSeconds(), vertx, workerExecutor);
    }

    public MetricsReporter(MetricsRegistry registry, long intervalSeconds, Vertx vertx, WorkerExecutor workerExecutor) {
        this.registry = registry;
        this.intervalSeconds = intervalSeconds;
        this.vertx = vertx;
        this.workerExecutor = workerExecutor;
    }

    @PostConstruct
    void init() {
        start(intervalSeconds);
    }

    public synchronized void start(long intervalSeconds) {
        if (intervalSeconds <= 0 || !running.compareAndSet(false, true)) {
            return;
        }
        long periodMillis = TimeUnit.SECONDS.toMillis(intervalSeconds);
        timerId = vertx.setPeriodic(periodMillis, id ->
                workerExecutor.executeBlocking(promise -> {
                    dump();
                    promise.complete();
                })
        );
    }

    public boolean isRunning() {
        return running.get();
    }

    private void dump() {
        System.out.println("=== METRICS ====");
        registry.counters().values().forEach(counter ->
                System.out.printf("counter %s = %d%n", counter.name(), counter.get())
        );
        registry.timers().values().forEach(timer -> {
            var sample = timer.snapshot();
            System.out.printf(
                    "timer %s count=%d avg=%.2fµs p50=%.2fµs p95=%.2fµs min=%.2fµs max=%.2fµs%n",
                    sample.name(),
                    sample.count(),
                    sample.avgNs() / 1_000.0,
                    sample.p50Ns() / 1_000.0,
                    sample.p95Ns() / 1_000.0,
                    sample.minNs() / 1_000.0,
                    sample.maxNs() / 1_000.0
            );
        });
    }

    @PreDestroy
    void shutdown() {
        close();
    }

    @Override
    public synchronized void close() {
        running.set(false);
        if (timerId >= 0L) {
            vertx.cancelTimer(timerId);
            timerId = -1L;
        }
    }
}
