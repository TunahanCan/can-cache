package com.can.metric;

import com.can.config.AppProperties;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService executor;

    @Inject
    public MetricsReporter(MetricsRegistry registry, AppProperties properties) {
        this(registry, properties.metrics().reportIntervalSeconds());
    }

    public MetricsReporter(MetricsRegistry registry, long intervalSeconds) {
        this.registry = registry;
        this.intervalSeconds = intervalSeconds;
    }

    @PostConstruct
    void init() {
        start(intervalSeconds);
    }

    public synchronized void start(long intervalSeconds) {
        if (intervalSeconds <= 0 || !running.compareAndSet(false, true)) {
            return;
        }
        executor = Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory());
        executor.scheduleAtFixedRate(this::dump, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
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
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }
}
