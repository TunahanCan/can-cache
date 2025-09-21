package com.can.metric;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Metrik kayıt defterindeki sayaç ve zamanlayıcıları belirli aralıklarla konsola
 * raporlayan yardımcı servistir. Sanal thread'ler üzerinden çalışan zamanlayıcı
 * görevleri yönetir ve kapatıldığında kaynakları serbest bırakır.
 */
public final class MetricsReporter implements AutoCloseable
{
    private final MetricsRegistry reg;
    private final ScheduledExecutorService exec =
            Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory());

    public MetricsReporter(MetricsRegistry reg){ this.reg = reg; }
    public void start(long intervalSeconds){ exec.scheduleAtFixedRate(this::dump,
            intervalSeconds, intervalSeconds, TimeUnit.SECONDS); }

    private void dump()
    {
        IO.println("=== METRICS ====");
        for (var c : reg.counters().values()) System.out.printf("counter %s = %d%n", c.name(), c.get());
        for (var t : reg.timers().values()){
            var s = t.snapshot();
            System.out.printf("timer %s count=%d avg=%.2fµs p50=%.2fµs p95=%.2fµs min=%.2fµs max=%.2fµs%n",
                    s.name(), s.count(), s.avgNs()/1_000.0, s.p50Ns()/1_000.0, s.p95Ns()/1_000.0, s.minNs()/1_000.0, s.maxNs()/1_000.0);
        }
    }

    @Override
    public void close(){ exec.shutdownNow(); }
}
