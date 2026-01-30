package com.can.metric;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MetricsRegistry, Counter ve Timer bileşenlerinin birim testleri.
 * <p>
 * Not: MetricsReporter artık Quarkus Micrometer extension'ını kullanıyor.
 * HTTP endpoint testleri entegrasyon testleri olarak yapılmalıdır.
 * </p>
 */
class MetricsComponentsTest
{
    @Nested
    class CounterBehavior
    {
        // Bu test sayaç artışının ve toplamanın değeri doğru güncellediğini doğrular.
        @Test
        void counter_handles_increment_and_add()
        {
            Counter counter = new Counter("hits");
            counter.inc();
            counter.add(4);
            assertEquals(5, counter.get());
            assertEquals("hits", counter.name());
        }
    }

    @Nested
    class TimerBehavior
    {
        // Bu test süre kayıtlarının istatistiklere yansıtıldığını gösterir.
        @Test
        void timer_aggregates_durations_into_statistics()
        {
            Timer timer = new Timer("latency", 128);
            timer.record(1_000);
            timer.record(2_000);
            Timer.Sample sample = timer.snapshot();
            assertEquals("latency", sample.name());
            assertEquals(2, sample.count());
            assertEquals(3_000, sample.totalNs());
            assertEquals(1_000, sample.minNs());
            assertEquals(2_000, sample.maxNs());
            assertTrue(sample.avgNs() >= 1_000);
        }
    }

    @Nested
    class RegistryBehavior
    {
        // Bu test aynı isim için aynı sayaç ve zamanlayıcının döndüğünü doğrular.
        @Test
        void registry_reuses_components_with_same_name()
        {
            MetricsRegistry registry = new MetricsRegistry();
            Counter firstCounter = registry.counter("requests");
            Counter secondCounter = registry.counter("requests");
            Timer firstTimer = registry.timer("latency");
            Timer secondTimer = registry.timer("latency");
            assertSame(firstCounter, secondCounter);
            assertSame(firstTimer, secondTimer);
            assertTrue(registry.counters().containsKey("requests"));
            assertTrue(registry.timers().containsKey("latency"));
        }
    }

    @Nested
    class ReporterBehavior
    {
        /**
         * MetricsReporter artık Micrometer tabanlı çalışıyor.
         * Quarkus Micrometer extension'ı /q/metrics endpoint'ini otomatik sağlar.
         * HTTP endpoint testleri Quarkus entegrasyon testleri (@QuarkusTest) olarak yapılmalıdır.
         */
        @Test
        void reporter_with_no_meter_registry_returns_not_running()
        {
            // Argümansız constructor test modunda null MeterRegistry kullanır
            MetricsReporter reporter = new MetricsReporter();
            assertFalse(reporter.isRunning());
            assertEquals(-1, reporter.actualPort());
            reporter.close(); // kapatma güvenli olmalı
        }
    }
}
