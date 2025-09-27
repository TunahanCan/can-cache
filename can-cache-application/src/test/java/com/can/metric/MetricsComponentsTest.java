package com.can.metric;

import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
        // Bu test geçerli aralıkla başlatılan raporlama görevlerinin çalıştığını doğrular.
        @Test
        void reporter_runs_with_valid_interval() throws Exception
        {
            MetricsRegistry registry = new MetricsRegistry();
            Vertx vertx = Vertx.vertx();
            WorkerExecutor worker = vertx.createSharedWorkerExecutor("metrics-test");
            try
            {
                MetricsReporter reporter = new MetricsReporter(registry, 1, vertx, worker);
                reporter.start(1);
                assertTrue(reporter.isRunning());
                reporter.close();
                assertFalse(reporter.isRunning());
            }
            finally
            {
                worker.close();
                vertx.close().toCompletionStage().toCompletableFuture().join();
            }
        }

        // Bu test geçersiz aralıkta raporlayıcının başlamadığını gösterir.
        @Test
        void reporter_ignores_invalid_interval()
        {
            MetricsRegistry registry = new MetricsRegistry();
            Vertx vertx = Vertx.vertx();
            WorkerExecutor worker = vertx.createSharedWorkerExecutor("metrics-test");
            try
            {
                MetricsReporter reporter = new MetricsReporter(registry, 0, vertx, worker);
                reporter.start(0);
                assertFalse(reporter.isRunning());
            }
            finally
            {
                worker.close();
                vertx.close().toCompletionStage().toCompletableFuture().join();
            }
        }
    }
}
