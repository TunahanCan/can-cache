package com.can.metric;

import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MetricsComponentsTest
{
    @Nested
    class CounterBehaviour
    {
        @Test
        void counter_increments_and_adds()
        {
            Counter counter = new Counter("hits");
            counter.inc();
            counter.add(4);
            assertEquals(5, counter.get());
            assertEquals("hits", counter.name());
        }
    }

    @Nested
    class TimerBehaviour
    {
        @Test
        void timer_records_statistics()
        {
            Timer timer = new Timer("latency", 128);
            timer.record(1000);
            timer.record(2000);
            Timer.Sample sample = timer.snapshot();

            assertEquals("latency", sample.name());
            assertEquals(2, sample.count());
            assertEquals(3000, sample.totalNs());
            assertEquals(1000, sample.minNs());
            assertEquals(2000, sample.maxNs());
            assertTrue(sample.avgNs() >= 1500 && sample.avgNs() <= 2000);
            assertTrue(sample.p95Ns() >= 0);
        }
    }

    @Nested
    class RegistryBehaviour
    {
        @Test
        void registry_reuses_same_instances()
        {
            MetricsRegistry registry = new MetricsRegistry();
            Counter c1 = registry.counter("requests");
            Counter c2 = registry.counter("requests");
            Timer t1 = registry.timer("latency");
            Timer t2 = registry.timer("latency");

            assertSame(c1, c2);
            assertSame(t1, t2);
            assertTrue(registry.counters().containsKey("requests"));
            assertTrue(registry.timers().containsKey("latency"));
        }
    }

    @Nested
    class ReporterBehaviour
    {
        @Test
        void reporter_starts_and_stops_safely() throws Exception
        {
            MetricsRegistry registry = new MetricsRegistry();
            Vertx vertx = Vertx.vertx();
            WorkerExecutor worker = vertx.createSharedWorkerExecutor("test-metrics");
            try {
                MetricsReporter reporter = new MetricsReporter(registry, 1, vertx, worker);
                reporter.start(1);
                assertTrue(reporter.isRunning());
                reporter.close();
                assertFalse(reporter.isRunning());
            } finally {
                worker.close();
                vertx.close().toCompletionStage().toCompletableFuture().join();
            }
        }

        @Test
        void reporter_ignores_invalid_interval()
        {
            MetricsRegistry registry = new MetricsRegistry();
            Vertx vertx = Vertx.vertx();
            WorkerExecutor worker = vertx.createSharedWorkerExecutor("test-metrics");
            try {
                MetricsReporter reporter = new MetricsReporter(registry, 0, vertx, worker);
                reporter.start(0);
                assertFalse(reporter.isRunning());
            } finally {
                worker.close();
                vertx.close().toCompletionStage().toCompletableFuture().join();
            }
        }
    }
}
