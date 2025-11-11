package com.can.metric;

import com.can.config.AppProperties;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

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
        // Bu test HTTP üzerinden sayaç ve zamanlayıcı metriklerinin dışa vurulduğunu doğrular.
        @Test
        void reporter_exposes_metrics_over_http() throws Exception
        {
            MetricsRegistry registry = new MetricsRegistry();
            registry.counter("cache_hits").add(3);
            registry.counter("hinted_handoff_replayed_total").add(2);
            registry.counter("hinted_handoff_failures_total").add(1);
            registry.timer("latency").record(1_000_000);

            Vertx vertx = Vertx.vertx();
            FakeMetricsConfig config = new FakeMetricsConfig();
            MetricsReporter reporter = new MetricsReporter(registry, config, () -> "node-1", vertx);

            try
            {
                reporter.start();
                assertTrue(reporter.isRunning());

                HttpClient client = HttpClient.newHttpClient();
                int port = reporter.actualPort();
                HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + config.endpointHost() + ":" + port + config.endpointPath()))
                        .GET()
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                assertEquals(200, response.statusCode());
                String body = response.body();
                assertTrue(body.contains("# TYPE cache_hits_total counter"));
                assertTrue(body.contains("cache_hits_total{node_id=\"node-1\",role=\"coordinator\"} 3"));
                assertTrue(body.contains("hinted_handoff_replayed_total{hint_replay_result=\"success\",node_id=\"node-1\",role=\"coordinator\"} 2"));
                assertTrue(body.contains("hinted_handoff_failures_total{hint_replay_result=\"failure\",node_id=\"node-1\",role=\"coordinator\"} 1"));
                assertTrue(body.contains("latency_seconds_count{node_id=\"node-1\",role=\"coordinator\"}"));
                assertTrue(body.contains("latency_seconds_sum{node_id=\"node-1\",role=\"coordinator\"}"));
            }
            finally
            {
                reporter.close();
                vertx.close().toCompletionStage().toCompletableFuture().join();
            }
        }

        // Bu test uç nokta devre dışı bırakıldığında sunucunun başlatılmadığını gösterir.
        @Test
        void reporter_respects_disabled_endpoint()
        {
            MetricsRegistry registry = new MetricsRegistry();
            Vertx vertx = Vertx.vertx();
            FakeMetricsConfig config = new FakeMetricsConfig();
            config.enabled = false;
            MetricsReporter reporter = new MetricsReporter(registry, config, () -> "node-1", vertx);
            try
            {
                reporter.start();
                assertFalse(reporter.isRunning());
            }
            finally
            {
                reporter.close();
                vertx.close().toCompletionStage().toCompletableFuture().join();
            }
        }
    }

    private static final class FakeMetricsConfig implements AppProperties.Metrics
    {
        private boolean enabled = true;
        private int port = 0;
        @Override
        public boolean endpointEnabled()
        {
            return enabled;
        }

        @Override
        public String endpointHost()
        {
            return "127.0.0.1";
        }

        @Override
        public int endpointPort()
        {
            return port;
        }

        @Override
        public String endpointPath()
        {
            return "/metrics";
        }

        @Override
        public String replicationRole()
        {
            return "coordinator";
        }
    }
}
