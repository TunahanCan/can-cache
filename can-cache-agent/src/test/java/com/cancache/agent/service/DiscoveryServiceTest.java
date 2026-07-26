package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DiscoveryServiceTest {

    @Test
    void shouldShareAnInFlightLookupAndNormalizeItsResult() {
        DiscoveryService service = service();
        AtomicInteger calls = new AtomicInteger();
        Promise<List<String>> lookup = Promise.promise();
        service.resolver = dnsName -> {
            calls.incrementAndGet();
            return lookup.future();
        };

        Future<Void> first = service.refreshNowAsync();
        Future<Void> second = service.refreshNowAsync();

        assertSame(first, second);
        assertEquals(1, calls.get());

        lookup.complete(List.of("10.0.0.2", "10.0.0.1", "10.0.0.2"));

        assertTrue(first.succeeded());
        assertEquals(List.of("10.0.0.1:11212", "10.0.0.2:11212"),
                service.registry.all().stream().map(node -> node.address()).toList());
        assertEquals(1, service.metrics.dnsChanges());
    }

    @Test
    void shouldIgnoreAResultCompletedAfterStop() {
        DiscoveryService service = service();
        Promise<List<String>> lookup = Promise.promise();
        service.resolver = ignored -> lookup.future();

        Future<Void> refresh = service.refreshNowAsync();
        service.stop();
        lookup.complete(List.of("10.0.0.1"));

        assertTrue(refresh.succeeded());
        assertEquals(0, service.registry.total());
        assertEquals(0, service.metrics.dnsChanges());
    }

    @Test
    void shouldPermitAnotherLookupAfterFailureWithoutDroppingCurrentNodes() {
        DiscoveryService service = service();
        AtomicInteger calls = new AtomicInteger();
        service.resolver = ignored -> {
            if (calls.incrementAndGet() == 1) {
                return Future.succeededFuture(List.of("10.0.0.1"));
            }
            return Future.failedFuture("temporary DNS failure");
        };

        assertTrue(service.refreshNowAsync().succeeded());
        assertTrue(service.refreshNowAsync().succeeded());

        assertEquals(2, calls.get());
        assertEquals(List.of("10.0.0.1:11212"),
                service.registry.all().stream().map(node -> node.address()).toList());
        assertEquals(1, service.metrics.dnsChanges());
        assertEquals(1, service.metrics.latestEvents().stream()
                .filter(event -> event.contains("temporary DNS failure"))
                .count());
    }

    private static DiscoveryService service() {
        DiscoveryService service = new DiscoveryService();
        service.config = config();
        service.registry = new UpstreamRegistry();
        service.metrics = new MetricsModel();
        return service;
    }

    private static AgentConfig config() {
        AgentConfig.Discovery discovery = new AgentConfig.Discovery() {
            @Override
            public boolean enabled() {
                return true;
            }

            @Override
            public String dns() {
                return "cache.internal";
            }

            @Override
            public Duration interval() {
                return Duration.ofSeconds(5);
            }
        };
        AgentConfig.Upstream upstream = () -> 11212;

        return new AgentConfig() {
            @Override
            public Listen listen() {
                return null;
            }

            @Override
            public Discovery discovery() {
                return discovery;
            }

            @Override
            public Upstream upstream() {
                return upstream;
            }

            @Override
            public Health health() {
                return null;
            }

            @Override
            public Selection selection() {
                return null;
            }

            @Override
            public Timeouts timeouts() {
                return null;
            }

            @Override
            public Registration registration() {
                return null;
            }

            @Override
            public Dashboard dashboard() {
                return null;
            }

            @Override
            public Shutdown shutdown() {
                return null;
            }
        };
    }
}
