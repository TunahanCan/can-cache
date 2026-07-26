package com.cancache.agent.service;

import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UpstreamRegistryTest {

    @Test
    void shouldKeepRegisteredNodesSeparateByPort() {
        UpstreamRegistry registry = new UpstreamRegistry();

        NodeStats first = registry.register("127.0.0.1", 11212, 5_000);
        NodeStats second = registry.register("127.0.0.1", 11213, 5_000);

        assertEquals(2, registry.total());
        assertEquals(List.of("127.0.0.1:11212", "127.0.0.1:11213"),
                registry.all().stream().map(NodeStats::address).toList());
        assertSame(first, registry.register("127.0.0.1", 11212, 5_000));
        assertSame(second, registry.register("127.0.0.1", 11213, 5_000));
    }

    @Test
    void shouldBoundUniqueRegistrationsWhileAllowingLeaseRenewal() {
        UpstreamRegistry registry = new UpstreamRegistry();

        NodeStats first = registry.register("127.0.0.1", 11212, 5_000, 2);
        registry.register("127.0.0.1", 11213, 5_000, 2);

        assertSame(first, registry.register("127.0.0.1", 11212, 5_000, 2));
        assertThrows(UpstreamRegistry.RegistrationCapacityExceededException.class,
                () -> registry.register("127.0.0.1", 11214, 5_000, 2));
        assertEquals(2, registry.total());
    }

    @Test
    void shouldExposeOnlyHealthyNodesAsReady() {
        UpstreamRegistry registry = new UpstreamRegistry();
        NodeStats first = registry.register("127.0.0.1", 11212, 5_000);
        NodeStats second = registry.register("127.0.0.1", 11213, 5_000);

        first.state(UpstreamState.UP);
        second.state(UpstreamState.DOWN);

        assertEquals(List.of("127.0.0.1:11212"),
                registry.ready().stream().map(NodeStats::address).toList());
    }

    @Test
    void shouldRemoveExpiredRegistrations() {
        AtomicLong clock = new AtomicLong();
        UpstreamRegistry registry = new UpstreamRegistry(clock::get);
        registry.register("127.0.0.1", 11212, 1);

        clock.set(1_001);
        registry.cleanupExpiredRegistrations();

        assertEquals(0, registry.total());
    }

    @Test
    void shouldRetainActiveOrphanWithoutRoutingNewConnections() {
        AtomicLong clock = new AtomicLong();
        UpstreamRegistry registry = new UpstreamRegistry(clock::get);
        NodeStats node = registry.register("127.0.0.1", 11212, 1);
        node.state(UpstreamState.UP);
        node.incActiveConn();

        clock.set(1_001);
        registry.cleanupExpiredRegistrations();

        assertEquals(1, registry.total());
        assertEquals(UpstreamState.DOWN, node.state());
        assertTrue(registry.ready().isEmpty());
        assertFalse(registry.isManaged(node));

        node.decActiveConn();
        registry.cleanupExpiredRegistrations();

        assertEquals(0, registry.total());
    }

    @Test
    void registrationRenewalAndCleanupShouldBeAtomic() throws Exception {
        AtomicLong clock = new AtomicLong();
        UpstreamRegistry registry = new UpstreamRegistry(clock::get);
        registry.register("127.0.0.1", 11212, 1);

        int iterations = 250;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < iterations; i++) {
                clock.addAndGet(5_001);
                CountDownLatch start = new CountDownLatch(1);
                var cleanup = executor.submit(() -> {
                    await(start);
                    registry.cleanupExpiredRegistrations();
                });
                var renewal = executor.submit(() -> {
                    await(start);
                    registry.register("127.0.0.1", 11212, 5_000);
                });

                start.countDown();
                cleanup.get(2, TimeUnit.SECONDS);
                renewal.get(2, TimeUnit.SECONDS);

                assertEquals(1, registry.total());
                assertTrue(registry.isManaged(registry.all().getFirst()));
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void invalidDiscoverySnapshotShouldNotBePartiallyApplied() {
        UpstreamRegistry registry = new UpstreamRegistry();

        assertThrows(IllegalArgumentException.class,
                () -> registry.replace(List.of("127.0.0.1", "not a valid host"), 11212));

        assertEquals(0, registry.total());
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(2, TimeUnit.SECONDS));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
