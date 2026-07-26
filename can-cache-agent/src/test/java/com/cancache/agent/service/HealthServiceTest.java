package com.cancache.agent.service;

import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamState;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HealthServiceTest {

    @Test
    void shouldUseHysteresisBeforeChangingNodeState() {
        UpstreamRegistry registry = new UpstreamRegistry();
        NodeStats node = registry.register("127.0.0.1", 11212, 5_000);
        HealthService service = service(registry);

        service.recordResult(node, true, null);
        assertEquals(UpstreamState.UNKNOWN, node.state());

        service.recordResult(node, true, null);
        assertEquals(UpstreamState.UP, node.state());

        node.recordPassiveFailure(1);
        service.recordResult(node, true, null);
        assertEquals(UpstreamState.DOWN, node.state());
        service.recordResult(node, true, null);
        assertEquals(UpstreamState.UP, node.state());

        service.recordResult(node, false, new IllegalStateException("first"));
        service.recordResult(node, false, new IllegalStateException("second"));
        assertEquals(UpstreamState.UP, node.state());

        service.recordResult(node, false, new IllegalStateException("third"));
        assertEquals(UpstreamState.DOWN, node.state());
        assertEquals(3, node.errorCount());

        service.recordResult(node, true, null);
        assertEquals(UpstreamState.DOWN, node.state());

        service.recordResult(node, true, null);
        assertEquals(UpstreamState.UP, node.state());
    }

    @Test
    void shouldAllowOnlyOneProbePerNodeAtATime() {
        UpstreamRegistry registry = new UpstreamRegistry();
        NodeStats node = registry.register("127.0.0.1", 11212, 5_000);
        HealthService service = service(registry);
        AtomicInteger calls = new AtomicInteger();
        List<Promise<Void>> probes = new ArrayList<>();
        service.probe = ignored -> {
            calls.incrementAndGet();
            Promise<Void> promise = Promise.promise();
            probes.add(promise);
            return promise.future();
        };

        service.check(node);
        service.check(node);

        assertEquals(1, calls.get());
        probes.getFirst().complete();
        assertEquals(UpstreamState.UNKNOWN, node.state());

        service.check(node);
        assertEquals(2, calls.get());
        probes.get(1).complete();
        assertEquals(UpstreamState.UP, node.state());
    }

    @Test
    void staleProbeShouldNotMutateRecreatedNode() {
        UpstreamRegistry registry = new UpstreamRegistry();
        registry.replace(List.of("127.0.0.1"), 11212);
        NodeStats oldNode = registry.all().getFirst();
        HealthService service = service(registry);
        List<Promise<Void>> probes = new ArrayList<>();
        service.probe = ignored -> {
            Promise<Void> promise = Promise.promise();
            probes.add(promise);
            return promise.future();
        };

        service.check(oldNode);
        registry.replace(List.of(), 11212);
        registry.replace(List.of("127.0.0.1"), 11212);
        NodeStats recreatedNode = registry.all().getFirst();
        assertNotSame(oldNode, recreatedNode);

        service.check(recreatedNode);
        assertEquals(2, probes.size());

        probes.getFirst().complete();
        assertEquals(UpstreamState.UNKNOWN, oldNode.state());
        assertEquals(UpstreamState.UNKNOWN, recreatedNode.state());

        probes.get(1).complete();
        service.check(recreatedNode);
        probes.get(2).complete();

        assertEquals(UpstreamState.UP, recreatedNode.state());
        assertSame(recreatedNode, registry.all().getFirst());
    }

    @Test
    void probeCompletedAfterStopShouldBeIgnored() {
        UpstreamRegistry registry = new UpstreamRegistry();
        NodeStats node = registry.register("127.0.0.1", 11212, 5_000);
        HealthService service = service(registry);
        Promise<Void> probe = Promise.promise();
        service.probe = ignored -> probe.future();

        service.check(node);
        service.stop();
        probe.complete();

        assertEquals(UpstreamState.UNKNOWN, node.state());
    }

    @Test
    void staleHealthyTransitionShouldNotUndoConcurrentPassiveEjection() {
        UpstreamRegistry registry = new UpstreamRegistry() {
            @Override
            synchronized boolean transitionIfManaged(
                    NodeStats node,
                    UpstreamState expected,
                    UpstreamState next) {
                node.recordPassiveFailure(1);
                return super.transitionIfManaged(node, expected, next);
            }
        };
        NodeStats node = registry.register("127.0.0.1", 11212, 5_000);
        node.state(UpstreamState.UP);
        HealthService service = service(registry);

        service.recordResult(node, true, null);
        service.recordResult(node, true, null);

        assertEquals(UpstreamState.DOWN, node.state());
        assertEquals(0, registry.upCount());
        assertTrue(registry.ready().isEmpty());
    }

    private static HealthService service(UpstreamRegistry registry) {
        HealthService service = new HealthService();
        service.registry = registry;
        service.metrics = new MetricsModel();
        service.probe = ignored -> Future.succeededFuture();
        return service;
    }
}
