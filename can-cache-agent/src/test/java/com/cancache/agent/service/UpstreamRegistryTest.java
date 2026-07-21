package com.cancache.agent.service;

import com.cancache.agent.model.UpstreamState;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UpstreamRegistryTest {

    @Test
    void reportsDiscoverySourceAndReadyNodes() {
        UpstreamRegistry registry = new UpstreamRegistry();
        registry.replace(List.of("10.0.0.2", "10.0.0.1"), 11212);
        registry.register("10.0.0.2", 11212, 5_000L);
        registry.register("cache-manual", 11212, 5_000L);

        registry.all().get(0).recordHealthCheck(UpstreamState.UP, null, 2L);

        assertEquals(3, registry.total());
        assertEquals("DNS", registry.sourceOf("10.0.0.1:11212"));
        assertEquals("DNS + REGISTRATION", registry.sourceOf("10.0.0.2:11212"));
        assertEquals("REGISTRATION", registry.sourceOf("cache-manual:11212"));
        assertEquals(List.of("10.0.0.1:11212"),
                registry.ready().stream().map(node -> node.address()).toList());
    }

    @Test
    void removesDnsNodesThatAreNoLongerDiscovered() {
        UpstreamRegistry registry = new UpstreamRegistry();
        registry.replace(List.of("10.0.0.1", "10.0.0.2"), 11212);

        registry.replace(List.of("10.0.0.2"), 11212);

        assertEquals(List.of("10.0.0.2:11212"),
                registry.all().stream().map(node -> node.address()).toList());
    }
}
