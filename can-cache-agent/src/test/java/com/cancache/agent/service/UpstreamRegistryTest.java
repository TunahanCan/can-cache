package com.cancache.agent.service;

import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamState;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

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
    void shouldRemoveExpiredRegistrations() throws Exception {
        UpstreamRegistry registry = new UpstreamRegistry();
        registry.register("127.0.0.1", 11212, 1);

        Thread.sleep(1_100);
        registry.cleanupExpiredRegistrations();

        assertEquals(0, registry.total());
    }
}
