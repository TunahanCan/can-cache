package com.cancache.agent;

import com.cancache.agent.cluster.ClusterClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
class CanCacheApplicationTests {

    @Inject
    ClusterClient cluster;

    @Nested
    class ContainerWiring {

        /**
         * Verifies that the Quarkus DI container successfully provides the ClusterClient bean.
         * This ensures basic inter-module integration is functioning upon application startup.
         */
        @Test
        void shouldInjectClusterClientSuccessfully() {
            // Given / When
            // (Injection handled by Quarkus)

            // Then
            assertNotNull(cluster, "ClusterClient bean should be injected by the container");
        }
    }
}
