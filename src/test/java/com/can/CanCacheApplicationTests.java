package com.can;

import com.can.cluster.ClusterClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
class CanCacheApplicationTests {

    @Inject
    ClusterClient<String, String> cluster;

    @Test
    void clusterClientIsAvailable() {
        assertNotNull(cluster);
    }
}
