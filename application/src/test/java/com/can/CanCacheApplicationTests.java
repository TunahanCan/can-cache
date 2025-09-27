package com.can;

import com.can.cluster.ClusterClient;
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
         * Quarkus DI konteyneri AppConfig tarafından üretilen ClusterClient bean'ini sağlar.
         * Uygulama ayağa kalktığında bu bean'in hazır olduğunu kontrol ederek modüller arası entegrasyonu doğrularız.
         */
        @Test
        void clusterClientIsAvailable() {
            assertNotNull(cluster);
        }
    }
}
