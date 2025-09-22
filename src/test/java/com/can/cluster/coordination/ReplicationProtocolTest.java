package com.can.cluster.coordination;

import com.can.codec.StringCodec;
import com.can.config.AppProperties;
import com.can.core.CacheEngine;
import com.can.core.EvictionPolicyType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationProtocolTest
{
    private CacheEngine<String, String> engine;
    private ReplicationServer server;
    private RemoteNode remoteNode;

    @BeforeEach
    void setUp() throws Exception {
        int port = availablePort();
        TestProperties properties = new TestProperties(port);
        engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                .segments(2)
                .maxCapacity(128)
                .cleanerPollMillis(25)
                .evictionPolicy(EvictionPolicyType.LRU)
                .build();

        server = new ReplicationServer(engine, properties);
        server.start();

        remoteNode = new RemoteNode("remote", properties.cluster().replication().advertiseHost(), port,
                properties.cluster().replication().connectTimeoutMillis());
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
        if (engine != null) {
            engine.close();
        }
    }

    @Test
    void remoteSetAndGetShouldReplicateToEngine() {
        remoteNode.set("foo", "bar", Duration.ofSeconds(1));

        assertEquals("bar", engine.get("foo"));
        assertEquals("bar", remoteNode.get("foo"));
    }

    @Test
    void remoteDeleteShouldRemoveEntry() {
        remoteNode.set("hello", "world", Duration.ofSeconds(5));
        assertTrue(engine.exists("hello"));

        assertTrue(remoteNode.delete("hello"));
        assertFalse(engine.exists("hello"));
    }

    @Test
    void ttlShouldBeRespectedAcrossReplication() throws InterruptedException {
        remoteNode.set("short", "lived", Duration.ofMillis(150));
        assertTrue(engine.exists("short"));

        Thread.sleep(400);

        assertFalse(engine.exists("short"));
        assertNull(remoteNode.get("short"));
    }

    private static int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static final class TestProperties implements AppProperties {
        private final Cluster cluster;
        private final Network network = new Network() {
            @Override
            public String host() { return "127.0.0.1"; }

            @Override
            public int port() { return 11211; }

            @Override
            public int backlog() { return 16; }

            @Override
            public int workerThreads() { return 4; }
        };

        private TestProperties(int port) {
            this.cluster = new TestCluster(port);
        }

        @Override
        public Metrics metrics() {
            return () -> 5L;
        }

        @Override
        public Rdb rdb() {
            return new Rdb() {
                @Override
                public String path() { return "target/test.rdb"; }

                @Override
                public long snapshotIntervalSeconds() { return 60; }
            };
        }

        @Override
        public Cache cache() {
            return new Cache() {
                @Override
                public int segments() { return 2; }

                @Override
                public int maxCapacity() { return 256; }

                @Override
                public long cleanerPollMillis() { return 25; }

                @Override
                public String evictionPolicy() { return "LRU"; }
            };
        }

        @Override
        public Cluster cluster() {
            return cluster;
        }

        @Override
        public Network network() {
            return network;
        }
    }

    private static final class TestCluster implements AppProperties.Cluster {
        private final Discovery discovery = new TestDiscovery();
        private final Replication replication;

        private TestCluster(int port) {
            this.replication = new TestReplication(port);
        }

        @Override
        public int virtualNodes() { return 16; }

        @Override
        public int replicationFactor() { return 1; }

        @Override
        public Discovery discovery() { return discovery; }

        @Override
        public Replication replication() { return replication; }
    }

    private static final class TestDiscovery implements AppProperties.Discovery {
        @Override
        public String multicastGroup() { return "230.0.0.1"; }

        @Override
        public int multicastPort() { return 45565; }

        @Override
        public long heartbeatIntervalMillis() { return 2000; }

        @Override
        public long failureTimeoutMillis() { return 6000; }

        @Override
        public String nodeId() { return "test-node"; }
    }

    private static final class TestReplication implements AppProperties.Replication {
        private final int port;

        private TestReplication(int port) {
            this.port = port;
        }

        @Override
        public String bindHost() { return "127.0.0.1"; }

        @Override
        public String advertiseHost() { return "127.0.0.1"; }

        @Override
        public int port() { return port; }

        @Override
        public int connectTimeoutMillis() { return 2000; }
    }
}
