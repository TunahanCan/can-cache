package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamState;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TcpProxyLifecycleTest {

    @Test
    void lifecycleShouldReflectListenerReadinessAndDrainWithoutFixedPortsOrSleeps() throws Exception {
        Vertx vertx = Vertx.vertx();
        TcpProxyServer server = new TcpProxyServer();
        UpstreamRegistry registry = new UpstreamRegistry();
        NodeStats node = registry.register("127.0.0.1", 11212, 60_000);
        node.state(UpstreamState.UP);

        server.vertx = vertx;
        server.config = config();
        server.registry = registry;
        server.metrics = new MetricsModel();
        server.tracker = new ConnectionTracker();

        try {
            assertFalse(server.isListening());
            assertFalse(server.isAccepting());
            assertFalse(server.isReady());
            assertEquals("STARTING", server.lifecycleState());

            server.start();

            assertTrue(server.isListening());
            assertTrue(server.isAccepting());
            assertTrue(server.isReady());
            assertEquals("READY", server.lifecycleState());
            assertEquals(0, server.activeSessions());
            assertEquals(0, server.pendingConnections());

            node.state(UpstreamState.DOWN);
            assertFalse(server.isReady());
            assertEquals("DEGRADED", server.lifecycleState());

            server.stop();
            server.stop();

            assertFalse(server.isListening());
            assertFalse(server.isAccepting());
            assertFalse(server.isReady());
            assertEquals("DRAINING", server.lifecycleState());
            assertEquals(0, server.activeSessions());
            assertEquals(0, server.pendingConnections());
        } finally {
            server.stop();
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void gracefulShutdownShouldWaitForAnEstablishedSessionToClose() throws Exception {
        Vertx vertx = Vertx.vertx();
        NetServer upstream = vertx.createNetServer();
        CompletableFuture<NetSocket> upstreamSocket = new CompletableFuture<>();
        int upstreamPort = upstream.connectHandler(upstreamSocket::complete)
                .listen(0, "127.0.0.1")
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS)
                .actualPort();

        AgentConfig config = config();
        UpstreamRegistry registry = new UpstreamRegistry();
        NodeStats node = registry.register("127.0.0.1", upstreamPort, 60_000);
        node.state(UpstreamState.UP);
        UpstreamSelector selector = new UpstreamSelector();
        selector.config = config;
        selector.init();

        TcpProxyServer proxy = new TcpProxyServer();
        proxy.vertx = vertx;
        proxy.config = config;
        proxy.registry = registry;
        proxy.selector = selector;
        proxy.metrics = new MetricsModel();
        proxy.tracker = new ConnectionTracker();
        NetClient client = vertx.createNetClient();
        NetSocket downstream = null;

        try {
            proxy.start();
            downstream = client.connect(proxy.actualPort(), "127.0.0.1")
                    .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            upstreamSocket.get(5, TimeUnit.SECONDS);
            awaitActiveConnection(proxy.metrics);

            CompletableFuture<Void> stopped = CompletableFuture.runAsync(proxy::stop);
            assertThrows(TimeoutException.class, () -> stopped.get(100, TimeUnit.MILLISECONDS));

            downstream.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            stopped.get(5, TimeUnit.SECONDS);

            assertEquals(0, proxy.activeSessions());
            assertFalse(proxy.isListening());
        } finally {
            if (downstream != null) {
                downstream.close();
            }
            proxy.stop();
            client.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            upstream.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    private static void awaitActiveConnection(MetricsModel metrics) throws TimeoutException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (metrics.activeConnections() != 1) {
            if (System.nanoTime() >= deadline) {
                throw new TimeoutException("proxy connection did not become active");
            }
            Thread.onSpinWait();
        }
    }

    private static AgentConfig config() {
        AgentConfig.Listen listen = new AgentConfig.Listen() {
            @Override
            public String host() {
                return "127.0.0.1";
            }

            @Override
            public int port() {
                return 0;
            }

            @Override
            public int maxConnections() {
                return 16;
            }

            @Override
            public int maxPendingConnections() {
                return 8;
            }

            @Override
            public int writeQueueMaxBytes() {
                return 65_536;
            }
        };
        AgentConfig.Timeouts timeouts = new AgentConfig.Timeouts() {
            @Override
            public Duration connect() {
                return Duration.ofSeconds(1);
            }

            @Override
            public Duration idle() {
                return Duration.ofSeconds(10);
            }
        };
        AgentConfig.Shutdown shutdown = () -> Duration.ofSeconds(1);
        AgentConfig.Selection selection = new AgentConfig.Selection() {
            @Override
            public AgentConfig.Policy policy() {
                return AgentConfig.Policy.RR;
            }

            @Override
            public int maxAttempts() {
                return 2;
            }
        };
        AgentConfig.Health health = new AgentConfig.Health() {
            @Override
            public Duration interval() {
                return Duration.ofSeconds(2);
            }

            @Override
            public Duration connectTimeout() {
                return Duration.ofSeconds(1);
            }

            @Override
            public int healthyThreshold() {
                return 2;
            }

            @Override
            public int unhealthyThreshold() {
                return 3;
            }

            @Override
            public int passiveFailureThreshold() {
                return 2;
            }
        };

        return new AgentConfig() {
            @Override
            public Listen listen() {
                return listen;
            }

            @Override
            public Discovery discovery() {
                return null;
            }

            @Override
            public Upstream upstream() {
                return null;
            }

            @Override
            public Health health() {
                return health;
            }

            @Override
            public Selection selection() {
                return selection;
            }

            @Override
            public Timeouts timeouts() {
                return timeouts;
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
                return shutdown;
            }
        };
    }
}
