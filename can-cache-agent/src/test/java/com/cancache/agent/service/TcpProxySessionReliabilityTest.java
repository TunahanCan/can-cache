package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamState;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TcpProxySessionReliabilityTest {

    @Test
    void consecutiveEstablishedStreamResetsShouldPassivelyEjectTheNode() throws Exception {
        try (ProxyFixture fixture = ProxyFixture.start(Duration.ofSeconds(5), true)) {
            fixture.resetEstablishedSession("first-session");

            assertEquals(UpstreamState.UP, fixture.node.state());
            assertEquals(0, fixture.node.activeConn());
            assertEquals(1, fixture.streamFailureEventCount());

            fixture.resetEstablishedSession("second-session");

            assertEquals(UpstreamState.DOWN, fixture.node.state());
            assertEquals(0, fixture.node.activeConn());
            assertEquals(2, fixture.streamFailureEventCount());
            assertTrue(fixture.metrics.latestEvents().stream()
                    .anyMatch(event -> event.contains("upstream stream failure") && event.contains("ejected=true")));
        }
    }

    @Test
    void cleanQuitEofShouldNotPassivelyEjectTheNode() throws Exception {
        try (ProxyFixture fixture = ProxyFixture.start(Duration.ofSeconds(5))) {
            fixture.closeEstablishedSession("quit\r\n");
            fixture.closeEstablishedSession("quit\r\n");

            assertEquals(UpstreamState.UP, fixture.node.state());
            assertEquals(0, fixture.node.activeConn());
            assertEquals(0, fixture.streamFailureEventCount());
        }
    }

    @Test
    void drainShouldRejectNewTrafficWhileAllowingEstablishedSessionToFinish() throws Exception {
        try (ProxyFixture fixture = ProxyFixture.start(Duration.ofSeconds(5))) {
            StreamingSession existing = fixture.openSession();
            existing.sendAndAwait("before-drain");

            CompletableFuture<Void> stopped = CompletableFuture.runAsync(fixture.proxy::stop);
            awaitCondition(() -> !fixture.proxy.isAccepting(), "proxy did not enter drain mode");

            NetSocket rejected = fixture.downstreamClient
                    .connect(fixture.proxy.actualPort(), "127.0.0.1")
                    .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            fixture.downstreamSockets.add(rejected);
            CompletableFuture<Void> rejectedClosed = closeFuture(rejected);
            CompletableFuture<Void> rejectedWriteFinished = new CompletableFuture<>();
            rejected.write(Buffer.buffer("must-not-reach-upstream"))
                    .onComplete(ignored -> rejectedWriteFinished.complete(null));

            rejectedClosed.get(5, TimeUnit.SECONDS);
            rejectedWriteFinished.get(5, TimeUnit.SECONDS);
            awaitCondition(() -> fixture.metrics.rejectedConnections() == 1,
                    "draining connection was not counted as rejected");

            assertEquals(1, fixture.upstreamConnectionCount());
            assertEquals(1, fixture.proxy.activeSessions());
            assertFalse(existing.capture().contains("must-not-reach-upstream"));
            assertFalse(stopped.isDone());

            existing.sendAndAwait("during-drain");
            assertFalse(stopped.isDone());

            existing.downstream().close()
                    .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            stopped.get(5, TimeUnit.SECONDS);

            assertEquals(1, fixture.upstreamConnectionCount());
            assertEquals(0, fixture.proxy.activeSessions());
            assertFalse(fixture.proxy.isListening());
        }
    }

    private static CompletableFuture<Void> closeFuture(NetSocket socket) {
        CompletableFuture<Void> closed = new CompletableFuture<>();
        socket.closeHandler(ignored -> closed.complete(null));
        socket.exceptionHandler(closed::completeExceptionally);
        return closed;
    }

    private static void awaitCondition(BooleanSupplier condition, String failureMessage) throws TimeoutException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new TimeoutException(failureMessage);
            }
            Thread.onSpinWait();
        }
    }

    private record StreamingSession(NetSocket downstream, NetSocket upstream, PayloadCapture capture,
                                    CompletableFuture<Void> downstreamClosed) {

        void sendAndAwait(String payload) throws Exception {
            CompletableFuture<Void> received = capture.expect(payload);
            downstream.write(Buffer.buffer(payload))
                    .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            received.get(5, TimeUnit.SECONDS);
        }
    }

    private static final class PayloadCapture {
        private final StringBuilder payload = new StringBuilder();
        private final List<Expectation> expectations = new CopyOnWriteArrayList<>();

        void append(Buffer buffer) {
            synchronized (payload) {
                payload.append(buffer.toString());
                for (Expectation expectation : expectations) {
                    if (payload.indexOf(expectation.value()) >= 0) {
                        expectation.completion().complete(null);
                        expectations.remove(expectation);
                    }
                }
            }
        }

        CompletableFuture<Void> expect(String value) {
            CompletableFuture<Void> completion = new CompletableFuture<>();
            synchronized (payload) {
                if (payload.indexOf(value) >= 0) {
                    completion.complete(null);
                } else {
                    expectations.add(new Expectation(value, completion));
                }
            }
            return completion;
        }

        boolean contains(String value) {
            synchronized (payload) {
                return payload.indexOf(value) >= 0;
            }
        }

        private record Expectation(String value, CompletableFuture<Void> completion) {
        }
    }

    private static final class ProxyFixture implements AutoCloseable {
        private final Vertx vertx;
        private final NetServer upstreamServer;
        private final NetClient downstreamClient;
        private final BlockingQueue<NetSocket> acceptedUpstreams = new LinkedBlockingQueue<>();
        private final List<NetSocket> upstreamSockets = new CopyOnWriteArrayList<>();
        private final List<NetSocket> downstreamSockets = new CopyOnWriteArrayList<>();
        private final TcpProxyServer proxy;
        private final MetricsModel metrics;
        private NodeStats node;

        private ProxyFixture(Vertx vertx, NetServer upstreamServer, NetClient downstreamClient,
                             TcpProxyServer proxy, MetricsModel metrics) {
            this.vertx = vertx;
            this.upstreamServer = upstreamServer;
            this.downstreamClient = downstreamClient;
            this.proxy = proxy;
            this.metrics = metrics;
        }

        static ProxyFixture start(Duration shutdownGrace) throws Exception {
            return start(shutdownGrace, false);
        }

        static ProxyFixture start(Duration shutdownGrace, boolean resetOnClose) throws Exception {
            Vertx vertx = Vertx.vertx();
            NetServerOptions upstreamOptions = new NetServerOptions();
            if (resetOnClose) {
                upstreamOptions.setSoLinger(0);
            }
            NetServer upstreamServer = vertx.createNetServer(upstreamOptions);
            NetClient downstreamClient = vertx.createNetClient();
            AgentConfig config = config(shutdownGrace);
            UpstreamRegistry registry = new UpstreamRegistry();
            MetricsModel metrics = new MetricsModel();
            TcpProxyServer proxy = new TcpProxyServer();

            ProxyFixture fixture = new ProxyFixture(
                    vertx, upstreamServer, downstreamClient, proxy, metrics);
            try {
                proxy.vertx = vertx;
                proxy.config = config;
                proxy.registry = registry;
                proxy.metrics = metrics;
                proxy.tracker = new ConnectionTracker();

                upstreamServer.connectHandler(socket -> {
                    socket.pause();
                    fixture.upstreamSockets.add(socket);
                    fixture.acceptedUpstreams.add(socket);
                });
                int upstreamPort = upstreamServer.listen(0, "127.0.0.1")
                        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS)
                        .actualPort();

                NodeStats node = registry.register("127.0.0.1", upstreamPort, 60_000);
                node.state(UpstreamState.UP);
                fixture.node = node;
                UpstreamSelector selector = new UpstreamSelector();
                selector.config = config;
                selector.init();

                proxy.selector = selector;
                proxy.start();

                return fixture;
            } catch (Throwable failure) {
                fixture.close();
                throw failure;
            }
        }

        StreamingSession openSession() throws Exception {
            NetSocket downstream = downstreamClient.connect(proxy.actualPort(), "127.0.0.1")
                    .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            downstreamSockets.add(downstream);
            CompletableFuture<Void> downstreamClosed = closeFuture(downstream);

            NetSocket upstream = acceptedUpstreams.poll(5, TimeUnit.SECONDS);
            if (upstream == null) {
                throw new TimeoutException("proxy did not establish an upstream connection");
            }
            PayloadCapture capture = new PayloadCapture();
            upstream.handler(capture::append);
            upstream.resume();

            return new StreamingSession(downstream, upstream, capture, downstreamClosed);
        }

        void resetEstablishedSession(String payload) throws Exception {
            int expectedFailures = streamFailureEventCount() + 1;
            closeEstablishedSession(payload);
            awaitCondition(() -> streamFailureEventCount() >= expectedFailures,
                    "stream reset was not recorded");
        }

        void closeEstablishedSession(String payload) throws Exception {
            StreamingSession session = openSession();
            session.sendAndAwait(payload);

            session.upstream().close()
                    .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            session.downstreamClosed().get(5, TimeUnit.SECONDS);
            awaitCondition(() -> node.activeConn() == 0, "active connection was not released");
        }

        int upstreamConnectionCount() {
            return upstreamSockets.size();
        }

        int streamFailureEventCount() {
            return (int) metrics.latestEvents().stream()
                    .filter(event -> event.contains("upstream stream failure"))
                    .count();
        }

        @Override
        public void close() throws Exception {
            for (NetSocket socket : downstreamSockets) {
                socket.close();
            }
            for (NetSocket socket : upstreamSockets) {
                socket.close();
            }
            proxy.stop();
            downstreamClient.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            upstreamServer.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }

        private static AgentConfig config(Duration shutdownGrace) {
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
            AgentConfig.Selection selection = new AgentConfig.Selection() {
                @Override
                public AgentConfig.Policy policy() {
                    return AgentConfig.Policy.RR;
                }

                @Override
                public int maxAttempts() {
                    return 1;
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
            AgentConfig.Timeouts timeouts = new AgentConfig.Timeouts() {
                @Override
                public Duration connect() {
                    return Duration.ofSeconds(1);
                }

                @Override
                public Duration idle() {
                    return Duration.ofSeconds(30);
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
                    return () -> shutdownGrace;
                }
            };
        }

    }
}
