package com.cancache.agent.cluster.coordination;

import com.cancache.agent.cluster.ClusterState;
import com.cancache.agent.cluster.ConsistentHashRing;
import com.cancache.agent.cluster.HintedHandoffService;
import com.cancache.agent.cluster.Node;
import com.cancache.agent.codec.StringCodec;
import com.cancache.agent.config.AppProperties;
import com.cancache.agent.constants.NodeProtocol;
import com.cancache.agent.core.CacheEngine;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinationServiceTest
{
    @Test
    void shouldRetryHintReplayOnConfiguredReconciliationInterval() throws Exception
    {
        try (Fixture fixture = new Fixture(75L);
             TestReplicationPeer peer = new TestReplicationPeer("remote")) {
            fixture.discovery.update(Set.of(peer.nodeInfo()));
            fixture.service.start();

            await(() -> fixture.ring.nodes().size() == 2 && peer.streamRequests() == 1,
                    Duration.ofSeconds(3));
            // Let the initial join task finish before recording the hint so only
            // the periodic reconciliation can observe it.
            Thread.sleep(150L);
            fixture.hints.recordSet("remote", "key", "value", null);

            await(() -> fixture.hints.pendingFor("remote") == 0 && peer.setRequests() == 1,
                    Duration.ofSeconds(3));
            assertEquals(1, peer.setRequests());
        }
    }

    @Test
    void shouldKeepExistingMemberWhenAddressChangeHandshakeFails() throws Exception
    {
        try (Fixture fixture = new Fixture(5_000L);
             TestReplicationPeer peer = new TestReplicationPeer("remote")) {
            fixture.discovery.update(Set.of(peer.nodeInfo()));
            fixture.service.start();

            await(() -> fixture.ring.nodes().size() == 2 && peer.streamRequests() == 1,
                    Duration.ofSeconds(3));
            Node<String, String> original = fixture.ring.nodes().stream()
                    .filter(node -> node.id().equals("remote"))
                    .findFirst()
                    .orElseThrow();

            int unavailablePort = findUnusedPort();
            fixture.discovery.update(Set.of(
                    new NodeInfo("remote", InetAddress.getLoopbackAddress().getHostAddress(), unavailablePort)));

            Thread.sleep(300L);
            Node<String, String> routed = fixture.ring.nodes().stream()
                    .filter(node -> node.id().equals("remote"))
                    .findFirst()
                    .orElseThrow();
            assertSame(original, routed);
            assertEquals(2, fixture.ring.nodes().size());
        }
    }

    private static int findUnusedPort() throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private static void await(BooleanSupplier condition, Duration timeout) throws InterruptedException
    {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(condition.getAsBoolean(), "condition was not met before timeout");
    }

    private static AppProperties properties(long hintReplayIntervalMillis)
    {
        AppProperties.Replication replication = proxy(AppProperties.Replication.class, (method, returnType) ->
                switch (method) {
                    case "connectTimeoutMillis" -> 100;
                    case "bindHost", "advertiseHost" -> InetAddress.getLoopbackAddress().getHostAddress();
                    case "port" -> 0;
                    default -> defaultValue(returnType);
                });
        AppProperties.Coordination coordination = proxy(AppProperties.Coordination.class, (method, returnType) ->
                switch (method) {
                    case "hintReplayIntervalMillis" -> hintReplayIntervalMillis;
                    case "antiEntropyIntervalMillis" -> 30_000L;
                    case "maxHintsPerNode" -> 100;
                    default -> defaultValue(returnType);
                });
        AppProperties.Cluster cluster = proxy(AppProperties.Cluster.class, (method, returnType) ->
                switch (method) {
                    case "replication" -> replication;
                    case "coordination" -> coordination;
                    case "replicationFactor", "virtualNodes" -> 1;
                    default -> defaultValue(returnType);
                });
        AppProperties.Network network = proxy(AppProperties.Network.class,
                (method, returnType) -> defaultValue(returnType));
        return proxy(AppProperties.class, (method, returnType) ->
                switch (method) {
                    case "cluster" -> cluster;
                    case "network" -> network;
                    default -> defaultValue(returnType);
                });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, ProxyValue value)
    {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (instance, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "test-" + type.getSimpleName();
                            case "hashCode" -> System.identityHashCode(instance);
                            case "equals" -> instance == args[0];
                            default -> null;
                        };
                    }
                    return value.value(method.getName(), method.getReturnType());
                });
    }

    private static Object defaultValue(Class<?> type)
    {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) return false;
        if (type == char.class) return '\0';
        if (type == byte.class) return (byte) 0;
        if (type == short.class) return (short) 0;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == float.class) return 0F;
        if (type == double.class) return 0D;
        throw new IllegalArgumentException("Unsupported primitive " + type);
    }

    @FunctionalInterface
    private interface ProxyValue
    {
        Object value(String method, Class<?> returnType);
    }

    private static final class Fixture implements AutoCloseable
    {
        private final Vertx vertx = Vertx.vertx();
        private final CacheEngine<String, String> engine = CacheEngine
                .builder(StringCodec.UTF8, StringCodec.UTF8)
                .segments(1)
                .maxCapacity(100)
                .vertx(vertx)
                .build();
        private final Node<String, String> localNode = new EngineNode("local", engine);
        private final ConsistentHashRing<Node<String, String>> ring =
                new ConsistentHashRing<>(Arrays::hashCode, 1);
        private final HintedHandoffService hints = new HintedHandoffService(null, 100);
        private final TestDiscovery discovery = new TestDiscovery();
        private final CoordinationService service;

        private Fixture(long hintReplayIntervalMillis)
        {
            service = new CoordinationService(
                    ring,
                    localNode,
                    new ClusterState(localNode.id(), null),
                    hints,
                    engine,
                    properties(hintReplayIntervalMillis),
                    vertx,
                    discovery);
        }

        @Override
        public void close()
        {
            service.close();
            engine.close();
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    private record EngineNode(String id, CacheEngine<String, String> engine)
            implements Node<String, String>
    {
        @Override
        public boolean set(String key, String value, Duration ttl)
        {
            return engine.set(key, value, ttl);
        }

        @Override
        public String get(String key)
        {
            return engine.get(key);
        }

        @Override
        public boolean delete(String key)
        {
            return engine.delete(key);
        }

        @Override
        public boolean compareAndSwap(String key, String value, long expectedCas, Duration ttl)
        {
            return engine.compareAndSwap(key, value, expectedCas, ttl);
        }

        @Override
        public void clear()
        {
            engine.clear();
        }
    }

    private static final class TestDiscovery implements DiscoveryStrategy
    {
        private volatile Set<NodeInfo> nodes = Set.of();
        private volatile Consumer<Set<NodeInfo>> listener;

        @Override
        public void start(Consumer<Set<NodeInfo>> membershipListener)
        {
            listener = membershipListener;
            membershipListener.accept(nodes);
        }

        @Override
        public void announce()
        {
        }

        @Override
        public Set<NodeInfo> getDiscoveredNodes()
        {
            return nodes;
        }

        void update(Set<NodeInfo> newNodes)
        {
            nodes = Set.copyOf(newNodes);
            Consumer<Set<NodeInfo>> currentListener = listener;
            if (currentListener != null) {
                currentListener.accept(nodes);
            }
        }

        @Override
        public void close()
        {
        }
    }

    private static final class TestReplicationPeer implements AutoCloseable
    {
        private final String nodeId;
        private final ServerSocket server;
        private final ExecutorService executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("test-replication-peer-", 0).factory());
        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        private final AtomicInteger streamRequests = new AtomicInteger();
        private final AtomicInteger setRequests = new AtomicInteger();
        private volatile boolean running = true;

        private TestReplicationPeer(String nodeId) throws IOException
        {
            this.nodeId = nodeId;
            this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            executor.execute(this::acceptConnections);
        }

        NodeInfo nodeInfo()
        {
            return new NodeInfo(nodeId, server.getInetAddress().getHostAddress(), server.getLocalPort());
        }

        int streamRequests()
        {
            return streamRequests.get();
        }

        int setRequests()
        {
            return setRequests.get();
        }

        private void acceptConnections()
        {
            while (running) {
                try {
                    Socket socket = server.accept();
                    sockets.add(socket);
                    executor.execute(() -> handle(socket));
                } catch (IOException e) {
                    if (running) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        private void handle(Socket socket)
        {
            try (socket;
                 DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                 DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
                while (running) {
                    int command;
                    try {
                        command = in.readUnsignedByte();
                    } catch (EOFException e) {
                        return;
                    }
                    switch (command) {
                        case NodeProtocol.CMD_JOIN -> respondToJoin(in, out);
                        case NodeProtocol.CMD_STREAM -> {
                            streamRequests.incrementAndGet();
                            out.writeByte(NodeProtocol.STREAM_END_MARKER);
                            out.flush();
                        }
                        case NodeProtocol.CMD_SET -> {
                            readSet(in);
                            setRequests.incrementAndGet();
                            out.writeByte(NodeProtocol.RESP_TRUE);
                            out.flush();
                        }
                        default -> throw new IOException("Unexpected command " + command);
                    }
                }
            } catch (IOException ignored) {
                // Expected when the fixture closes pooled connections.
            } finally {
                sockets.remove(socket);
            }
        }

        private void respondToJoin(DataInputStream in, DataOutputStream out) throws IOException
        {
            int localIdLength = in.readInt();
            in.readNBytes(localIdLength);
            in.readLong();
            byte[] nodeIdBytes = nodeId.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeByte(NodeProtocol.RESP_ACCEPT);
            out.writeInt(nodeIdBytes.length);
            out.write(nodeIdBytes);
            out.writeLong(1L);
            out.flush();
        }

        private static void readSet(DataInputStream in) throws IOException
        {
            int keyLength = in.readInt();
            int valueLength = in.readInt();
            in.readLong();
            in.readNBytes(keyLength);
            in.readNBytes(valueLength);
        }

        @Override
        public void close() throws IOException
        {
            running = false;
            server.close();
            for (Socket socket : sockets) {
                socket.close();
            }
            executor.shutdownNow();
            try {
                executor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
