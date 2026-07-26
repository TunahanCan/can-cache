package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.config.AgentConfigValidator;
import com.cancache.agent.model.ConnectionContext;
import com.cancache.agent.model.ConnectionRecord;
import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamAddress;
import io.quarkus.runtime.Startup;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
@Startup
public class TcpProxyServer {

    private static final Logger LOG = Logger.getLogger(TcpProxyServer.class);

    @Inject
    Vertx vertx;

    @Inject
    AgentConfig config;

    @Inject
    AgentConfigValidator configValidator;

    @Inject
    UpstreamRegistry registry;

    @Inject
    UpstreamSelector selector;

    @Inject
    MetricsModel metrics;

    @Inject
    ConnectionTracker tracker;

    private final Set<ProxySession> sessions = ConcurrentHashMap.newKeySet();
    private final AtomicInteger sessionCount = new AtomicInteger();
    private final AtomicInteger pendingDials = new AtomicInteger();
    private final AtomicBoolean accepting = new AtomicBoolean(false);
    private final AtomicBoolean listening = new AtomicBoolean(false);
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final Object drainMonitor = new Object();

    private NetServer server;
    private NetClient client;
    private long idleSweepTimerId = -1L;

    @PostConstruct
    void start() {
        client = vertx.createNetClient(new NetClientOptions()
                .setConnectTimeout(toIntMillis(config.timeouts().connect()))
                .setTcpNoDelay(true)
                .setTcpKeepAlive(true)
                .setReuseAddress(true));

        server = vertx.createNetServer(new NetServerOptions()
                .setHost(config.listen().host())
                .setPort(config.listen().port())
                .setTcpNoDelay(true)
                .setTcpKeepAlive(true)
                .setReuseAddress(true));

        try {
            server.connectHandler(this::handleClient)
                    .listen()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();
        } catch (CompletionException error) {
            closeQuietly(client);
            throw new IllegalStateException("Proxy listener could not bind to "
                    + config.listen().host() + ":" + config.listen().port(), rootCause(error));
        }

        listening.set(true);
        accepting.set(true);
        idleSweepTimerId = vertx.setPeriodic(idleSweepIntervalMillis(), ignored -> sweepIdleSessions());
        metrics.addEvent("[READY] proxy listening on " + config.listen().host() + ":" + config.listen().port());
        LOG.infof("proxy listening on %s:%d; maxConnections=%d, maxPending=%d",
                config.listen().host(), config.listen().port(),
                config.listen().maxConnections(), config.listen().maxPendingConnections());
    }

    private void handleClient(NetSocket downstream) {
        if (!accepting.get()) {
            reject(downstream, "agent is draining");
            return;
        }

        int currentSessions = sessionCount.incrementAndGet();
        if (currentSessions > config.listen().maxConnections()) {
            sessionCount.decrementAndGet();
            reject(downstream, "connection limit reached");
            return;
        }

        ProxySession session = new ProxySession(downstream);
        sessions.add(session);
        session.start();
    }

    private void reject(NetSocket downstream, String reason) {
        metrics.incRejectedConnections();
        metrics.addEvent("[WARN] connection rejected client=" + remoteAddress(downstream) + " reason=" + reason);
        downstream.close();
    }

    private void sweepIdleSessions() {
        long now = System.nanoTime();
        long idleNanos = config.timeouts().idle().toNanos();
        for (ProxySession session : sessions) {
            if (session.isIdle(now, idleNanos)) {
                metrics.incIdleTimeouts();
                metrics.addEvent("[WARN] idle connection closed client=" + session.clientAddress());
                session.close();
            }
        }
    }

    private long idleSweepIntervalMillis() {
        long halfIdle = Math.max(1_000L, config.timeouts().idle().toMillis() / 2L);
        return Math.min(5_000L, halfIdle);
    }

    public boolean isListening() {
        return listening.get();
    }

    public boolean isAccepting() {
        return accepting.get();
    }

    public boolean isReady() {
        return listening.get() && accepting.get() && registry.upCount() > 0;
    }

    public int activeSessions() {
        return sessionCount.get();
    }

    public int pendingConnections() {
        return pendingDials.get();
    }

    int actualPort() {
        return server == null ? -1 : server.actualPort();
    }

    public String lifecycleState() {
        if (stopping.get()) {
            return "DRAINING";
        }
        if (!listening.get()) {
            return "STARTING";
        }
        return isReady() ? "READY" : "DEGRADED";
    }

    @PreDestroy
    void stop() {
        if (!stopping.compareAndSet(false, true)) {
            return;
        }

        accepting.set(false);
        listening.set(false);
        metrics.addEvent("[INFO] proxy draining active=" + sessions.size());

        if (idleSweepTimerId != -1L) {
            vertx.cancelTimer(idleSweepTimerId);
            idleSweepTimerId = -1L;
        }

        long graceMillis = config.shutdown().grace().toMillis();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(graceMillis);

        for (ProxySession session : sessions) {
            session.beginDrain();
        }
        awaitSessions(deadlineNanos);

        if (!sessions.isEmpty()) {
            LOG.warnf("graceful shutdown timed out; force-closing %d connection(s)", sessions.size());
            for (ProxySession session : List.copyOf(sessions)) {
                session.close();
            }
            awaitSessions(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250L));
        }

        awaitFuture(server == null ? null : server.close(), Math.max(100L, remainingMillis(deadlineNanos)));
        awaitFuture(client == null ? null : client.close(), Math.max(100L, remainingMillis(deadlineNanos)));
        LOG.info("proxy stopped");
    }

    private void awaitSessions(long deadlineNanos) {
        synchronized (drainMonitor) {
            while (!sessions.isEmpty()) {
                long remainingMillis = remainingMillis(deadlineNanos);
                if (remainingMillis <= 0L) {
                    return;
                }
                try {
                    drainMonitor.wait(Math.min(remainingMillis, 100L));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void awaitFuture(Future<?> future, long timeoutMillis) {
        if (future == null || timeoutMillis <= 0L) {
            return;
        }
        try {
            future.toCompletionStage().toCompletableFuture().get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            LOG.debug("Timed out while closing a proxy resource");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (Exception error) {
            LOG.debug("Could not close a proxy resource cleanly", error);
        }
    }

    private long remainingMillis(long deadlineNanos) {
        return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
    }

    private final class ProxySession {

        private final NetSocket downstream;
        private final String clientAddress;
        private final Set<String> attemptedNodes = new HashSet<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicBoolean activeCounted = new AtomicBoolean(false);
        private final AtomicBoolean pendingCounted = new AtomicBoolean(false);
        private final AtomicBoolean passiveFailureRecorded = new AtomicBoolean(false);
        private final AtomicReference<NodeStats> reservation = new AtomicReference<>();

        private volatile SessionState state = SessionState.ACCEPTED;
        private volatile NetSocket upstream;
        private volatile NodeStats activeNode;
        private volatile ConnectionContext context;
        private volatile long lastActivityNanos = System.nanoTime();

        private ProxySession(NetSocket downstream) {
            this.downstream = downstream;
            this.clientAddress = remoteAddress(downstream);
            downstream.pause();
            downstream.closeHandler(ignored -> close());
            downstream.exceptionHandler(error -> {
                recordIoError("downstream", error);
                close();
            });
        }

        private void start() {
            if (!accepting.get()) {
                close();
                return;
            }

            int pending = pendingDials.incrementAndGet();
            pendingCounted.set(true);
            if (pending > config.listen().maxPendingConnections()) {
                metrics.incRejectedConnections();
                metrics.addEvent("[WARN] pending connection limit reached client=" + clientAddress);
                close();
                return;
            }

            state = SessionState.DIALING;
            dialNext();
        }

        private void dialNext() {
            if (closed.get()) {
                releasePendingCount();
                return;
            }

            List<NodeStats> candidates = registry.ready().stream()
                    .filter(node -> !attemptedNodes.contains(node.address()))
                    .toList();
            if (candidates.isEmpty() || attemptedNodes.size() >= config.selection().maxAttempts()) {
                if (attemptedNodes.isEmpty()) {
                    metrics.incRejectedConnections();
                }
                metrics.addEvent("[ERR ] no reachable upstream for client=" + clientAddress
                        + " attempts=" + attemptedNodes.size());
                close();
                return;
            }

            selector.select(candidates).ifPresentOrElse(this::dial, this::close);
        }

        private void dial(NodeStats node) {
            attemptedNodes.add(node.address());
            node.reservePendingConnection();
            reservation.set(node);

            UpstreamAddress address = node.upstreamAddress();
            client.connect(address.port(), address.host())
                    .onSuccess(socket -> onDialSuccess(node, socket))
                    .onFailure(error -> onDialFailure(node, error));
        }

        private synchronized void onDialSuccess(NodeStats node, NetSocket socket) {
            releaseReservation(node);
            if (closed.get() || stopping.get()) {
                socket.close();
                close();
                return;
            }

            releasePendingCount();
            upstream = socket;
            activeNode = node;
            context = new ConnectionContext(clientAddress, node.address());
            lastActivityNanos = System.nanoTime();
            state = SessionState.STREAMING;
            node.incActiveConn();
            activeCounted.set(true);
            metrics.incActiveConnections();
            metrics.addEvent("[CONN] client=" + clientAddress + " -> upstream=" + node.address());

            setupForwarding(socket, node);
            downstream.resume();
        }

        private void onDialFailure(NodeStats node, Throwable error) {
            releaseReservation(node);
            if (closed.get()) {
                releasePendingCount();
                return;
            }

            node.incError();
            metrics.incDialFailures();
            boolean ejected = node.recordPassiveFailure(config.health().passiveFailureThreshold());
            metrics.addEvent("[ERR ] dial failed upstream=" + node.address() + " cause=" + errorMessage(error)
                    + (ejected ? " ejected=true" : ""));

            boolean hasAlternative = attemptedNodes.size() < config.selection().maxAttempts()
                    && registry.ready().stream().anyMatch(candidate -> !attemptedNodes.contains(candidate.address()));
            if (hasAlternative) {
                metrics.incFailovers();
                vertx.runOnContext(ignored -> dialNext());
            } else {
                close();
            }
        }

        private void setupForwarding(NetSocket upstreamSocket, NodeStats node) {
            int queueSize = config.listen().writeQueueMaxBytes();
            downstream.setWriteQueueMaxSize(queueSize);
            upstreamSocket.setWriteQueueMaxSize(queueSize);

            // A clean upstream EOF is valid protocol behavior for commands such as
            // memcached "quit". Resets surface through the exception/write paths.
            upstreamSocket.closeHandler(ignored -> close());
            upstreamSocket.exceptionHandler(error -> {
                recordIoError("upstream", error);
                recordPassiveFailure(node, errorMessage(error));
                close();
            });

            downstream.handler(buffer -> forward(buffer, upstreamSocket, downstream, true, node));
            upstreamSocket.handler(buffer -> forward(buffer, downstream, upstreamSocket, false, node));
        }

        private void forward(Buffer buffer, NetSocket target, NetSocket source, boolean inbound, NodeStats node) {
            if (closed.get()) {
                return;
            }

            lastActivityNanos = System.nanoTime();
            int length = buffer.length();
            target.write(buffer, result -> {
                if (result.failed()) {
                    String targetSide = inbound ? "upstream" : "downstream";
                    if (inbound) {
                        node.incError();
                        recordPassiveFailure(node, errorMessage(result.cause()));
                    }
                    metrics.addEvent("[ERR ] proxy write failed target=" + targetSide
                            + " node=" + node.address()
                            + " cause=" + errorMessage(result.cause()));
                    close();
                    return;
                }
                if (inbound) {
                    context.addBytesIn(length);
                    node.addBytesIn(length);
                    metrics.addBytesIn(length);
                } else {
                    context.addBytesOut(length);
                    node.addBytesOut(length);
                    metrics.addBytesOut(length);
                    if (!closed.get() && state == SessionState.STREAMING) {
                        node.clearPassiveFailures();
                    }
                }
            });

            if (target.writeQueueFull()) {
                source.pause();
                target.drainHandler(ignored -> {
                    target.drainHandler(null);
                    if (!closed.get() && (state == SessionState.STREAMING || state == SessionState.DRAINING)) {
                        source.resume();
                    }
                });
            }
        }

        private boolean isIdle(long nowNanos, long idleNanos) {
            return state == SessionState.STREAMING && nowNanos - lastActivityNanos >= idleNanos;
        }

        private String clientAddress() {
            return clientAddress;
        }

        private synchronized void beginDrain() {
            if (state != SessionState.STREAMING) {
                close();
            } else {
                state = SessionState.DRAINING;
            }
        }

        private void recordIoError(String side, Throwable error) {
            NodeStats node = activeNode;
            if (node != null) {
                node.incError();
            }
            metrics.addEvent("[ERR ] " + side + " io client=" + clientAddress + " cause=" + errorMessage(error));
        }

        private void recordPassiveFailure(NodeStats node, String reason) {
            if (state != SessionState.STREAMING
                    || closed.get()
                    || !passiveFailureRecorded.compareAndSet(false, true)) {
                return;
            }
            boolean ejected = node.recordPassiveFailure(config.health().passiveFailureThreshold());
            metrics.addEvent("[WARN] upstream stream failure node=" + node.address()
                    + " cause=" + reason
                    + (ejected ? " ejected=true" : ""));
        }

        private void releaseReservation(NodeStats expected) {
            if (reservation.compareAndSet(expected, null)) {
                expected.releasePendingConnection();
            }
        }

        private void releasePendingCount() {
            if (pendingCounted.compareAndSet(true, false)) {
                pendingDials.updateAndGet(current -> Math.max(0, current - 1));
            }
        }

        private synchronized void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            state = SessionState.CLOSED;
            NodeStats reserved = reservation.getAndSet(null);
            if (reserved != null) {
                reserved.releasePendingConnection();
            }
            releasePendingCount();

            NetSocket upstreamSocket = upstream;
            if (upstreamSocket != null) {
                upstreamSocket.close();
            }
            downstream.close();

            NodeStats node = activeNode;
            ConnectionContext connectionContext = context;
            if (activeCounted.compareAndSet(true, false) && node != null) {
                node.decActiveConn();
                metrics.decActiveConnections();
            }
            registry.pruneOrphans();

            if (connectionContext != null) {
                tracker.add(new ConnectionRecord(
                        connectionContext.startTime(),
                        Instant.now(),
                        connectionContext.clientAddr(),
                        connectionContext.upstreamAddr(),
                        connectionContext.bytesIn(),
                        connectionContext.bytesOut()));
            }

            sessions.remove(this);
            sessionCount.updateAndGet(current -> Math.max(0, current - 1));
            synchronized (drainMonitor) {
                drainMonitor.notifyAll();
            }
        }
    }

    private enum SessionState {
        ACCEPTED,
        DIALING,
        STREAMING,
        DRAINING,
        CLOSED
    }

    private static int toIntMillis(Duration duration) {
        return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, duration.toMillis()));
    }

    private static String remoteAddress(NetSocket socket) {
        return socket.remoteAddress() == null ? "unknown" : socket.remoteAddress().toString();
    }

    private static String errorMessage(Throwable error) {
        if (error == null || error.getMessage() == null || error.getMessage().isBlank()) {
            return error == null ? "unknown" : error.getClass().getSimpleName();
        }
        String sanitized = error.getMessage().replace('\n', ' ').replace('\r', ' ');
        return sanitized.length() <= 256 ? sanitized : sanitized.substring(0, 256);
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static void closeQuietly(NetClient client) {
        if (client != null) {
            try {
                client.close();
            } catch (RuntimeException ignored) {
                // Startup is already failing; preserve the bind error.
            }
        }
    }
}
