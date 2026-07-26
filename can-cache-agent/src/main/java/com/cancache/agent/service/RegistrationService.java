package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.config.AgentConfigValidator;
import com.cancache.agent.model.NodeStats;
import io.quarkus.runtime.Startup;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
@Startup
public class RegistrationService {

    private static final Logger LOG = Logger.getLogger(RegistrationService.class);
    private static final int MAX_MESSAGE_LENGTH = 512;

    @Inject
    Vertx vertx;

    @Inject
    AgentConfig config;

    @Inject
    AgentConfigValidator configValidator;

    @Inject
    UpstreamRegistry registry;

    @Inject
    MetricsModel metrics;

    @Inject
    HealthService healthService;

    private final Set<RegistrationConnection> activeConnections = ConcurrentHashMap.newKeySet();
    private final AtomicInteger connectionCount = new AtomicInteger();
    private final AtomicBoolean listening = new AtomicBoolean(false);

    private NetServer server;
    private long cleanupTimerId = -1L;

    @PostConstruct
    void start() {
        if (!config.registration().enabled()) {
            LOG.info("registration server disabled");
            return;
        }

        server = vertx.createNetServer(new NetServerOptions()
                .setHost(config.registration().host())
                .setPort(config.registration().port())
                .setTcpNoDelay(true)
                .setReuseAddress(true));

        try {
            server.connectHandler(this::handleConnection)
                    .listen()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();
        } catch (CompletionException error) {
            throw new IllegalStateException("Registration listener could not bind to "
                    + config.registration().host() + ":" + config.registration().port(), rootCause(error));
        }

        listening.set(true);
        Duration cleanupInterval = config.registration().cleanupInterval();
        cleanupTimerId = vertx.setPeriodic(Math.max(1_000L, cleanupInterval.toMillis()),
                ignored -> registry.cleanupExpiredRegistrations());
        LOG.infof("registration server listening on %s:%d; authentication=%s",
                config.registration().host(), config.registration().port(),
                registrationToken().isBlank() ? "disabled" : "token");
    }

    private void handleConnection(NetSocket socket) {
        int current = connectionCount.incrementAndGet();
        if (current > config.registration().maxConnections()) {
            connectionCount.decrementAndGet();
            metrics.addEvent("[WARN] registration connection limit reached");
            writeAndClose(socket, error("BUSY", "Registration service is busy"));
            return;
        }

        RegistrationConnection connection = new RegistrationConnection(socket);
        activeConnections.add(connection);
        connection.start();
    }

    public boolean isListening() {
        return listening.get();
    }

    public boolean isOperational() {
        return !config.registration().enabled() || listening.get();
    }

    public int activeConnections() {
        return connectionCount.get();
    }

    int actualPort() {
        return server == null ? -1 : server.actualPort();
    }

    @PreDestroy
    void stop() {
        listening.set(false);
        if (cleanupTimerId != -1L) {
            vertx.cancelTimer(cleanupTimerId);
            cleanupTimerId = -1L;
        }
        for (RegistrationConnection connection : Set.copyOf(activeConnections)) {
            connection.close();
        }
        await(server == null ? null : server.close(), 1_000L);
    }

    private final class RegistrationConnection {

        private final NetSocket socket;
        private final StringBuilder payload = new StringBuilder(128);
        private final AtomicBoolean processed = new AtomicBoolean(false);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private long timeoutId = -1L;

        private RegistrationConnection(NetSocket socket) {
            this.socket = socket;
        }

        private void start() {
            timeoutId = vertx.setTimer(config.registration().readTimeout().toMillis(),
                    ignored -> respond(error("TIMEOUT", "Registration request timed out")));
            socket.handler(this::onBuffer);
            socket.exceptionHandler(failure -> {
                metrics.addEvent("[ERR ] registration io=" + errorMessage(failure));
                close();
            });
            socket.closeHandler(ignored -> onClosed());
        }

        private void onBuffer(Buffer buffer) {
            if (processed.get()) {
                return;
            }
            payload.append(buffer.toString(StandardCharsets.UTF_8));
            if (payload.length() > MAX_MESSAGE_LENGTH) {
                respond(error("MESSAGE_TOO_LARGE", "Registration request is too large"));
                return;
            }

            int newline = findNewline(payload);
            if (newline < 0) {
                return;
            }

            String trailing = payload.substring(newline + 1);
            if (!trailing.isBlank()) {
                respond(error("INVALID_REQUEST", "Only one registration command is allowed"));
                return;
            }

            String line = payload.substring(0, newline).strip();
            if (line.endsWith("\r")) {
                line = line.substring(0, line.length() - 1).strip();
            }
            handleLine(line);
        }

        private void handleLine(String line) {
            String[] parts = line.isBlank() ? new String[0] : line.split("\\s+");
            String configuredToken = registrationToken();
            boolean tokenRequired = !configuredToken.isBlank();
            int expectedParts = tokenRequired ? 4 : 3;
            if (parts.length != expectedParts || !"REGISTER".equalsIgnoreCase(parts[0])) {
                respond(error("INVALID_REQUEST", tokenRequired
                        ? "Expected REGISTER <host> <port> <token>"
                        : "Expected REGISTER <host> <port>"));
                return;
            }

            if (tokenRequired && !constantTimeEquals(configuredToken, parts[3])) {
                metrics.addEvent("[WARN] unauthorized registration attempt");
                respond(error("UNAUTHORIZED", "Registration token is invalid"));
                return;
            }

            int port;
            try {
                port = Integer.parseInt(parts[2]);
            } catch (NumberFormatException invalidPort) {
                respond(error("INVALID_PORT", "Registration port must be numeric"));
                return;
            }

            long ttlMillis = config.registration().ttl().toMillis();
            NodeStats node;
            try {
                node = registry.register(parts[1], port, ttlMillis, config.registration().maxNodes());
            } catch (UpstreamRegistry.RegistrationCapacityExceededException capacityReached) {
                metrics.addEvent("[WARN] registration node capacity reached");
                respond(error("CAPACITY", "Registration node capacity has been reached"));
                return;
            } catch (IllegalArgumentException invalidAddress) {
                respond(error("INVALID_ADDRESS", "Registration address is invalid"));
                return;
            }

            metrics.addEvent("[REG ] registered upstream=" + node.address());
            healthService.check(node);
            respond("OK REGISTERED " + node.address() + " lease-ms=" + ttlMillis + "\n");
        }

        private void respond(String response) {
            if (!processed.compareAndSet(false, true)) {
                return;
            }
            cancelTimeout();
            socket.write(response).onComplete(ignored -> socket.close());
        }

        private void close() {
            processed.set(true);
            cancelTimeout();
            socket.close();
            onClosed();
        }

        private void onClosed() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            cancelTimeout();
            activeConnections.remove(this);
            connectionCount.updateAndGet(current -> Math.max(0, current - 1));
        }

        private void cancelTimeout() {
            if (timeoutId != -1L) {
                vertx.cancelTimer(timeoutId);
                timeoutId = -1L;
            }
        }
    }

    private static int findNewline(StringBuilder payload) {
        for (int i = 0; i < payload.length(); i++) {
            if (payload.charAt(i) == '\n') {
                return i;
            }
        }
        return -1;
    }

    private String registrationToken() {
        return config.registration().token().orElse("");
    }

    private static boolean constantTimeEquals(String expected, String actual) {
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                actual.getBytes(StandardCharsets.UTF_8));
    }

    private static String error(String code, String message) {
        return "ERROR " + code + " " + message + "\n";
    }

    private static void writeAndClose(NetSocket socket, String response) {
        socket.write(response).onComplete(ignored -> socket.close());
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

    private static void await(Future<?> future, long timeoutMillis) {
        if (future == null) {
            return;
        }
        try {
            future.toCompletionStage().toCompletableFuture().get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception ignored) {
            // Shutdown remains best effort after active registration sockets have been closed.
        }
    }
}
