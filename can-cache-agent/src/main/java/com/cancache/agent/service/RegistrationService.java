package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.model.NodeStats;
import io.quarkus.runtime.Startup;
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

import java.time.Duration;

@ApplicationScoped
@Startup
public class RegistrationService {

    private static final Logger LOG = Logger.getLogger(RegistrationService.class);

    @Inject
    Vertx vertx;

    @Inject
    AgentConfig config;

    @Inject
    UpstreamRegistry registry;

    @Inject
    MetricsModel metrics;

    @Inject
    HealthService healthService;

    private NetServer server;
    private long cleanupTimerId = -1L;

    @PostConstruct
    void start() {
        if (!config.registration().enabled()) {
            LOG.infov("registration server disabled");
            return;
        }

        server = vertx.createNetServer(new NetServerOptions()
                .setHost(config.registration().host())
                .setPort(config.registration().port()));

        server.connectHandler(this::handleConnection)
                .listen()
                .onSuccess(ok -> LOG.infov("registration server listening on {0}:{1}",
                        config.registration().host(), config.registration().port()))
                .onFailure(err -> LOG.error("registration server start failed", err));

        Duration cleanupInterval = config.registration().cleanupInterval();
        cleanupTimerId = vertx.setPeriodic(Math.max(1000, cleanupInterval.toMillis()), id -> registry.cleanupExpiredRegistrations());
    }

    private void handleConnection(NetSocket socket) {
        StringBuilder payload = new StringBuilder(128);
        socket.handler(buffer -> processPayload(socket, buffer, payload));
        socket.exceptionHandler(err -> {
            metrics.addEvent("[ERR ] registration io=" + err.getMessage());
            socket.close();
        });
    }

    private void processPayload(NetSocket socket, Buffer buffer, StringBuilder payload) {
        payload.append(buffer.toString());

        int newlineIndex;
        boolean processed = false;
        while ((newlineIndex = findNewline(payload)) >= 0) {
            String line = payload.substring(0, newlineIndex).trim();
            payload.delete(0, newlineIndex + 1);
            if (!line.isEmpty()) {
                handleLine(line);
                processed = true;
            }
        }

        if (processed) {
            socket.close();
            return;
        }

        if (payload.length() > 256) {
            metrics.addEvent("[ERR ] registration message too large");
            socket.close();
        }
    }

    private void handleLine(String line) {
        String[] parts = line.split("\\s+");
        if (parts.length < 3 || !"REGISTER".equalsIgnoreCase(parts[0])) {
            metrics.addEvent("[ERR ] invalid registration message=" + line);
            return;
        }

        String host = parts[1];
        int port;
        try {
            port = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            metrics.addEvent("[ERR ] invalid registration port=" + parts[2]);
            return;
        }

        long ttlMillis = config.registration().ttl().toMillis();
        NodeStats node;
        try {
            node = registry.register(host, port, ttlMillis);
        } catch (IllegalArgumentException e) {
            metrics.addEvent("[ERR ] invalid registration address=" + host + ":" + port);
            return;
        }
        metrics.addEvent("[REG ] registered upstream=" + host + ":" + port);
        healthService.check(node);
    }

    private int findNewline(StringBuilder payload) {
        for (int i = 0; i < payload.length(); i++) {
            if (payload.charAt(i) == '\n') {
                return i;
            }
        }
        return -1;
    }

    @PreDestroy
    void stop() {
        if (cleanupTimerId != -1L) {
            vertx.cancelTimer(cleanupTimerId);
        }
        if (server != null) {
            server.close();
        }
    }
}
