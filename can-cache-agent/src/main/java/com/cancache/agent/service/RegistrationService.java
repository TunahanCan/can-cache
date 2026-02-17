package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
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
import java.util.regex.Pattern;

@ApplicationScoped
public class RegistrationService {

    private static final Logger LOG = Logger.getLogger(RegistrationService.class);
    private static final Pattern IPV4 = Pattern.compile("^[a-zA-Z0-9_.:-]+$");

    @Inject
    Vertx vertx;

    @Inject
    AgentConfig config;

    @Inject
    UpstreamRegistry registry;

    @Inject
    MetricsModel metrics;

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
        socket.handler(buffer -> processPayload(socket, buffer));
        socket.exceptionHandler(err -> {
            metrics.addEvent("[ERR ] registration io=" + err.getMessage());
            socket.close();
        });
    }

    private void processPayload(NetSocket socket, Buffer buffer) {
        String[] lines = buffer.toString().split("\\r?\\n");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            handleLine(trimmed);
        }
        socket.close();
    }

    private void handleLine(String line) {
        String[] parts = line.split("\\s+");
        if (parts.length < 3 || !"REGISTER".equalsIgnoreCase(parts[0])) {
            metrics.addEvent("[ERR ] invalid registration message=" + line);
            return;
        }

        String host = parts[1];
        if (!IPV4.matcher(host).matches()) {
            metrics.addEvent("[ERR ] invalid registration host=" + host);
            return;
        }

        int port;
        try {
            port = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            metrics.addEvent("[ERR ] invalid registration port=" + parts[2]);
            return;
        }

        long ttlMillis = config.registration().ttl().toMillis();
        registry.register(host, port, ttlMillis);
        metrics.addEvent("[REG ] registered upstream=" + host + ":" + port);
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
