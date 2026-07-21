package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamState;
import io.quarkus.runtime.Startup;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Startup
@ApplicationScoped
public class HealthService {

    private static final Logger LOG = Logger.getLogger(HealthService.class);

    @Inject
    Vertx vertx;

    @Inject
    AgentConfig config;

    @Inject
    UpstreamRegistry registry;

    @Inject
    MetricsModel metrics;

    private NetClient client;
    private long timerId = -1;
    private final Set<String> checksInFlight = ConcurrentHashMap.newKeySet();

    @PostConstruct
    void start() {
        client = vertx.createNetClient(new NetClientOptions()
                .setConnectTimeout((int) config.health().connectTimeout().toMillis()));

        timerId = vertx.setPeriodic(config.health().interval().toMillis(), id -> checkAll());
    }

    @PreDestroy
    void stop() {
        if (timerId != -1) {
            vertx.cancelTimer(timerId);
        }
        if (client != null) {
            client.close();
        }
    }

    private void checkAll() {
        for (NodeStats node : registry.all()) {
            if (!checksInFlight.add(node.address())) {
                continue;
            }
            HostPort hostPort;
            try {
                hostPort = parseAddress(node.address());
            } catch (RuntimeException error) {
                node.incError();
                transition(node, UpstreamState.DOWN, error.getMessage(), 0L);
                checksInFlight.remove(node.address());
                continue;
            }
            long startedAt = System.nanoTime();
            client.connect(hostPort.port(), hostPort.host())
                    .onSuccess(socket -> {
                        socket.close();
                        transition(node, UpstreamState.UP, null, elapsedMillis(startedAt));
                    })
                    .onFailure(err -> {
                        node.incError();
                        transition(node, UpstreamState.DOWN, err.getMessage(), elapsedMillis(startedAt));
                    })
                    .onComplete(ignored -> checksInFlight.remove(node.address()));
        }
    }

    private long elapsedMillis(long startedAt) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
    }

    private HostPort parseAddress(String address) {
        int separator = address.lastIndexOf(':');
        if (separator <= 0 || separator == address.length() - 1) {
            throw new IllegalArgumentException("Invalid upstream address: " + address);
        }
        String host = address.substring(0, separator);
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        return new HostPort(host, Integer.parseInt(address.substring(separator + 1)));
    }

    private void transition(NodeStats node, UpstreamState next, String error, long latencyMillis) {
        UpstreamState prev = node.state();
        node.recordHealthCheck(next, error, latencyMillis);
        if (prev != next) {
            String msg = "[HEALTH] " + node.address() + " " + next + (error == null ? "" : " (" + error + ")");
            metrics.addEvent(msg);
            LOG.infov("{0}", msg);
        }
    }

    private record HostPort(String host, int port) {
    }
}
