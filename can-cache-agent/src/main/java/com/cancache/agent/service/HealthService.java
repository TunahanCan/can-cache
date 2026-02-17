package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamState;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

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
            String[] hostPort = node.address().split(":");
            client.connect(Integer.parseInt(hostPort[1]), hostPort[0])
                    .onSuccess(socket -> {
                        socket.close();
                        transition(node, UpstreamState.UP, null);
                    })
                    .onFailure(err -> {
                        node.incError();
                        transition(node, UpstreamState.DOWN, err.getMessage());
                    });
        }
    }

    private void transition(NodeStats node, UpstreamState next, String error) {
        UpstreamState prev = node.state();
        node.markCheck(error);
        node.state(next);
        if (prev != next) {
            String msg = "[HEALTH] " + node.address() + " " + next + (error == null ? "" : " (" + error + ")");
            metrics.addEvent(msg);
            LOG.infov("{0}", msg);
        }
    }
}
