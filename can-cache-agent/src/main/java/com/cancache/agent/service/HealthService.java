package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamAddress;
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

@ApplicationScoped
@Startup
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

        checkAll();
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

    public void checkAll() {
        for (NodeStats node : registry.all()) {
            check(node);
        }
    }

    public void check(NodeStats node) {
        if (client == null) {
            return;
        }

        UpstreamAddress address = node.upstreamAddress();
        client.connect(address.port(), address.host())
                .onSuccess(socket -> {
                    socket.close();
                    transition(node, UpstreamState.UP, null);
                })
                .onFailure(err -> {
                    node.incError();
                    transition(node, UpstreamState.DOWN, err.getMessage());
                });
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
