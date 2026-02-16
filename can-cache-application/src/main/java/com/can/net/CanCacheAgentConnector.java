package com.can.net;

import com.can.config.AppProperties;
import io.quarkus.runtime.Startup;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * İsteğe bağlı can-cache-agent erişilebilirlik denetleyicisidir.
 * Agent etkinleştirildiğinde belirli aralıklarla TCP bağlantısı açarak
 * erişilebilirlik durumunu izler ve bağlantı durum geçişlerini loglar.
 */
@Startup
@Singleton
public class CanCacheAgentConnector {
    private static final Logger LOG = Logger.getLogger(CanCacheAgentConnector.class);

    private final Vertx vertx;
    private final AppProperties.Agent agentConfig;
    private final AtomicBoolean healthy = new AtomicBoolean(false);
    private final AtomicBoolean probing = new AtomicBoolean(false);

    private NetClient netClient;
    private long timerId = -1L;

    @Inject
    public CanCacheAgentConnector(Vertx vertx, AppProperties properties) {
        this.vertx = Objects.requireNonNull(vertx, "vertx");
        this.agentConfig = Objects.requireNonNull(properties.agent(), "agentConfig");
    }

    @PostConstruct
    void start() {
        if (!agentConfig.enabled()) {
            LOG.info("can-cache-agent connector disabled (app.agent.enabled=false)");
            return;
        }

        Duration timeout = sanitizeDuration(agentConfig.connectTimeout(), Duration.ofSeconds(1));
        NetClientOptions options = new NetClientOptions()
                .setConnectTimeout((int) Math.min(Integer.MAX_VALUE, timeout.toMillis()))
                .setTcpKeepAlive(true)
                .setReconnectAttempts(0);
        netClient = vertx.createNetClient(options);

        Duration probeInterval = sanitizeDuration(agentConfig.probeInterval(), Duration.ofSeconds(5));
        long periodMillis = Math.max(250L, probeInterval.toMillis());
        timerId = vertx.setPeriodic(periodMillis, id -> probeAgent());
        probeAgent();

        LOG.infof("can-cache-agent connector enabled, probing %s:%d every %d ms",
                agentConfig.host(), agentConfig.port(), periodMillis);
    }

    private void probeAgent() {
        if (!agentConfig.enabled() || netClient == null || !probing.compareAndSet(false, true)) {
            return;
        }

        netClient.connect(agentConfig.port(), agentConfig.host())
                .onSuccess(socket -> {
                    socket.close();
                    onProbeResult(true, null);
                    probing.set(false);
                })
                .onFailure(error -> {
                    onProbeResult(false, error);
                    probing.set(false);
                });
    }

    private void onProbeResult(boolean isHealthy, Throwable error) {
        boolean previous = healthy.getAndSet(isHealthy);
        if (previous == isHealthy) {
            if (!isHealthy && LOG.isDebugEnabled()) {
                LOG.debugf(error, "can-cache-agent probe still failing for %s:%d",
                        agentConfig.host(), agentConfig.port());
            }
            return;
        }

        if (isHealthy) {
            LOG.infof("Connected to can-cache-agent at %s:%d", agentConfig.host(), agentConfig.port());
            return;
        }

        LOG.warnf(error, "Lost connectivity to can-cache-agent at %s:%d", agentConfig.host(), agentConfig.port());
    }

    private static Duration sanitizeDuration(Duration configured, Duration fallback) {
        if (configured == null || configured.isNegative() || configured.isZero()) {
            return fallback;
        }
        return configured;
    }

    @PreDestroy
    void stop() {
        if (timerId >= 0) {
            vertx.cancelTimer(timerId);
            timerId = -1L;
        }
        if (netClient != null) {
            netClient.close();
            netClient = null;
        }
    }
}
