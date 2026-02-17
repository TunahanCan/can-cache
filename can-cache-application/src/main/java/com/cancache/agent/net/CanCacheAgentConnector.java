package com.cancache.agent.net;

import com.cancache.agent.config.AppProperties;
import io.quarkus.runtime.Startup;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
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
    private final AppProperties.Network networkConfig;
    private final AppProperties.Cluster clusterConfig;
    private final AtomicBoolean healthy = new AtomicBoolean(false);
    private final AtomicBoolean probing = new AtomicBoolean(false);
    private final AtomicBoolean registering = new AtomicBoolean(false);
    private final AtomicBoolean registrationHealthy = new AtomicBoolean(false);

    private NetClient netClient;
    private long timerId = -1L;

    @Inject
    public CanCacheAgentConnector(Vertx vertx, AppProperties properties) {
        this.vertx = Objects.requireNonNull(vertx, "vertx");
        this.agentConfig = Objects.requireNonNull(properties.agent(), "agentConfig");
        this.networkConfig = Objects.requireNonNull(properties.network(), "networkConfig");
        this.clusterConfig = Objects.requireNonNull(properties.cluster(), "clusterConfig");
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

        waitForAgentOnStartupIfRequired(timeout);

        Duration probeInterval = sanitizeDuration(agentConfig.probeInterval(), Duration.ofSeconds(5));
        long periodMillis = Math.max(250L, probeInterval.toMillis());
        timerId = vertx.setPeriodic(periodMillis, id -> {
            probeAgent();
            registerToAgent();
        });
        probeAgent();
        registerToAgent();

        LOG.infof("can-cache-agent connector enabled, probing %s:%d every %d ms",
                agentConfig.host(), agentConfig.port(), periodMillis);
    }

    private void waitForAgentOnStartupIfRequired(Duration singleAttemptTimeout) {
        if (!agentConfig.requiredOnStartup()) {
            return;
        }

        Duration startupWait = sanitizeNonNegativeDuration(agentConfig.startupWait(), Duration.ZERO);
        if (startupWait.isZero()) {
            if (!tryConnectOnce(singleAttemptTimeout)) {
                throw new IllegalStateException(String.format(
                        "can-cache-agent is required on startup but unreachable at %s:%d",
                        agentConfig.host(), agentConfig.port()));
            }
            LOG.infof("can-cache-agent was reachable during startup at %s:%d", agentConfig.host(), agentConfig.port());
            return;
        }

        long deadlineNanos = System.nanoTime() + startupWait.toNanos();
        Throwable lastError = null;
        do {
            try {
                if (tryConnectOnce(singleAttemptTimeout)) {
                    LOG.infof("can-cache-agent became reachable during startup at %s:%d", agentConfig.host(), agentConfig.port());
                    return;
                }
            } catch (RuntimeException e) {
                lastError = e;
            }

            long remainingMillis = Math.max(0L, (deadlineNanos - System.nanoTime()) / 1_000_000L);
            if (remainingMillis <= 0L) {
                break;
            }

            long pauseMillis = Math.min(Math.max(100L, singleAttemptTimeout.toMillis()), remainingMillis);
            try {
                Thread.sleep(pauseMillis);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for can-cache-agent to become reachable", interruptedException);
            }
        } while (System.nanoTime() < deadlineNanos);

        throw new IllegalStateException(String.format(
                "can-cache-agent is required on startup but unreachable at %s:%d after waiting %d ms",
                agentConfig.host(), agentConfig.port(), startupWait.toMillis()), lastError);
    }

    private boolean tryConnectOnce(Duration timeout) {
        if (netClient == null) {
            return false;
        }

        Promise<Boolean> promise = Promise.promise();
        long timeoutId = vertx.setTimer(Math.max(50L, timeout.toMillis()), id -> {
            if (!promise.future().isComplete()) {
                promise.tryComplete(false);
            }
        });

        netClient.connect(agentConfig.port(), agentConfig.host())
                .onSuccess(socket -> {
                    socket.close();
                    vertx.cancelTimer(timeoutId);
                    promise.tryComplete(true);
                })
                .onFailure(error -> {
                    vertx.cancelTimer(timeoutId);
                    promise.tryComplete(false);
                });

        return promise.future().toCompletionStage().toCompletableFuture().join();
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

    private void registerToAgent() {
        if (!agentConfig.enabled() || netClient == null || !registering.compareAndSet(false, true)) {
            return;
        }

        String advertisedHost = resolveAdvertisedHost();
        int servicePort = networkConfig.port();
        String registrationLine = "REGISTER " + advertisedHost + " " + servicePort + "\n";

        netClient.connect(agentConfig.registrationPort(), agentConfig.host())
                .onSuccess(socket -> socket.write(Buffer.buffer(registrationLine))
                        .onComplete(done -> {
                            socket.close();
                            if (done.succeeded()) {
                                onRegistrationResult(true, null, advertisedHost, servicePort);
                            } else {
                                onRegistrationResult(false, done.cause(), advertisedHost, servicePort);
                            }
                            registering.set(false);
                        }))
                .onFailure(error -> {
                    onRegistrationResult(false, error, advertisedHost, servicePort);
                    registering.set(false);
                });
    }

    private void onRegistrationResult(boolean success, Throwable error, String advertisedHost, int servicePort) {
        boolean previous = registrationHealthy.getAndSet(success);
        if (previous == success) {
            if (!success && LOG.isDebugEnabled()) {
                LOG.debugf(error, "can-cache-agent registration still failing via %s:%d for %s:%d",
                        agentConfig.host(), agentConfig.registrationPort(), advertisedHost, servicePort);
            }
            return;
        }

        if (success) {
            LOG.infof("Registered to can-cache-agent via %s:%d as %s:%d",
                    agentConfig.host(), agentConfig.registrationPort(), advertisedHost, servicePort);
            return;
        }

        LOG.warnf(error, "Lost can-cache-agent registration connectivity via %s:%d for %s:%d",
                agentConfig.host(), agentConfig.registrationPort(), advertisedHost, servicePort);
    }

    private String resolveAdvertisedHost() {
        String configured = agentConfig.advertisedHost();
        if (configured != null && !configured.isBlank()) {
            return configured.trim();
        }

        String replicationAdvertise = clusterConfig.replication().advertiseHost();
        if (replicationAdvertise != null && !replicationAdvertise.isBlank() && !"0.0.0.0".equals(replicationAdvertise)) {
            return replicationAdvertise.trim();
        }

        String networkHost = networkConfig.host();
        if (networkHost != null && !networkHost.isBlank() && !"0.0.0.0".equals(networkHost)) {
            return networkHost.trim();
        }

        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
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

    private static Duration sanitizeNonNegativeDuration(Duration configured, Duration fallback) {
        if (configured == null || configured.isNegative()) {
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
