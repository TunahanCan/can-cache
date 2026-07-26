package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.config.AgentConfigValidator;
import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamAddress;
import com.cancache.agent.model.UpstreamState;
import io.quarkus.runtime.Startup;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
@Startup
public class HealthService {

    private static final Logger LOG = Logger.getLogger(HealthService.class);
    private static final int DEFAULT_SUCCESS_THRESHOLD = 2;
    private static final int DEFAULT_FAILURE_THRESHOLD = 3;

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

    private NetClient client;
    private long timerId = -1;
    private final ConcurrentMap<String, ProbeState> probeStates = new ConcurrentHashMap<>();
    volatile Probe probe;
    private volatile boolean stopped;

    @PostConstruct
    void start() {
        stopped = false;
        client = vertx.createNetClient(new NetClientOptions()
                .setConnectTimeout((int) Math.min(Integer.MAX_VALUE,
                        Math.max(1L, config.health().connectTimeout().toMillis()))));
        probe = this::probeProtocol;

        checkAll();
        timerId = vertx.setPeriodic(config.health().interval().toMillis(), id -> checkAll());
    }

    @PreDestroy
    void stop() {
        stopped = true;
        if (timerId != -1) {
            vertx.cancelTimer(timerId);
        }
        probe = null;
        probeStates.values().forEach(ProbeState::invalidate);
        probeStates.clear();
        if (client != null) {
            client.close();
        }
    }

    public void checkAll() {
        ListSnapshot snapshot = snapshotManagedNodes();
        for (NodeStats node : snapshot.nodes()) {
            check(node);
        }
        probeStates.forEach((address, state) -> {
            if (!snapshot.addresses().contains(address)
                    && !registry.isManagedAddress(address)
                    && probeStates.remove(address, state)) {
                state.invalidate();
            }
        });
    }

    public void check(NodeStats node) {
        Probe activeProbe = probe;
        if (stopped || activeProbe == null || !registry.isManaged(node)) {
            return;
        }

        ProbeState state = probeStates.computeIfAbsent(node.address(), ignored -> new ProbeState());
        ProbeTicket ticket = state.tryStart(node);
        if (ticket == null) {
            return;
        }

        Future<Void> result;
        try {
            result = activeProbe.check(node.upstreamAddress());
            if (result == null) {
                result = Future.failedFuture("health probe returned no result");
            }
        } catch (Throwable err) {
            result = Future.failedFuture(err);
        }

        result.onComplete(outcome -> {
            if (stopped) {
                return;
            }
            ProbeCompletion completion = state.finishAndRecord(
                    ticket, outcome.succeeded(), successThreshold(), failureThreshold());
            if (!completion.accepted() || !registry.isManaged(node)) {
                return;
            }
            applyResult(node, outcome.succeeded(), outcome.cause(), completion.transition());
        });
    }

    void recordResult(NodeStats node, boolean success, Throwable failure) {
        if (stopped || !registry.isManaged(node)) {
            return;
        }

        ProbeState state = probeStates.computeIfAbsent(node.address(), ignored -> new ProbeState());
        StateTransition transition = state.record(node, success, successThreshold(), failureThreshold());
        applyResult(node, success, failure, transition);
    }

    private void applyResult(NodeStats node, boolean success, Throwable failure, StateTransition stateTransition) {
        if (!registry.isManaged(node)) {
            return;
        }
        String error = success ? null : errorMessage(failure);
        node.markCheck(error);
        if (!success) {
            node.incError();
        }
        if (stateTransition != null) {
            transition(node, stateTransition, error);
        }
    }

    private void transition(NodeStats node, StateTransition transition, String error) {
        if (!registry.transitionIfManaged(node, transition.expected(), transition.next())) {
            return;
        }
        if (transition.next() == UpstreamState.UP) {
            node.clearPassiveFailures();
        }
        if (transition.expected() != transition.next()) {
            String msg = "[HEALTH] " + node.address() + " " + transition.next()
                    + (error == null ? "" : " (" + error + ")");
            metrics.addEvent(msg);
            LOG.infov("{0}", msg);
        }
    }

    private ListSnapshot snapshotManagedNodes() {
        var nodes = registry.managed();
        Set<String> addresses = new HashSet<>();
        for (NodeStats node : nodes) {
            addresses.add(node.address());
        }
        return new ListSnapshot(nodes, Set.copyOf(addresses));
    }

    private static String errorMessage(Throwable failure) {
        if (failure == null) {
            return "unknown health probe failure";
        }
        String message = failure.getMessage();
        return message == null || message.isBlank()
                ? failure.getClass().getSimpleName()
                : oneLine(message);
    }

    private static String oneLine(String value) {
        String sanitized = value.replace('\n', ' ').replace('\r', ' ');
        return sanitized.length() <= 256 ? sanitized : sanitized.substring(0, 256);
    }

    private int successThreshold() {
        return config == null || config.health() == null
                ? DEFAULT_SUCCESS_THRESHOLD
                : Math.max(1, config.health().healthyThreshold());
    }

    private int failureThreshold() {
        return config == null || config.health() == null
                ? DEFAULT_FAILURE_THRESHOLD
                : Math.max(1, config.health().unhealthyThreshold());
    }

    private Future<Void> probeProtocol(UpstreamAddress address) {
        Promise<Void> result = Promise.promise();
        client.connect(address.port(), address.host())
                .onSuccess(socket -> new ProtocolProbeAttempt(socket, result).start())
                .onFailure(result::tryFail);
        return result.future();
    }

    private final class ProtocolProbeAttempt {

        private static final int MAX_RESPONSE_LENGTH = 256;

        private final NetSocket socket;
        private final Promise<Void> result;
        private final AtomicBoolean completed = new AtomicBoolean(false);
        private final StringBuilder response = new StringBuilder(64);
        private long timeoutId = -1L;

        private ProtocolProbeAttempt(NetSocket socket, Promise<Void> result) {
            this.socket = socket;
            this.result = result;
        }

        private void start() {
            timeoutId = vertx.setTimer(config.health().connectTimeout().toMillis(),
                    ignored -> finish(new IllegalStateException("health response timed out")));
            socket.handler(buffer -> {
                if (completed.get()) {
                    return;
                }
                response.append(buffer.toString(StandardCharsets.UTF_8));
                if (response.length() > MAX_RESPONSE_LENGTH) {
                    finish(new IllegalStateException("health response was too large"));
                    return;
                }
                int newline = response.indexOf("\n");
                if (newline < 0) {
                    return;
                }
                String line = response.substring(0, newline).strip();
                if (line.startsWith("VERSION ")) {
                    finish(null);
                } else {
                    finish(new IllegalStateException("unexpected health response"));
                }
            });
            socket.exceptionHandler(this::finish);
            socket.closeHandler(ignored -> {
                if (!completed.get()) {
                    finish(new IllegalStateException("upstream closed health connection"));
                }
            });
            socket.write(Buffer.buffer("version\r\n")).onFailure(this::finish);
        }

        private void finish(Throwable failure) {
            if (!completed.compareAndSet(false, true)) {
                return;
            }
            if (timeoutId != -1L) {
                vertx.cancelTimer(timeoutId);
                timeoutId = -1L;
            }
            socket.close();
            if (failure == null) {
                result.tryComplete();
            } else {
                result.tryFail(failure);
            }
        }
    }

    @FunctionalInterface
    interface Probe {
        Future<Void> check(UpstreamAddress address);
    }

    private record ListSnapshot(java.util.List<NodeStats> nodes, Set<String> addresses) {
    }

    private record ProbeTicket(NodeStats node, long generation) {
    }

    private record StateTransition(UpstreamState expected, UpstreamState next) {
    }

    private record ProbeCompletion(boolean accepted, StateTransition transition) {

        private static ProbeCompletion rejected() {
            return new ProbeCompletion(false, null);
        }
    }

    private static final class ProbeState {
        private NodeStats node;
        private long generation;
        private boolean inFlight;
        private int consecutiveSuccesses;
        private int consecutiveFailures;
        private UpstreamState observedState;

        synchronized ProbeTicket tryStart(NodeStats candidate) {
            resetFor(candidate);
            if (inFlight) {
                return null;
            }
            inFlight = true;
            return new ProbeTicket(candidate, generation);
        }

        synchronized ProbeCompletion finishAndRecord(
                ProbeTicket ticket,
                boolean success,
                int successThreshold,
                int failureThreshold) {
            if (!inFlight || node != ticket.node() || generation != ticket.generation()) {
                return ProbeCompletion.rejected();
            }
            inFlight = false;
            return new ProbeCompletion(true,
                    recordCurrent(success, successThreshold, failureThreshold));
        }

        synchronized StateTransition record(
                NodeStats candidate,
                boolean success,
                int successThreshold,
                int failureThreshold) {
            resetFor(candidate);
            return recordCurrent(success, successThreshold, failureThreshold);
        }

        private StateTransition recordCurrent(boolean success, int successThreshold, int failureThreshold) {
            UpstreamState currentState = node.state();
            if (observedState != currentState) {
                consecutiveSuccesses = 0;
                consecutiveFailures = 0;
                observedState = currentState;
            }
            if (success) {
                consecutiveSuccesses = Math.min(successThreshold, consecutiveSuccesses + 1);
                consecutiveFailures = 0;
                if (consecutiveSuccesses >= successThreshold) {
                    observedState = UpstreamState.UP;
                    return new StateTransition(currentState, UpstreamState.UP);
                }
                return null;
            }

            consecutiveFailures = Math.min(failureThreshold, consecutiveFailures + 1);
            consecutiveSuccesses = 0;
            if (consecutiveFailures >= failureThreshold) {
                observedState = UpstreamState.DOWN;
                return new StateTransition(currentState, UpstreamState.DOWN);
            }
            return null;
        }

        synchronized void invalidate() {
            generation++;
            node = null;
            inFlight = false;
            consecutiveSuccesses = 0;
            consecutiveFailures = 0;
            observedState = null;
        }

        private void resetFor(NodeStats candidate) {
            if (node == candidate) {
                return;
            }
            generation++;
            node = candidate;
            inFlight = false;
            consecutiveSuccesses = 0;
            consecutiveFailures = 0;
            observedState = candidate.state();
        }
    }
}
