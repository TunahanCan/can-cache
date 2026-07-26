package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.config.AgentConfigValidator;
import io.quarkus.runtime.Startup;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.dns.DnsClient;
import io.vertx.core.dns.DnsClientOptions;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
@Startup
public class DiscoveryService {

    private static final Logger LOG = Logger.getLogger(DiscoveryService.class);

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

    private DnsClient dnsClient;
    private long timerId = -1;
    private final AtomicReference<List<String>> current = new AtomicReference<>(List.of());
    private Future<Void> refreshInFlight;
    private long refreshGeneration;
    AddressResolver resolver;

    @PostConstruct
    void start() {
        if (!config.discovery().enabled() || config.discovery().dns().isBlank()) {
            LOG.infov("dns discovery disabled");
            return;
        }

        dnsClient = vertx.createDnsClient(new DnsClientOptions());
        synchronized (this) {
            resolver = dnsClient::resolveA;
        }
        refreshNow();
        timerId = vertx.setPeriodic(config.discovery().interval().toMillis(), id -> refreshNow());
    }

    @PreDestroy
    void stop() {
        if (timerId != -1) {
            vertx.cancelTimer(timerId);
        }
        synchronized (this) {
            refreshGeneration++;
            resolver = null;
            refreshInFlight = null;
        }
    }

    public Future<Void> refreshNowAsync() {
        final AddressResolver activeResolver;
        final Promise<Void> completion;
        final long generation;
        synchronized (this) {
            if (resolver == null || !config.discovery().enabled() || config.discovery().dns().isBlank()) {
                return Future.succeededFuture();
            }
            if (refreshInFlight != null) {
                return refreshInFlight;
            }

            activeResolver = resolver;
            generation = ++refreshGeneration;
            completion = Promise.promise();
            refreshInFlight = completion.future();
        }

        Future<List<String>> lookup;
        try {
            lookup = activeResolver.resolve(config.discovery().dns());
            if (lookup == null) {
                lookup = Future.failedFuture("dns resolver returned no result");
            }
        } catch (Throwable err) {
            lookup = Future.failedFuture(err);
        }
        lookup.onComplete(result -> finishRefresh(generation, completion, result));
        return completion.future();
    }

    public void refreshNow() {
        refreshNowAsync();
    }

    private void finishRefresh(long generation, Promise<Void> completion, AsyncResult<List<String>> result) {
        boolean stale;
        String failureMessage = null;
        synchronized (this) {
            stale = generation != refreshGeneration;
            if (!stale) {
                try {
                    if (result.succeeded()) {
                        apply(result.result());
                    } else {
                        failureMessage = discoveryFailureMessage(result.cause());
                    }
                } catch (Throwable err) {
                    failureMessage = discoveryFailureMessage(err);
                } finally {
                    refreshInFlight = null;
                }
            }
        }

        if (!stale && failureMessage != null) {
            LOG.warn(failureMessage);
            metrics.addEvent(failureMessage);
        }
        completion.tryComplete();
    }

    private void apply(List<String> addrs) {
        if (addrs == null) {
            throw new IllegalStateException("DNS resolver returned a null address list");
        }
        List<String> next = List.copyOf(new TreeSet<>(addrs));

        List<String> prev = current.get();
        if (!prev.equals(next)) {
            registry.replace(next, config.upstream().port());
            current.set(next);
            metrics.incDnsChanges();
            String msg = "[DISCOVERY] upstream list updated old=" + prev.size() + " new=" + next.size();
            metrics.addEvent(msg);
            LOG.infov("{0} entries={1}", msg, next);
        }
    }

    private String discoveryFailureMessage(Throwable failure) {
        return "[ERR ] discovery failed dns=" + config.discovery().dns() + " cause=" + errorMessage(failure);
    }

    private static String errorMessage(Throwable failure) {
        if (failure == null) {
            return "unknown DNS failure";
        }
        String message = failure.getMessage();
        if (message == null || message.isBlank()) {
            return failure.getClass().getSimpleName();
        }
        String sanitized = message.replace('\n', ' ').replace('\r', ' ');
        return sanitized.length() <= 256 ? sanitized : sanitized.substring(0, 256);
    }

    @FunctionalInterface
    interface AddressResolver {
        Future<List<String>> resolve(String dnsName);
    }
}
