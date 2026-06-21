package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import io.quarkus.runtime.Startup;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.dns.DnsClient;
import io.vertx.core.dns.DnsClientOptions;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
    UpstreamRegistry registry;

    @Inject
    MetricsModel metrics;

    private DnsClient dnsClient;
    private long timerId = -1;
    private final AtomicReference<List<String>> current = new AtomicReference<>(List.of());

    @PostConstruct
    void start() {
        if (!config.discovery().enabled() || config.discovery().dns().isBlank()) {
            LOG.infov("dns discovery disabled");
            return;
        }

        dnsClient = vertx.createDnsClient(new DnsClientOptions());
        refreshNow();
        timerId = vertx.setPeriodic(config.discovery().interval().toMillis(), id -> refreshNow());
    }

    @PreDestroy
    void stop() {
        if (timerId != -1) {
            vertx.cancelTimer(timerId);
        }
    }

    public Future<Void> refreshNowAsync() {
        if (dnsClient == null || !config.discovery().enabled() || config.discovery().dns().isBlank()) {
            return Future.succeededFuture();
        }

        return dnsClient.resolveA(config.discovery().dns())
                .onSuccess(this::apply)
                .recover(err -> {
                    String message = "[ERR ] discovery failed dns=" + config.discovery().dns() + " cause=" + err.getMessage();
                    LOG.warn(message);
                    metrics.addEvent(message);
                    return Future.succeededFuture(List.of());
                })
                .mapEmpty();
    }

    public void refreshNow() {
        if (dnsClient == null || !config.discovery().enabled() || config.discovery().dns().isBlank()) {
            return;
        }
        refreshNowAsync();
    }

    private void apply(List<String> addrs) {
        List<String> next = new ArrayList<>(addrs);
        next.sort(Comparator.naturalOrder());

        List<String> prev = current.getAndSet(next);
        if (!prev.equals(next)) {
            registry.replace(next, config.upstream().port());
            metrics.incDnsChanges();
            String msg = "[DISCOVERY] upstream list updated old=" + prev.size() + " new=" + next.size();
            metrics.addEvent(msg);
            LOG.infov("{0} entries={1}", msg, next);
        }
    }
}
