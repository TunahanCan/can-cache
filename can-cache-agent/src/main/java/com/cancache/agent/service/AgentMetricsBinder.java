package com.cancache.agent.service;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@Startup
@ApplicationScoped
public class AgentMetricsBinder {

    @Inject
    MeterRegistry meterRegistry;

    @Inject
    MetricsModel metrics;

    @Inject
    TcpProxyServer proxyServer;

    @Inject
    UpstreamRegistry upstreamRegistry;

    @PostConstruct
    void bind() {
        Gauge.builder("can.cache.agent.connections.active", metrics, MetricsModel::activeConnections)
                .description("Currently active proxied connections")
                .register(meterRegistry);
        Gauge.builder("can.cache.agent.connections.pending", proxyServer, TcpProxyServer::pendingConnections)
                .description("Connections waiting for an upstream dial")
                .register(meterRegistry);
        Gauge.builder("can.cache.agent.upstreams.total", upstreamRegistry, UpstreamRegistry::total)
                .description("Known upstream nodes")
                .register(meterRegistry);
        Gauge.builder("can.cache.agent.upstreams.ready", upstreamRegistry, UpstreamRegistry::upCount)
                .description("Ready upstream nodes")
                .register(meterRegistry);

        functionCounter("can.cache.agent.connections", "accepted", MetricsModel::totalConnections);
        functionCounter("can.cache.agent.connections", "rejected", MetricsModel::rejectedConnections);
        functionCounter("can.cache.agent.dials", "failed", MetricsModel::dialFailures);
        functionCounter("can.cache.agent.failovers", "attempted", MetricsModel::failovers);
        functionCounter("can.cache.agent.idle.timeouts", "closed", MetricsModel::idleTimeouts);
        functionCounter("can.cache.agent.traffic.bytes", "received", MetricsModel::bytesIn);
        functionCounter("can.cache.agent.traffic.bytes", "sent", MetricsModel::bytesOut);
    }

    private void functionCounter(String name, String result, java.util.function.ToDoubleFunction<MetricsModel> value) {
        FunctionCounter.builder(name, metrics, value)
                .tag("result", result)
                .register(meterRegistry);
    }
}
