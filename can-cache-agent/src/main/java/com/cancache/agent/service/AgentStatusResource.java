package com.cancache.agent.service;

import com.cancache.agent.model.ConnectionRecord;
import com.cancache.agent.model.NodeStats;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Path("/agent")
@Produces(MediaType.APPLICATION_JSON)
public class AgentStatusResource {

    @Inject
    UpstreamRegistry registry;

    @Inject
    MetricsModel metrics;

    @Inject
    ConnectionTracker tracker;

    @GET
    @Path("/instances")
    public AgentStatusResponse status() {
        List<InstanceStatus> instances = registry.all().stream()
                .map(this::toInstance)
                .toList();

        List<ConnectionSummary> recentConnections = tracker.latest().stream()
                .map(this::toConnection)
                .toList();

        return new AgentStatusResponse(
                Instant.now(),
                metrics.startedAt(),
                Duration.between(metrics.startedAt(), Instant.now()).toSeconds(),
                registry.total(),
                registry.upCount(),
                metrics.activeConnections(),
                metrics.bytesIn(),
                metrics.bytesOut(),
                metrics.dnsChanges(),
                metrics.latestEvents(),
                instances,
                recentConnections
        );
    }

    private InstanceStatus toInstance(NodeStats node) {
        return new InstanceStatus(
                node.address(),
                node.state().name(),
                node.activeConn(),
                node.totalConn(),
                node.bytesIn(),
                node.bytesOut(),
                node.errorCount(),
                node.lastCheck(),
                node.lastCheckAge().toSeconds(),
                node.lastError()
        );
    }

    private ConnectionSummary toConnection(ConnectionRecord record) {
        return new ConnectionSummary(
                record.client(),
                record.upstream(),
                record.start(),
                record.end(),
                record.duration().toMillis(),
                record.bytesIn(),
                record.bytesOut()
        );
    }

    public record AgentStatusResponse(
            Instant now,
            Instant startedAt,
            long uptimeSeconds,
            int totalInstances,
            int healthyInstances,
            int activeConnections,
            long bytesIn,
            long bytesOut,
            long dnsChanges,
            List<String> latestEvents,
            List<InstanceStatus> instances,
            List<ConnectionSummary> recentConnections
    ) {
    }

    public record InstanceStatus(
            String address,
            String state,
            int activeConnections,
            long totalConnections,
            long bytesIn,
            long bytesOut,
            long errorCount,
            Instant lastCheck,
            long lastCheckAgeSeconds,
            String lastError
    ) {
    }

    public record ConnectionSummary(
            String client,
            String upstream,
            Instant start,
            Instant end,
            long durationMs,
            long bytesIn,
            long bytesOut
    ) {
    }
}
