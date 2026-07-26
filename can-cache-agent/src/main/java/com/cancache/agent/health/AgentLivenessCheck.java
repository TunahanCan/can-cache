package com.cancache.agent.health;

import com.cancache.agent.service.TcpProxyServer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;

@Liveness
@ApplicationScoped
public class AgentLivenessCheck implements HealthCheck {

    @Inject
    TcpProxyServer proxyServer;

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("can-cache-agent-listener")
                .status(proxyServer.isListening())
                .withData("state", proxyServer.lifecycleState())
                .build();
    }
}
