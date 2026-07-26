package com.cancache.agent.health;

import com.cancache.agent.service.RegistrationService;
import com.cancache.agent.service.TcpProxyServer;
import com.cancache.agent.service.UpstreamRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

@Readiness
@ApplicationScoped
public class AgentReadinessCheck implements HealthCheck {

    @Inject
    TcpProxyServer proxyServer;

    @Inject
    RegistrationService registrationService;

    @Inject
    UpstreamRegistry registry;

    @Override
    public HealthCheckResponse call() {
        boolean ready = proxyServer.isReady() && registrationService.isOperational();
        return HealthCheckResponse.named("can-cache-agent-ready")
                .status(ready)
                .withData("state", proxyServer.lifecycleState())
                .withData("readyUpstreams", registry.upCount())
                .withData("totalUpstreams", registry.total())
                .withData("pendingConnections", proxyServer.pendingConnections())
                .build();
    }
}
