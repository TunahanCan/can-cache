package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistrationServiceTest {

    @Test
    void registrationProtocolShouldAuthenticateAndAcknowledgeAcceptedLeases() throws Exception {
        Vertx vertx = Vertx.vertx();
        NetClient client = vertx.createNetClient();
        AgentConfig config = config();
        UpstreamRegistry registry = new UpstreamRegistry();
        MetricsModel metrics = new MetricsModel();
        HealthService healthService = new HealthService();
        healthService.config = config;
        healthService.registry = registry;
        healthService.metrics = metrics;
        healthService.probe = ignored -> Future.succeededFuture();

        RegistrationService service = new RegistrationService();
        service.vertx = vertx;
        service.config = config;
        service.registry = registry;
        service.metrics = metrics;
        service.healthService = healthService;

        try {
            service.start();

            String unauthorized = request(client, service.actualPort(),
                    "REGISTER cache-node 11212 wrong-token\n");
            assertTrue(unauthorized.startsWith("ERROR UNAUTHORIZED "));
            assertEquals(0, registry.total());

            String accepted = request(client, service.actualPort(),
                    "REGISTER cache-node 11212 shared-token\n");
            assertEquals("OK REGISTERED cache-node:11212 lease-ms=15000", accepted);
            assertEquals(1, registry.total());

            String capacityReached = request(client, service.actualPort(),
                    "REGISTER another-node 11213 shared-token\n");
            assertTrue(capacityReached.startsWith("ERROR CAPACITY "));
            assertEquals(1, registry.total());
        } finally {
            service.stop();
            healthService.stop();
            client.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    private static String request(NetClient client, int port, String command) throws Exception {
        CompletableFuture<String> response = new CompletableFuture<>();
        client.connect(port, "127.0.0.1")
                .onSuccess(socket -> {
                    StringBuilder payload = new StringBuilder();
                    socket.handler(buffer -> {
                        payload.append(buffer);
                        int newline = payload.indexOf("\n");
                        if (newline >= 0) {
                            response.complete(payload.substring(0, newline).strip());
                        }
                    });
                    socket.exceptionHandler(response::completeExceptionally);
                    socket.write(command).onFailure(response::completeExceptionally);
                })
                .onFailure(response::completeExceptionally);
        return response.get(5, TimeUnit.SECONDS);
    }

    private static AgentConfig config() {
        AgentConfig.Registration registration = new AgentConfig.Registration() {
            @Override
            public boolean enabled() {
                return true;
            }

            @Override
            public String host() {
                return "127.0.0.1";
            }

            @Override
            public int port() {
                return 0;
            }

            @Override
            public Duration ttl() {
                return Duration.ofSeconds(15);
            }

            @Override
            public Duration cleanupInterval() {
                return Duration.ofSeconds(2);
            }

            @Override
            public Duration readTimeout() {
                return Duration.ofSeconds(1);
            }

            @Override
            public int maxConnections() {
                return 8;
            }

            @Override
            public int maxNodes() {
                return 1;
            }

            @Override
            public Optional<String> token() {
                return Optional.of("shared-token");
            }
        };
        AgentConfig.Health health = new AgentConfig.Health() {
            @Override
            public Duration interval() {
                return Duration.ofSeconds(2);
            }

            @Override
            public Duration connectTimeout() {
                return Duration.ofSeconds(1);
            }

            @Override
            public int healthyThreshold() {
                return 1;
            }

            @Override
            public int unhealthyThreshold() {
                return 1;
            }

            @Override
            public int passiveFailureThreshold() {
                return 2;
            }
        };

        return new AgentConfig() {
            @Override
            public Listen listen() {
                return null;
            }

            @Override
            public Discovery discovery() {
                return null;
            }

            @Override
            public Upstream upstream() {
                return null;
            }

            @Override
            public Health health() {
                return health;
            }

            @Override
            public Selection selection() {
                return null;
            }

            @Override
            public Timeouts timeouts() {
                return null;
            }

            @Override
            public Registration registration() {
                return registration;
            }

            @Override
            public Dashboard dashboard() {
                return null;
            }

            @Override
            public Shutdown shutdown() {
                return null;
            }
        };
    }
}
