package com.cancache.agent.net;

import com.cancache.agent.config.AppProperties;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetServer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CanCacheAgentConnectorRegistrationTest {

    @Test
    void shouldSendTokenAndWaitForRegistrationAcknowledgement() throws Exception {
        Vertx vertx = Vertx.vertx();
        CompletableFuture<String> command = new CompletableFuture<>();
        CompletableFuture<Void> acknowledgedConnectionClosed = new CompletableFuture<>();
        NetServer fakeAgent = vertx.createNetServer();

        fakeAgent.connectHandler(socket -> {
            StringBuilder request = new StringBuilder();
            socket.handler(buffer -> {
                request.append(buffer);
                int newline = request.indexOf("\n");
                if (newline < 0 || command.isDone()) {
                    return;
                }

                String line = request.substring(0, newline + 1);
                command.complete(line);
                socket.closeHandler(ignored -> acknowledgedConnectionClosed.complete(null));
                socket.write("OK REGISTERED cache-node:11212 lease-ms=15000\n");
            });
        });

        CanCacheAgentConnector connector = null;
        try {
            int port = fakeAgent.listen(0, "127.0.0.1")
                    .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS)
                    .actualPort();
            connector = new CanCacheAgentConnector(vertx, properties(port, "cache-token"));

            connector.start();

            assertEquals("REGISTER cache-node 11212 cache-token\n", command.get(5, TimeUnit.SECONDS));
            acknowledgedConnectionClosed.get(5, TimeUnit.SECONDS);
        } finally {
            if (connector != null) {
                connector.stop();
            }
            fakeAgent.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldRejectWhitespaceInRegistrationTokenBeforeOpeningClients() throws Exception {
        Vertx vertx = Vertx.vertx();
        CanCacheAgentConnector connector = new CanCacheAgentConnector(vertx, properties(1, "bad token"));
        try {
            assertThrows(IllegalArgumentException.class, connector::start);
        } finally {
            connector.stop();
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldRejectOversizedRegistrationTokenBeforeOpeningClients() throws Exception {
        Vertx vertx = Vertx.vertx();
        CanCacheAgentConnector connector = new CanCacheAgentConnector(vertx, properties(1, "x".repeat(129)));
        try {
            assertThrows(IllegalArgumentException.class, connector::start);
        } finally {
            connector.stop();
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldRejectNonAsciiRegistrationTokenBeforeOpeningClients() throws Exception {
        Vertx vertx = Vertx.vertx();
        CanCacheAgentConnector connector = new CanCacheAgentConnector(vertx, properties(1, "gizli-şifre"));
        try {
            assertThrows(IllegalArgumentException.class, connector::start);
        } finally {
            connector.stop();
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    private static AppProperties properties(int agentPort, String token) {
        AppProperties.Agent agent = new AppProperties.Agent() {
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
                return agentPort;
            }

            @Override
            public int registrationPort() {
                return agentPort;
            }

            @Override
            public Optional<String> registrationToken() {
                return Optional.ofNullable(token);
            }

            @Override
            public Duration registrationAckTimeout() {
                return Duration.ofSeconds(5);
            }

            @Override
            public String advertisedHost() {
                return "cache-node";
            }

            @Override
            public Duration probeInterval() {
                return Duration.ofSeconds(30);
            }

            @Override
            public Duration connectTimeout() {
                return Duration.ofSeconds(1);
            }

            @Override
            public Duration startupWait() {
                return Duration.ZERO;
            }

            @Override
            public boolean requiredOnStartup() {
                return false;
            }
        };
        AppProperties.Network network = new AppProperties.Network() {
            @Override
            public String host() {
                return "127.0.0.1";
            }

            @Override
            public int port() {
                return 11212;
            }

            @Override
            public int backlog() {
                return 128;
            }

            @Override
            public int eventLoopThreads() {
                return 0;
            }

            @Override
            public int workerThreads() {
                return 1;
            }

            @Override
            public String agreementPackMessage() {
                return "HELLO";
            }
        };
        AppProperties.Cluster cluster = new AppProperties.Cluster() {
            @Override
            public int virtualNodes() {
                return 1;
            }

            @Override
            public int replicationFactor() {
                return 1;
            }

            @Override
            public AppProperties.Discovery discovery() {
                return null;
            }

            @Override
            public AppProperties.Replication replication() {
                return null;
            }

            @Override
            public AppProperties.Coordination coordination() {
                return null;
            }

            @Override
            public AppProperties.ReadRepair readRepair() {
                return null;
            }
        };

        return new AppProperties() {
            @Override
            public Metrics metrics() {
                return null;
            }

            @Override
            public Cache cache() {
                return null;
            }

            @Override
            public Cluster cluster() {
                return cluster;
            }

            @Override
            public Network network() {
                return network;
            }

            @Override
            public Cancache cancache() {
                return null;
            }

            @Override
            public Agent agent() {
                return agent;
            }
        };
    }
}
