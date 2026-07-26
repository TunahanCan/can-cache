package com.cancache.agent.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;
import java.util.Optional;

@ConfigMapping(prefix = "agent")
public interface AgentConfig {

    Listen listen();

    Discovery discovery();

    Upstream upstream();

    Health health();

    Selection selection();

    Timeouts timeouts();

    Registration registration();

    Dashboard dashboard();

    Shutdown shutdown();

    interface Listen
    {
        @WithDefault("0.0.0.0")
        String host();

        @WithDefault("11211")
        int port();

        @WithDefault("10000")
        int maxConnections();

        @WithDefault("1024")
        int maxPendingConnections();

        @WithDefault("65536")
        int writeQueueMaxBytes();
    }

    interface Discovery {
        @WithDefault("false")
        boolean enabled();

        @WithDefault("")
        String dns();

        @WithDefault("5s")
        Duration interval();
    }

    interface Upstream {
        @WithDefault("11211")
        int port();
    }

    interface Health {
        @WithDefault("2s")
        Duration interval();

        @WithDefault("1500ms")
        Duration connectTimeout();

        @WithDefault("2")
        int healthyThreshold();

        @WithDefault("3")
        int unhealthyThreshold();

        @WithDefault("2")
        int passiveFailureThreshold();
    }

    interface Selection {
        @WithDefault("RR")
        Policy policy();

        @WithDefault("2")
        int maxAttempts();
    }

    enum Policy {
        RR,
        LEAST_CONN
    }

    interface Timeouts {
        @WithDefault("3s")
        Duration connect();

        @WithDefault("60s")
        Duration idle();
    }

    interface Dashboard {
        @WithDefault("1s")
        Duration refresh();

        @WithDefault("auto")
        String mode();

        @WithDefault("5s")
        Duration snapshotInterval();
    }

    interface Registration {
        @WithDefault("true")
        boolean enabled();

        @WithDefault("127.0.0.1")
        String host();

        @WithDefault("11311")
        int port();

        @WithDefault("15s")
        Duration ttl();

        @WithDefault("2s")
        Duration cleanupInterval();

        @WithDefault("2s")
        Duration readTimeout();

        @WithDefault("128")
        int maxConnections();

        @WithDefault("256")
        int maxNodes();

        Optional<String> token();
    }

    interface Shutdown {
        @WithDefault("5s")
        Duration grace();
    }
}
