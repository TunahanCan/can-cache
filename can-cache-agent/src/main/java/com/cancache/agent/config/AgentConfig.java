package com.cancache.agent.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;

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
    }

    interface Discovery {
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
    }

    interface Selection {
        @WithDefault("RR")
        Policy policy();
    }

    enum Policy {
        RR,
        LEAST_CONN
    }

    interface Timeouts {
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

        @WithDefault("0.0.0.0")
        String host();

        @WithDefault("11311")
        int port();

        @WithDefault("15s")
        Duration ttl();

        @WithDefault("2s")
        Duration cleanupInterval();
    }

    interface Shutdown {
        @WithDefault("5s")
        Duration grace();
    }
}
