package com.cancache.agent.config;

import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Set;

@Startup
@ApplicationScoped
public class AgentConfigValidator {

    private static final int MAX_REGISTRATION_TOKEN_BYTES = 128;
    private static final Set<String> DASHBOARD_MODES = Set.of("auto", "tui", "log", "compact", "off", "none");

    @Inject
    AgentConfig config;

    @PostConstruct
    void validate() {
        requirePort("agent.listen.port", config.listen().port());
        requirePositive("agent.listen.max-connections", config.listen().maxConnections());
        requirePositive("agent.listen.max-pending-connections", config.listen().maxPendingConnections());
        requirePositive("agent.listen.write-queue-max-bytes", config.listen().writeQueueMaxBytes());

        requirePort("agent.upstream.port", config.upstream().port());
        requirePositiveDuration("agent.health.interval", config.health().interval());
        requirePositiveDuration("agent.health.connect-timeout", config.health().connectTimeout());
        requirePositive("agent.health.healthy-threshold", config.health().healthyThreshold());
        requirePositive("agent.health.unhealthy-threshold", config.health().unhealthyThreshold());
        requirePositive("agent.health.passive-failure-threshold", config.health().passiveFailureThreshold());
        requirePositive("agent.selection.max-attempts", config.selection().maxAttempts());
        requirePositiveDuration("agent.timeouts.connect", config.timeouts().connect());
        requirePositiveDuration("agent.timeouts.idle", config.timeouts().idle());
        requirePositiveDuration("agent.shutdown.grace", config.shutdown().grace());
        requirePositiveDuration("agent.dashboard.refresh", config.dashboard().refresh());
        requirePositiveDuration("agent.dashboard.snapshot-interval", config.dashboard().snapshotInterval());

        String dashboardMode = config.dashboard().mode().toLowerCase(Locale.ROOT);
        if (!DASHBOARD_MODES.contains(dashboardMode)) {
            throw new IllegalArgumentException("agent.dashboard.mode must be one of " + DASHBOARD_MODES
                    + ", but was: " + config.dashboard().mode());
        }

        if (config.registration().enabled()) {
            requirePort("agent.registration.port", config.registration().port());
            requirePositiveDuration("agent.registration.ttl", config.registration().ttl());
            requirePositiveDuration("agent.registration.cleanup-interval", config.registration().cleanupInterval());
            requirePositiveDuration("agent.registration.read-timeout", config.registration().readTimeout());
            requirePositive("agent.registration.max-connections", config.registration().maxConnections());
            requirePositive("agent.registration.max-nodes", config.registration().maxNodes());
            String registrationToken = config.registration().token().orElse("");
            if (registrationToken.chars().anyMatch(Character::isWhitespace)) {
                throw new IllegalArgumentException("agent.registration.token must not contain whitespace");
            }
            if (!isPrintableAscii(registrationToken)) {
                throw new IllegalArgumentException("agent.registration.token must contain printable ASCII only");
            }
            if (registrationToken.getBytes(StandardCharsets.UTF_8).length > MAX_REGISTRATION_TOKEN_BYTES) {
                throw new IllegalArgumentException("agent.registration.token must not exceed "
                        + MAX_REGISTRATION_TOKEN_BYTES + " UTF-8 bytes");
            }

            if (registrationToken.isBlank() && !isLoopbackBind(config.registration().host())) {
                throw new IllegalArgumentException("agent.registration.token is required when "
                        + "agent.registration.host is not loopback");
            }
        }
    }

    private static boolean isLoopbackBind(String host) {
        return "127.0.0.1".equalsIgnoreCase(host)
                || "localhost".equalsIgnoreCase(host)
                || "::1".equals(host)
                || "[::1]".equals(host);
    }

    private static boolean isPrintableAscii(String value) {
        return value.chars().allMatch(character -> character >= 0x21 && character <= 0x7e);
    }

    private static void requirePort(String key, int value) {
        if (value < 1 || value > 65_535) {
            throw new IllegalArgumentException(key + " must be between 1 and 65535");
        }
    }

    private static void requirePositive(String key, int value) {
        if (value < 1) {
            throw new IllegalArgumentException(key + " must be greater than zero");
        }
    }

    private static void requirePositiveDuration(String key, Duration value) {
        if (value == null || value.compareTo(Duration.ofMillis(1)) < 0) {
            throw new IllegalArgumentException(key + " must be at least 1ms");
        }
    }
}
