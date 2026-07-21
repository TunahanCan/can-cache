package com.cancache.agent.cluster.handoff;

import com.cancache.agent.cluster.Node;

import java.time.Duration;
import java.util.Objects;

/**
 * Uzak düğüme tekrar gönderilmesi gereken set operasyonunu temsil eder.
 */
public record SetHint(String key, String value, long expireAtMillis) implements Hint
{
    public SetHint
    {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (expireAtMillis < 0L) {
            throw new IllegalArgumentException("expireAtMillis must not be negative");
        }
    }

    @Override
    public ReplayResult replay(Node<String, String> node, long nowMillis)
    {
        if (expireAtMillis > 0L && expireAtMillis <= nowMillis) {
            return ReplayResult.SATISFIED;
        }
        Duration remainingTtl = expireAtMillis == 0L
                ? null
                : Duration.ofMillis(expireAtMillis - nowMillis);
        return node.set(key, value, remainingTtl) ? ReplayResult.APPLIED : ReplayResult.RETRY;
    }

    @Override
    public long estimatedBytes()
    {
        return 64L + 2L * (key.length() + value.length());
    }

    @Override
    public String toString()
    {
        return "SetHint{" + key + '}';
    }
}
