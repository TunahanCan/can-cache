package com.cancache.agent.cluster.handoff;

import com.cancache.agent.cluster.Node;

import java.time.Duration;
import java.util.Objects;

/**
 * CAS işlemlerinin yeniden oynatılmasını sağlayan ipucu temsilidir.
 */
public record CasHint(String key, String value, long expectedCas, long expireAtMillis) implements Hint
{
    public CasHint
    {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (expireAtMillis < -1L) {
            throw new IllegalArgumentException("expireAtMillis must be -1 or a non-negative timestamp");
        }
    }

    @Override
    public ReplayResult replay(Node<String, String> node, long nowMillis)
    {
        if (expireAtMillis > 0L && expireAtMillis <= nowMillis) {
            return ReplayResult.SATISFIED;
        }
        Duration remainingTtl = expireAtMillis < 0L
                ? Duration.ZERO
                : expireAtMillis == 0L ? null : Duration.ofMillis(expireAtMillis - nowMillis);
        return node.compareAndSwap(key, value, expectedCas, remainingTtl)
                ? ReplayResult.APPLIED
                : ReplayResult.SATISFIED;
    }

    @Override
    public long estimatedBytes()
    {
        return 80L + 2L * (key.length() + value.length());
    }

    @Override
    public String toString()
    {
        return "CasHint{" + key + '}';
    }
}
