package com.can.cluster.handoff;

import com.can.cluster.Node;

import java.time.Duration;
import java.util.Objects;

/**
 * Uzak düğüme tekrar gönderilmesi gereken set operasyonunu temsil eder.
 */
public record SetHint(String key, String value, Duration ttl) implements Hint
{
    public SetHint
    {
        Objects.requireNonNull(key, "key");
    }

    @Override
    public boolean replay(Node<String, String> node)
    {
        return node.set(key, value, ttl);
    }

    @Override
    public String toString()
    {
        return "SetHint{" + key + '}';
    }
}
