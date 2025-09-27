package com.can.cluster.handoff;

import com.can.cluster.Node;

import java.time.Duration;
import java.util.Objects;

/**
 * CAS işlemlerinin yeniden oynatılmasını sağlayan ipucu temsilidir.
 */
public record CasHint(String key, String value, long expectedCas, Duration ttl) implements Hint
{
    public CasHint
    {
        Objects.requireNonNull(key, "key");
    }

    @Override
    public boolean replay(Node<String, String> node)
    {
        return node.compareAndSwap(key, value, expectedCas, ttl);
    }

    @Override
    public String toString()
    {
        return "CasHint{" + key + '}';
    }
}
