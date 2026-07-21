package com.cancache.agent.cluster.handoff;

import com.cancache.agent.cluster.Node;

import java.util.Objects;

/**
 * İpucu kuyruğundaki bir silme operasyonunu temsil eder.
 */
public record DeleteHint(String key) implements Hint
{
    public DeleteHint
    {
        Objects.requireNonNull(key, "key");
    }

    @Override
    public ReplayResult replay(Node<String, String> node, long nowMillis)
    {
        node.delete(key);
        return ReplayResult.SATISFIED;
    }

    @Override
    public long estimatedBytes()
    {
        return 32L + 2L * key.length();
    }

    @Override
    public String toString()
    {
        return "DeleteHint{" + key + '}';
    }
}
