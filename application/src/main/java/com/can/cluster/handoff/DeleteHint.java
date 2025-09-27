package com.can.cluster.handoff;

import com.can.cluster.Node;

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
    public boolean replay(Node<String, String> node)
    {
        return node.delete(key);
    }

    @Override
    public String toString()
    {
        return "DeleteHint{" + key + '}';
    }
}
