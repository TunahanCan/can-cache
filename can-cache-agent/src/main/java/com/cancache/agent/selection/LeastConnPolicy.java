package com.cancache.agent.selection;

import com.cancache.agent.model.NodeStats;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class LeastConnPolicy implements SelectionPolicy
{

    @Override
    public Optional<NodeStats> select(List<NodeStats> available)
    {
        return available.stream()
                .min(Comparator.comparingInt(NodeStats::load)
                        .thenComparing(NodeStats::address));
    }
}
