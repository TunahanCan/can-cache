package com.cancache.agent.selection;

import com.cancache.agent.model.NodeStats;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinPolicy implements SelectionPolicy {

    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public Optional<NodeStats> select(List<NodeStats> available) {
        if (available.isEmpty()) {
            return Optional.empty();
        }
        int idx = Math.floorMod(counter.getAndIncrement(), available.size());
        return Optional.of(available.get(idx));
    }
}
