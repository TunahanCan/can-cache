package com.cancache.agent.selection;

import com.cancache.agent.model.NodeStats;

import java.util.List;
import java.util.Optional;

public interface SelectionPolicy {
    Optional<NodeStats> select(List<NodeStats> available);
}
