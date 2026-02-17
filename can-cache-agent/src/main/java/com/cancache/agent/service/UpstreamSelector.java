package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.model.NodeStats;
import com.cancache.agent.selection.LeastConnPolicy;
import com.cancache.agent.selection.RoundRobinPolicy;
import com.cancache.agent.selection.SelectionPolicy;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class UpstreamSelector {

    @Inject
    AgentConfig config;

    private SelectionPolicy policy;

    @PostConstruct
    void init() {
        policy = switch (config.selection().policy()) {
            case LEAST_CONN -> new LeastConnPolicy();
            case RR -> new RoundRobinPolicy();
        };
    }

    public Optional<NodeStats> select(List<NodeStats> readyNodes)
    {
        return policy.select(readyNodes);
    }
}
