package com.cancache.agent.service;

import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamState;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class UpstreamRegistry {

    private final ConcurrentMap<String, NodeStats> nodes = new ConcurrentHashMap<>();

    public void replace(List<String> ips, int port) {
        Set<String> expected = new HashSet<>();
        for (String ip : ips) {
            String address = ip + ":" + port;
            expected.add(address);
            nodes.computeIfAbsent(address, NodeStats::new);
        }
        nodes.keySet().removeIf(key -> !expected.contains(key));
    }

    public List<NodeStats> all() {
        List<NodeStats> list = new ArrayList<>(nodes.values());
        list.sort(Comparator.comparing(NodeStats::address));
        return list;
    }

    public List<NodeStats> ready() {
        List<NodeStats> ready = new ArrayList<>();
        for (NodeStats node : nodes.values()) {
            if (node.state() == UpstreamState.UP) {
                ready.add(node);
            }
        }
        ready.sort(Comparator.comparing(NodeStats::address));
        return ready;
    }

    public int total() {
        return nodes.size();
    }

    public int upCount() {
        int up = 0;
        for (NodeStats node : nodes.values()) {
            if (node.state() == UpstreamState.UP) {
                up++;
            }
        }
        return up;
    }
}
