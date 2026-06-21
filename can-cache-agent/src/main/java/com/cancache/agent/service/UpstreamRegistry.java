package com.cancache.agent.service;

import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamAddress;
import com.cancache.agent.model.UpstreamState;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class UpstreamRegistry {

    private final ConcurrentMap<String, NodeStats> nodes = new ConcurrentHashMap<>();
    private final Set<String> discoveredAddresses = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, Long> registeredUntilEpochMillis = new ConcurrentHashMap<>();

    public synchronized void replace(List<String> ips, int port) {
        Set<String> nextDiscovered = new HashSet<>();
        for (String ip : ips) {
            UpstreamAddress address = UpstreamAddress.of(ip, port);
            nextDiscovered.add(address.toString());
            nodes.computeIfAbsent(address.toString(), ignored -> new NodeStats(address));
        }

        discoveredAddresses.clear();
        discoveredAddresses.addAll(nextDiscovered);
        pruneOrphans();
    }

    public NodeStats register(String host, int port, long ttlMillis) {
        UpstreamAddress address = UpstreamAddress.of(host, port);
        NodeStats node = nodes.computeIfAbsent(address.toString(), ignored -> new NodeStats(address));
        long expiresAt = System.currentTimeMillis() + Math.max(1000L, ttlMillis);
        registeredUntilEpochMillis.put(address.toString(), expiresAt);
        return node;
    }

    public void cleanupExpiredRegistrations() {
        long now = System.currentTimeMillis();
        registeredUntilEpochMillis.entrySet().removeIf(e -> e.getValue() <= now);
        pruneOrphans();
    }

    private synchronized void pruneOrphans() {
        nodes.keySet().removeIf(key -> !discoveredAddresses.contains(key) && !registeredUntilEpochMillis.containsKey(key));
    }

    public List<NodeStats> all() {
        List<NodeStats> list = new ArrayList<>(nodes.values());
        list.sort(Comparator.comparing(NodeStats::upstreamAddress));
        return list;
    }

    public List<NodeStats> ready() {
        List<NodeStats> ready = new ArrayList<>();
        for (NodeStats node : nodes.values()) {
            if (node.state() == UpstreamState.UP) {
                ready.add(node);
            }
        }
        ready.sort(Comparator.comparing(NodeStats::upstreamAddress));
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
