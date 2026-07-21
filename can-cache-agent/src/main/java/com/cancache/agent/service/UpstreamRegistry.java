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
    private final Set<String> discoveredAddresses = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, Long> registeredUntilEpochMillis = new ConcurrentHashMap<>();

    public synchronized void replace(List<String> ips, int port) {
        Set<String> nextDiscovered = new HashSet<>();
        for (String ip : ips) {
            String address = ip + ":" + port;
            nextDiscovered.add(address);
            nodes.computeIfAbsent(address, NodeStats::new);
        }

        discoveredAddresses.clear();
        discoveredAddresses.addAll(nextDiscovered);
        pruneOrphans();
    }

    public void register(String host, int port, long ttlMillis) {
        String address = host + ":" + port;
        nodes.computeIfAbsent(address, NodeStats::new);
        long expiresAt = System.currentTimeMillis() + Math.max(1000L, ttlMillis);
        registeredUntilEpochMillis.put(address, expiresAt);
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

    public String sourceOf(String address) {
        boolean discovered = discoveredAddresses.contains(address);
        boolean registered = registeredUntilEpochMillis.containsKey(address);
        if (discovered && registered) {
            return "DNS + REGISTRATION";
        }
        if (discovered) {
            return "DNS";
        }
        if (registered) {
            return "REGISTRATION";
        }
        return "UNKNOWN";
    }
}
