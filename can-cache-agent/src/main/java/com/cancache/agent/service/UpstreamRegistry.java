package com.cancache.agent.service;

import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamAddress;
import com.cancache.agent.model.UpstreamState;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

@ApplicationScoped
public class UpstreamRegistry {

    private final ConcurrentMap<String, NodeStats> nodes = new ConcurrentHashMap<>();
    private final Set<String> discoveredAddresses = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, Long> registeredUntilEpochMillis = new ConcurrentHashMap<>();
    private final LongSupplier currentTimeMillis;

    public UpstreamRegistry() {
        this(System::currentTimeMillis);
    }

    UpstreamRegistry(LongSupplier currentTimeMillis) {
        this.currentTimeMillis = Objects.requireNonNull(currentTimeMillis, "currentTimeMillis");
    }

    public synchronized void replace(List<String> ips, int port) {
        Set<String> nextDiscovered = new HashSet<>();
        List<UpstreamAddress> addresses = new ArrayList<>(ips.size());
        for (String ip : ips) {
            UpstreamAddress address = UpstreamAddress.of(ip, port);
            addresses.add(address);
            nextDiscovered.add(address.toString());
        }
        for (UpstreamAddress address : addresses) {
            nodes.computeIfAbsent(address.toString(), ignored -> new NodeStats(address));
        }

        discoveredAddresses.clear();
        discoveredAddresses.addAll(nextDiscovered);
        pruneOrphans();
    }

    public synchronized NodeStats register(String host, int port, long ttlMillis) {
        return register(host, port, ttlMillis, Integer.MAX_VALUE);
    }

    public synchronized NodeStats register(String host, int port, long ttlMillis, int maxRegisteredNodes) {
        UpstreamAddress address = UpstreamAddress.of(host, port);
        String key = address.toString();
        if (!registeredUntilEpochMillis.containsKey(key)
                && registeredUntilEpochMillis.size() >= Math.max(1, maxRegisteredNodes)) {
            throw new RegistrationCapacityExceededException();
        }
        NodeStats node = nodes.computeIfAbsent(key, ignored -> new NodeStats(address));
        long now = currentTimeMillis.getAsLong();
        long ttl = Math.max(1000L, ttlMillis);
        long expiresAt = ttl > Long.MAX_VALUE - now ? Long.MAX_VALUE : now + ttl;
        registeredUntilEpochMillis.put(key, expiresAt);
        return node;
    }

    public synchronized void cleanupExpiredRegistrations() {
        long now = currentTimeMillis.getAsLong();
        registeredUntilEpochMillis.entrySet().removeIf(e -> e.getValue() <= now);
        pruneOrphans();
    }

    synchronized void pruneOrphans() {
        nodes.forEach((key, node) -> {
            if (isSourced(key)) {
                return;
            }

            if (node.load() > 0) {
                // Keep counters visible until the last existing connection leaves,
                // including an in-flight dial, but never route a new connection
                // to an orphan.
                node.state(UpstreamState.DOWN);
                return;
            }

            nodes.remove(key, node);
        });
    }

    synchronized List<NodeStats> managed() {
        List<NodeStats> managed = new ArrayList<>();
        for (Map.Entry<String, NodeStats> entry : nodes.entrySet()) {
            if (isSourced(entry.getKey())) {
                managed.add(entry.getValue());
            }
        }
        managed.sort(Comparator.comparing(NodeStats::upstreamAddress));
        return managed;
    }

    synchronized boolean isManaged(NodeStats node) {
        return node != null
                && nodes.get(node.address()) == node
                && isSourced(node.address());
    }

    synchronized boolean isManagedAddress(String address) {
        return nodes.containsKey(address) && isSourced(address);
    }

    synchronized boolean transitionIfManaged(
            NodeStats node,
            UpstreamState expected,
            UpstreamState next) {
        return isManaged(node) && node.compareAndSetState(expected, next);
    }

    private boolean isSourced(String address) {
        return discoveredAddresses.contains(address) || registeredUntilEpochMillis.containsKey(address);
    }

    public List<NodeStats> all() {
        List<NodeStats> list = new ArrayList<>(nodes.values());
        list.sort(Comparator.comparing(NodeStats::upstreamAddress));
        return list;
    }

    public synchronized List<NodeStats> ready() {
        List<NodeStats> ready = new ArrayList<>();
        for (Map.Entry<String, NodeStats> entry : nodes.entrySet()) {
            if (isSourced(entry.getKey()) && entry.getValue().state() == UpstreamState.UP) {
                ready.add(entry.getValue());
            }
        }
        ready.sort(Comparator.comparing(NodeStats::upstreamAddress));
        return ready;
    }

    public int total() {
        return nodes.size();
    }

    public synchronized int upCount() {
        int up = 0;
        for (Map.Entry<String, NodeStats> entry : nodes.entrySet()) {
            if (isSourced(entry.getKey()) && entry.getValue().state() == UpstreamState.UP) {
                up++;
            }
        }
        return up;
    }

    static final class RegistrationCapacityExceededException extends IllegalStateException {

        private RegistrationCapacityExceededException() {
            super("registration capacity reached");
        }
    }
}
