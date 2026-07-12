package com.cancache.agent.cluster;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Küme düğümlerini ve anahtarları sabit bir hash halkasında konumlandırarak
 * yük dağılımını sağlayan veri yapısıdır. Sanal düğüm kavramını destekler,
 * düğüm ekleme/çıkarma işlemlerini yönetir ve verilen anahtar için replikaları
 * deterministik şekilde seçer.
 */
public final class ConsistentHashRing<N>
{
    private final NavigableMap<Integer, List<VirtualNode<N>>> ring = new TreeMap<>();
    private final HashFn hash;
    private final int vnodes;

    public ConsistentHashRing(HashFn hash, int virtualNodes) {
        this.hash = hash;
        this.vnodes = Math.max(1, virtualNodes);
    }

    public synchronized void addNode(N node, byte[] idBytes) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(idBytes, "idBytes");
        for (int i = 0; i < vnodes; i++) {
            int vnodeIndex = i;
            int position = hash.hash(join(idBytes, vnodeIndex));
            List<VirtualNode<N>> bucket = ring.computeIfAbsent(position, ignored -> new ArrayList<>());
            bucket.removeIf(existing -> existing.vnodeIndex() == vnodeIndex
                    && Arrays.equals(existing.idBytes(), idBytes));
            bucket.add(new VirtualNode<>(node, idBytes.clone(), vnodeIndex));
            bucket.sort(ConsistentHashRing::compareVirtualNodes);
        }
    }
    public synchronized void removeNode(N node, byte[] idBytes) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(idBytes, "idBytes");
        for (int i = 0; i < vnodes; i++) {
            int vnodeIndex = i;
            int position = hash.hash(join(idBytes, vnodeIndex));
            List<VirtualNode<N>> bucket = ring.get(position);
            if (bucket == null) {
                continue;
            }
            bucket.removeIf(existing -> existing.vnodeIndex() == vnodeIndex
                    && Objects.equals(existing.node(), node)
                    && Arrays.equals(existing.idBytes(), idBytes));
            if (bucket.isEmpty()) {
                ring.remove(position);
            }
        }
    }
    public synchronized List<N> getReplicas(byte[] key, int rf)
    {
        var out = new ArrayList<N>(Math.max(0, rf));
        if (rf <= 0 || ring.isEmpty()) return out;

        int h = hash.hash(key);
        Set<N> unique = new LinkedHashSet<>();

        collectReplicas(ring.tailMap(h, true), unique, rf);
        if (unique.size() < rf) {
            collectReplicas(ring.headMap(h, false), unique, rf);
        }

        out.addAll(unique);
        return out;
    }

    public synchronized List<N> nodes() {
        Set<N> unique = new LinkedHashSet<>();
        collectReplicas(ring, unique, Integer.MAX_VALUE);
        return new ArrayList<>(unique);
    }

    private void collectReplicas(NavigableMap<Integer, List<VirtualNode<N>>> section,
                                 Set<N> destination,
                                 int requested)
    {
        for (List<VirtualNode<N>> bucket : section.values()) {
            for (VirtualNode<N> virtualNode : bucket) {
                destination.add(virtualNode.node());
                if (destination.size() >= requested) {
                    return;
                }
            }
        }
    }

    private static int compareVirtualNodes(VirtualNode<?> left, VirtualNode<?> right)
    {
        int identityOrder = Arrays.compareUnsigned(left.idBytes(), right.idBytes());
        return identityOrder != 0
                ? identityOrder
                : Integer.compare(left.vnodeIndex(), right.vnodeIndex());
    }

    private static byte[] join(byte[] id, int i){
        byte[] suffix = ("#" + i).getBytes(StandardCharsets.UTF_8);
        byte[] combined = new byte[id.length + suffix.length];
        System.arraycopy(id, 0, combined, 0, id.length);
        System.arraycopy(suffix, 0, combined, id.length, suffix.length);
        return combined;
    }

    private record VirtualNode<N>(N node, byte[] idBytes, int vnodeIndex)
    {
    }
}
