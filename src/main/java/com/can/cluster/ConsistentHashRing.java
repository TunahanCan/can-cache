package com.can.cluster;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Küme düğümlerini ve anahtarları sabit bir hash halkasında konumlandırarak
 * yük dağılımını sağlayan veri yapısıdır. Sanal düğüm kavramını destekler,
 * düğüm ekleme/çıkarma işlemlerini yönetir ve verilen anahtar için replikaları
 * deterministik şekilde seçer.
 */
public final class ConsistentHashRing<N>
{
    private final SortedMap<Integer,N> ring = new TreeMap<>();
    private final HashFn hash;
    private final int vnodes;

    public ConsistentHashRing(HashFn hash, int virtualNodes) {
        this.hash = hash; this.vnodes = Math.max(1, virtualNodes);
    }

    public synchronized void addNode(N node, byte[] idBytes) {
        for (int i = 0; i < vnodes; i++) ring.put(hash.hash(join(idBytes, i)), node);
    }
    public synchronized void removeNode(N node, byte[] idBytes) {
        for (int i = 0; i < vnodes; i++) ring.remove(hash.hash(join(idBytes, i)));
    }
    public synchronized List<N> getReplicas(byte[] key, int rf) {
        var out = new ArrayList<N>(Math.max(0, Math.min(rf, ring.size())));
        if (rf <= 0 || ring.isEmpty()) return out;

        int h = hash.hash(key);
        Set<N> unique = new LinkedHashSet<>();

        SortedMap<Integer, N> tail = ring.tailMap(h);
        for (N node : tail.values()) {
            unique.add(node);
            if (unique.size() >= rf) break;
        }

        if (unique.size() < rf) {
            for (N node : ring.headMap(h).values()) {
                unique.add(node);
                if (unique.size() >= rf) break;
            }
        }

        out.addAll(unique);
        return out;
    }

    public synchronized List<N> nodes() {
        return new ArrayList<>(new LinkedHashSet<>(ring.values()));
    }
    private static byte[] join(byte[] id, int i){
        byte[] suffix = ("#" + i).getBytes(StandardCharsets.UTF_8);
        byte[] combined = new byte[id.length + suffix.length];
        System.arraycopy(id, 0, combined, 0, id.length);
        System.arraycopy(suffix, 0, combined, id.length, suffix.length);
        return combined;
    }
}
