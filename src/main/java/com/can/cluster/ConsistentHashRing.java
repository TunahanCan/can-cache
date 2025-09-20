package com.can.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public final class ConsistentHashRing<N> {
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
        var out = new ArrayList<N>(rf);
        if (ring.isEmpty()) return out;
        int h = hash.hash(key);
        var tail = ring.tailMap(h);
        var it = tail.isEmpty()? ring.values().iterator() : tail.values().iterator();
        while (out.size() < rf) {
            if (!it.hasNext()) it = ring.values().iterator();
            N n = it.next();
            if (out.isEmpty() || out.get(out.size()-1) != n) out.add(n);
        }
        return out;
    }
    private static byte[] join(byte[] id, int i){ return (new String(id)+"#"+i).getBytes(); }
}
