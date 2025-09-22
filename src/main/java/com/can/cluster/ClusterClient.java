package com.can.cluster;

import com.can.codec.Codec;
import java.time.Duration;
import java.util.List;

/**
 * Tutarlı hash halkası üzerinden anahtarları ilgili düğümlere yönlendirerek
 * küme içinde okuma/yazma işlemlerini çoğaltan istemci katmanıdır. Anahtarları
 * verilen codec ile bayt dizisine çevirir, belirlenen replikasyon faktörü kadar
 * düğüm seçer ve her işlemde tüm kopyalara ulaşıp tutarlılığı sağlar.
 */
public record ClusterClient<K, V>(ConsistentHashRing<Node<K, V>> ring, int replicationFactor, Codec<K> keyCodec)
{
    public ClusterClient(ConsistentHashRing<Node<K, V>> ring, int replicationFactor, Codec<K> keyCodec) {
        this.ring = ring;
        this.replicationFactor = Math.max(1, replicationFactor);
        this.keyCodec = keyCodec;
    }

    private List<Node<K, V>> replicas(K key) {
        return ring.getReplicas(keyCodec.encode(key), replicationFactor);
    }

    public void set(K key, V value, Duration ttl) {
        for (var n : replicas(key)) n.set(key, value, ttl);
    }

    public V get(K key) {
        for (var n : replicas(key)) {
            V v = n.get(key);
            if (v != null) return v;
        }
        return null;
    }

    public boolean delete(K key) {
        boolean ok = false;
        for (var n : replicas(key)) ok |= n.delete(key);
        return ok;
    }

    public void clear() {
        for (var node : ring.nodes()) {
            node.clear();
        }
    }
}
