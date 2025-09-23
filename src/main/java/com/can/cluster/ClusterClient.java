package com.can.cluster;

import com.can.codec.Codec;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Tutarlı hash halkası üzerinden anahtarları ilgili düğümlere yönlendirerek
 * yazma/okuma işlemlerini gerçekleştiren istemci katmanıdır. Lider izleme
 * yaklaşımıyla ilk düğüm lider kabul edilir, çoğunluk onayı alındığında işlem
 * başarılı sayılır. Uzak düğümler geçici olarak ulaşılamadığında ipucu-handoff
 * mekanizması devreye girer.
 */
public final class ClusterClient
{
    private static final Logger LOG = Logger.getLogger(ClusterClient.class);

    private final ConsistentHashRing<Node<String, String>> ring;
    private final int replicationFactor;
    private final Codec<String> keyCodec;
    private final HintedHandoffService hintedHandoffService;

    public ClusterClient(ConsistentHashRing<Node<String, String>> ring,
                         int replicationFactor,
                         Codec<String> keyCodec,
                         HintedHandoffService hintedHandoffService)
    {
        this.ring = Objects.requireNonNull(ring, "ring");
        this.replicationFactor = Math.max(1, replicationFactor);
        this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec");
        this.hintedHandoffService = Objects.requireNonNull(hintedHandoffService, "hintedHandoffService");
    }

    private List<Node<String, String>> replicas(String key)
    {
        return new ArrayList<>(ring.getReplicas(keyCodec.encode(key), replicationFactor));
    }

    private int majority(int nodes)
    {
        return (nodes / 2) + 1;
    }

    public boolean set(String key, String value, Duration ttl)
    {
        List<Node<String, String>> nodes = replicas(key);
        if (nodes.isEmpty()) {
            return false;
        }
        int quorum = majority(nodes.size());
        int successes = 0;
        RuntimeException leaderFailure = null;

        for (int i = 0; i < nodes.size(); i++) {
            Node<String, String> node = nodes.get(i);
            boolean ok;
            try {
                ok = node.set(key, value, ttl);
            } catch (RuntimeException e) {
                LOG.debugf(e, "Failed to write key %s on node %s", key, node.id());
                hintedHandoffService.recordSet(node.id(), key, value, ttl);
                if (i == 0) {
                    leaderFailure = e;
                }
                continue;
            }

            if (ok) {
                successes++;
            } else if (i > 0) {
                hintedHandoffService.recordSet(node.id(), key, value, ttl);
            }
        }

        if (successes >= quorum) {
            return true;
        }
        if (leaderFailure != null) {
            throw leaderFailure;
        }
        return false;
    }

    public String get(String key)
    {
        List<Node<String, String>> nodes = replicas(key);
        for (Node<String, String> node : nodes) {
            String value = node.get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    public boolean delete(String key)
    {
        List<Node<String, String>> nodes = replicas(key);
        if (nodes.isEmpty()) {
            return false;
        }
        int quorum = majority(nodes.size());
        int successes = 0;
        for (int i = 0; i < nodes.size(); i++) {
            Node<String, String> node = nodes.get(i);
            try {
                if (node.delete(key)) {
                    successes++;
                } else if (i > 0) {
                    hintedHandoffService.recordDelete(node.id(), key);
                }
            } catch (RuntimeException e) {
                LOG.debugf(e, "Failed to delete key %s on node %s", key, node.id());
                hintedHandoffService.recordDelete(node.id(), key);
            }
        }
        return successes >= quorum;
    }

    public boolean compareAndSwap(String key, String value, long expectedCas, Duration ttl)
    {
        List<Node<String, String>> nodes = replicas(key);
        if (nodes.isEmpty()) {
            return false;
        }
        int quorum = majority(nodes.size());
        int successes = 0;
        RuntimeException leaderFailure = null;

        for (int i = 0; i < nodes.size(); i++) {
            Node<String, String> node = nodes.get(i);
            boolean ok;
            try {
                ok = node.compareAndSwap(key, value, expectedCas, ttl);
            } catch (RuntimeException e) {
                LOG.debugf(e, "Failed to CAS key %s on node %s", key, node.id());
                hintedHandoffService.recordCas(node.id(), key, value, expectedCas, ttl);
                if (i == 0) {
                    leaderFailure = e;
                }
                continue;
            }
            if (ok) {
                successes++;
            }
        }

        if (successes >= quorum) {
            return true;
        }
        if (leaderFailure != null) {
            throw leaderFailure;
        }
        return false;
    }

    public void clear()
    {
        for (Node<String, String> node : ring.nodes()) {
            try {
                node.clear();
            } catch (RuntimeException e) {
                LOG.debugf(e, "Failed to clear node %s", node.id());
            }
        }
    }
}
