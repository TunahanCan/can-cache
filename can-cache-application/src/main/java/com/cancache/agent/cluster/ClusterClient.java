package com.cancache.agent.cluster;

import com.cancache.agent.codec.Codec;
import com.cancache.agent.constants.NodeProtocol;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * Tutarlı hash halkası üzerinden anahtarları ilgili düğümlere yönlendirerek
 * yazma/okuma işlemlerini gerçekleştiren istemci katmanıdır. Lider izleme
 * yaklaşımıyla ilk düğüm lider kabul edilir, çoğunluk onayı alındığında işlem
 * başarılı sayılır. Uzak düğümler geçici olarak ulaşılamadığında ipucu-handoff
 * mekanizması devreye girer.
 */
public final class ClusterClient implements AutoCloseable
{
    private static final Logger LOG = Logger.getLogger(ClusterClient.class);

    private final ConsistentHashRing<Node<String, String>> ring;
    private final int replicationFactor;
    private final Codec<String> keyCodec;
    private final HintedHandoffService hintedHandoffService;
    private final ExecutorService replicaExecutor;

    public ClusterClient(ConsistentHashRing<Node<String, String>> ring,
                         int replicationFactor,
                         Codec<String> keyCodec,
                         HintedHandoffService hintedHandoffService)
    {
        this.ring = Objects.requireNonNull(ring, "ring");
        this.replicationFactor = Math.max(1, replicationFactor);
        this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec");
        this.hintedHandoffService = Objects.requireNonNull(hintedHandoffService, "hintedHandoffService");
        this.replicaExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("cluster-replica-", 0).factory());
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
        int quorum = majority(replicationFactor);
        int successes = 0;
        RuntimeException leaderFailure = null;

        List<ReplicaResult<Boolean>> results = executeInParallel(nodes, node -> node.set(key, value, ttl));
        for (int i = 0; i < nodes.size(); i++) {
            Node<String, String> node = nodes.get(i);
            ReplicaResult<Boolean> result = results.get(i);
            if (result.failure() != null) {
                RuntimeException e = result.failure();
                LOG.debugf(e, "Failed to write key %s on node %s", key, node.id());
                hintedHandoffService.recordSet(node.id(), key, value, ttl);
                if (i == 0) {
                    leaderFailure = e;
                }
                continue;
            }

            if (Boolean.TRUE.equals(result.value())) {
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

    public String get(String key)
    {
        List<Node<String, String>> nodes = replicas(key);
        for (Node<String, String> node : nodes) {
            try {
                String value = node.get(key);
                if (value != null) {
                    return value;
                }
            } catch (RuntimeException e) {
                LOG.debugf(e, "Failed to read key %s from node %s, trying next", key, node.id());
                continue;
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
        int quorum = majority(replicationFactor);
        int successes = 0;
        List<ReplicaResult<Boolean>> results = executeInParallel(nodes, node -> node.delete(key));
        for (int i = 0; i < nodes.size(); i++) {
            Node<String, String> node = nodes.get(i);
            ReplicaResult<Boolean> result = results.get(i);
            if (result.failure() != null) {
                RuntimeException e = result.failure();
                LOG.debugf(e, "Failed to delete key %s on node %s", key, node.id());
                hintedHandoffService.recordDelete(node.id(), key);
            } else if (Boolean.TRUE.equals(result.value())) {
                successes++;
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
        int quorum = majority(replicationFactor);
        int successes = 0;
        RuntimeException leaderFailure = null;

        List<ReplicaResult<Boolean>> results = executeInParallel(nodes,
                node -> node.compareAndSwap(key, value, expectedCas, ttl));
        for (int i = 0; i < nodes.size(); i++) {
            Node<String, String> node = nodes.get(i);
            ReplicaResult<Boolean> result = results.get(i);
            if (result.failure() != null) {
                RuntimeException e = result.failure();
                LOG.debugf(e, "Failed to CAS key %s on node %s", key, node.id());
                hintedHandoffService.recordCas(node.id(), key, value, expectedCas, ttl);
                if (i == 0) {
                    leaderFailure = e;
                }
                continue;
            }
            if (Boolean.TRUE.equals(result.value())) {
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

    /**
     * Atomically stores a value only when the key is absent on each replica.
     */
    public boolean add(String key, String value, Duration ttl)
    {
        return compareAndSwap(key, value, NodeProtocol.CAS_EXPECT_ABSENT, ttl);
    }

    public void clear()
    {
        List<Node<String, String>> nodes = new ArrayList<>(ring.nodes());
        List<ReplicaResult<Void>> results = executeInParallel(nodes, node -> {
            node.clear();
            return null;
        });
        for (int i = 0; i < nodes.size(); i++) {
            RuntimeException e = results.get(i).failure();
            if (e != null) {
                Node<String, String> node = nodes.get(i);
                LOG.debugf(e, "Failed to clear node %s", node.id());
            }
        }
    }

    private <T> List<ReplicaResult<T>> executeInParallel(List<Node<String, String>> nodes,
                                                          Function<Node<String, String>, T> action)
    {
        List<CompletableFuture<ReplicaResult<T>>> futures = new ArrayList<>(nodes.size());
        for (Node<String, String> node : nodes) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return new ReplicaResult<>(action.apply(node), null);
                } catch (RuntimeException e) {
                    return new ReplicaResult<>(null, e);
                }
            }, replicaExecutor));
        }

        List<ReplicaResult<T>> results = new ArrayList<>(nodes.size());
        for (CompletableFuture<ReplicaResult<T>> future : futures) {
            try {
                results.add(future.join());
            } catch (CompletionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                RuntimeException failure = cause instanceof RuntimeException runtime
                        ? runtime
                        : new IllegalStateException("Replica operation failed", cause);
                results.add(new ReplicaResult<>(null, failure));
            }
        }
        return results;
    }

    @Override
    public void close()
    {
        replicaExecutor.shutdownNow();
    }

    private record ReplicaResult<T>(T value, RuntimeException failure) {}
}
