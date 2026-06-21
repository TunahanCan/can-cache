package com.cancache.agent.cluster;

import com.cancache.agent.codec.Codec;
import com.cancache.agent.core.StoredValueCodec;
import com.cancache.agent.metric.Counter;
import com.cancache.agent.metric.MetricsRegistry;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final ReadRepairSettings readRepairSettings;
    private final ExecutorService repairExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Counter readRepairAttempts;
    private final Counter readRepairRepairs;
    private final Counter readRepairConflicts;

    public ClusterClient(ConsistentHashRing<Node<String, String>> ring,
                         int replicationFactor,
                         Codec<String> keyCodec,
                         HintedHandoffService hintedHandoffService)
    {
        this(ring, replicationFactor, keyCodec, hintedHandoffService, null, ReadRepairSettings.defaults());
    }

    public ClusterClient(ConsistentHashRing<Node<String, String>> ring,
                         int replicationFactor,
                         Codec<String> keyCodec,
                         HintedHandoffService hintedHandoffService,
                         MetricsRegistry metrics,
                         ReadRepairSettings readRepairSettings)
    {
        this.ring = Objects.requireNonNull(ring, "ring");
        this.replicationFactor = Math.max(1, replicationFactor);
        this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec");
        this.hintedHandoffService = Objects.requireNonNull(hintedHandoffService, "hintedHandoffService");
        this.readRepairSettings = readRepairSettings == null ? ReadRepairSettings.defaults() : readRepairSettings;
        this.repairExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("read-repair-", 0).factory());
        this.readRepairAttempts = counter(metrics, "read_repair_attempts_total");
        this.readRepairRepairs = counter(metrics, "read_repair_repairs_total");
        this.readRepairConflicts = counter(metrics, "read_repair_conflicts_total");
    }

    public record ReadRepairSettings(boolean enabled, ReadRepairMode mode, boolean async)
    {
        public ReadRepairSettings
        {
            mode = mode == null ? ReadRepairMode.FAST : mode;
        }

        public static ReadRepairSettings defaults()
        {
            return new ReadRepairSettings(true, ReadRepairMode.FAST, true);
        }
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
        if (!readRepairSettings.enabled()) {
            return getFirstAvailable(key, nodes);
        }
        if (readRepairSettings.mode() == ReadRepairMode.QUORUM) {
            return getWithQuorumReadRepair(key, nodes);
        }
        return getWithFastReadRepair(key, nodes);
    }

    private String getFirstAvailable(String key, List<Node<String, String>> nodes)
    {
        for (Node<String, String> node : nodes) {
            ReplicaRead read = readReplica(key, node);
            if (read.value() != null) {
                return read.value();
            }
        }
        return null;
    }

    private String getWithFastReadRepair(String key, List<Node<String, String>> nodes)
    {
        for (Node<String, String> node : nodes) {
            ReplicaRead read = readReplica(key, node);
            if (read.value() != null) {
                scheduleRepair(() -> repairMissingReplicas(key, read.value(), nodes, read.node()));
                return read.value();
            }
        }
        return null;
    }

    private String getWithQuorumReadRepair(String key, List<Node<String, String>> nodes)
    {
        List<ReplicaRead> reads = new ArrayList<>(nodes.size());
        Map<String, Integer> counts = new HashMap<>();
        int reachableNodes = 0;
        for (Node<String, String> node : nodes) {
            ReplicaRead read = readReplica(key, node);
            reads.add(read);
            if (read.reachable()) {
                reachableNodes++;
            }
            if (read.value() != null) {
                counts.merge(read.value(), 1, Integer::sum);
            }
        }

        int quorum = majority(reachableNodes);
        String winner = null;
        int winnerCount = 0;
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            if (entry.getValue() >= quorum && entry.getValue() > winnerCount) {
                winner = entry.getKey();
                winnerCount = entry.getValue();
            }
        }

        if (winner == null) {
            if (!counts.isEmpty()) {
                increment(readRepairConflicts);
            }
            return null;
        }

        String winningValue = winner;
        scheduleRepair(() -> repairKnownMissingReplicas(key, winningValue, reads));
        return winningValue;
    }

    private ReplicaRead readReplica(String key, Node<String, String> node)
    {
        try {
            String value = node.get(key);
            if (value != null && encodedValueExpired(value, System.currentTimeMillis())) {
                value = null;
            }
            return new ReplicaRead(node, value, true);
        } catch (RuntimeException e) {
            LOG.debugf(e, "Failed to read key %s from node %s, trying next", key, node.id());
            return new ReplicaRead(node, null, false);
        }
    }

    private void scheduleRepair(Runnable task)
    {
        if (closed.get()) {
            return;
        }
        if (!readRepairSettings.async()) {
            task.run();
            return;
        }
        try {
            repairExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            LOG.debug("Read repair task rejected", e);
        }
    }

    private void repairMissingReplicas(String key, String sourceValue, List<Node<String, String>> nodes,
                                       Node<String, String> sourceNode)
    {
        increment(readRepairAttempts);
        Duration ttl = ttlForRepair(sourceValue, System.currentTimeMillis());
        if (Duration.ZERO.equals(ttl)) {
            return;
        }
        for (Node<String, String> node : nodes) {
            if (Objects.equals(node.id(), sourceNode.id())) {
                continue;
            }
            repairMissingReplica(key, sourceValue, ttl, node);
        }
    }

    private void repairKnownMissingReplicas(String key, String sourceValue, List<ReplicaRead> reads)
    {
        increment(readRepairAttempts);
        Duration ttl = ttlForRepair(sourceValue, System.currentTimeMillis());
        if (Duration.ZERO.equals(ttl)) {
            return;
        }
        for (ReplicaRead read : reads) {
            if (!read.reachable()) {
                continue;
            }
            if (read.value() == null) {
                setRepairValue(key, sourceValue, ttl, read.node());
            } else if (!Objects.equals(read.value(), sourceValue)) {
                increment(readRepairConflicts);
            }
        }
    }

    private void repairMissingReplica(String key, String sourceValue, Duration ttl, Node<String, String> node)
    {
        try {
            String current = node.get(key);
            if (current == null || encodedValueExpired(current, System.currentTimeMillis())) {
                setRepairValue(key, sourceValue, ttl, node);
                return;
            }
            if (!Objects.equals(current, sourceValue)) {
                increment(readRepairConflicts);
            }
        } catch (RuntimeException e) {
            LOG.debugf(e, "Read repair skipped unreachable node %s for key %s", node.id(), key);
        }
    }

    private void setRepairValue(String key, String sourceValue, Duration ttl, Node<String, String> node)
    {
        try {
            if (node.set(key, sourceValue, ttl)) {
                increment(readRepairRepairs);
            }
        } catch (RuntimeException e) {
            LOG.debugf(e, "Read repair failed to set key %s on node %s", key, node.id());
        }
    }

    private static Duration ttlForRepair(String encodedValue, long now)
    {
        StoredValueCodec.StoredValue storedValue = StoredValueCodec.decode(encodedValue);
        if (!storedValue.hasMetadata()) {
            return null;
        }
        long expireAt = storedValue.expireAt();
        if (expireAt <= 0L || expireAt == Long.MAX_VALUE) {
            return null;
        }
        long remaining = expireAt - now;
        return remaining <= 0L ? Duration.ZERO : Duration.ofMillis(remaining);
    }

    private static boolean encodedValueExpired(String encodedValue, long now)
    {
        StoredValueCodec.StoredValue storedValue = StoredValueCodec.decode(encodedValue);
        return storedValue.hasMetadata() && storedValue.expired(now);
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

    @Override
    public void close()
    {
        if (closed.compareAndSet(false, true)) {
            repairExecutor.shutdownNow();
        }
    }

    private static Counter counter(MetricsRegistry metrics, String name)
    {
        return metrics == null ? null : metrics.counter(name);
    }

    private static void increment(Counter counter)
    {
        if (counter != null) {
            counter.inc();
        }
    }

    private record ReplicaRead(Node<String, String> node, String value, boolean reachable)
    {
    }
}
