package com.cancache.agent.cluster;

import com.cancache.agent.core.CacheEngine;
import com.cancache.agent.core.StoredValueCodec;
import com.cancache.agent.metric.Counter;
import com.cancache.agent.metric.MetricsRegistry;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class AntiEntropyRepairer
{
    private static final Logger LOG = Logger.getLogger(AntiEntropyRepairer.class);

    private final ConsistentHashRing<Node<String, String>> ring;
    private final Node<String, String> localNode;
    private final CacheEngine<String, String> localEngine;
    private final int replicationFactor;
    private final int maxRepairsPerRun;
    private final int repairRatePerSecond;
    private final Set<String> keysInRepair = ConcurrentHashMap.newKeySet();
    private final AtomicLong nextRepairNanos = new AtomicLong();
    private final Counter runs;
    private final Counter repairs;
    private final Counter conflicts;
    private final Counter failures;

    public AntiEntropyRepairer(ConsistentHashRing<Node<String, String>> ring,
                               Node<String, String> localNode,
                               CacheEngine<String, String> localEngine,
                               int replicationFactor,
                               MetricsRegistry metrics)
    {
        this(ring, localNode, localEngine, replicationFactor, metrics, 1_000, 100);
    }

    public AntiEntropyRepairer(ConsistentHashRing<Node<String, String>> ring,
                               Node<String, String> localNode,
                               CacheEngine<String, String> localEngine,
                               int replicationFactor,
                               MetricsRegistry metrics,
                               int maxRepairsPerRun,
                               int repairRatePerSecond)
    {
        this.ring = Objects.requireNonNull(ring, "ring");
        this.localNode = Objects.requireNonNull(localNode, "localNode");
        this.localEngine = Objects.requireNonNull(localEngine, "localEngine");
        this.replicationFactor = Math.max(1, replicationFactor);
        this.maxRepairsPerRun = Math.max(1, maxRepairsPerRun);
        this.repairRatePerSecond = Math.max(0, repairRatePerSecond);
        this.runs = counter(metrics, "anti_entropy_runs_total");
        this.repairs = counter(metrics, "anti_entropy_repairs_total");
        this.conflicts = counter(metrics, "anti_entropy_conflicts_total");
        this.failures = counter(metrics, "anti_entropy_failures_total");
    }

    public void runOnce()
    {
        increment(runs);
        long now = System.currentTimeMillis();
        AtomicInteger repairBudget = new AtomicInteger(maxRepairsPerRun);
        localEngine.forEachEntry((key, valueBytes, engineExpireAt) ->
                repairEntry(key, new String(valueBytes, StandardCharsets.UTF_8), engineExpireAt, now, repairBudget));
    }

    private void repairEntry(String key, String value, long engineExpireAt, long now, AtomicInteger repairBudget)
    {
        if (repairBudget.get() <= 0 || !keysInRepair.add(key)) {
            return;
        }
        if (!tryAcquireRepairSlot()) {
            keysInRepair.remove(key);
            return;
        }
        try {
            repairEntryLocked(key, value, engineExpireAt, now, repairBudget);
        } finally {
            keysInRepair.remove(key);
        }
    }

    private void repairEntryLocked(String key, String value, long engineExpireAt, long now, AtomicInteger repairBudget)
    {
        Duration ttl = ttlForRepair(value, engineExpireAt, now);
        if (Duration.ZERO.equals(ttl)) {
            return;
        }

        List<Node<String, String>> replicas = ring.getReplicas(key.getBytes(StandardCharsets.UTF_8), replicationFactor);
        if (replicas.stream().noneMatch(node -> Objects.equals(node.id(), localNode.id()))) {
            return;
        }

        for (Node<String, String> replica : replicas) {
            if (Objects.equals(replica.id(), localNode.id())) {
                continue;
            }
            if (repairBudget.get() <= 0) {
                return;
            }
            repairReplica(key, value, ttl, replica);
            repairBudget.decrementAndGet();
        }
    }

    private boolean tryAcquireRepairSlot()
    {
        if (repairRatePerSecond <= 0) {
            return true;
        }
        long now = System.nanoTime();
        long interval = TimeUnit.SECONDS.toNanos(1) / repairRatePerSecond;
        while (true) {
            long next = nextRepairNanos.get();
            if (now < next) {
                return false;
            }
            long updated = now + interval;
            if (nextRepairNanos.compareAndSet(next, updated)) {
                return true;
            }
        }
    }

    private void repairReplica(String key, String value, Duration ttl, Node<String, String> replica)
    {
        try {
            String remoteValue = replica.get(key);
            if (remoteValue == null || encodedValueExpired(remoteValue, System.currentTimeMillis())) {
                if (replica.set(key, value, ttl)) {
                    increment(repairs);
                } else {
                    increment(failures);
                }
                return;
            }
            if (!Objects.equals(remoteValue, value)) {
                increment(conflicts);
            }
        } catch (RuntimeException e) {
            increment(failures);
            LOG.debugf(e, "Anti-entropy repair failed for key %s on node %s", key, replica.id());
        }
    }

    private static Duration ttlForRepair(String encodedValue, long engineExpireAt, long now)
    {
        StoredValueCodec.StoredValue storedValue = StoredValueCodec.decode(encodedValue);
        if (storedValue.hasMetadata()) {
            return ttlFromExpireAt(storedValue.expireAt(), now);
        }
        return ttlFromExpireAt(engineExpireAt, now);
    }

    private static boolean encodedValueExpired(String encodedValue, long now)
    {
        StoredValueCodec.StoredValue storedValue = StoredValueCodec.decode(encodedValue);
        return storedValue.hasMetadata() && storedValue.expired(now);
    }

    private static Duration ttlFromExpireAt(long expireAt, long now)
    {
        if (expireAt <= 0L || expireAt == Long.MAX_VALUE) {
            return null;
        }
        long remaining = expireAt - now;
        return remaining <= 0L ? Duration.ZERO : Duration.ofMillis(remaining);
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
}
