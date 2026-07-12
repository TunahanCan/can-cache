package com.cancache.agent.cluster;

import com.cancache.agent.cluster.handoff.CasHint;
import com.cancache.agent.cluster.handoff.DeleteHint;
import com.cancache.agent.cluster.handoff.Hint;
import com.cancache.agent.cluster.handoff.SetHint;
import com.cancache.agent.metric.Counter;
import com.cancache.agent.metric.MetricsRegistry;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Uzak düğümlere gönderilemeyen yazma işlemlerini kuyrukta tutarak düğüm geri
 * geldiğinde yeniden oynatılmasını sağlayan basit ipucu-handoff hizmetidir.
 */
public final class HintedHandoffService
{
    private static final Logger LOG = Logger.getLogger(HintedHandoffService.class);
    private static final int DEFAULT_MAX_HINTS_PER_NODE = 10_000;

    private final Map<String, HintQueue> hints = new ConcurrentHashMap<>();
    private final int maxHintsPerNode;
    private final LongSupplier currentTimeMillis;
    private final Counter enqueued;
    private final Counter replayed;
    private final Counter replayFailures;
    private final Counter dropped;

    public HintedHandoffService(MetricsRegistry metrics)
    {
        this(metrics, DEFAULT_MAX_HINTS_PER_NODE, System::currentTimeMillis);
    }

    public HintedHandoffService(MetricsRegistry metrics, int maxHintsPerNode)
    {
        this(metrics, maxHintsPerNode, System::currentTimeMillis);
    }

    HintedHandoffService(MetricsRegistry metrics, int maxHintsPerNode, LongSupplier currentTimeMillis)
    {
        if (maxHintsPerNode < 1) {
            throw new IllegalArgumentException("maxHintsPerNode must be at least 1");
        }
        this.maxHintsPerNode = maxHintsPerNode;
        this.currentTimeMillis = Objects.requireNonNull(currentTimeMillis, "currentTimeMillis");
        if (metrics != null)
        {
            this.enqueued = metrics.counter("hinted_handoff_enqueued_total");
            this.replayed = metrics.counter("hinted_handoff_replayed_total");
            this.replayFailures = metrics.counter("hinted_handoff_failures_total");
            this.dropped = metrics.counter("hinted_handoff_dropped_total");
        } else {
            this.enqueued = null;
            this.replayed = null;
            this.replayFailures = null;
            this.dropped = null;
        }
    }

    public void recordSet(String nodeId, String key, String value, Duration ttl)
    {
        enqueue(nodeId, new SetHint(key, value, calculateExpireAt(ttl)));
    }

    public void recordDelete(String nodeId, String key)
    {
        enqueue(nodeId, new DeleteHint(key));
    }

    public void recordCas(String nodeId, String key, String value, long expectedCas, Duration ttl)
    {
        enqueue(nodeId, new CasHint(key, value, expectedCas, calculateExpireAt(ttl)));
    }

    private void enqueue(String nodeId, Hint hint)
    {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(hint, "hint");
        boolean[] droppedOldest = new boolean[1];
        hints.compute(nodeId, (ignored, queue) -> {
            HintQueue target = queue == null ? new HintQueue(maxHintsPerNode) : queue;
            droppedOldest[0] = target.addLast(hint);
            return target;
        });
        if (enqueued != null) {
            enqueued.inc();
        }
        if (droppedOldest[0] && dropped != null) {
            dropped.inc();
        }
    }

    public int pendingFor(String nodeId)
    {
        var queue = hints.get(nodeId);
        return queue == null ? 0 : queue.size();
    }

    public void replay(String nodeId, Node<String, String> node)
    {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(node, "node");
        var queue = hints.get(nodeId);
        if (queue == null || !queue.tryStartReplay()) {
            return;
        }

        int replayedCount = 0;
        try {
            while (true) {
                Hint hint = queue.pollFirst();
                if (hint == null) {
                    break;
                }
                try {
                    Hint.ReplayResult result = hint.replay(node, currentTimeMillis.getAsLong());
                    if (result == Hint.ReplayResult.RETRY) {
                        recordDropped(queue.addFirst(hint));
                        incrementReplayFailure();
                        LOG.debugf("Hint replay requested retry for %s on node %s", hint, nodeId);
                        break;
                    }
                    replayedCount++;
                } catch (RuntimeException e) {
                    recordDropped(queue.addFirst(hint));
                    incrementReplayFailure();
                    LOG.debugf(e, "Failed to replay hint %s for node %s", hint, nodeId);
                    break;
                }
            }
        } finally {
            queue.finishReplay();
        }

        hints.computeIfPresent(nodeId,
                (ignored, current) -> current == queue && current.isEmpty() ? null : current);
        if (replayed != null && replayedCount > 0) {
            replayed.add(replayedCount);
        }
    }

    private long calculateExpireAt(Duration ttl)
    {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return 0L;
        }
        long now = currentTimeMillis.getAsLong();
        long ttlMillis;
        try {
            ttlMillis = ttl.toMillis();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
        if (ttlMillis > Long.MAX_VALUE - now) {
            return Long.MAX_VALUE;
        }
        return now + ttlMillis;
    }

    private void incrementReplayFailure()
    {
        if (replayFailures != null) {
            replayFailures.inc();
        }
    }

    private void recordDropped(boolean didDrop)
    {
        if (didDrop && dropped != null) {
            dropped.inc();
        }
    }

    private static final class HintQueue
    {
        private final ArrayDeque<Hint> queue = new ArrayDeque<>();
        private final int capacity;
        private boolean replaying;

        private HintQueue(int capacity)
        {
            this.capacity = capacity;
        }

        synchronized boolean addLast(Hint hint)
        {
            boolean dropped = queue.size() == capacity;
            if (dropped) {
                queue.removeFirst();
            }
            queue.addLast(hint);
            return dropped;
        }

        synchronized boolean addFirst(Hint hint)
        {
            boolean dropped = queue.size() == capacity;
            if (dropped) {
                queue.removeLast();
            }
            queue.addFirst(hint);
            return dropped;
        }

        synchronized Hint pollFirst()
        {
            return queue.pollFirst();
        }

        synchronized int size()
        {
            return queue.size();
        }

        synchronized boolean isEmpty()
        {
            return queue.isEmpty();
        }

        synchronized boolean tryStartReplay()
        {
            if (replaying || queue.isEmpty()) {
                return false;
            }
            replaying = true;
            return true;
        }

        synchronized void finishReplay()
        {
            replaying = false;
        }
    }
}
