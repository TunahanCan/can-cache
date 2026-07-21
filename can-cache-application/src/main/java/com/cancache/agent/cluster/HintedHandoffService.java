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
    private static final long DEFAULT_MAX_HINT_BYTES_PER_NODE = 32L * 1024L * 1024L;

    private final Map<String, HintQueue> hints = new ConcurrentHashMap<>();
    private final int maxHintsPerNode;
    private final long maxHintBytesPerNode;
    private final LongSupplier currentTimeMillis;
    private final Counter enqueued;
    private final Counter replayed;
    private final Counter replayFailures;
    private final Counter dropped;

    public HintedHandoffService(MetricsRegistry metrics)
    {
        this(metrics, DEFAULT_MAX_HINTS_PER_NODE, DEFAULT_MAX_HINT_BYTES_PER_NODE,
                System::currentTimeMillis);
    }

    public HintedHandoffService(MetricsRegistry metrics, int maxHintsPerNode)
    {
        this(metrics, maxHintsPerNode, DEFAULT_MAX_HINT_BYTES_PER_NODE,
                System::currentTimeMillis);
    }

    HintedHandoffService(MetricsRegistry metrics, int maxHintsPerNode, LongSupplier currentTimeMillis)
    {
        this(metrics, maxHintsPerNode, DEFAULT_MAX_HINT_BYTES_PER_NODE, currentTimeMillis);
    }

    public HintedHandoffService(MetricsRegistry metrics,
                                int maxHintsPerNode,
                                long maxHintBytesPerNode)
    {
        this(metrics, maxHintsPerNode, maxHintBytesPerNode, System::currentTimeMillis);
    }

    HintedHandoffService(MetricsRegistry metrics,
                         int maxHintsPerNode,
                         long maxHintBytesPerNode,
                         LongSupplier currentTimeMillis)
    {
        if (maxHintsPerNode < 1) {
            throw new IllegalArgumentException("maxHintsPerNode must be at least 1");
        }
        if (maxHintBytesPerNode < 1L) {
            throw new IllegalArgumentException("maxHintBytesPerNode must be at least 1");
        }
        this.maxHintsPerNode = maxHintsPerNode;
        this.maxHintBytesPerNode = maxHintBytesPerNode;
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
        long expireAt = ttl != null && (ttl.isZero() || ttl.isNegative())
                ? -1L
                : calculateExpireAt(ttl);
        enqueue(nodeId, new CasHint(key, value, expectedCas, expireAt));
    }

    private void enqueue(String nodeId, Hint hint)
    {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(hint, "hint");
        int[] droppedCount = new int[1];
        hints.compute(nodeId, (ignored, queue) -> {
            HintQueue target = queue == null
                    ? new HintQueue(maxHintsPerNode, maxHintBytesPerNode)
                    : queue;
            droppedCount[0] = target.addLast(hint);
            return target.isEmpty() ? null : target;
        });
        if (enqueued != null) {
            enqueued.inc();
        }
        if (droppedCount[0] > 0 && dropped != null) {
            dropped.add(droppedCount[0]);
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

    private void recordDropped(int droppedCount)
    {
        if (droppedCount > 0 && dropped != null) {
            dropped.add(droppedCount);
        }
    }

    private static final class HintQueue
    {
        private final ArrayDeque<Hint> queue = new ArrayDeque<>();
        private final int capacity;
        private boolean replaying;

        private long bytes;
        private final long maxBytes;

        private HintQueue(int capacity, long maxBytes)
        {
            this.capacity = capacity;
            this.maxBytes = maxBytes;
        }

        synchronized int addLast(Hint hint)
        {
            queue.addLast(hint);
            bytes += hint.estimatedBytes();
            return trimOldest();
        }

        synchronized int addFirst(Hint hint)
        {
            queue.addFirst(hint);
            bytes += hint.estimatedBytes();
            return trimNewest();
        }

        private int trimOldest()
        {
            int dropped = 0;
            while (queue.size() > capacity || bytes > maxBytes) {
                Hint removed = queue.removeFirst();
                bytes -= removed.estimatedBytes();
                dropped++;
            }
            return dropped;
        }

        private int trimNewest()
        {
            int dropped = 0;
            while (queue.size() > capacity || bytes > maxBytes) {
                Hint removed = queue.removeLast();
                bytes -= removed.estimatedBytes();
                dropped++;
            }
            return dropped;
        }

        synchronized Hint pollFirst()
        {
            Hint hint = queue.pollFirst();
            if (hint != null) {
                bytes -= hint.estimatedBytes();
            }
            return hint;
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
