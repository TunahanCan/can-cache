package com.can.cluster;

import com.can.cluster.handoff.CasHint;
import com.can.cluster.handoff.DeleteHint;
import com.can.cluster.handoff.Hint;
import com.can.cluster.handoff.SetHint;
import com.can.metric.Counter;
import com.can.metric.MetricsRegistry;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Uzak düğümlere gönderilemeyen yazma işlemlerini kuyrukta tutarak düğüm geri
 * geldiğinde yeniden oynatılmasını sağlayan basit ipucu-handoff hizmetidir.
 */
public final class HintedHandoffService
{
    private static final Logger LOG = Logger.getLogger(HintedHandoffService.class);

    private final Map<String, ConcurrentLinkedDeque<Hint>> hints = new ConcurrentHashMap<>();
    private final Counter enqueued;
    private final Counter replayed;
    private final Counter replayFailures;

    public HintedHandoffService(MetricsRegistry metrics)
    {
        if (metrics != null) {
            this.enqueued = metrics.counter("hinted_handoff_enqueued_total");
            this.replayed = metrics.counter("hinted_handoff_replayed_total");
            this.replayFailures = metrics.counter("hinted_handoff_failures_total");
        } else {
            this.enqueued = null;
            this.replayed = null;
            this.replayFailures = null;
        }
    }

    public void recordSet(String nodeId, String key, String value, Duration ttl)
    {
        enqueue(nodeId, new SetHint(key, value, ttl));
    }

    public void recordDelete(String nodeId, String key)
    {
        enqueue(nodeId, new DeleteHint(key));
    }

    public void recordCas(String nodeId, String key, String value, long expectedCas, Duration ttl)
    {
        enqueue(nodeId, new CasHint(key, value, expectedCas, ttl));
    }

    private void enqueue(String nodeId, Hint hint)
    {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(hint, "hint");
        hints.computeIfAbsent(nodeId, ignored -> new ConcurrentLinkedDeque<>()).add(hint);
        if (enqueued != null) {
            enqueued.inc();
        }
    }

    public int pendingFor(String nodeId)
    {
        var queue = hints.get(nodeId);
        return queue == null ? 0 : queue.size();
    }

    public void replay(String nodeId, com.can.cluster.Node<String, String> node)
    {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(node, "node");
        var queue = hints.get(nodeId);
        if (queue == null || queue.isEmpty()) {
            return;
        }

        int replayedCount = 0;
        while (true) {
            Hint hint = queue.poll();
            if (hint == null) {
                break;
            }
            try {
                if (hint.replay(node)) {
                    replayedCount++;
                } else {
                    LOG.debugf("Hint replay returned false for %s on node %s", hint, nodeId);
                }
            } catch (RuntimeException e) {
                queue.addFirst(hint);
                if (replayFailures != null) {
                    replayFailures.inc();
                }
                LOG.debugf(e, "Failed to replay hint %s for node %s", hint, nodeId);
                break;
            }
        }

        if (queue.isEmpty()) {
            hints.remove(nodeId, queue);
        }
        if (replayed != null && replayedCount > 0) {
            replayed.add(replayedCount);
        }
    }
}
