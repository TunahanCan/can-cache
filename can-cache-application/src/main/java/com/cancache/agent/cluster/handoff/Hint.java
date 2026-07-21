package com.cancache.agent.cluster.handoff;

import com.cancache.agent.cluster.Node;

/**
 * Hinted handoff sırasında yeniden oynatılacak işlemleri temsil eder.
 */
public sealed interface Hint permits SetHint, DeleteHint, CasHint
{
    ReplayResult replay(Node<String, String> node, long nowMillis);

    /** Conservative heap-accounting estimate used to bound pending handoff data. */
    long estimatedBytes();

    enum ReplayResult
    {
        APPLIED,
        SATISFIED,
        RETRY
    }
}
