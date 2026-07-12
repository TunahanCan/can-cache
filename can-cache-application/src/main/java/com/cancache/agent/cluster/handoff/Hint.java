package com.cancache.agent.cluster.handoff;

import com.cancache.agent.cluster.Node;

/**
 * Hinted handoff sırasında yeniden oynatılacak işlemleri temsil eder.
 */
public sealed interface Hint permits SetHint, DeleteHint, CasHint
{
    ReplayResult replay(Node<String, String> node, long nowMillis);

    enum ReplayResult
    {
        APPLIED,
        SATISFIED,
        RETRY
    }
}
