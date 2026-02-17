package com.cancache.agent.cluster.handoff;

import com.cancache.agent.cluster.Node;

/**
 * Hinted handoff sırasında yeniden oynatılacak işlemleri temsil eder.
 */
public interface Hint
{
    boolean replay(Node<String, String> node);
}
