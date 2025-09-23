package com.can.cluster.handoff;

import com.can.cluster.Node;

/**
 * Hinted handoff sırasında yeniden oynatılacak işlemleri temsil eder.
 */
public interface Hint
{
    boolean replay(Node<String, String> node);
}
