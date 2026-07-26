package com.cancache.agent.selection;

import com.cancache.agent.model.NodeStats;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeastConnPendingLoadTest {

    private final LeastConnPolicy policy = new LeastConnPolicy();

    @Test
    void pendingReservationShouldMoveTheNextSelectionToAnotherNode() {
        NodeStats first = new NodeStats("cache-a:11212");
        NodeStats second = new NodeStats("cache-b:11212");
        List<NodeStats> candidates = List.of(second, first);

        NodeStats initial = policy.select(candidates).orElseThrow();
        assertSame(first, initial);

        initial.reservePendingConnection();

        assertSame(second, policy.select(candidates).orElseThrow());
    }

    @Test
    void selectionShouldCompareCombinedPendingAndActiveLoad() {
        NodeStats first = new NodeStats("cache-a:11212");
        NodeStats second = new NodeStats("cache-b:11212");
        first.reservePendingConnection();
        first.reservePendingConnection();
        second.incActiveConn();

        assertSame(second, policy.select(List.of(first, second)).orElseThrow());

        second.reservePendingConnection();
        assertSame(first, policy.select(List.of(second, first)).orElseThrow());
    }

    @Test
    void emptyCandidateListShouldNotSelectANode() {
        assertTrue(policy.select(List.of()).isEmpty());
    }
}
