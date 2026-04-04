package com.cancache.agent.cluster;

import com.cancache.agent.metric.MetricsRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ClusterStateTest
{
    private MetricsRegistry metrics;
    private ClusterState state;

    @BeforeEach
    void setup()
    {
        metrics = new MetricsRegistry();
        state = new ClusterState("node-1", metrics);
    }

    @Nested
    class IdentityInformation
    {
        /**
         * Verifies that the local node ID and its byte representation are returned correctly.
         */
        @Test
        void shouldReturnLocalNodeIdAndBytes()
        {
            // Given / When / Then
            assertEquals("node-1", state.localNodeId(), "Local node ID should match the initialized value");
            assertArrayEquals("node-1".getBytes(StandardCharsets.UTF_8), state.localNodeIdBytes(), "Byte representation of node ID should match");
        }
    }

    @Nested
    class EpochManagement
    {
        /**
         * Shows that calling bumpEpoch increments the value and updates the metrics.
         */
        @Test
        void shouldIncrementValueOnBumpEpoch()
        {
            // Given
            long initial = state.currentEpoch();
            
            // When
            long next = state.bumpEpoch();
            
            // Then
            assertEquals(initial + 1, next, "Epoch should be incremented by 1");
            assertEquals(1L, metrics.counter("cluster_epoch_increments").get(), "Epoch increments counter should be updated");
        }

        /**
         * Verifies that a larger epoch value coming from remote is adopted.
         */
        @Test
        void shouldAcceptHigherValueOnObserveEpoch()
        {
            // Given
            long expected = state.currentEpoch() + 5;
            
            // When
            state.observeEpoch(expected);
            
            // Then
            assertEquals(expected, state.currentEpoch(), "Current epoch should be updated to the larger observed epoch");
            assertEquals(1L, metrics.counter("cluster_epoch_observed_updates").get(), "Epoch observed updates counter should be updated");
        }

        /**
         * Verifies that smaller or invalid epoch values are ignored.
         */
        @Test
        void shouldIgnoreLowerValuesOnObserveEpoch()
        {
            // Given
            long initial = state.currentEpoch();
            
            // When
            state.observeEpoch(initial - 1);
            state.observeEpoch(0);
            
            // Then
            assertEquals(initial, state.currentEpoch(), "Current epoch should remain unchanged when smaller values are observed");
        }
    }
}
