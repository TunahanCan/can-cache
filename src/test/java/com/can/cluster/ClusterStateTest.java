package com.can.cluster;

import com.can.metric.MetricsRegistry;
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
    class KimlikBilgisi
    {
        // Bu test yerel düğüm kimliğinin ve bayt temsilinin doğru döndüğünü doğrular.
        @Test
        void local_node_id_ve_baytlarini_doner()
        {
            assertEquals("node-1", state.localNodeId());
            assertArrayEquals("node-1".getBytes(StandardCharsets.UTF_8), state.localNodeIdBytes());
        }
    }

    @Nested
    class EpochYonetimi
    {
        // Bu test bumpEpoch çağrısının değer artırdığını ve metrikleri güncellediğini gösterir.
        @Test
        void bump_epoch_degeri_arttirir()
        {
            long initial = state.currentEpoch();
            long next = state.bumpEpoch();
            assertEquals(initial + 1, next);
            assertEquals(1L, metrics.counter("cluster_epoch_increments").get());
        }

        // Bu test uzaktan gelen daha büyük epoch değerinin benimsendiğini doğrular.
        @Test
        void observe_epoch_daha_buyuk_degeri_kabul_eder()
        {
            long expected = state.currentEpoch() + 5;
            state.observeEpoch(expected);
            assertEquals(expected, state.currentEpoch());
            assertEquals(1L, metrics.counter("cluster_epoch_observed_updates").get());
        }

        // Bu test daha küçük veya geçersiz epoch değerlerinin dikkate alınmadığını doğrular.
        @Test
        void observe_epoch_kucuk_degerleri_yoksayar()
        {
            long initial = state.currentEpoch();
            state.observeEpoch(initial - 1);
            state.observeEpoch(0);
            assertEquals(initial, state.currentEpoch());
        }
    }
}
