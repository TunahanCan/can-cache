package com.can.cluster;

import com.can.metric.Counter;
import com.can.metric.MetricsRegistry;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kümeye ait kimlik ve sürüm bilgisini takip eden hafif durum nesnesidir. Üyelik
 * değiştikçe epoch değeri artırılır, uzak düğümlerden gelen epoch bilgileri
 * görüldüğünde daha büyük değerler benimsenir. Metrik kayıt defteri ile
 * entegredir ve kümeye dair gözlemlenebilirlik ihtiyacını destekler.
 */
public final class ClusterState
{
    private final String localNodeId;
    private final byte[] localNodeIdBytes;
    private final AtomicLong epoch = new AtomicLong(1L);
    private final Counter epochIncrements;
    private final Counter observedEpochUpdates;

    public ClusterState(String localNodeId, MetricsRegistry metrics)
    {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
        this.localNodeIdBytes = localNodeId.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (metrics != null) {
            this.epochIncrements = metrics.counter("cluster_epoch_increments");
            this.observedEpochUpdates = metrics.counter("cluster_epoch_observed_updates");
        } else {
            this.epochIncrements = null;
            this.observedEpochUpdates = null;
        }
    }

    public String localNodeId()
    {
        return localNodeId;
    }

    public byte[] localNodeIdBytes()
    {
        return localNodeIdBytes.clone();
    }

    public long currentEpoch()
    {
        return epoch.get();
    }

    public long bumpEpoch()
    {
        long value = epoch.incrementAndGet();
        if (epochIncrements != null) {
            epochIncrements.inc();
        }
        return value;
    }

    public void observeEpoch(long remoteEpoch)
    {
        if (remoteEpoch <= 0) {
            return;
        }
        epoch.updateAndGet(current -> {
            if (remoteEpoch > current) {
                if (observedEpochUpdates != null) {
                    observedEpochUpdates.inc();
                }
                return remoteEpoch;
            }
            return current;
        });
    }
}
