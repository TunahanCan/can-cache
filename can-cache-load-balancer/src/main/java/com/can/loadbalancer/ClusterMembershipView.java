package com.can.loadbalancer;

import jakarta.inject.Singleton;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Yük dengeleyicinin proxy edeceği arka uç uç noktalarının anlık görüntüsünü
 * sağlar. Düğümlerin katılımı, ayrılması veya zaman aşımı gibi durumlar,
 * multicast duyurularını dinleyen {@link ClusterAnnouncementListener} tarafından
 * güncellenir ve {@link CanCacheLoadBalancer} bağlantıları bu görünüm üzerinden
 * yönlendirir.
 */
@Singleton
public class ClusterMembershipView
{
    private final Map<String, BackendEndpoint> endpoints = new ConcurrentHashMap<>();

    private volatile List<BackendEndpoint> snapshot = List.of();

    public void upsert(String nodeId, String host, int port)
    {
        if (port <= 0) return;
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(host, "host");
        endpoints.put(nodeId, new BackendEndpoint(nodeId, host, port));
        snapshot = List.copyOf(endpoints.values());
    }

    public void remove(String nodeId)
    {
        if (nodeId == null) {
            return;
        }
        endpoints.remove(nodeId);
        snapshot = List.copyOf(endpoints.values());
    }

    public List<BackendEndpoint> snapshot()
    {
        return snapshot;
    }

    public void clear()
    {
        endpoints.clear();
        snapshot = List.of();
    }
}
