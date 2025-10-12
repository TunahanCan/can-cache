package com.can.loadbalancer;

/**
 * Kümedeki her bir can-cache örneğinin istemci trafiğini kabul ettiği TCP uç
 * noktasını temsil eder. Yük dengeleyici, gelen bağlantıları bu uç noktalara
 * yönlendirmek için {@link ClusterMembershipView} tarafından sağlanan listeleri
 * kullanır.
 */
public record BackendEndpoint(String nodeId, String host, int port) { }
