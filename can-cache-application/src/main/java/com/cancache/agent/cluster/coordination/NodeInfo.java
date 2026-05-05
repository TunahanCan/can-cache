package com.cancache.agent.cluster.coordination;

/**
 * Represents essential information about a cluster node for discovery purposes.
 *
 * @param nodeId The unique identifier of the node.
 * @param host The advertised host of the node.
 * @param port The replication port of the node.
 */
public record NodeInfo(String nodeId, String host, int port) {
}