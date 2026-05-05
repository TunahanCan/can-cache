package com.cancache.agent.cluster.coordination;

import java.util.Set;
import java.util.function.Consumer;

/**
 * Defines the contract for cluster node discovery strategies.
 * Implementations are responsible for finding other nodes and notifying
 * the system about membership changes.
 */
public interface DiscoveryStrategy extends AutoCloseable
{

    /**
     * Initializes and starts the discovery process.
     *
     * @param membershipListener A callback to notify about changes in the discovered node set.
     */
    void start(Consumer<Set<NodeInfo>> membershipListener);

    /**
     * Announces the presence of the local node to the cluster (if applicable for the strategy).
     * This might involve sending heartbeats or registering with a central service.
     */
    void announce();

    /**
     * Returns the current set of discovered nodes.
     * @return An immutable set of currently known nodes.
     */
    Set<NodeInfo> getDiscoveredNodes();

    /**
     * Closes any resources held by the discovery strategy.
     */
    @Override
    void close();
}