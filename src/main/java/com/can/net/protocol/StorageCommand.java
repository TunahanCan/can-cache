package com.can.net.protocol;

import java.util.Objects;

/**
 * İstemciden gövde verisi bekleyen komutları temsil eder.
 */
public final class StorageCommand implements CommandAction
{
    private final CancachedProtocol protocol;

    private final PendingStorageCommand pending;

    public StorageCommand(CancachedProtocol protocol)
    {
        this.protocol = Objects.requireNonNull(protocol, "protocol");
        if (!protocol.expectsCacheRequest()) {
            throw new IllegalArgumentException("Protocol does not describe a cache request payload");
        }
        this.pending = new PendingStorageCommand(protocol.cacheRequest());
    }

    public StorageCommand(PendingStorageCommand pending)
    {
        this(new CancachedProtocol(pending.command(), pending.request()));
    }

    public CancachedProtocol protocol()
    {
        return protocol;
    }

    public PendingStorageCommand pending()
    {
        return pending;
    }
}
