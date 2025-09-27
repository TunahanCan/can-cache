package com.can.net.protocol;

import java.util.Objects;

/**
 * İstemciden gövde verisi bekleyen komutları temsil eder.
 */
public final class StorageCommand implements CommandAction
{
    private final PendingStorageCommand pending;

    public StorageCommand(PendingStorageCommand pending)
    {
        this.pending = Objects.requireNonNull(pending, "pending");
    }

    public PendingStorageCommand pending()
    {
        return pending;
    }
}
