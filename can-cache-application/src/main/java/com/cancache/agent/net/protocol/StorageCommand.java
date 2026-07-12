package com.cancache.agent.net.protocol;

import java.util.Objects;

/**
 * İstemciden gövde verisi bekleyen komutları temsil eder.
 */
public record StorageCommand(PendingStorageCommand pending) implements CommandAction
{
    public StorageCommand
    {
        Objects.requireNonNull(pending, "pending");
    }
}
