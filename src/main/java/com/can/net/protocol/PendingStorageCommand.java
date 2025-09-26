package com.can.net.protocol;

import java.time.Duration;
import java.util.Objects;

/**
 * İstemciden okunacak gövde verisini tanımlar.
 */
public record PendingStorageCommand(CancachedProtocol.CacheRequest request)
{
    public PendingStorageCommand
    {
        this.request = Objects.requireNonNull(request, "request");
    }

    public PendingStorageCommand(String command,
                                 String key,
                                 int flags,
                                 Duration ttl,
                                 int bytes,
                                 boolean noreply,
                                 boolean isCas,
                                 long casUnique)
    {
        this(new CancachedProtocol.CacheRequest(command, key, flags, ttl, bytes, noreply, isCas, casUnique));
    }

    public String command()
    {
        return request.command();
    }

    public String key()
    {
        return request.key();
    }

    public int flags()
    {
        return request.flags();
    }

    public Duration ttl()
    {
        return request.ttl();
    }

    public int bytes()
    {
        return request.bytes();
    }

    public boolean noreply()
    {
        return request.noreply();
    }

    public boolean isCas()
    {
        return request.cas();
    }

    public long casUnique()
    {
        return request.casUnique();
    }

    public int totalLength()
    {
        return request.totalLength();
    }
}
