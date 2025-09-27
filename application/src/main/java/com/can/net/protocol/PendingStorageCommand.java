package com.can.net.protocol;

import java.time.Duration;

/**
 * İstemciden okunacak gövde verisini tanımlar.
 */
public record PendingStorageCommand(String command,
                                    String key,
                                    int flags,
                                    Duration ttl,
                                    int bytes,
                                    boolean noreply,
                                    boolean isCas,
                                    long casUnique)
{
    private static final int CRLF_LENGTH = 2;

    public int totalLength()
    {
        return bytes + CRLF_LENGTH;
    }
}
