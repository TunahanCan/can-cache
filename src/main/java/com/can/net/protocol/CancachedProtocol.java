package com.can.net.protocol;

import java.time.Duration;
import java.util.Objects;

/**
 * Cancached sunucusunun kabullendiği protokolü temsil eden veri sınıfıdır.
 *
 * <p>Şu anda yalnızca storage komutlarının ayrıştırılması için kullanılmaktadır
 * ancak ileride farklı protokol türlerini genişletebilmek için isimlendirilmiş
 * bir veri modeli sağlar.</p>
 */
public record CancachedProtocol(String name, CacheRequest cacheRequest)
{
    public CancachedProtocol
    {
        this.name = Objects.requireNonNull(name, "name");
        // cacheRequest null olabilir; bu durumda protokol yalnızca komut ismi ile temsil edilir.
        if (cacheRequest != null && !Objects.equals(cacheRequest.command(), name)) {
            throw new IllegalArgumentException("Cache request command must match protocol name");
        }
    }

    public boolean expectsCacheRequest()
    {
        return cacheRequest != null;
    }

    public record CacheRequest(String command,
                               String key,
                               int flags,
                               Duration ttl,
                               int bytes,
                               boolean noreply,
                               boolean cas,
                               long casUnique)
    {
        private static final int CRLF_LENGTH = 2;

        public CacheRequest
        {
            this.command = Objects.requireNonNull(command, "command");
            this.key = Objects.requireNonNull(key, "key");
            if (bytes < 0) {
                throw new IllegalArgumentException("bytes must be greater than or equal to 0");
            }
        }

        public int totalLength()
        {
            return bytes + CRLF_LENGTH;
        }
    }
}
