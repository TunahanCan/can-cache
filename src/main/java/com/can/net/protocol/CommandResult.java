package com.can.net.protocol;

import io.vertx.core.buffer.Buffer;

/**
 * Bir komutun sonucunda istemciye gönderilecek cevabı ve bağlantı durumunu belirtir.
 */
public record CommandResult(Buffer response, boolean keepAlive)
{
    public static CommandResult continueWith(Buffer response)
    {
        return new CommandResult(response, true);
    }

    public static CommandResult continueWithoutResponse()
    {
        return new CommandResult(null, true);
    }

    public static CommandResult terminate()
    {
        return new CommandResult(null, false);
    }
}
