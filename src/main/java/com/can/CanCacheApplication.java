package com.can;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;

/**
 * Quarkus uygulaması için giriş noktasıdır. Ana thread'i Quarkus runtime üzerinde
 * tutarak servislerin ayakta kalmasını sağlar ve komut satırından başlatılan
 * süreçler için çıkış kodunu yönetir.
 */
@QuarkusMain
public class CanCacheApplication implements QuarkusApplication
{
    @Override
    public int run(String... args)
    {
        Quarkus.waitForExit();
        return 0;
    }

    static void main(String... args) {
        Quarkus.run(CanCacheApplication.class, args);
    }
}
