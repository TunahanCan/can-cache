package com.can.scripting;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ScriptEngineApi kayıtlı fonksiyonları cache operasyonlarıyla çalıştırır, ScriptRegistry ise isteğe bağlı JS motoru sağlar.
 * Testler API'nin bağlamı nasıl kullandığını ve JS motorunun var/yok durumunu açıklayan yorumlarla birlikte sunar.
 */
class ScriptingComponentsTest {

    @Nested
    class ScriptEngineApiBehaviour {
        /**
         * register-run akışı fonksiyonu isimle eşleyip Context üzerinden cache işlemleri yaptırır.
         * Burada basit bir toplama fonksiyonu tanımlayıp set/get/del kombinasyonunu doğruluyoruz.
         */
        @Test
        void executesRegisteredFunctionsWithContext() {
            ScriptEngineApi<String, String> api = new ScriptEngineApi<>();
            AtomicReference<String> stored = new AtomicReference<>();
            AtomicReference<Boolean> deleted = new AtomicReference<>(false);
            ScriptEngineApi.Context.CacheOps<String, String> ops = new ScriptEngineApi.Context.CacheOps<>() {
                @Override
                public void set(String key, String value, Duration ttl) {
                    stored.set(key + value + ttl.toMillis());
                }

                @Override
                public String get(String key) {
                    return stored.get();
                }

                @Override
                public boolean del(String key) {
                    deleted.set(true);
                    return true;
                }
            };

            api.register("demo", (ctx, args) -> {
                ctx.cache().set("k", "v", Duration.ofSeconds(1));
                String read = ctx.cache().get("k");
                ctx.cache().del("k");
                return read;
            });

            Object result = api.run("demo", new ScriptEngineApi.Context<>(ops));
            assertEquals("kv1000", stored.get());
            assertEquals("kv1000", result);
            assertTrue(deleted.get());
        }
    }

    @Nested
    class ScriptRegistryBehaviour {
        /**
         * ScriptRegistry JVM'de JavaScript motoru varsa jsAvailable true döndürür ve kod çalıştırabilir.
         * Motor yoksa IllegalStateException fırlatır; her iki durum da kontrollü şekilde ele alınır.
         */
        @Test
        void handlesOptionalJavaScriptEngine() {
            ScriptRegistry registry = new ScriptRegistry();
            if (registry.jsAvailable()) {
                Map<String, Object> bindings = new HashMap<>();
                bindings.put("x", 2);
                Object value = registry.runJs("x + 3", bindings);
                assertEquals(5.0, value);
            } else {
                assertThrows(IllegalStateException.class, () -> registry.runJs("1+1", Map.of()));
            }
        }
    }
}
