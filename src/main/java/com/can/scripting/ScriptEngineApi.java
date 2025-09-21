package com.can.scripting;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Script motorları ile önbellek operasyonlarını entegre etmek için küçük bir
 * kayıt defteri sunar. Kullanıcı fonksiyonları isimleriyle kaydedilir ve
 * çalıştırıldıklarında önbelleğe erişimi sağlayan bir bağlam objesi ile
 * parametreler gönderilir.
 */
public final class ScriptEngineApi<K,V>
{
    private final Map<String, BiFunction<Context<K,V>, Object[], Object>> funcs = new ConcurrentHashMap<>();

    /**
     * Script fonksiyonlarına geçirilen çalışma zamanı bağlamını temsil eder ve
     * önbellek işlemlerine erişimi soyutlar.
     */
    public static final class Context<K,V>
    {
        /**
         * Script'lerin anahtarlara müdahale etmesini sağlayan minimal önbellek API'sidir.
         */
        public interface CacheOps<K,V>
        {
            void set(K key, V value, Duration ttl);
            V get(K key);
            boolean del(K key);
        }
        private final CacheOps<K,V> ops;
        public Context(CacheOps<K,V> ops){ this.ops = ops; }
        public CacheOps<K,V> cache(){ return ops; }
    }

    public void register(String name, BiFunction<Context<K,V>, Object[], Object> fn)
    {
        funcs.put(name, fn);
    }
    public Object run(String name, Context<K,V> ctx, Object... args) {
        var f = funcs.get(name);
        if (f == null) throw new IllegalArgumentException("No function: " + name);
        return f.apply(ctx, args);
    }
}