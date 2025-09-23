package com.can.core;

import com.can.codec.Codec;
import com.can.metric.Counter;
import com.can.metric.MetricsRegistry;
import com.can.metric.Timer;
import com.can.pubsub.Broker;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Anahtar-değer çiftlerini segmentlere bölerek depolayan, TTL yönetimi yapan,
 * metrik ve yayın mekanizmalarıyla entegre çalışan önbellek motorudur. Verileri
 * serileştirmek için codec'ler kullanır, arka planda TTL kuyruğunu temizleyen
 * bir görev çalıştırır ve persistans katmanından gelen kayıtları tekrar oynatabilir.
 */
public final class CacheEngine<K,V> implements AutoCloseable
{

    private final int segments;
    private final CacheSegment<K>[] table;
    private final DelayQueue<ExpiringKey> ttlQueue = new DelayQueue<>();
    private final ScheduledExecutorService cleaner; // virtual thread scheduled
    private final long cleanerPollMillis;

    private final Codec<K> keyCodec;
    private final Codec<V> valCodec;
    private final MetricsRegistry metrics;   // nullable
    private final Broker broker;             // nullable

    private final Counter hits, misses, evictions;
    private final Timer tGet, tSet, tDel;
    private final CopyOnWriteArrayList<RemovalListener<K>> removalListeners = new CopyOnWriteArrayList<>();

    @SuppressWarnings("unchecked")
    private CacheEngine(int segments, int maxCapacity, long cleanerPollMillis,
                        EvictionPolicyType evictionPolicy,
                        Codec<K> keyCodec, Codec<V> valCodec,
                        MetricsRegistry metrics, Broker broker) {
        this.segments = segments;
        this.table = new CacheSegment[segments];
        int per = Math.max(1, maxCapacity / segments);
        for (int i=0;i<segments;i++) table[i] = new CacheSegment<>(per, evictionPolicy.create(per), this::notifyRemoval);

        this.cleanerPollMillis = cleanerPollMillis;
        this.keyCodec = keyCodec; this.valCodec = valCodec;
        this.metrics = metrics; this.broker = broker;

        if (metrics != null){
            this.hits = metrics.counter("cache_hits");
            this.misses = metrics.counter("cache_misses");
            this.evictions = metrics.counter("cache_evictions");
            this.tGet = metrics.timer("cache_get");
            this.tSet = metrics.timer("cache_set");
            this.tDel = metrics.timer("cache_del");
        } else {
            this.hits=this.misses=this.evictions=null; this.tGet=this.tSet=this.tDel=null;
        }

        this.cleaner = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
        startCleaner();
    }

    public static <K,V> Builder<K,V> builder(Codec<K> keyCodec, Codec<V> valCodec) { return new Builder<>(keyCodec, valCodec); }

    /**
     * Önbellek motorunun kurulumu sırasında segment sayısı, kapasite gibi
     * parametreleri ayarlamaya yarayan akıcı yapılandırma sınıfıdır.
     */
    public static final class Builder<K,V>
    {
        private int segments = 8, maxCapacity = 10_000; private long cleanerPollMillis = 100;
        private final Codec<K> keyCodec; private final Codec<V> valCodec;
        private MetricsRegistry metrics; private Broker broker;
        private EvictionPolicyType evictionPolicy = EvictionPolicyType.LRU;
        public Builder(Codec<K> keyCodec, Codec<V> valCodec){ this.keyCodec=keyCodec; this.valCodec=valCodec; }
        public Builder<K,V> segments(int s){ this.segments=s; return this; }
        public Builder<K,V> maxCapacity(int c){ this.maxCapacity=c; return this; }
        public Builder<K,V> cleanerPollMillis(long ms){ this.cleanerPollMillis=ms; return this; }
        public Builder<K,V> metrics(MetricsRegistry m){ this.metrics=m; return this; }
        public Builder<K,V> broker(Broker b){ this.broker=b; return this; }
        public Builder<K,V> evictionPolicy(EvictionPolicyType p){ this.evictionPolicy=Objects.requireNonNull(p); return this; }
        public CacheEngine<K,V> build(){ return new CacheEngine<>(segments, maxCapacity, cleanerPollMillis, evictionPolicy,
                keyCodec, valCodec, metrics, broker); }
    }
    private int segIndex(Object key){ return (key.hashCode() & 0x7fffffff) % segments; }
    private CacheSegment<K> seg(Object key){ return table[segIndex(key)]; }

    private void startCleaner() {
        cleaner.scheduleWithFixedDelay(() -> {
            try {
                ExpiringKey ek;
                while ((ek = ttlQueue.poll()) != null) {
                    CacheSegment<K> segment = table[ek.segmentIndex()];
                    if (segment.removeIfMatches((K) ek.key(), ek.expireAtMillis()) && evictions != null) {
                        evictions.inc();
                    }
                }
            } catch (Throwable ignored) {}
        }, cleanerPollMillis, cleanerPollMillis, TimeUnit.MILLISECONDS);
    }

    private void notifyRemoval(K key) {
        if (broker != null) {
            broker.publish("keyspace:del", keyCodec.encode(key));
        }
        for (RemovalListener<K> listener : removalListeners) {
            try {
                listener.onRemoval(key);
            } catch (RuntimeException ignored) {
            }
        }
    }

    public AutoCloseable onRemoval(RemovalListener<K> listener) {
        Objects.requireNonNull(listener);
        removalListeners.add(listener);
        return () -> removalListeners.remove(listener);
    }

    public boolean set(K key, V value){
        return set(key, value, null);
    }

    public boolean set(K key, V value, Duration ttl)
    {
        long t0 = System.nanoTime();
        Objects.requireNonNull(key);
        long now = System.currentTimeMillis();
        long expireAt = computeExpireAt(ttl, now);
        int idx = segIndex(key);
        boolean stored = seg(key).put(key, new CacheValue(valCodec.encode(value), expireAt));
        if (!stored) {
            if (tSet != null) tSet.record(System.nanoTime() - t0);
            return false;
        }
        if (expireAt > 0) ttlQueue.offer(new ExpiringKey(key, idx, expireAt));
        if (broker != null) broker.publish("keyspace:set", keyCodec.encode(key));
        if (tSet != null) tSet.record(System.nanoTime() - t0);
        return true;
    }

    public boolean compareAndSwap(K key, V value, long expectedCas, Duration ttl) {
        long t0 = System.nanoTime();
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        CacheSegment<K> segment = seg(key);
        int idx = segIndex(key);
        long now = System.currentTimeMillis();
        CacheSegment.CasResult result = segment.compareAndSwap(key, existing -> {
            if (existing == null) {
                return CacheSegment.CasDecision.fail();
            }
            if (existing.expired(now)) {
                return CacheSegment.CasDecision.expired();
            }
            @SuppressWarnings("unchecked")
            String encoded = (String) valCodec.decode(existing.value());
            StoredValueCodec.StoredValue stored = StoredValueCodec.decode(encoded);
            if (stored.cas() != expectedCas) {
                return CacheSegment.CasDecision.fail();
            }
            long expireAt = existing.expireAtMillis();
            if (ttl != null) {
                expireAt = computeExpireAt(ttl, now);
            }
            return CacheSegment.CasDecision.success(new CacheValue(valCodec.encode(value), expireAt));
        });
        boolean success = result.success();
        if (success) {
            CacheValue newValue = result.newValue();
            if (newValue != null && newValue.expireAtMillis() > 0) {
                ttlQueue.offer(new ExpiringKey(key, idx, newValue.expireAtMillis()));
            }
            if (broker != null) {
                broker.publish("keyspace:set", keyCodec.encode(key));
            }
        }
        if (tSet != null) {
            tSet.record(System.nanoTime() - t0);
        }
        return success;
    }

    private long computeExpireAt(Duration ttl, long now) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return 0L;
        }
        long expireAt = now + ttl.toMillis();
        if (expireAt <= 0L) {
            return Long.MAX_VALUE;
        }
        return expireAt;
    }

    public V get(K key)
    {
        long t0 = System.nanoTime();
        CacheValue cv = seg(key).get(key);
        V out = null;
        if (cv != null){
            if (cv.expired(System.currentTimeMillis())) {
                delete(key);
                if (misses != null) misses.inc();
            } else {
                out = valCodec.decode(cv.value());
                if (hits != null) hits.inc();
            }
        } else if (misses != null) misses.inc();
        if (tGet != null) tGet.record(System.nanoTime() - t0);
        return out;
    }

    public boolean delete(K key)
    {
        long t0 = System.nanoTime();
        boolean ok = seg(key).remove(key) != null;
        if (ok && broker != null) broker.publish("keyspace:del", keyCodec.encode(key));
        if (tDel != null) tDel.record(System.nanoTime() - t0);
        return ok;
    }

    public void clear() {
        for (CacheSegment<K> segment : table) {
            segment.clear();
        }
        ttlQueue.clear();
    }

    public boolean exists(K key){
        CacheValue cv = seg(key).get(key);
        return cv != null && !cv.expired(System.currentTimeMillis());
    }

    public int size(){ int t=0; for (CacheSegment<K> s : table) t += s.size(); return t; }

    public void forEachEntry(EntryConsumer<K> consumer) {
        Objects.requireNonNull(consumer);
        long now = System.currentTimeMillis();
        for (CacheSegment<K> segment : table) {
            segment.forEach((key, value) -> {
                if (!value.expired(now)) {
                    consumer.accept(key, value.value(), value.expireAtMillis());
                }
            });
        }
    }

    public long fingerprint()
    {
        final long[] hash = {1125899906842597L};
        forEachEntry((key, value, expireAt) -> {
            long entryHash = 31L * key.hashCode() + Arrays.hashCode(value);
            entryHash = 31L * entryHash + Long.hashCode(expireAt);
            hash[0] = 31L * hash[0] + entryHash;
        });
        return hash[0];
    }

    // Replay entry from persistence layer
    public void replay(byte[] op, byte[] k, byte[] v, long expireAt){
        K key = keyCodec.decode(k);
        if (op[0] == 'S') {
            applyReplayEntry(key, v, expireAt);
        } else if (op[0] == 'D') {
            applyReplayDelete(key);
        }
    }

    private void applyReplayEntry(K key, byte[] value, long expireAt) {
        Objects.requireNonNull(key);
        int idx = segIndex(key);
        CacheSegment<K> segment = table[idx];
        if (expireAt > 0 && expireAt <= System.currentTimeMillis()) {
            segment.remove(key);
            return;
        }
        if (segment.putForce(key, new CacheValue(value, expireAt)) && expireAt > 0)
            ttlQueue.offer(new ExpiringKey(key, idx, expireAt));
    }

    private void applyReplayDelete(K key) {
        seg(key).remove(key);
    }

    @Override
    public void close(){
        cleaner.shutdownNow();
    }

    @FunctionalInterface
    public interface RemovalListener<K> {
        void onRemoval(K key);
    }

    /**
     * Persistans katmanına veri aktarımı yapılırken her anahtar için çağrılan fonksiyonel arayüzdür.
     */
    @FunctionalInterface
    public interface EntryConsumer<K>
    {
        void accept(K key, byte[] value, long expireAtMillis);
    }
}
