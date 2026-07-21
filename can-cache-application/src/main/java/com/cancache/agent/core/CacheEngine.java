package com.cancache.agent.core;

import com.cancache.agent.codec.Codec;
import com.cancache.agent.constants.NodeProtocol;
import com.cancache.agent.core.model.CacheValue;
import com.cancache.agent.core.model.CasDecision;
import com.cancache.agent.core.model.CasResult;
import com.cancache.agent.core.model.ExpiringKey;
import com.cancache.agent.metric.Counter;
import com.cancache.agent.metric.MetricsRegistry;
import com.cancache.agent.metric.Timer;
import com.cancache.agent.pubsub.Broker;
import io.vertx.core.Vertx;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Anahtar-değer çiftlerini segmentlere bölerek depolayan önbellek motorudur.
 * <p>
 * Temel özellikler:
 * <ul>
 *   <li>Segmentlere bölünmüş yapı ile yüksek eşzamanlılık</li>
 *   <li>TTL (Time-To-Live) desteği ile otomatik süre dolumu</li>
 *   <li>LRU/LFU eviction politikaları</li>
 *   <li>Compare-And-Swap (CAS) desteği</li>
 *   <li>Metrik toplama (hit/miss/eviction sayaçları, latency timer'ları)</li>
 *   <li>Pub/Sub entegrasyonu ile değişiklik bildirimleri</li>
 * </ul>
 *
 * @param <K> Anahtar tipi
 * @param <V> Değer tipi
 */
public final class CacheEngine<K, V> implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(CacheEngine.class);

    // ========================================
    // Sabitler
    // ========================================
    private static final long NO_EXPIRATION = 0L;
    private static final int MIN_EXPIRATION_COMPACTION_THRESHOLD = 64;

    // ========================================
    // Yapılandırma Alanları
    // ========================================
    private final int segmentCount;
    private final int maxCapacity;
    private final long maxWeightBytes;
    private final long cleanerPollMillis;
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;

    // ========================================
    // Çekirdek Veri Yapıları
    // ========================================
    private final CacheSegment<K>[] segments;
    private final DelayQueue<ExpiringKey> expirationQueue;
    private final int expirationCompactionThreshold;
    private final int expirationHardLimit;
    private final Object expirationMaintenanceLock = new Object();
    private final AtomicBoolean expirationCompactionScheduled = new AtomicBoolean();

    // ========================================
    // Bağımlılıklar
    // ========================================
    private final Vertx vertx;
    private final MetricsRegistry metricsRegistry;
    private final Broker eventBroker;

    // ========================================
    // Metrikler
    // ========================================
    private final Counter hitCounter;
    private final Counter missCounter;
    private final Counter evictionCounter;
    private final Timer getTimer;
    private final Timer setTimer;
    private final Timer deleteTimer;

    // ========================================
    // Dinleyiciler ve Durum
    // ========================================
    private final CopyOnWriteArrayList<RemovalListener<K>> removalListeners;
    private volatile long cleanerTimerId = -1L;

    // ========================================
    // Constructor (Private - Builder kullanılmalı)
    // ========================================

    @SuppressWarnings("unchecked")
    private CacheEngine(Builder<K, V> builder) {
        // Yapılandırma
        this.segmentCount = (int) Math.min(builder.segments,
                Math.min((long) builder.maxCapacity, builder.maxWeightBytes));
        this.maxCapacity = builder.maxCapacity;
        this.maxWeightBytes = builder.maxWeightBytes;
        this.cleanerPollMillis = builder.cleanerPollMillis;
        this.keyCodec = builder.keyCodec;
        this.valueCodec = builder.valueCodec;

        // Bağımlılıklar
        this.vertx = Objects.requireNonNull(builder.vertx, "Vertx instance gereklidir");
        this.metricsRegistry = builder.metricsRegistry;
        this.eventBroker = builder.broker;

        // Veri yapıları
        this.expirationQueue = new DelayQueue<>();
        long desiredCompactionThreshold = Math.max(MIN_EXPIRATION_COMPACTION_THRESHOLD,
                (long) maxCapacity * 2L);
        this.expirationCompactionThreshold = (int) Math.min(Integer.MAX_VALUE, desiredCompactionThreshold);
        this.expirationHardLimit = (int) Math.min(Integer.MAX_VALUE,
                Math.max((long) expirationCompactionThreshold + 1L,
                        (long) expirationCompactionThreshold * 2L));
        this.removalListeners = new CopyOnWriteArrayList<>();

        // Segmentleri oluştur
        this.segments = createSegments(builder);

        // Metrikleri başlat
        this.hitCounter = createCounter("cache_hits");
        this.missCounter = createCounter("cache_misses");
        this.evictionCounter = createCounter("cache_evictions");
        this.getTimer = createTimer("cache_get");
        this.setTimer = createTimer("cache_set");
        this.deleteTimer = createTimer("cache_del");

        // Arka plan temizleyiciyi başlat
        startExpirationCleaner();
    }

    @SuppressWarnings("unchecked")
    private CacheSegment<K>[] createSegments(Builder<K, V> builder) {
        CacheSegment<K>[] result = new CacheSegment[segmentCount];
        int capacityPerSegment = builder.maxCapacity / segmentCount;
        int remainder = builder.maxCapacity % segmentCount;
        long weightPerSegment = builder.maxWeightBytes / segmentCount;
        long weightRemainder = builder.maxWeightBytes % segmentCount;

        for (int i = 0; i < segmentCount; i++) {
            int segmentCapacity = capacityPerSegment + (i < remainder ? 1 : 0);
            long segmentWeight = weightPerSegment + (i < weightRemainder ? 1L : 0L);
            EvictionPolicy<K> evictionPolicy = builder.evictionPolicy.create(segmentCapacity);
            result[i] = new CacheSegment<>(segmentCapacity, segmentWeight, evictionPolicy, this::onKeyRemoved);
        }
        return result;
    }

    private Counter createCounter(String name) {
        return metricsRegistry != null ? metricsRegistry.counter(name) : null;
    }

    private Timer createTimer(String name) {
        return metricsRegistry != null ? metricsRegistry.timer(name) : null;
    }

    // ========================================
    // Builder Pattern
    // ========================================

    /**
     * Yeni bir CacheEngine builder'ı oluşturur.
     *
     * @param keyCodec   Anahtar serileştirme codec'i
     * @param valueCodec Değer serileştirme codec'i
     * @return Builder instance
     */
    public static <K, V> Builder<K, V> builder(Codec<K> keyCodec, Codec<V> valueCodec) {
        return new Builder<>(keyCodec, valueCodec);
    }

    /**
     * CacheEngine yapılandırma builder'ı.
     */
    public static final class Builder<K, V> {

        // Zorunlu alanlar
        private final Codec<K> keyCodec;
        private final Codec<V> valueCodec;

        // Opsiyonel alanlar (varsayılan değerlerle)
        private int segments = 8;
        private int maxCapacity = 10_000;
        private long maxWeightBytes = 256L * 1024L * 1024L;
        private long cleanerPollMillis = 100;
        private EvictionPolicyType evictionPolicy = EvictionPolicyType.LRU;
        private MetricsRegistry metricsRegistry;
        private Broker broker;
        private Vertx vertx;

        private Builder(Codec<K> keyCodec, Codec<V> valueCodec) {
            this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec gereklidir");
            this.valueCodec = Objects.requireNonNull(valueCodec, "valueCodec gereklidir");
        }

        /**
         * Segment sayısını ayarlar. Daha fazla segment = daha az lock contention.
         */
        public Builder<K, V> segments(int segments) {
            if (segments < 1) {
                throw new IllegalArgumentException("Segment sayısı en az 1 olmalıdır");
            }
            this.segments = segments;
            return this;
        }

        /**
         * Maksimum önbellek kapasitesini ayarlar.
         */
        public Builder<K, V> maxCapacity(int maxCapacity) {
            if (maxCapacity < 1) {
                throw new IllegalArgumentException("Kapasite en az 1 olmalıdır");
            }
            this.maxCapacity = maxCapacity;
            return this;
        }

        /**
         * Sets the approximate encoded-payload budget retained by the cache.
         */
        public Builder<K, V> maxWeightBytes(long maxWeightBytes) {
            if (maxWeightBytes < 1L) {
                throw new IllegalArgumentException("Maksimum ağırlık en az 1 byte olmalıdır");
            }
            this.maxWeightBytes = maxWeightBytes;
            return this;
        }

        /**
         * TTL temizleyici kontrol aralığını milisaniye cinsinden ayarlar.
         */
        public Builder<K, V> cleanerPollMillis(long millis) {
            if (millis < 1) {
                throw new IllegalArgumentException("Temizleyici aralığı en az 1ms olmalıdır");
            }
            this.cleanerPollMillis = millis;
            return this;
        }

        /**
         * Eviction politikasını ayarlar (LRU, LFU vb.).
         */
        public Builder<K, V> evictionPolicy(EvictionPolicyType policy) {
            this.evictionPolicy = Objects.requireNonNull(policy);
            return this;
        }

        /**
         * Metrik registry'sini ayarlar (opsiyonel).
         */
        public Builder<K, V> metrics(MetricsRegistry registry) {
            this.metricsRegistry = registry;
            return this;
        }

        /**
         * Event broker'ı ayarlar (opsiyonel).
         */
        public Builder<K, V> broker(Broker broker) {
            this.broker = broker;
            return this;
        }

        /**
         * Vertx instance'ı ayarlar (zorunlu).
         */
        public Builder<K, V> vertx(Vertx vertx) {
            this.vertx = Objects.requireNonNull(vertx, "Vertx gereklidir");
            return this;
        }

        /**
         * CacheEngine instance'ı oluşturur.
         */
        public CacheEngine<K, V> build() {
            return new CacheEngine<>(this);
        }
    }

    // ========================================
    // Temel Cache Operasyonları
    // ========================================

    /**
     * Önbelleğe değer ekler (TTL olmadan).
     *
     * @param key   Anahtar
     * @param value Değer
     * @return Başarılı ise true
     */
    public boolean set(K key, V value) {
        return set(key, value, null);
    }

    /**
     * Önbelleğe değer ekler.
     *
     * @param key   Anahtar
     * @param value Değer
     * @param ttl   Yaşam süresi (null = sonsuz)
     * @return Başarılı ise true
     */
    public boolean set(K key, V value, Duration ttl) {
        Objects.requireNonNull(key, "Anahtar null olamaz");
        Objects.requireNonNull(value, "Değer null olamaz");

        long startTime = System.nanoTime();
        try {
            long now = System.currentTimeMillis();
            long expireAt = calculateExpiration(ttl, now);
            int segmentIndex = getSegmentIndex(key);

            byte[] encodedValue = valueCodec.encode(value);
            CacheValue cacheValue = new CacheValue(encodedValue, expireAt);

            boolean stored = segments[segmentIndex].put(key, cacheValue);
            if (!stored) {
                return false;
            }

            // TTL varsa expiration queue'ya ekle
            if (expireAt > NO_EXPIRATION) {
                scheduleExpiration(key, segmentIndex, expireAt);
            }

            // Event yayınla
            publishEvent("keyspace:set", key);

            return true;
        } finally {
            recordTiming(setTimer, startTime);
        }
    }

    /**
     * Önbellekten değer okur.
     *
     * @param key Anahtar
     * @return Değer veya null (bulunamazsa)
     */
    public V get(K key) {
        Objects.requireNonNull(key, "Anahtar null olamaz");

        long startTime = System.nanoTime();
        try {
            CacheSegment<K> segment = getSegment(key);
            CacheValue cacheValue = segment.get(key);

            if (cacheValue == null) {
                incrementCounter(missCounter);
                return null;
            }

            // Süre dolmuş mu kontrol et
            if (cacheValue.expired(System.currentTimeMillis())) {
                if (segment.removeIfSame(key, cacheValue)) {
                    incrementCounter(evictionCounter);
                }
                incrementCounter(missCounter);
                return null;
            }

            incrementCounter(hitCounter);
            return valueCodec.decode(cacheValue.value());
        } finally {
            recordTiming(getTimer, startTime);
        }
    }

    /**
     * Önbellekten değer siler.
     *
     * @param key Anahtar
     * @return Silindiyse true
     */
    public boolean delete(K key) {
        Objects.requireNonNull(key, "Anahtar null olamaz");

        long startTime = System.nanoTime();
        try {
            return getSegment(key).remove(key) != null;
        } finally {
            recordTiming(deleteTimer, startTime);
        }
    }

    /**
     * Anahtarın var olup olmadığını kontrol eder.
     *
     * @param key Anahtar
     * @return Varsa ve süresi dolmamışsa true
     */
    public boolean exists(K key) {
        Objects.requireNonNull(key, "Anahtar null olamaz");

        CacheSegment<K> segment = getSegment(key);
        CacheValue cacheValue = segment.get(key);
        if (cacheValue == null) {
            return false;
        }
        if (!cacheValue.expired(System.currentTimeMillis())) {
            return true;
        }
        if (segment.removeIfSame(key, cacheValue)) {
            incrementCounter(evictionCounter);
        }
        return false;
    }

    /**
     * Tüm önbelleği temizler.
     */
    public void clear() {
        // Clear scheduled tasks first. A concurrent TTL write that starts after this
        // point keeps its own task; entries already present are removed below.
        synchronized (expirationMaintenanceLock) {
            expirationQueue.clear();
        }
        for (CacheSegment<K> segment : segments) {
            segment.clear();
        }
    }

    /**
     * Önbellekteki toplam eleman sayısını döner.
     */
    public int size() {
        int total = 0;
        for (CacheSegment<K> segment : segments) {
            total += segment.size();
        }
        return total;
    }

    /**
     * Returns the encoded payload bytes currently retained by all segments.
     */
    public long estimatedPayloadBytes() {
        long total = 0L;
        for (CacheSegment<K> segment : segments) {
            total += segment.weight();
        }
        return total;
    }

    // ========================================
    // Compare-And-Swap (CAS) Operasyonu
    // ========================================

    /**
     * Atomik compare-and-swap operasyonu yapar.
     *
     * @param key         Anahtar
     * @param newValue    Yeni değer
     * @param expectedCas Beklenen CAS değeri
     * @param ttl         Yeni TTL ({@code null} = süresiz, zero/negative = hemen expire)
     * @return Başarılı ise true
     */
    public boolean compareAndSwap(K key, V newValue, long expectedCas, Duration ttl) {
        Objects.requireNonNull(key, "Anahtar null olamaz");
        Objects.requireNonNull(newValue, "Değer null olamaz");

        long startTime = System.nanoTime();
        try {
            CacheSegment<K> segment = getSegment(key);
            int segmentIndex = getSegmentIndex(key);
            long now = System.currentTimeMillis();

            CasResult result = segment.compareAndSwap(key, existing -> {
                boolean createIfAbsent = expectedCas == NodeProtocol.CAS_EXPECT_ABSENT;

                if (existing == null) {
                    if (!createIfAbsent) {
                        return CasDecision.fail();
                    }
                    if (ttl != null && (ttl.isZero() || ttl.isNegative())) {
                        return CasDecision.noValueSuccess();
                    }
                    long newExpireAt = ttl == null ? NO_EXPIRATION : calculateExpiration(ttl, now);
                    return CasDecision.success(new CacheValue(valueCodec.encode(newValue), newExpireAt));
                }

                if (existing.expired(now)) {
                    if (createIfAbsent) {
                        if (ttl != null && (ttl.isZero() || ttl.isNegative())) {
                            return CasDecision.removeSuccess();
                        }
                        long newExpireAt = ttl == null ? NO_EXPIRATION : calculateExpiration(ttl, now);
                        return CasDecision.replaceExpired(
                                new CacheValue(valueCodec.encode(newValue), newExpireAt));
                    }
                    return CasDecision.expired();
                }

                if (createIfAbsent) {
                    return CasDecision.fail();
                }

                // CAS değerini kontrol et
                @SuppressWarnings("unchecked")
                String encoded = (String) valueCodec.decode(existing.value());
                StoredValueCodec.StoredValue stored = StoredValueCodec.decode(encoded);

                if (stored.cas() != expectedCas) {
                    return CasDecision.fail();
                }

                if (ttl != null && (ttl.isZero() || ttl.isNegative())) {
                    return CasDecision.removeSuccess();
                }

                // Yeni expiration hesapla
                long newExpireAt = ttl == null ? NO_EXPIRATION : calculateExpiration(ttl, now);

                byte[] encodedNewValue = valueCodec.encode(newValue);
                return CasDecision.success(new CacheValue(encodedNewValue, newExpireAt));
            });

            if (result.success()) {
                handleSuccessfulCas(key, segmentIndex, result.newValue());
            }

            return result.success();
        } finally {
            recordTiming(setTimer, startTime);
        }
    }

    private void handleSuccessfulCas(K key, int segmentIndex, CacheValue newValue) {
        if (newValue != null && newValue.expireAtMillis() > NO_EXPIRATION) {
            scheduleExpiration(key, segmentIndex, newValue.expireAtMillis());
        }
        if (newValue != null) {
            publishEvent("keyspace:set", key);
        }
    }

    // ========================================
    // Listener Yönetimi
    // ========================================

    /**
     * Anahtar silindiğinde çağrılacak listener ekler.
     *
     * @param listener Listener
     * @return Listener'ı kaldırmak için kullanılabilecek handle
     */
    public AutoCloseable onRemoval(RemovalListener<K> listener) {
        Objects.requireNonNull(listener, "Listener null olamaz");
        removalListeners.add(listener);
        return () -> removalListeners.remove(listener);
    }

    // ========================================
    // İterasyon ve Fingerprint
    // ========================================

    /**
     * Tüm geçerli (süresi dolmamış) entry'ler üzerinde iterasyon yapar.
     *
     * @param consumer Her entry için çağrılacak fonksiyon
     */
    public void forEachEntry(EntryConsumer<K> consumer) {
        Objects.requireNonNull(consumer, "Consumer null olamaz");

        long now = System.currentTimeMillis();
        for (CacheSegment<K> segment : segments) {
            segment.forEach((key, value) -> {
                if (!value.expired(now)) {
                    consumer.accept(key, value.value(), value.expireAtMillis());
                }
            });
        }
    }

    /**
     * Önbellek içeriğinin parmak izini hesaplar.
     * Anti-entropy senkronizasyonu için kullanılır.
     *
     * @return 64-bit parmak izi
     */
    public long fingerprint() {
        ArrayList<Long> entryHashes = new ArrayList<>();

        forEachEntry((key, value, expireAt) -> {
            long entryHash = computeEntryHash(key, value, expireAt);
            entryHashes.add(entryHash);
        });

        // Sıralayarak deterministik sonuç elde et
        Collections.sort(entryHashes);

        // Merkle-tree benzeri hash hesapla
        long hash = 1125899906842597L; // Büyük asal sayı
        for (long entryHash : entryHashes) {
            hash = 31L * hash + entryHash;
        }
        return 31L * hash + entryHashes.size();
    }

    private long computeEntryHash(K key, byte[] value, long expireAt) {
        long hash = 31L * key.hashCode() + Arrays.hashCode(value);
        return 31L * hash + Long.hashCode(expireAt);
    }

    // ========================================
    // Persistans Replay
    // ========================================

    /**
     * Persistans katmanından gelen kaydı tekrar oynatır.
     *
     * @param operation SET veya DELETE operasyonu
     * @param keyBytes  Serileştirilmiş anahtar
     * @param valueBytes Serileştirilmiş değer
     * @param expireAt  Süre dolum zamanı (epoch millis)
     */
    public void replay(byte[] operation, byte[] keyBytes, byte[] valueBytes, long expireAt) {
        Objects.requireNonNull(operation, "operation");
        if (operation.length != 1) {
            throw new IllegalArgumentException("Operation must contain exactly one command byte");
        }
        K key = keyCodec.decode(keyBytes);

        if (operation[0] == NodeProtocol.CMD_SET) {
            replaySet(key, valueBytes, expireAt);
        } else if (operation[0] == NodeProtocol.CMD_DELETE) {
            replayDelete(key);
        } else {
            throw new IllegalArgumentException("Unsupported replay command: " + operation[0]);
        }
    }

    private void replaySet(K key, byte[] value, long expireAt) {
        Objects.requireNonNull(key);

        int segmentIndex = getSegmentIndex(key);
        CacheSegment<K> segment = segments[segmentIndex];

        // Süre zaten dolmuşsa silip çık
        if (expireAt > NO_EXPIRATION && expireAt <= System.currentTimeMillis()) {
            segment.remove(key);
            return;
        }

        // Admission history is ignored during replay, but configured memory
        // bounds still apply. Never retain an older value when the latest log
        // record cannot fit, as that would resurrect stale data.
        if (!segment.putForce(key, new CacheValue(value, expireAt))) {
            segment.remove(key);
            LOG.warnf("Skipped replay value for key %s because it exceeds the cache byte budget", key);
            return;
        }
        if (expireAt > NO_EXPIRATION) {
            scheduleExpiration(key, segmentIndex, expireAt);
        }
    }

    private void replayDelete(K key) {
        getSegment(key).remove(key);
    }

    // ========================================
    // Arka Plan Temizleyici
    // ========================================

    private void startExpirationCleaner() {
        cleanerTimerId = vertx.setPeriodic(cleanerPollMillis, _ ->
                vertx.executeBlocking(() -> {
                    processExpiredKeys();
                    return null;
                })
        );
    }

    private void processExpiredKeys() {
        try {
            ExpiringKey expiredKey;
            while ((expiredKey = expirationQueue.poll()) != null) {
                evictExpiredKey(expiredKey);
            }
        } catch (Exception e) {
            LOG.warn("Expiration cleaner failed; the next run will retry pending entries", e);
        }
    }

    private void scheduleExpiration(K key, int segmentIndex, long expireAtMillis) {
        synchronized (expirationMaintenanceLock) {
            expirationQueue.offer(new ExpiringKey(key, segmentIndex, expireAtMillis));
            // The async compactor keeps the write path cheap under normal load. A
            // synchronous safety valve prevents a hot key from allocating stale
            // delay entries faster than the worker pool can compact them.
            if (expirationQueue.size() > expirationHardLimit) {
                compactExpirationQueueLocked();
            }
        }
        requestExpirationCompactionIfNeeded();
    }

    private void requestExpirationCompactionIfNeeded() {
        if (expirationQueue.size() <= expirationCompactionThreshold
                || !expirationCompactionScheduled.compareAndSet(false, true)) {
            return;
        }
        vertx.executeBlocking(() -> {
            compactExpirationQueue();
            return null;
        }).onComplete(result -> {
            expirationCompactionScheduled.set(false);
            if (result.failed()) {
                LOG.warn("Expiration queue compaction failed", result.cause());
            }
            if (expirationQueue.size() > expirationCompactionThreshold) {
                requestExpirationCompactionIfNeeded();
            }
        });
    }

    void compactExpirationQueue() {
        synchronized (expirationMaintenanceLock) {
            compactExpirationQueueLocked();
        }
    }

    private void compactExpirationQueueLocked() {
        Set<ExpirationIdentity> live = new HashSet<>(Math.max(16, size()));
        for (int i = 0; i < segments.length; i++) {
            final int segmentIndex = i;
            segments[i].forEach((key, value) -> {
                if (value.expireAtMillis() > NO_EXPIRATION) {
                    live.add(new ExpirationIdentity(key, segmentIndex, value.expireAtMillis()));
                }
            });
        }

        Set<ExpirationIdentity> retained = new HashSet<>(live.size());
        expirationQueue.removeIf(expiringKey -> {
            ExpirationIdentity identity = new ExpirationIdentity(
                    expiringKey.key(), expiringKey.segmentIndex(), expiringKey.expireAtMillis());
            return !live.contains(identity) || !retained.add(identity);
        });
    }

    int scheduledExpirationCount() {
        return expirationQueue.size();
    }

    @SuppressWarnings("unchecked")
    private void evictExpiredKey(ExpiringKey expiredKey) {
        CacheSegment<K> segment = segments[expiredKey.segmentIndex()];
        K key = (K) expiredKey.key();

        if (segment.removeIfMatches(key, expiredKey.expireAtMillis())) {
            incrementCounter(evictionCounter);
        }
    }

    // ========================================
    // Yardımcı Metodlar
    // ========================================

    private int getSegmentIndex(Object key) {
        return (key.hashCode() & 0x7FFFFFFF) % segmentCount;
    }

    private CacheSegment<K> getSegment(Object key) {
        return segments[getSegmentIndex(key)];
    }

    private long calculateExpiration(Duration ttl, long now) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return NO_EXPIRATION;
        }

        long ttlMillis;
        try {
            ttlMillis = ttl.toMillis();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
        if (ttlMillis <= 0L) {
            return NO_EXPIRATION;
        }
        if (ttlMillis > Long.MAX_VALUE - now) {
            return Long.MAX_VALUE;
        }
        return now + ttlMillis;
    }

    private void onKeyRemoved(K key) {
        // Event yayınla
        if (eventBroker != null) {
            eventBroker.publish("keyspace:del", keyCodec.encode(key));
        }

        // Listener'ları bilgilendir
        for (RemovalListener<K> listener : removalListeners) {
            try {
                listener.onRemoval(key);
            } catch (RuntimeException e) {
                LOG.debug("Cache removal listener failed", e);
            }
        }
    }

    private void publishEvent(String topic, K key) {
        if (eventBroker != null) {
            eventBroker.publish(topic, keyCodec.encode(key));
        }
    }

    private void incrementCounter(Counter counter) {
        if (counter != null) {
            counter.inc();
        }
    }

    private void recordTiming(Timer timer, long startTimeNanos) {
        if (timer != null) {
            timer.record(System.nanoTime() - startTimeNanos);
        }
    }

    // ========================================
    // Lifecycle
    // ========================================

    @Override
    public void close() {
        if (cleanerTimerId >= 0L) {
            vertx.cancelTimer(cleanerTimerId);
            cleanerTimerId = -1L;
        }
    }

    // ========================================
    // Fonksiyonel Arayüzler
    // ========================================

    /**
     * Anahtar silindiğinde çağrılacak listener arayüzü.
     */
    @FunctionalInterface
    public interface RemovalListener<K> {
        void onRemoval(K key);
    }

    /**
     * Entry iterasyonu için consumer arayüzü.
     */
    @FunctionalInterface
    public interface EntryConsumer<K> {
        void accept(K key, byte[] value, long expireAtMillis);
    }

    private record ExpirationIdentity(Object key, int segmentIndex, long expireAtMillis) {}
}
