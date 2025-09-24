# can-cache

<p align="center">
  <img src="https://img.shields.io/badge/quarkus-3.x-4695EB?logo=quarkus&logoColor=white" alt="Built with Quarkus 3" />
  <img src="https://img.shields.io/badge/protocol-cancached-orange" alt="Protocol compatible" />
  <img src="https://img.shields.io/badge/distribution-consistent%20hashing-4c1" alt="Consistent hashing" />
</p>

> **can-cache**, cancached metin protokolü ile %100 uyumlu, Quarkus 3 tabanlı hafif ama küme ölçekli bir bellek içi anahtar–değer sunucusudur. Tek JVM ile başlayıp saniyeler içinde tutarlı hash halkasına katılan, gecikmeye duyarlı replikasyon ve anlık snapshot alma özellikleriyle modern uygulamaların cache katmanına güç verir.

---

## İçindekiler
- [Animasyonlu Yaşam Döngüsü](#animasyonlu-yaşam-döngüsü)
- [Öne Çıkan Özellikler](#öne-çıkan-özellikler)
- [Neden can-cache?](#neden-can-cache)
- [Mimari Anahat](#mimari-anahat)
- [2 Dakikada Demo](#2-dakikada-demo)
- [Küme Kurulum Örneği](#küme-kurulum-örneği)
- [Yapılandırma Sihirbazı](#yapılandırma-sihirbazı)
- [Proje Yapısı](#proje-yapısı)
- [Yol Haritası](#yol-haritası)
- [Katkı & Geri Bildirim](#katkı--geri-bildirim)
- [Lisans](#lisans)

## Animasyonlu Yaşam Döngüsü

<p align="center">
  <img src="docs/assets/cluster-lifecycle.svg" alt="İki node'lu can-cache veri yolculuğu animasyonu" width="860" />
</p>

Yukarıdaki animasyon, ikinci node kümeye katıldığında bir `set` isteğinin istemciden çıkıp quorum onayına kadar izlediği sekiz karelik rotayı özetler:

1. **Node keşfi:** Multicast heartbeat ile Node B tanınır ve tutarlı hash halkasına eklenir.
2. **Bootstrap:** `ReplicationServer` snapshot/ipucu akışıyla yeni node'u sıcak yedeğe çevirir.
3. **İstek kabulü:** `CanCachedServer` protokol satırını ayrıştırır ve `ClusterClient` çağrısını hazırlar.
4. **Replika seçimi:** Hash halkası lideri (Node A) ve takipçiyi (Node B) belirler.
5. **Yerel yazım:** Lider `CacheEngine` segmentine yazar, TTL ve metrikler güncellenir.
6. **Uzak çoğaltma:** `RemoteNode` vekili komutu Node B `ReplicationServer`ına aktarır.
7. **Quorum cevabı:** Yeterli ACK sonrası `STORED` istemciye döner.
8. **Art alan:** TTL temizleme, hinted handoff ve anti-entropy döngüsü sürekli işler.

## Öne Çıkan Özellikler

### ⚡ Protokol & Performans
- cancached metin protokolünün tüm çekirdek komutlarını (`set/add/replace/append/prepend/cas/get/gets/delete/incr/decr/touch/flush_all/stats/version/quit`) bire bir uygular, 1 MB üzerindeki yükleri reddeder ve 30 günü aşan TTL değerlerini epoch olarak yorumlar.
- CAS sayaçları atomik olarak üretilir; `StoredValueCodec` sayesinde CAS, bayrak ve TTL tek bir Base64 dizesinde taşınır.
- Segmentlenmiş `CacheEngine` ile seçilebilir LRU ya da TinyLFU tahliye politikaları, milisaniye hassasiyetinde TTL temizliği ve yüksek isabet oranı sağlar.

### 🛡️ Dayanıklılık & Tutarlılık
- Sanal düğüm destekli **tutarlı hash halkası** üzerinde çalışan `ClusterClient`, replikasyon faktörü kadar kopyayı deterministik biçimde seçer ve yazmaları çoğunluk quorum'una taşır.
- `HintedHandoffService`, başarısız kopyalar için ipuçlarını kalıcılaştırıp node geri döndüğünde otomatik oynatır; veri kayıplarını en aza indirir.
- `SnapshotScheduler`, RDB benzeri dosya formatıyla periyodik snapshot alır; uygulama yeniden başladığında belleği aynı dosyadan doldurur.

### 🔍 Gözlemlenebilirlik & Operasyon
- `MetricsRegistry` + `MetricsReporter`, mikro saniye hassasiyetinde sayaç ve zamanlayıcı istatistiklerini periyodik olarak raporlar.
- `Broker` yayınla-abone ol modeliyle `keyspace:set` ve `keyspace:del` olaylarını servis eder; `CacheEngine.onRemoval` abonelikleri ile cache yaşam döngüsü izlenebilir.
- Multicast tabanlı `CoordinationService`, yeni JVM örneklerini otomatik keşfeder, zaman aşımına uğrayan node'ları temizler.

## Neden can-cache?
- **Ciddi üretim senaryoları için tasarlandı:** Gecikmeye duyarlı replikasyon, hinted handoff ve anti-entropy döngüleri ile ağ kesintilerini tolere eder.
- **Modern JVM özelliklerinden faydalanır:** Sanal thread'ler, reaktif IO ve Quarkus ekosisteminin hızını kullanır.
- **Basit kurulum, hızlı ölçekleme:** Tek bir komutla ayağa kalkar; yeni node'lar multicast ile kümeye otomatik katılır.
- **Genişletilebilir çekirdek:** Yeni codec'ler, tahliye stratejileri ve gözlemleyiciler kolayca eklenebilir.

## Mimari Anahat

```mermaid
flowchart LR
    subgraph Client
        MC[cancached istemcisi]
    end
    MC -- TCP komutları --> S[CanCachedServer]
    S -- yönlendirme --> CC[ClusterClient]
    CC -- hash --> R[ConsistentHashRing]
    R -- yerel --> LN[(Yerel Node \nCacheEngine)]
    R -- uzak --> RN[(RemoteNode vekilleri)]
    RN --> RS[ReplicationServer]
    LN --> CE[CacheEngine]
    RS --> CE
    CE -- TTL temizliği --> DQ[DelayQueue]
    CE -- snapshot --> Snap[SnapshotFile/Scheduler]
    CE -- metrik/olay --> Obs[MetricsRegistry & Broker]
```

### Katmanlar
- **Komut işleme:** `CanCachedServer`, Quarkus ayaklandığında konfigüre edilen portu dinler, satır bazlı ayrıştırma yapar ve cancached protokolünün kenar durumlarını (CAS çakışması, `noreply`, `flush_all` gecikmesi vb.) bire bir uygular.
- **Kümeleme:** `ConsistentHashRing`, `HashFn` implementasyonu ile sanal düğümler kullanır; `CoordinationService` multicast kalp atışları ile üyeleri güncel tutar.
- **Replikasyon:** `RemoteNode` kısa ömürlü soketlerle `ReplicationServer`'a bağlanır; `'S'/'G'/'D'/'X'/'C'` komutlarıyla veri aktarımı yaparken fingerprint karşılaştırmaları ile tutarlılığı doğrular.
- **Bellek motoru:** `CacheEngine`, segmentler, TTL kuyruğu (`DelayQueue<ExpiringKey>`) ve CAS işlemlerini tek noktada yönetir; `AutoCloseable` aboneliklerle `curr_items` gibi istatistikler güncel tutulur.
- **Kalıcılık & gözlemlenebilirlik:** `SnapshotFile` atomik dosya taşımayla tutarlılığı korur; `MetricsReporter` rapor periyodu > 0 olduğunda saniyeler içinde metrikleri yazdırır.

## 2 Dakikada Demo

> Gereksinimler: Maven Wrapper (`./mvnw`) ve JDK 25.

```bash
# 1) Geliştirme modunda sunucuyu başlatın
./mvnw quarkus:dev

# 2) Temel bir doğrulama yapın
printf 'set foo 0 5 3\r\nbar\r\nget foo\r\n' | nc 127.0.0.1 11211
# Beklenen çıktı: STORED / VALUE foo 0 3
```

Paketleme sonrası çalıştırmak için:

```bash
./mvnw package
java -jar target/quarkus-app/quarkus-run.jar
```

## Küme Kurulum Örneği

Tek JVM'den kümelenmiş bir yapıya geçiş bu kadar kolay:

```bash
# Varsayılan node
./mvnw quarkus:dev

# İkinci node (farklı portlarla)
./mvnw quarkus:dev \
    -Dquarkus.http.port=0 \
    -Dapp.network.port=11212 \
    -Dapp.cluster.replication.port=18081 \
    -Dapp.cluster.discovery.node-id=node-b
```

Multicast koordinasyon, yeni node'u otomatik keşfeder ve tutarlı hash halkasına ekler. Yazmalar quorum tamamlanana kadar bekler; başarısız takipçiler için hinted handoff devreye girer.

## Yapılandırma Sihirbazı

`application.properties` altında sizi bekleyen başlıca anahtarlar:

| Anahtar | Açıklama | Varsayılan |
| --- | --- | --- |
| `app.cache.segments` | Segment sayısı; eşzamanlılık/kapasite dengesini belirler. | 8 |
| `app.cache.max-capacity` | Toplam giriş sınırı. | 10000 |
| `app.cache.cleaner-poll-millis` | TTL temizleyicisinin kuyruğu yoklama aralığı (ms). | 100 |
| `app.cache.eviction-policy` | `LRU` veya `TINY_LFU`. | LRU |
| `app.rdb.path` | Snapshot dosya yolu. | `data.rdb` |
| `app.rdb.snapshot-interval-seconds` | Snapshot periyodu; 0 yalnızca başlangıçta. | 60 |
| `app.cluster.virtual-nodes` | Her fiziksel düğüm için sanal düğüm sayısı. | 64 |
| `app.cluster.replication-factor` | Anahtar başına kopya sayısı. | 1 |
| `app.cluster.discovery.multicast-group/port` | Multicast koordinasyon adresi. | 230.0.0.1 / 45565 |
| `app.cluster.discovery.heartbeat-interval-millis` | Kalp atışı aralığı. | 5000 |
| `app.cluster.discovery.failure-timeout-millis` | Üye zaman aşımı eşiği. | 15000 |
| `app.cluster.discovery.node-id` | Opsiyonel sabit düğüm kimliği. | (boş) |
| `app.cluster.replication.bind-host/advertise-host/port` | Replikasyon sunucusu adres bilgileri. | 0.0.0.0 / 127.0.0.1 / 18080 |
| `app.cluster.replication.connect-timeout-millis` | Uzak düğüme bağlanma zaman aşımı. | 5000 |
| `app.cluster.coordination.hint-replay-interval-millis` | Hinted handoff kuyruğu için yeniden oynatma denemeleri arasındaki minimum süre. | 5000 |
| `app.cluster.coordination.anti-entropy-interval-millis` | Anti-entropy taramalarının periyodu (ms). | 30000 |
| `app.network.host/port/backlog/worker-threads` | cancached TCP sunucusu ayarları. | 0.0.0.0 / 11211 / 128 / 16 |
| `app.memcache.max-item-size-bytes` | Tek bir değerin saklanabileceği maksimum boyut (bayt). | 1048576 |
| `app.memcache.max-cas-retries` | Başarısız CAS işlemleri için tekrar deneme sayısı. | 16 |
| `app.metrics.report-interval-seconds` | Metrik raporlama periyodu; 0 devre dışı. | 5 |

## Proje Yapısı

| Dizin | İçerik |
| --- | --- |
| `src/main/java/com/can/net` | cancached TCP sunucusu ve protokol ayrıştırıcıları. |
| `src/main/java/com/can/cluster` | Tutarlı hash halkası, küme istemcisi ve node arayüzleri. |
| `src/main/java/com/can/cluster/coordination` | Multicast koordinasyonu, uzak node vekilleri ve replikasyon sunucusu. |
| `src/main/java/com/can/core` | Önbellek motoru, segmentler, TTL kuyruğu ve tahliye politikaları. |
| `src/main/java/com/can/codec` | Anahtar/değer codec implementasyonları (UTF-8, Java Serializable). |
| `src/main/java/com/can/rdb` | Snapshot dosyası ve zamanlayıcı bileşenleri. |
| `src/main/java/com/can/metric` | Sayaç, zamanlayıcı ve konsol raporlayıcısı. |
| `src/main/java/com/can/pubsub` | Uygulama içi yayınla-abone ol altyapısı. |
| `src/main/java/com/can/config` | CDI yapılandırması ve tip güvenli konfigürasyon arayüzleri. |
| `integration-tests/` | Docker Compose ile çalışan uçtan uca cancached uyumluluk testleri. |
| `performance-tests/` | JMeter planları ve NFR dokümanları. |
| `scripts/` | Yardımcı komut dosyaları (`run-integration-tests.sh` vb.). |

## Yol Haritası

- [ ] Ek replikasyon stratejileri (örn. aktif-aktif senaryolar için CRDT araştırması)
- [ ] Opsiyonel REST/HTTP yönetim ucu
- [ ] Prometheus metrik ihracı
- [ ] Otomatik benchmark pipeline'ı (JMeter + GitHub Actions)
- [ ] Helm chart ile Kubernetes dağıtımı

> Fikirlerin mi var? [Issue aç](../../issues) veya PR gönder!

## Katkı & Geri Bildirim

1. Depoyu forklayın ve `main` üzerine değişikliklerinizi rebase edin.
2. Kod stilini koruyarak anlamlı commit mesajları yazın.
3. `./mvnw test` ve gerekiyorsa `./scripts/run-integration-tests.sh` ile doğrulayın.
4. Deneyimlerinizi, performans ölçümlerinizi veya yeni kullanım senaryolarınızı paylaşın — proje bu geri bildirimlerle büyüyor.

Sorularınız mı var? Bir [issue](../../issues/new) açabilir veya doğrudan Pull Request ile gelebilirsiniz.

## Lisans

Bu proje, MIT Lisansı veya Apache Lisansı Sürüm 2.0 koşulları altında çift lisanslanmıştır. Ayrıntılar için [LICENSE-MIT](./LICENSE-MIT) ve [LICENSE-APACHE](./LICENSE-APACHE) dosyalarına göz atabilirsiniz.

Bu depoya katkıda bulunan herkes, katkılarının her iki lisansın koşulları altında da kullanılabileceğini kabul eder.
