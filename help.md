# Help / Yardım

<div align="center">
  <a href="#tr">🇹🇷 Türkçe</a>
  &nbsp;|&nbsp;
  <a href="#en">🇬🇧 English</a>
</div>

---

<a id="tr"></a>
## Türkçe Mimari Dokümanı

Bu doküman, kod tabanındaki güncel mimariyi uçtan uca özetler ve eski/yanlış kalan noktaları temizlenmiş haliyle sunar.

### 1) Modül Haritası

- `can-cache-application`: Ana cache sunucusu.
- `can-cache-agent`: Dış dünyaya açılan TCP proxy ve upstream yönetimi.
- `can-cache-integration-tests`: Docker ile entegrasyon testleri.
- `can-cache-performance-tests`: JMeter bazlı yük testleri.

### 2) can-cache-application Akışı

#### 2.1 Ağ katmanı
- `CanCachedServer`, Vert.x `NetServer` ile memcached metin protokolünü işler.
- `app.network.*` ayarları ile host/port/event-loop/worker thread davranışı yönetilir.

#### 2.2 Veri katmanı
- `CacheEngine` segmentli bir bellek modeli kullanır (`CacheSegment`).
- TTL takibi `DelayQueue<ExpiringKey>` üzerinden çalışır.
- CAS desteği `CacheValue`, `CasDecision`, `CasResult` modelleri ile sağlanır.

#### 2.3 Kümeleme ve replikasyon
- `ConsistentHashRing` + `HashFn` + sanal node yaklaşımıyla anahtar dağıtımı yapılır.
- `ClusterClient`, local/remote düğüm yönlendirmesini gerçekleştirir.
- `ReplicationServer` ve `RemoteNode`, düğümler arası veri kopyalama ve koordinasyonu taşır.
- `HintedHandoffService`, ulaşılamayan düğümler için yazma ipuçlarını sıraya alır ve tekrar oynatır.

#### 2.4 Dayanıklılık ve metrik
- Snapshot altyapısı `app.rdb.path` ve `app.rdb.snapshot-interval-seconds` ile yönetilir.
- Micrometer/Prometheus entegrasyonu `application.properties` içinde etkin durumdadır ve `/metrics` endpoint'i kullanılabilir.

### 3) can-cache-agent Akışı

- `TcpProxyServer`: Tek dış porttan istemci trafiğini alır.
- `DiscoveryService`: DNS üzerinden upstream keşfeder.
- `RegistrationService`: `REGISTER <host> <port>` ile gelen uygulama kayıtlarını TTL ile saklar.
- `HealthService`: upstream sağlık kontrollerini yapar.
- `UpstreamSelector`: `RR` veya `LEAST_CONN` ile hedef seçer.
- `TuiDashboard`: TTY ortamında canlı görünüm sağlar.

### 4) Konfigürasyon Kısa Rehberi

#### Uygulama (can-cache-application)
- `app.network.port=11211`
- `app.cluster.replication.port=18080`
- `app.cluster.discovery.multicast-group=230.0.0.1`
- `app.cluster.discovery.multicast-port=45565`
- `app.agent.enabled=true`
- `app.agent.registration-port=11311`

#### Agent (can-cache-agent)
- `agent.listen.port=11211`
- `agent.registration.enabled=true`
- `agent.registration.port=11311`
- `agent.discovery.dns=<headless-service-dns>`
- `agent.selection.policy=RR`

### 5) İşletim Notları

- Çoklu instance senaryosunda istemcileri doğrudan cache node'larına değil agent'a yönlendirin.
- Node başına unique `app.cluster.discovery.node-id` vermek operasyonel görünürlüğü artırır.
- Üretimde snapshot yolu (`app.rdb.path`) kalıcı disk üzerinde olmalıdır.
- Yük testlerinde `can-cache-performance-tests/README.md` içindeki profilleri (`small/medium/large/xl`) kullanın.

---

<a id="en"></a>
## English Architecture Document

This document summarizes the current end-to-end architecture and removes stale references.

### 1) Module Map

- `can-cache-application`: Main cache server.
- `can-cache-agent`: Public TCP proxy and upstream management layer.
- `can-cache-integration-tests`: Docker-based integration tests.
- `can-cache-performance-tests`: JMeter-based load tests.

### 2) can-cache-application Flow

#### 2.1 Network layer
- `CanCachedServer` handles the memcached text protocol through Vert.x `NetServer`.
- `app.network.*` controls host/port/event-loop/worker-thread behavior.

#### 2.2 Data layer
- `CacheEngine` uses segmented in-memory storage (`CacheSegment`).
- TTL tracking is implemented via `DelayQueue<ExpiringKey>`.
- CAS semantics are modeled with `CacheValue`, `CasDecision`, and `CasResult`.

#### 2.3 Clustering and replication
- Key distribution uses `ConsistentHashRing` + `HashFn` + virtual nodes.
- `ClusterClient` routes commands to local or remote nodes.
- `ReplicationServer` and `RemoteNode` handle inter-node replication.
- `HintedHandoffService` queues writes for unreachable members and replays them later.

#### 2.4 Durability and metrics
- Snapshot behavior is driven by `app.rdb.path` and `app.rdb.snapshot-interval-seconds`.
- Micrometer/Prometheus integration is enabled in `application.properties`, exposed at `/metrics`.

### 3) can-cache-agent Flow

- `TcpProxyServer`: accepts client traffic on one external port.
- `DiscoveryService`: discovers upstreams via DNS.
- `RegistrationService`: stores app-registered upstreams from `REGISTER <host> <port>` with TTL.
- `HealthService`: runs upstream probes.
- `UpstreamSelector`: selects targets using `RR` or `LEAST_CONN`.
- `TuiDashboard`: live terminal dashboard for TTY environments.

### 4) Configuration Quick Guide

#### Application (can-cache-application)
- `app.network.port=11211`
- `app.cluster.replication.port=18080`
- `app.cluster.discovery.multicast-group=230.0.0.1`
- `app.cluster.discovery.multicast-port=45565`
- `app.agent.enabled=true`
- `app.agent.registration-port=11311`

#### Agent (can-cache-agent)
- `agent.listen.port=11211`
- `agent.registration.enabled=true`
- `agent.registration.port=11311`
- `agent.discovery.dns=<headless-service-dns>`
- `agent.selection.policy=RR`

### 5) Operations Notes

- In multi-instance deployments, route clients to agent instead of directly to cache nodes.
- Setting a unique `app.cluster.discovery.node-id` per node improves observability.
- Use persistent storage for `app.rdb.path` in production.
- Use load profiles (`small/medium/large/xl`) documented in `can-cache-performance-tests/README.md`.
