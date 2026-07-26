# can-cache

<p align="center">
  <img src="docs/assets/logo.png" alt="can-cache logo" width="260" />
</p>

<div align="center">
  <a href="#english">🇬🇧 English</a>
  &nbsp;|&nbsp;
  <a href="#turkce">🇹🇷 Türkçe</a>
</div>

---

<a id="english"></a>
## English

`can-cache` is a Quarkus 3 based, memcached-text-protocol compatible distributed in-memory cache.

### What is in this repository?

- `can-cache-application`: core cache server (protocol parser, in-memory engine, clustering, replication, hinted handoff, metrics).
- `can-cache-agent`: hardened TCP edge proxy, registration/discovery service, and live operations dashboard.
- `can-cache-integration-tests`: Docker-based protocol and metrics integration tests.
- `can-cache-performance-tests`: JMeter sampler and load profiles (`small/medium/large/xl`).

### Architecture (end-to-end)

1. **Client command path**
   - Clients connect over TCP to `CanCachedServer` (`app.network.port`, default `11211`).
   - Command parsing supports classic memcached text commands (`set/get/add/replace/append/prepend/cas/delete/incr/decr/touch/flush_all/stats/version/quit`).

2. **Storage and consistency path**
   - `ClusterClient` routes keys with `ConsistentHashRing` + virtual nodes.
   - `CacheEngine` + `CacheSegment` store data in memory and enforce TTL/CAS semantics.
   - Replication between nodes is handled by `RemoteNode` and `ReplicationServer`.
   - `HintedHandoffService` replays missed writes when a node recovers.

3. **Reliability and observability path**
   - Durability is in-memory only; process restart drops data unless clients repopulate.
   - Metrics are exported with Micrometer/Prometheus (`/metrics`).
   - Cluster membership and failure detection run via multicast heartbeat settings under `app.cluster.discovery.*`.

### Quick start

Requirements: Java 25 + Gradle Wrapper.

```bash
# run core server
./gradlew :can-cache-application:quarkusDev

# basic protocol test
printf 'set foo 0 60 3\r\nbar\r\nget foo\r\n' | nc 127.0.0.1 11211
```

### Cluster example

```bash
# node A
./gradlew :can-cache-application:quarkusDev

# node B
./gradlew :can-cache-application:quarkusDev \
  -Dquarkus.http.port=0 \
  -Dapp.network.port=11212 \
  -Dapp.cluster.replication.port=18081 \
  -Dapp.cluster.discovery.node-id=node-b
```

### Agent integration (recommended for multi-instance)

In each `can-cache-application` instance:

```properties
app.agent.enabled=true
app.agent.host=127.0.0.1
app.agent.port=11211
app.agent.registration-port=11311
app.agent.registration-token=<shared-secret>
app.agent.advertised-host=<instance-ip-or-dns>
```

When registration crosses the local host, configure the shared token on both sides. Registration uses plain TCP, so
use mTLS or an authenticated tunnel on untrusted networks.

Run agent:

```bash
./gradlew :can-cache-agent:quarkusDev
```

Clients connect only to the agent (`agent.listen.port`), while agent balances traffic across healthy cache nodes.
Open `http://localhost:8080/agent/` for the live dashboard. Health and Prometheus endpoints are available at
`/q/health/live`, `/q/health/ready`, and `/q/metrics`.
The HTTP operations surface binds to loopback by default. Put it behind an authenticated reverse proxy before
exposing it outside a trusted host or network.

Run one local agent + two local cache nodes:

```bash
./local-setup.sh start
./local-setup.sh status
./local-setup.sh test
# connect client to 127.0.0.1:11211
./local-setup.sh stop
```

### Documentation index

- Detailed architecture: `ARCHITECTURE.md`
- Usage and operations guide: `help.md`
- Agent-specific guide: `can-cache-agent/README.md`
- Performance test guide: `can-cache-performance-tests/README.md`

---

<a id="turkce"></a>
## Türkçe

`can-cache`, Quarkus 3 üzerinde çalışan, memcached metin protokolü ile uyumlu dağıtık bellek içi cache sunucusudur.

### Bu repoda neler var?

- `can-cache-application`: çekirdek cache sunucusu (protokol ayrıştırma, bellek motoru, kümeleme, replikasyon, hinted handoff, metrik).
- `can-cache-agent`: çoklu node dağıtımları için sertleştirilmiş TCP edge proxy, kayıt/keşif servisi ve canlı operasyon paneli.
- `can-cache-integration-tests`: Docker tabanlı protokol ve metrik entegrasyon testleri.
- `can-cache-performance-tests`: JMeter sampler ve yük profilleri (`small/medium/large/xl`).

### Uçtan uca mimari

1. **İstemci komut akışı**
   - İstemciler TCP üzerinden `CanCachedServer`'a bağlanır (`app.network.port`, varsayılan `11211`).
   - Klasik memcached metin komutları desteklenir (`set/get/add/replace/append/prepend/cas/delete/incr/decr/touch/flush_all/stats/version/quit`).

2. **Veri ve tutarlılık akışı**
   - `ClusterClient`, anahtarları `ConsistentHashRing` + sanal node ile yönlendirir.
   - `CacheEngine` + `CacheSegment`, veriyi bellekte tutar; TTL/CAS kurallarını uygular.
   - Node'lar arası replikasyon `RemoteNode` ve `ReplicationServer` ile yürür.
   - `HintedHandoffService`, kesinti sırasında kaçan yazmaları node geri dönünce tekrar oynatır.

3. **Dayanıklılık ve gözlemlenebilirlik**
   - Dayanıklılık bellek içidir; süreç yeniden başlatılırsa veri kaybolur ve istemci yeniden doldurmalıdır.
   - Metrikler Micrometer/Prometheus ile `/metrics` altında sunulur.
   - Küme üyeliği ve hata tespiti `app.cluster.discovery.*` multicast heartbeat ayarlarıyla yapılır.

### Hızlı başlangıç

Gereksinimler: Java 25 + Gradle Wrapper.

```bash
# çekirdek sunucuyu çalıştır
./gradlew :can-cache-application:quarkusDev

# temel protokol testi
printf 'set foo 0 60 3\r\nbar\r\nget foo\r\n' | nc 127.0.0.1 11211
```

### Küme örneği

```bash
# node A
./gradlew :can-cache-application:quarkusDev

# node B
./gradlew :can-cache-application:quarkusDev \
  -Dquarkus.http.port=0 \
  -Dapp.network.port=11212 \
  -Dapp.cluster.replication.port=18081 \
  -Dapp.cluster.discovery.node-id=node-b
```

### Agent entegrasyonu (çoklu instance için önerilir)

Her `can-cache-application` instance'ında:

```properties
app.agent.enabled=true
app.agent.host=127.0.0.1
app.agent.port=11211
app.agent.registration-port=11311
app.agent.registration-token=<ortak-gizli-deger>
app.agent.advertised-host=<instance-ip-veya-dns>
```

Registration aynı host dışına çıkıyorsa ortak token'ı iki tarafta da ayarlayın. Kayıt trafiği düz TCP kullandığı
için güvenilmeyen ağlarda mTLS veya kimlik doğrulamalı bir tünel kullanın.

Agent çalıştırma:

```bash
./gradlew :can-cache-agent:quarkusDev
```

İstemciler sadece agent'a bağlanır (`agent.listen.port`), agent sağlıklı cache node'ları arasında trafiği dağıtır.
Canlı panel için `http://localhost:8080/agent/` adresini açın. Sağlık ve Prometheus endpoint'leri
`/q/health/live`, `/q/health/ready` ve `/q/metrics` altında sunulur.
HTTP operasyon yüzeyi varsayılan olarak yalnız loopback üzerinde dinler. Güvenilir host veya ağ dışına açmadan
önce kimlik doğrulamalı bir reverse proxy arkasına alın.

Localde 1 agent + 2 cache node ayağa kaldırma:

```bash
./local-setup.sh start
./local-setup.sh status
./local-setup.sh test
# istemciyi 127.0.0.1:11211 adresine bağla
./local-setup.sh stop
```

### Doküman dizini

- Detaylı mimari: `ARCHITECTURE.md`
- Kullanım ve operasyon rehberi: `help.md`
- Agent rehberi: `can-cache-agent/README.md`
- Performans test rehberi: `can-cache-performance-tests/README.md`
