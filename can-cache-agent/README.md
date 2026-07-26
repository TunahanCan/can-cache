# can-cache-agent

<div align="center">
  <a href="#english">🇬🇧 English</a>
  &nbsp;|&nbsp;
  <a href="#turkce">🇹🇷 Türkçe</a>
</div>

---

<a id="english"></a>
## English

`can-cache-agent` is a Quarkus + Vert.x TCP edge proxy designed for `can-cache` clusters.

### Core capabilities

- Single external TCP listener (`agent.listen.port`, default `11211`).
- Optional upstream discovery by DNS (`agent.discovery.enabled`, `agent.discovery.dns`) and app-driven registration.
- Registration protocol support on `agent.registration.port` (default `11311`).
- Health checks and upstream status tracking.
- Selection policies: `RR` and `LEAST_CONN`.
- Terminal dashboard (`tui`) and configurable dashboard modes.
- Responsive web dashboard with light/dark themes (`GET /agent/`).
- REST status endpoint for connected instances and recent connection state (`GET /agent/instances`).
- Liveness/readiness checks and Prometheus metrics.
- Connection admission limits, dial failover, passive node ejection, and graceful draining.

### Registration protocol

Cache nodes can register themselves by opening a TCP connection to registration port and sending:

```text
REGISTER <host> <port> [token]
```

Example:

```text
REGISTER 10.42.1.9 11212 my-shared-token
```

The agent replies before closing the registration connection:

```text
OK REGISTERED 10.42.1.9:11212 lease-ms=15000
```

Configure the same secret as `agent.registration.token` on the agent and
`app.agent.registration-token` on each cache node. A token is mandatory whenever the registration listener is not
bound to loopback; use a random printable-ASCII value of at most 128 bytes. The registration protocol is plain TCP;
on an untrusted network, carry it through mTLS (for example, a service mesh or authenticated tunnel) so the bearer
token cannot be observed and replayed.
Registration binds to `127.0.0.1` by default; multi-host or multi-pod deployments must explicitly set the bind host
and token.

### Web dashboard and operations endpoints

Open the end-user dashboard:

```text
http://localhost:8080/agent/
```

The underlying status API is:

```bash
curl http://localhost:8080/agent/instances
```

This endpoint returns current instance list, health state, active/total connections, traffic counters, latest events, and recent connection summaries.

- Liveness: `GET /q/health/live`
- Readiness: `GET /q/health/ready`
- Prometheus: `GET /q/metrics`

The HTTP operations surface binds to `127.0.0.1` by default. If remote access is required, explicitly change
`quarkus.http.host` and place the dashboard, status API, health endpoints, and metrics behind an authenticated
reverse proxy or an equivalent trusted network boundary.
Load balancers should use `/q/health/ready`, not a bare TCP-connect check: during graceful shutdown the proxy keeps
established sessions alive for the configured grace period while rejecting new application traffic.

### Quick run

```bash
./gradlew :can-cache-agent:quarkusDev
```

Package + run:

```bash
./gradlew :can-cache-agent:build
java -jar can-cache-agent/build/quarkus-app/quarkus-run.jar
```

### Key configuration

See `src/main/resources/application.yaml`.

- `agent.listen.host/port`
- `agent.listen.max-connections/max-pending-connections/write-queue-max-bytes`
- `agent.registration.enabled/host/port/ttl/cleanup-interval/read-timeout/max-connections/max-nodes/token`
- `agent.discovery.enabled/dns/interval`
- `agent.health.interval/connect-timeout/healthy-threshold/unhealthy-threshold/passive-failure-threshold`
- `agent.selection.policy/max-attempts`
- `agent.timeouts.connect/idle`
- `agent.dashboard.mode/refresh/snapshot-interval`
- `agent.shutdown.grace`

---

<a id="turkce"></a>
## Türkçe

`can-cache-agent`, `can-cache` kümeleri için tasarlanmış Quarkus + Vert.x tabanlı bir TCP edge proxy'dir.

### Temel yetenekler

- Tek dış TCP dinleme noktası (`agent.listen.port`, varsayılan `11211`).
- Opsiyonel DNS ile upstream keşfi (`agent.discovery.enabled`, `agent.discovery.dns`) ve uygulama tabanlı kayıt.
- `agent.registration.port` (varsayılan `11311`) üzerinden kayıt protokolü desteği.
- Sağlık kontrolleri ve upstream durum takibi.
- Seçim politikaları: `RR` ve `LEAST_CONN`.
- Terminal dashboard (`tui`) ve yapılandırılabilir dashboard modları.
- Açık/koyu temalı responsive web paneli (`GET /agent/`).
- Liveness/readiness kontrolleri ve Prometheus metrikleri.
- Bağlantı limitleri, dial failover, pasif node ejection ve güvenli bağlantı drain.

### Kayıt protokolü

Cache node'ları, registration portuna TCP bağlantısı açıp şu satırı göndererek kendini kaydedebilir:

```text
REGISTER <host> <port> [token]
```

Örnek:

```text
REGISTER 10.42.1.9 11212 ortak-gizli-deger
```

Agent, registration bağlantısını kapatmadan önce sonucu bildirir:

```text
OK REGISTERED 10.42.1.9:11212 lease-ms=15000
```

Agent'ta `agent.registration.token`, cache node'larında ise `app.agent.registration-token` için aynı değer
kullanılmalıdır. Registration listener loopback dışında dinliyorsa token zorunludur; en fazla 128 baytlık rastgele,
yazdırılabilir bir ASCII değeri kullanın. Kayıt protokolü düz TCP kullanır; güvenilmeyen bir ağda bearer token'ın
izlenip tekrar kullanılmasını önlemek için trafiği mTLS (örneğin service mesh veya kimlik doğrulamalı tünel)
üzerinden taşıyın.
Registration varsayılan olarak `127.0.0.1` üzerinde dinler; farklı host veya pod'lardaki kurulumlar bind hostunu ve
token'ı açıkça ayarlamalıdır.

### Web paneli ve operasyon endpoint'leri

Son kullanıcı panelini açın:

```text
http://localhost:8080/agent/
```

Panelin kullandığı durum API'si:

```bash
curl http://localhost:8080/agent/instances
```

Bu endpoint; anlık instance listesi, sağlık durumu, aktif/toplam bağlantı sayıları, trafik sayaçları, son eventler ve son bağlantı özetlerini JSON olarak döner.

- Liveness: `GET /q/health/live`
- Readiness: `GET /q/health/ready`
- Prometheus: `GET /q/metrics`

HTTP operasyon yüzeyi varsayılan olarak `127.0.0.1` üzerinde dinler. Uzak erişim gerekiyorsa
`quarkus.http.host` değerini bilinçli olarak değiştirin; paneli, durum API'sini, sağlık endpoint'lerini ve
metrikleri kimlik doğrulamalı bir reverse proxy veya eşdeğer güvenilir ağ sınırı arkasına alın.
Load balancer kontrollerinde yalnız TCP bağlantısı yerine `/q/health/ready` kullanın: agent güvenli kapanış sırasında
mevcut oturumları grace süresince açık tutarken yeni uygulama trafiğini reddeder.

### Hızlı çalıştırma

```bash
./gradlew :can-cache-agent:quarkusDev
```

Paketleyip çalıştırma:

```bash
./gradlew :can-cache-agent:build
java -jar can-cache-agent/build/quarkus-app/quarkus-run.jar
```

### Önemli konfigürasyon alanları

`src/main/resources/application.yaml` dosyasına bakın.

- `agent.listen.host/port`
- `agent.listen.max-connections/max-pending-connections/write-queue-max-bytes`
- `agent.registration.enabled/host/port/ttl/cleanup-interval/read-timeout/max-connections/max-nodes/token`
- `agent.discovery.enabled/dns/interval`
- `agent.health.interval/connect-timeout/healthy-threshold/unhealthy-threshold/passive-failure-threshold`
- `agent.selection.policy/max-attempts`
- `agent.timeouts.connect/idle`
- `agent.dashboard.mode/refresh/snapshot-interval`
- `agent.shutdown.grace`
