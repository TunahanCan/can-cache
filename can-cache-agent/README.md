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
- REST status endpoint for connected instances and recent connection state (`GET /agent/instances`).

### Registration protocol

Cache nodes can register themselves by opening a TCP connection to registration port and sending:

```text
REGISTER <host> <port>
```

Example:

```text
REGISTER 10.42.1.9 11212
```

### REST status endpoint

When dashboard output in terminal is not enough, use:

```bash
curl http://localhost:8080/agent/instances
```

This endpoint returns current instance list, health state, active/total connections, traffic counters, latest events, and recent connection summaries.

### Quick run

```bash
./mvnw -f can-cache-agent/pom.xml quarkus:dev
```

Package + run:

```bash
./mvnw -f can-cache-agent/pom.xml package
java -jar can-cache-agent/target/quarkus-app/quarkus-run.jar
```

### Key configuration

See `src/main/resources/application.yaml`.

- `agent.listen.host/port`
- `agent.registration.enabled/host/port/ttl/cleanup-interval`
- `agent.discovery.enabled/dns/interval`
- `agent.health.interval/connect-timeout`
- `agent.selection.policy`
- `agent.timeouts.idle`
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

### Kayıt protokolü

Cache node'ları, registration portuna TCP bağlantısı açıp şu satırı göndererek kendini kaydedebilir:

```text
REGISTER <host> <port>
```

Örnek:

```text
REGISTER 10.42.1.9 11212
```

### REST durum endpoint

Terminal dashboard yerine dışarıdan gözlemek için:

```bash
curl http://localhost:8080/agent/instances
```

Bu endpoint; anlık instance listesi, sağlık durumu, aktif/toplam bağlantı sayıları, trafik sayaçları, son eventler ve son bağlantı özetlerini JSON olarak döner.

### Hızlı çalıştırma

```bash
./mvnw -f can-cache-agent/pom.xml quarkus:dev
```

Paketleyip çalıştırma:

```bash
./mvnw -f can-cache-agent/pom.xml package
java -jar can-cache-agent/target/quarkus-app/quarkus-run.jar
```

### Önemli konfigürasyon alanları

`src/main/resources/application.yaml` dosyasına bakın.

- `agent.listen.host/port`
- `agent.registration.enabled/host/port/ttl/cleanup-interval`
- `agent.discovery.enabled/dns/interval`
- `agent.health.interval/connect-timeout`
- `agent.selection.policy`
- `agent.timeouts.idle`
- `agent.dashboard.mode/refresh/snapshot-interval`
- `agent.shutdown.grace`
