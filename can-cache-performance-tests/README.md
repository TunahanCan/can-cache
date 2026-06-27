# can-cache-performance-tests

This document is bilingual (English + Türkçe).

---

## English

`can-cache-performance-tests` contains the JMeter plans and the custom Java sampler used for non-functional testing of `can-cache`.

The Docker flow also runs JMeter. It starts one `can-cache-agent`, a configurable
number of `can-cache-application` containers, waits until every application is
registered as healthy behind the agent, validates cross-connection data transfer,
and then executes the selected `.jmx` profile from a JMeter container.

### Directory layout

- `jmeter/`: JMeter plans (`can-cache-small.jmx`, `medium`, `large`, `xl`).
- `nfr/`: profile-specific NFR targets.
- `src/main/java/.../CancachedRoundTripSampler.java`: custom sampler implementation.
- `run-local.sh`: run tests with local JMeter.
- `docker-compose.performance.yml`: reference performance topology; `run-docker.sh` generates the active topology from `APP_COUNT`.
- `run-docker.sh`: run the full Docker/JMeter performance flow.
- `results/`: output folder for `.jtl` files.

### Build sampler locally

```bash
./mvnw -f can-cache-performance-tests/pom.xml package
```

The sampler is compiled with Java 21 bytecode and the Docker flow uses the
Java 21 based `anasoid/jmeter:5.6.3-plugins-21-jre` image by default. The full
cache applications still use the repository's Java 25 Docker build.

### Run with Docker + JMeter

Docker is the recommended path because it brings the agent, cache applications,
build images, a JMeter image, and the custom Java sampler on JMeter's classpath:

```bash
./can-cache-performance-tests/run-docker.sh small
./can-cache-performance-tests/run-docker.sh medium
./can-cache-performance-tests/run-docker.sh large
./can-cache-performance-tests/run-docker.sh xl
```

Scale one agent behind multiple cache applications:

```bash
APP_COUNT=2 ./can-cache-performance-tests/run-docker.sh small
APP_COUNT=4 ./can-cache-performance-tests/run-docker.sh small
APP_COUNT=8 ./can-cache-performance-tests/run-docker.sh small
```

### Run with local JMeter

Use this only when the target stack is already running locally and JMeter is
installed on the host:

```bash
./can-cache-performance-tests/run-local.sh small
./can-cache-performance-tests/run-local.sh medium
./can-cache-performance-tests/run-local.sh large
./can-cache-performance-tests/run-local.sh xl
```

### Typical overrides

Use extra JMeter args after `--`:

```bash
PAYLOAD_SIZE=512 DURATION_SECONDS=60 ./can-cache-performance-tests/run-docker.sh small
APP_COUNT=8 CONNECTION_MODE=separate DURATION_SECONDS=30 ./can-cache-performance-tests/run-docker.sh small
READ_REPAIR_ENABLED=false ANTI_ENTROPY_INTERVAL_MILLIS=0 ./can-cache-performance-tests/run-docker.sh medium
REMOTE_NODE_POOL_SIZE=16 REMOTE_NODE_REQUEST_QUEUE_CAPACITY=512 ./can-cache-performance-tests/run-docker.sh medium
./can-cache-performance-tests/run-local.sh medium -- -JtargetHost=127.0.0.1 -JtargetPort=11211
```

### Notes

- `run-docker.sh` targets `can-cache-agent:11211` inside the Compose network by default.
- `APP_COUNT` accepts `2`, `4`, or `8` cache applications behind one agent.
- Docker runs use `CONNECTION_MODE=single` by default so SET, GET, and DELETE reuse one sampler connection per round trip.
- Use `CONNECTION_MODE=separate` when you intentionally want to stress TCP churn, agent routing, and cross-connection data transfer.
- The default Docker JMeter image is `anasoid/jmeter:5.6.3-plugins-21-jre`; override with `JMETER_IMAGE`.
- JMeter uses a bounded default heap; override with `JMETER_HEAP` or `HEAP`.
- The scripts fail when a `.jtl` contains failed samples; set `ALLOW_JMETER_ERRORS=1` to only collect results.
- `run-local.sh` targets `127.0.0.1:11211` by default.
- `KEEP_STACK=1` leaves the Docker performance stack running after the JMeter run.
- Compare `.jtl` results with the corresponding `nfr/*.md` acceptance criteria.

---

## Türkçe

`can-cache-performance-tests`, `can-cache` için fonksiyonel olmayan testlerde kullanılan JMeter planlarını ve özel Java sampler'ı içerir.

Docker akışı da JMeter çalıştırır. Bir `can-cache-agent` ve yapılandırılabilir
sayıda `can-cache-application` container'ı ayağa kaldırır, tüm uygulamalar agent
arkasında healthy görünene kadar bekler, ayrı bağlantılarla data transferini
doğrular ve seçilen `.jmx` profilini JMeter container'ında çalıştırır.

### Dizin yapısı

- `jmeter/`: JMeter planları (`can-cache-small.jmx`, `medium`, `large`, `xl`).
- `nfr/`: profile özel NFR hedefleri.
- `src/main/java/.../CancachedRoundTripSampler.java`: özel sampler implementasyonu.
- `run-local.sh`: testleri yerel JMeter ile çalıştırır.
- `docker-compose.performance.yml`: referans performans topolojisi; aktif topoloji `APP_COUNT` ile `run-docker.sh` tarafından üretilir.
- `run-docker.sh`: tüm Docker/JMeter performans akışını çalıştırır.
- `results/`: `.jtl` çıktı klasörü.

### Sampler'ı yerelde derleme

```bash
./mvnw -f can-cache-performance-tests/pom.xml package
```

Sampler Java 21 bytecode ile derlenir ve Docker akışı varsayılan olarak Java 21
tabanlı `anasoid/jmeter:5.6.3-plugins-21-jre` imajını kullanır. Cache
uygulamalarının tam Docker build'i repo'nun Java 25 akışını kullanmaya devam
eder.

### Docker + JMeter ile çalıştırma

Önerilen yol Docker'dır; agent, cache uygulamaları, build imajları, JMeter imajı
ve classpath'e eklenmiş özel Java sampler'ı birlikte getirir:

```bash
./can-cache-performance-tests/run-docker.sh small
./can-cache-performance-tests/run-docker.sh medium
./can-cache-performance-tests/run-docker.sh large
./can-cache-performance-tests/run-docker.sh xl
```

Tek agent arkasında birden fazla cache uygulamasıyla ölçek testi:

```bash
APP_COUNT=2 ./can-cache-performance-tests/run-docker.sh small
APP_COUNT=4 ./can-cache-performance-tests/run-docker.sh small
APP_COUNT=8 ./can-cache-performance-tests/run-docker.sh small
```

### Yerel JMeter ile çalıştırma

Bunu hedef stack zaten yerelde çalışırken ve host üzerinde JMeter kuruluysa
kullanın:

```bash
./can-cache-performance-tests/run-local.sh small
./can-cache-performance-tests/run-local.sh medium
./can-cache-performance-tests/run-local.sh large
./can-cache-performance-tests/run-local.sh xl
```

### Sık kullanılan override'lar

`--` sonrasında ekstra JMeter argümanları verilebilir:

```bash
PAYLOAD_SIZE=512 DURATION_SECONDS=60 ./can-cache-performance-tests/run-docker.sh small
APP_COUNT=8 CONNECTION_MODE=separate DURATION_SECONDS=30 ./can-cache-performance-tests/run-docker.sh small
READ_REPAIR_ENABLED=false ANTI_ENTROPY_INTERVAL_MILLIS=0 ./can-cache-performance-tests/run-docker.sh medium
REMOTE_NODE_POOL_SIZE=16 REMOTE_NODE_REQUEST_QUEUE_CAPACITY=512 ./can-cache-performance-tests/run-docker.sh medium
./can-cache-performance-tests/run-local.sh medium -- -JtargetHost=127.0.0.1 -JtargetPort=11211
```

### Notlar

- `run-docker.sh` varsayılan olarak Compose ağı içinde `can-cache-agent:11211` hedefine gider.
- `APP_COUNT`, tek agent arkasında `2`, `4` veya `8` cache uygulaması kabul eder.
- Docker koşuları varsayılan olarak `CONNECTION_MODE=single` kullanır; SET, GET ve DELETE aynı sampler bağlantısını yeniden kullanır.
- TCP churn, agent routing ve ayrı bağlantılarla data transferini özellikle zorlamak için `CONNECTION_MODE=separate` kullanın.
- Varsayılan Docker JMeter imajı `anasoid/jmeter:5.6.3-plugins-21-jre`; `JMETER_IMAGE` ile değiştirilebilir.
- JMeter sınırlı heap ile başlar; `JMETER_HEAP` veya `HEAP` ile değiştirilebilir.
- Scriptler `.jtl` içinde başarısız sample görürse hata koduyla çıkar; yalnızca sonuç toplamak için `ALLOW_JMETER_ERRORS=1` verilebilir.
- `run-local.sh` varsayılan olarak `127.0.0.1:11211` hedefine gider.
- `KEEP_STACK=1`, JMeter koşusundan sonra Docker performans stack'ini açık bırakır.
- `.jtl` sonuçlarını ilgili `nfr/*.md` kabul kriterleriyle karşılaştırın.
