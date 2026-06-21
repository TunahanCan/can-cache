# can-cache-performance-tests

This document is bilingual (English + Türkçe).

---

## English

`can-cache-performance-tests` contains the JMeter plans and the custom Java sampler used for non-functional testing of `can-cache`.

The Docker flow also runs JMeter. It starts one `can-cache-agent`, two
`can-cache-application` containers, waits until both applications are registered
as healthy behind the agent, and then executes the selected `.jmx` profile from
a JMeter container.

### Directory layout

- `jmeter/`: JMeter plans (`can-cache-small.jmx`, `medium`, `large`, `xl`).
- `nfr/`: profile-specific NFR targets.
- `src/main/java/.../CancachedRoundTripSampler.java`: custom sampler implementation.
- `run-local.sh`: run tests with local JMeter.
- `docker-compose.performance.yml`: performance topology (`agent + 2 apps + JMeter`).
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

Docker is the recommended path because it brings the agent, both cache
applications, build images, a JMeter image, and the custom Java sampler on
JMeter's classpath:

```bash
./can-cache-performance-tests/run-docker.sh small
./can-cache-performance-tests/run-docker.sh medium
./can-cache-performance-tests/run-docker.sh large
./can-cache-performance-tests/run-docker.sh xl
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
./can-cache-performance-tests/run-local.sh medium -- -JtargetHost=127.0.0.1 -JtargetPort=11211
```

### Notes

- `run-docker.sh` targets `can-cache-agent:11211` inside the Compose network by default.
- The default Docker JMeter image is `anasoid/jmeter:5.6.3-plugins-21-jre`; override with `JMETER_IMAGE`.
- JMeter uses a bounded default heap; override with `JMETER_HEAP` or `HEAP`.
- The scripts fail when a `.jtl` contains failed samples; set `ALLOW_JMETER_ERRORS=1` to only collect results.
- `run-local.sh` targets `127.0.0.1:11211` by default.
- `KEEP_STACK=1` leaves the Docker performance stack running after the JMeter run.
- Compare `.jtl` results with the corresponding `nfr/*.md` acceptance criteria.

---

## Türkçe

`can-cache-performance-tests`, `can-cache` için fonksiyonel olmayan testlerde kullanılan JMeter planlarını ve özel Java sampler'ı içerir.

Docker akışı da JMeter çalıştırır. Bir `can-cache-agent`, iki
`can-cache-application` container'ı ayağa kaldırır, iki uygulama agent arkasında
healthy görünene kadar bekler ve seçilen `.jmx` profilini JMeter container'ında
çalıştırır.

### Dizin yapısı

- `jmeter/`: JMeter planları (`can-cache-small.jmx`, `medium`, `large`, `xl`).
- `nfr/`: profile özel NFR hedefleri.
- `src/main/java/.../CancachedRoundTripSampler.java`: özel sampler implementasyonu.
- `run-local.sh`: testleri yerel JMeter ile çalıştırır.
- `docker-compose.performance.yml`: performans topolojisi (`agent + 2 app + JMeter`).
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

Önerilen yol Docker'dır; agent, iki cache uygulaması, build imajları, JMeter
imajı ve classpath'e eklenmiş özel Java sampler'ı birlikte getirir:

```bash
./can-cache-performance-tests/run-docker.sh small
./can-cache-performance-tests/run-docker.sh medium
./can-cache-performance-tests/run-docker.sh large
./can-cache-performance-tests/run-docker.sh xl
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
./can-cache-performance-tests/run-local.sh medium -- -JtargetHost=127.0.0.1 -JtargetPort=11211
```

### Notlar

- `run-docker.sh` varsayılan olarak Compose ağı içinde `can-cache-agent:11211` hedefine gider.
- Varsayılan Docker JMeter imajı `anasoid/jmeter:5.6.3-plugins-21-jre`; `JMETER_IMAGE` ile değiştirilebilir.
- JMeter sınırlı heap ile başlar; `JMETER_HEAP` veya `HEAP` ile değiştirilebilir.
- Scriptler `.jtl` içinde başarısız sample görürse hata koduyla çıkar; yalnızca sonuç toplamak için `ALLOW_JMETER_ERRORS=1` verilebilir.
- `run-local.sh` varsayılan olarak `127.0.0.1:11211` hedefine gider.
- `KEEP_STACK=1`, JMeter koşusundan sonra Docker performans stack'ini açık bırakır.
- `.jtl` sonuçlarını ilgili `nfr/*.md` kabul kriterleriyle karşılaştırın.
