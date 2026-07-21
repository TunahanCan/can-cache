# can-cache-performance-tests

This document is bilingual (English + Türkçe).

---

## English

`can-cache-performance-tests` contains JMeter plans and a custom Java sampler for non-functional testing of `can-cache`.

### Directory layout

- `jmeter/`: JMeter plans (`can-cache-small.jmx`, `medium`, `large`, `xl`).
- `nfr/`: profile-specific NFR targets.
- `src/main/java/.../CancachedRoundTripSampler.java`: custom sampler implementation.
- `run-local.sh`: run tests with local JMeter.
- `run-docker.sh`: run tests in Docker.
- `results/`: output folder for `.jtl` files.

### Build sampler

```bash
./mvnw -f can-cache-performance-tests/pom.xml package
```

### Run profiles

```bash
./can-cache-performance-tests/run-local.sh small
./can-cache-performance-tests/run-local.sh medium
./can-cache-performance-tests/run-local.sh large
./can-cache-performance-tests/run-local.sh xl
```

Docker alternative:

```bash
./can-cache-performance-tests/run-docker.sh medium
```

The Docker runner targets `host.docker.internal` by default (including a Linux
host-gateway mapping). Set `TARGET_HOST` when the cache is reachable elsewhere.
The sampler JAR targets Java 17. The default multi-architecture container runs
JMeter 5.6.3 on a Java 21 runtime, so it can load that bytecode on Intel and ARM.

### Typical overrides

Use extra JMeter args after `--`:

```bash
./can-cache-performance-tests/run-local.sh medium -- -JtargetHost=127.0.0.1 -JtargetPort=11211
```

### Notes

- Start `can-cache-application` before running load tests.
- Each JMeter worker keeps one TCP connection open, reconnecting once after a
  transport failure, so latency reflects cache traffic rather than a handshake
  on every iteration.
- Compare `.jtl` results with the corresponding `nfr/*.md` acceptance criteria.

---

## Türkçe

`can-cache-performance-tests`, `can-cache` için fonksiyonel olmayan testlerde kullanılan JMeter planlarını ve özel Java sampler'ı içerir.

### Dizin yapısı

- `jmeter/`: JMeter planları (`can-cache-small.jmx`, `medium`, `large`, `xl`).
- `nfr/`: profile özel NFR hedefleri.
- `src/main/java/.../CancachedRoundTripSampler.java`: özel sampler implementasyonu.
- `run-local.sh`: testleri yerel JMeter ile çalıştırır.
- `run-docker.sh`: testleri Docker içinde çalıştırır.
- `results/`: `.jtl` çıktı klasörü.

### Sampler derleme

```bash
./mvnw -f can-cache-performance-tests/pom.xml package
```

### Profil çalıştırma

```bash
./can-cache-performance-tests/run-local.sh small
./can-cache-performance-tests/run-local.sh medium
./can-cache-performance-tests/run-local.sh large
./can-cache-performance-tests/run-local.sh xl
```

Docker alternatifi:

```bash
./can-cache-performance-tests/run-docker.sh medium
```

Docker çalıştırıcısı varsayılan olarak `host.docker.internal` adresini kullanır
(Linux için host-gateway eşlemesi de eklenir). Cache başka bir adresten
erişiliyorsa `TARGET_HOST` ayarlayın. Sampler JAR'ı Java 17 hedefiyle derlenir;
varsayılan çok mimarili container JMeter 5.6.3'ü Java 21 runtime üzerinde
çalıştırdığı için Intel ve ARM sistemlerde bu bytecode'u yükleyebilir.

### Sık kullanılan override'lar

`--` sonrasında ekstra JMeter argümanları verilebilir:

```bash
./can-cache-performance-tests/run-local.sh medium -- -JtargetHost=127.0.0.1 -JtargetPort=11211
```

### Notlar

- Yük testinden önce `can-cache-application` çalışıyor olmalıdır.
- Her JMeter worker'ı bir TCP bağlantısını açık tutar; taşıma hatasında bir kez
  yeniden bağlanır. Böylece her iterasyonda TCP el sıkışması ölçülmez.
- `.jtl` sonuçlarını ilgili `nfr/*.md` kabul kriterleriyle karşılaştırın.
