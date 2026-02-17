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

### Typical overrides

Use extra JMeter args after `--`:

```bash
./can-cache-performance-tests/run-local.sh medium -- -JtargetHost=127.0.0.1 -JtargetPort=11211
```

### Notes

- Start `can-cache-application` before running load tests.
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

### Sık kullanılan override'lar

`--` sonrasında ekstra JMeter argümanları verilebilir:

```bash
./can-cache-performance-tests/run-local.sh medium -- -JtargetHost=127.0.0.1 -JtargetPort=11211
```

### Notlar

- Yük testinden önce `can-cache-application` çalışıyor olmalıdır.
- `.jtl` sonuçlarını ilgili `nfr/*.md` kabul kriterleriyle karşılaştırın.
