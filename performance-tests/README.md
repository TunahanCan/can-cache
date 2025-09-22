# JMeter Load Test Suite for Can Cache

This directory contains non-functional performance tests for the Can Cache
cancached-compatible TCP service. The scenarios are grouped by target load
profile and can be executed with the Apache JMeter command line interface.

## Prerequisites

* Apache JMeter 5.6 or newer with the bundled Groovy runtime.
* A running Can Cache instance that listens on the cancached port (default
  `127.0.0.1:11211`). Start the application with `./mvnw quarkus:dev` or use the
  packaged JAR as described in the project README.
* Optional: a writable `results/` directory to store `.jtl` output files. The
  paths can be overridden with JMeter properties.

## Running the plans

Each load profile has its own `.jmx` file under `performance-tests/jmeter` and a
corresponding non-functional requirement (NFR) under `performance-tests/nfr`.
Execute a plan with the JMeter CLI:

```bash
jmeter -n \
  -t performance-tests/jmeter/can-cache-small.jmx \
  -l results/can-cache-small.jtl \
  -JtargetHost=127.0.0.1 \
  -JtargetPort=11211
```

Commonly used override properties:

| Property | Description | Default |
| --- | --- | --- |
| `targetHost` | Hostname or IP of the Can Cache node. | `127.0.0.1` |
| `targetPort` | TCP port of the cancached endpoint. | `11211` |
| `ttlSeconds` | TTL assigned to the `set` command. | `60` |
| `connectTimeoutMillis` | Socket connect timeout in milliseconds. | `1000` |
| `readTimeoutMillis` | Socket read timeout in milliseconds. | `3000` |
| `keyPrefix` | Prefix used for generated cache keys. | `perf-` |
| `payloadSize` | Size of the generated payload in bytes (plan-specific default). | varies |
| `durationSeconds` | Total runtime of the thread group (plan-specific default). | varies |
| `resultFile` | Output `.jtl` path for aggregated metrics. | varies |

All plans rely on a Groovy JSR223 sampler that performs a full cancached
round-trip (set, get, delete) and validates responses. The thread groups run for
fixed durations with no loop limits to make the execution time deterministic.
Adjust thread counts, payload sizes, or timers in each plan to tune the pressure
exerted on the cache node.

## Load profiles

| Profile | Threads | Ramp-up | Duration | Payload | Think time | Purpose |
| --- | --- | --- | --- | --- | --- | --- |
| Small (`can-cache-small.jmx`) | 5 | 10 s | 120 s | 64 B | 100 ms | Baseline health & smoke under light load. |
| Medium (`can-cache-medium.jmx`) | 20 | 30 s | 300 s | 128 B | 75 ms | Steady mid-tier concurrency to validate scaling behavior. |
| Large (`can-cache-large.jmx`) | 50 | 60 s | 600 s | 256 B | 50 ms | High concurrency stressing CPU and network queues. |
| Extra Large (`can-cache-xl.jmx`) | 100 | 90 s | 900 s | 512 B | 25 ms | Saturation-level workload for capacity planning. |

Review the matching NFR files for success criteria, latency/error budgets, and
operational guardrails associated with each load level.
