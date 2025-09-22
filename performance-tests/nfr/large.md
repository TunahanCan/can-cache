# Large Load NFR (L)

## Objective
Stress CPU, networking, and TTL eviction logic under heavy concurrent access.
This profile simulates peak trading or promotion windows where cache throughput
must remain high without cascading failures or timeouts.

## Workload model

| Parameter | Value |
| --- | --- |
| Concurrent users | 50 threads |
| Ramp-up | 60 seconds |
| Duration | 600 seconds |
| Payload | 256 byte values |
| Think time | 50 ms constant timer |
| Operations | `set` + `get` + `delete` per iteration |

## Success criteria

* **Availability:** ≥ 99.0% successful samples (≤ 1.0% errors).
* **Latency:**
  * Average response time ≤ 55 ms.
  * 95th percentile response time ≤ 110 ms.
  * 99th percentile response time ≤ 180 ms.
* **Resource utilisation:** CPU below 80%, heap usage below 80%, and no more than
  two full GC cycles during the steady-state phase.
* **Capacity:** Cache hit ratio remains ≥ 90% assuming working-set residency.

## Observability and reporting

* Persist detailed metrics to `results/can-cache-large.jtl` and capture a copy of
  JMeter’s console summariser output.
* Record host-level CPU, memory, and network throughput along with application
  logs for correlation.
* Capture Quarkus metrics output (if enabled) or export JMX metrics for offline
  analysis of evictions, TTL expirations, and replication lag.

## Exit conditions

* Success criteria satisfied for the final 5 minutes of the steady-state window.
* No replication failures, snapshot write errors, or thread starvation detected
  in logs or monitoring.
* System recovers to nominal resource consumption within 2 minutes after the
  test completes.
