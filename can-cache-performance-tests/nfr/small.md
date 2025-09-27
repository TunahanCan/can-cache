# Small Load NFR (S)

## Objective
Validate basic availability and response time of a single Can Cache node under a
light, smoke-test style workload. This profile ensures the deployment is healthy
before executing heavier scenarios or promoting changes.

## Workload model

| Parameter | Value |
| --- | --- |
| Concurrent users | 5 threads |
| Ramp-up | 10 seconds |
| Duration | 120 seconds |
| Payload | 64 byte values |
| Think time | 100 ms constant timer |
| Operations | `set` + `get` + `delete` per iteration |

## Success criteria

* **Availability:** ≥ 99.9% successful samples (no more than 0.1% errors).
* **Latency:**
  * Average response time ≤ 25 ms.
  * 95th percentile response time ≤ 50 ms.
* **Resource utilisation:** CPU below 40% and heap usage below 60% on the
  targeted node during the steady-state window.

## Observability and reporting

* Capture CLI output from JMeter’s summariser and persist `results/can-cache-small.jtl`.
* Track host metrics (CPU, memory, network) via operating system tooling or
  an external monitor while the test runs.
* Annotate dashboards with the test identifier `perf-small` for traceability.

## Exit conditions

* All success criteria met for at least the final minute of the run.
* No critical or high severity errors in the Can Cache server logs.
* Snapshot and replication subsystems remain operational (no stuck threads).
