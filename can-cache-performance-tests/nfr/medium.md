# Medium Load NFR (M)

## Objective
Assess the system’s behaviour under moderate, production-like concurrency. The
medium profile should validate that the node scales predictably and remains
within latency/error budgets with realistic read/write pressure.

## Workload model

| Parameter | Value |
| --- | --- |
| Concurrent users | 20 threads |
| Ramp-up | 30 seconds |
| Duration | 300 seconds |
| Payload | 128 byte values |
| Think time | 75 ms constant timer |
| Operations | `set` + `get` + `delete` per iteration |

## Success criteria

* **Availability:** ≥ 99.5% successful samples (≤ 0.5% errors).
* **Latency:**
  * Average response time ≤ 35 ms.
  * 95th percentile response time ≤ 75 ms.
  * 99th percentile response time ≤ 120 ms.
* **Resource utilisation:** CPU below 60% and heap usage below 70% with no
  sustained GC pauses longer than 150 ms.

## Observability and reporting

* Persist detailed results to `results/can-cache-medium.jtl` and capture JMeter
  summary output.
* Monitor operating system and JVM metrics (CPU, GC, thread counts, network) for
  regressions relative to small-load baselines.
* Record Can Cache metrics (hit rate, eviction counts) if metric reporting is
  enabled during the run.

## Exit conditions

* All success criteria satisfied for the final 3 minutes of the steady-state
  period.
* No WARN/ERROR spikes in the application log beyond transient multicast
  discovery messages.
* Cluster membership remains stable with no unexpected node drops.
