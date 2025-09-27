# Extra Large Load NFR (XL)

## Objective
Characterise saturation behaviour, capacity headroom, and failure handling by
pushing the cache node to its upper concurrency limit. This test supports
capacity planning and burn-in exercises prior to high-scale events.

## Workload model

| Parameter | Value |
| --- | --- |
| Concurrent users | 100 threads |
| Ramp-up | 90 seconds |
| Duration | 900 seconds |
| Payload | 512 byte values |
| Think time | 25 ms constant timer |
| Operations | `set` + `get` + `delete` per iteration |

## Success criteria

* **Availability:** ≥ 98.5% successful samples (≤ 1.5% errors).
* **Latency:**
  * Average response time ≤ 80 ms.
  * 95th percentile response time ≤ 160 ms.
  * 99th percentile response time ≤ 250 ms.
* **Resource utilisation:** CPU may peak up to 90% but should avoid sustained
  pegging (> 30 seconds). Heap usage must stay below 85% with no allocation
  failures or `OutOfMemoryError`.
* **Stability:** Application remains reachable, accepts new connections, and
  continues to replicate data without exceeding 1 second of lag.

## Observability and reporting

* Persist detailed metrics to `results/can-cache-xl.jtl` and retain the console
  summary output for historical comparison.
* Capture per-second CPU, memory, GC, and network telemetry. Flag any GC pause
  longer than 250 ms for investigation.
* Monitor multicast discovery, replication, and snapshot logs to surface back
  pressure or retry loops triggered by the high load.

## Exit conditions

* Success criteria satisfied for the final 8 minutes of the run.
* The system remains responsive and recovers to baseline CPU (< 50%) and heap
  (< 60%) within 5 minutes of test completion.
* No data loss or cache corruption detected from spot-checks performed after the
  test (e.g., manual `get` on recently written keys).
