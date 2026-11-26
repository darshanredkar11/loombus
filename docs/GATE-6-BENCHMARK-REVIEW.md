# Gate 6 — Benchmark Review

**Status:** Repeated-round benchmark harness and smoke results recorded; final readiness still requires CI on the latest accumulated commit.

## Thesis and scope

LoomBus is **not** attempting to prove parallel streams are bad or to beat them for CPU transformation. CPU transformation is a control workload. The benchmark focuses on operations where a blocking-oriented execution model with explicit endpoint admission/concurrency may be operationally preferable:

1. Blocking I/O fan-out.
2. A scarce downstream resource pool.
3. CPU transformation as a control.

## Baseline limitations identified

The previous benchmark had one measured run per mode, fixed mode order, and only batch elapsed time/count. It did not warm up, repeat, summarize medians, or report operation latency distributions. Those numbers are useful as a smoke baseline, not as performance conclusions.

### Single-run smoke baseline (Java 21, GitHub Actions, 4 reported processors)

**Run:** [45872249032](https://github.com/darshanredkar11/loombus/actions/runs/45872249032)

| Workload | Parallel stream | LoomBus | Interpretation |
|---|---:|---:|---|
| CPU control, 100k transforms | 18 ms | 61 ms | Stream is faster in this control case; not a target workload |
| Blocking fan-out, 1,000 × 15 ms | 3,773 ms | 244 ms | Illustrates the blocking fan-out target |
| Downstream semaphore, 500 × 10 ms, 16 slots | 1,262 ms | 325 ms | Illustrates limited-resource fan-out |

These are **single-round observations**, not reproducible performance claims. The benchmark is synthetic (sleep-based service time) and GitHub runner timing is noisy.

## Repeated-round smoke result

**Run:** [45873722347](https://github.com/darshanredkar11/loombus/actions/runs/45873722347)  
**Runtime:** Temurin 21.0.13, 4 reported processors. One warmup + three measured rounds. This run corrects latency sample start to the measured submission boundary. It is an Actions smoke run, not an independent performance study.

| Workload | Stream median | LoomBus median | Stream throughput | LoomBus throughput | Stream p50/p95/p99 ms | LoomBus p50/p95/p99 ms |
|---|---:|---:|---:|---:|---|---|
| CPU control, 100k | 16.415 ms | 44.055 ms | 6.09M/s | 2.27M/s | 7.621 / 15.040 / 15.988 | 16.064 / 28.649 / 29.983 |
| Blocking fan-out, 1,000 × 15 ms | 3,788.234 ms | 485.546 ms | 264.0/s | 2,059.5/s | 1,886.412 / 3,591.977 / 3,742.843 | 242.656 / 455.038 / 470.186 |
| Downstream pool, 500 × 10 ms, 16 slots | 1,271.190 ms | 324.755 ms | 393.3/s | 1,539.6/s | 635.778 / 1,200.985 / 1,251.449 | 162.389 / 304.407 / 314.553 |

The repeated-run results preserve the intended interpretation: parallel streams lead the CPU control; LoomBus is materially faster in these synthetic blocking and constrained-pool scenarios. The measured p95/p99 are batch-submission-to-completion latencies, not just handler service time. These observations motivate further target-workload investigation but are not proof of universal superiority.

## Revised benchmark protocol

The benchmark harness now:

- Runs one warmup and three measured rounds per workload/mode.
- Alternates mode order across rounds to reduce fixed-order bias.
- Reports item count, batch elapsed time, throughput, and per-item end-to-end latency p50/p95/p99.
- Reports median batch elapsed, throughput, and each latency percentile across measured rounds.
- Includes JDK/runtime version and processor count in output.
- Fails the round if result or latency-sample count is incomplete.
- Reuses one LoomBusOps endpoint per helper instance across rounds, so map-call measurements do not repeatedly register/close endpoints; lifecycle churn is not mixed into the map-call comparison.
- Guards against reentrant `LOOMBUS` mapping on the same helper instance to fail explicitly rather than risk a self-deadlock at concurrency 1.

Latency samples are measured from the round's submission start to each item's completion, so they include queue/admission wait and handler service time. This is a lightweight harness—not JMH, not a statistically rigorous benchmark, and not a substitute for independent local runs with controlled hardware, forks, GC profiling, and confidence intervals.

## Result interpretation rules

- Workload-specific only; never generalize a blocking I/O result to all Java streams.
- Correctness, admission, capacity, lifecycle, and ownership tests must pass before timing results matter.
- Do not tune based on one bottleneck or one noisy run.
- A future tuning change must alter one implementation dimension, run all cumulative correctness tests, and compare repeated measurements.
- Native Java remains preferable when it is simpler or sufficient for the use case.

## Gate 6 exit

Gate 6 is accepted when the revised harness compiles and runs in CI, emits complete repeated-round summaries for all three workloads, the artifact/log evidence is available, and cumulative tests pass on the same latest commit. The results document must remain honest about limitations.