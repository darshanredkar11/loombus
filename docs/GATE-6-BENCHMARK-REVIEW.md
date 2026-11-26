# Gate 6 — Benchmark Review

**Status:** Accepted for the current benchmark harness. The latest code commit `98f6fb3` passed the benchmark workflow [45874246811](https://github.com/darshanredkar11/loombus/actions/runs/45874246811); the latest documentation-only commit is still being revalidated.

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

**Run:** [45874246811](https://github.com/darshanredkar11/loombus/actions/runs/45874246811)  
**Runtime:** Temurin 21.0.13, 4 reported processors. One warmup + three measured rounds. This run uses the corrected latency sample boundary and is an Actions smoke run, not an independent performance study.
**Artifact:** [loombus-java21-baseline](https://github.com/darshanredkar11/loombus/actions/runs/45874246811/artifacts/15145070448)

| Workload | Stream median | LoomBus median | Stream throughput | LoomBus throughput | Stream p50/p95/p99 ms | LoomBus p50/p95/p99 ms |
|---|---:|---:|---:|---:|---|---|
| CPU control, 100k | 20.802 ms | 46.751 ms | 4.81M/s | 2.14M/s | 10.024 / 17.979 / 19.579 | 31.298 / 43.292 / 45.190 |
| Blocking fan-out, 1,000 × 15 ms | 3,774.141 ms | 486.295 ms | 265.0/s | 2,056.4/s | 1,887.216 / 3,592.951 / 3,743.897 | 243.025 / 455.792 / 470.997 |
| Downstream pool, 500 × 10 ms, 16 slots | 1,261.426 ms | 325.183 ms | 396.4/s | 1,537.6/s | 635.939 / 1,200.973 / 1,251.317 | 162.312 / 304.763 / 314.931 |

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

Gate 6 acceptance criteria are met for the current code: the revised harness compiled and ran all three workloads with one warmup/three measured rounds, summaries and p50/p95/p99 latency, and the artifact is available at [15145070448](https://github.com/darshanredkar11/loombus/actions/runs/45874246811/artifacts/15145070448). The cumulative test workflow also passed at `98f6fb3`. The latest documentation-only commit still requires final CI confirmation. Results remain workload-specific and non-statistical.