# LoomBus Benchmark Plan

## Objective

Benchmarks are not a general-purpose contest between LoomBus and parallel streams. They identify workloads where parallel-stream execution is the wrong operational abstraction, and they measure whether LoomBus can be tuned to handle those workloads better.

## Target workload envelope

1. **Blocking fan-out:** many blocking operations, with configurable service time and fan-out size.
2. **Scarce downstream resource:** many operations sharing a limited DB/HTTP connection semaphore.
3. **Admission pressure:** producer rate exceeds endpoint service capacity; evaluate bounded queue and WAIT/REJECT/TIMEOUT behavior.
4. **Slow/failing handlers:** evaluate error completion, caller cancellation, and workload recovery.
5. **Endpoint isolation:** busy endpoint versus latency-sensitive endpoint; detect interference.
6. **State ownership:** serialized mutation workload with multiple producers; validate correctness and throughput.
7. **Fan-out/fan-in:** multiple independent endpoints and aggregation overhead.
8. **Endpoint lifecycle churn:** repeated registration, use, close, and teardown; inspect virtual-thread and lifecycle costs.

## Non-target workload

Pure CPU transformation is a control case. Parallel streams may be expected to perform better; LoomBus is not intended to win this case.

## Metrics

Every benchmark should capture total elapsed time and throughput; completed, failed, and rejected operations; configured capacity and concurrency; downstream limit where relevant; warmup and measured iteration counts; runtime/JDK, processor count, and OS; latency distribution (at least p50/p95/p99 for workload tests where samples can be collected); memory/GC indicators where practical.

## Experimental discipline

- Keep workloads and inputs deterministic.
- Warm up each implementation before recording.
- Run multiple measured rounds; report each round and a median, not a single result.
- Do not compare modes sequentially with a single cold run.
- Avoid treating wall-clock noise as a regression.
- Separate correctness/stress tests from timing measurements.
- Keep CPU controls visible as controls, not LoomBus performance goals.

## Tuning loop

1. Choose one target workload and state its failure mode.
2. Reproduce with a baseline and correctness assertions.
3. Identify the dominant cost using evidence.
4. Change one implementation dimension.
5. Run correctness tests, stress tests, and the benchmark.
6. Compare results across repeated rounds and ensure the target workload improves without harming semantics.
7. Keep the change, revert it, or move to the next bottleneck.

## First investigation

The current implementation creates a fixed set of long-lived virtual-thread workers per endpoint and uses a ConcurrentLinkedQueue plus Semaphore capacity/work signals. First investigate worker scheduling, queue contention, admission overhead, and endpoint creation/teardown before redesigning the primitive. Do not optimize based on the CPU control result.

## Benchmark-specific sequence (sub-gates, distinct from repository Gates 0–6)

- **B0 — benchmark discipline:** define workload categories, metrics, and repeatability.
- **B1 — correctness/pressure tests:** admission accounting, producer blocking/rejection, cancellation and failure behavior under pressure, queue drain on close.
- **B2 — diagnostics baseline:** repeatable workload rounds, latency distributions, queue-pressure and endpoint lifecycle scenarios.
- **B3 — tuning:** one measured bottleneck at a time.
- **B4 — stress and regression:** larger producer counts, varied capacity/concurrency, sustained workload, repeated CI runs.
- **B5 — publish:** report supported workload envelope, known limits, and reproducible results.

## Interpretation

A win is meaningful only for the workload it targets and only if correctness, overload behavior, and resource limits remain intact. No single benchmark number establishes that LoomBus is universally faster or that parallel streams are generally inferior.
