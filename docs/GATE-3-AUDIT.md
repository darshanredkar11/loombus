# Gate 3 — Implementation Audit

**Branch:** `benchmark/java21-baseline`  
**Status:** Gate 3 complete; correctness fixes applied. Latest code commit `98f6fb3` passed both workflows. CI on the current branch remains the final accumulated-readiness authority. Not a production-readiness sign-off.

## Audit baseline

The implementation was reviewed against the original ownership-first thesis, Gate 0/1 foundation, and Gate 2 flow specifications. Source was reviewed through GitHub; local clone/test execution is blocked by network DNS in the execution environment, so CI is used for actual compilation and test evidence.

## Findings and disposition

| ID | Severity | Finding | Status |
|---|---|---|---|
| G3-01 | High | Endpoint close and admission had a race between final closed check and enqueue. | **Fixed:** admission enqueue/signal and close transition use the same lock. |
| G3-02 | High | WAIT producers could remain blocked when endpoint closed while capacity stayed occupied. | **Fixed:** WAIT admission periodically checks closed (50 ms implementation interval); it rejects without fabricating capacity or leaking permits. |
| G3-03 | Medium | Publish is sequential per-subscriber admission; rejection may leave partial delivery. | **Contract/test:** partial admission is explicit; no atomic fan-out or receipt is claimed; recovery test added. |
| G3-04 | Medium | Subscriber failures have weak observability through the worker uncaught-exception handler. | **Deferred:** documented; no retries or new reporting API invented. Worker continues and capacity is released. |
| G3-05 | Medium | Endpoint close is non-blocking; no endpoint-level await/drain API. | **Deferred API:** close/drain behavior is tested; bus close waits through executor termination. |
| G3-06 | Medium | Injected executor ownership was unclear. | **Resolved:** executor ownership transfers to LoomBus; close shuts it down; tested. |
| G3-07 | Medium | Handler/subscriber failure recovery and capacity accounting needed evidence. | **Tested:** request failure completion/capacity release and subscriber worker recovery. |
| G3-08 | Medium | Caller cancellation could be mistaken for operation cancellation. | **Resolved:** cancellation is caller-only; regression test confirms admitted work continues and capacity is recovered. |
| G3-09 | Low | No unsubscribe/unregister API. | **Deferred:** not required by current selected flows. |
| G3-10 | Low | Endpoint isolation is not hard resource isolation. | **Resolved claim boundary:** per-endpoint mailbox/concurrency budgets only; independent budget test added. |
| G3-11 | High | Bus close could race registration and leave a newly registered endpoint live on a closed executor. | **Fixed:** registration and close transition serialize; registration after close rejects; race test added. |
| G3-12 | Medium | An interrupted idle worker could exit permanently and reduce endpoint concurrency. | **Fixed/tested:** worker consumes the idle interruption and remains alive while endpoint is open. |
| G3-13 | Medium | Executor rejection during partial endpoint worker startup could leave earlier workers alive. | **Fixed/tested:** constructor marks endpoint closed before propagating submission failure; earlier workers exit. |
| G3-14 | Medium | Benchmark helper created and closed a new endpoint for every map call, accumulating closed endpoints in the bus registry. | **Fixed:** `LoomBusOps` now reuses one helper endpoint per instance; concurrent mapper isolation is regression-tested. |

## Gate 3 changes

- Linearized endpoint admission/close.
- Bounded close-aware WAIT rejection.
- Bus lifecycle lock and idempotent close; no registration after bus close.
- Reject publish/subscription after endpoint close.
- Worker recovery after idle interruption.
- Cleanup on partial worker startup failure.
- Reusable `LoomBusOps` endpoint to avoid per-call endpoint churn.

## Non-correctness limitations intentionally retained

- Subscriber failure reporting is still uncaught-handler based.
- Endpoint close does not expose an await-termination/drain API.
- Unsubscribe/unregister, structured cancellation, operation cancellation, durable delivery, atomic fan-out, fairness, and hard resource isolation remain deferred.
- Ownership is a Java API convention, not compiler-enforced move semantics.

## Evidence

- Cumulative Maven test workflow passed at commit `e931c89`: [run 45873339070](https://github.com/darshanredkar11/loombus/actions/runs/45873339070).
- Repeated benchmark harness compiled and ran at commit `08e743e`: [run 45873155850](https://github.com/darshanredkar11/loombus/actions/runs/45873155850).
- The latest branch commits must still have both test and benchmark workflows green before accumulated testing is declared ready.

## Gate 3 exit

- [x] Reviewed the implementation against the thesis and flow specs.
- [x] Recorded source-grounded gaps.
- [x] Fixed the correctness-critical admission, close, registration, worker, and partial-startup lifecycle gaps.
- [x] Added regression coverage for the fixes and current supported flows.
- [x] Confirmed latest code commit `98f6fb3` has green test and benchmark workflows: [test 45874246417](https://github.com/darshanredkar11/loombus/actions/runs/45874246417) and [benchmark 45874246811](https://github.com/darshanredkar11/loombus/actions/runs/45874246811).

Gate 4 decides observable contracts, Gate 5 verifies cumulative flows, and Gate 6 reviews target-workload measurements. None may broaden the original goal.
