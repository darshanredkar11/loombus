# Gate 3 — Implementation Audit

**Branch:** `benchmark/java21-baseline`  
**Status:** Source audit completed; fixes started. This is not a production-readiness sign-off.

## Audit baseline

The audit was performed against the Gate 0/1 foundation, Gate 2 functional-flow specifications, and the implementation on this branch. Source could be reviewed remotely, but the repository could not be cloned into the local execution environment; local Maven execution is therefore unavailable. CI is the execution authority for the changes.

## Findings

| ID | Severity | Finding | Status |
|---|---|---|---|
| G3-01 | High | Admission and endpoint close had no shared linearization lock. The final closed check and enqueue were separate, allowing a close/admission race with unclear acceptance semantics. | **Fixed:** admission enqueue/signal and close flag now synchronize on the same lock. Envelopes accepted before close must drain; post-close admission rejects. |
| G3-02 | High | WAIT producers can block on capacity; close does not independently interrupt them. They rely on capacity being released by draining work, then observe closed and reject. | **Contract retained/documented for validation:** add/verify deterministic close-waiter tests. |
| G3-03 | Medium | Publish performs per-subscriber enqueue sequentially. A rejection midway can leave earlier subscribers admitted and later subscribers not delivered. No delivery receipt exists. | **Known partial-delivery contract:** explicitly documented; atomic fan-out is not promised. |
| G3-04 | Medium | Subscriber failures have no structured result channel and currently route to the executing thread's uncaught-exception handler. | **Open:** observability mechanism and isolation contract need a deliberate API decision; don't invent retries. |
| G3-05 | Medium | Endpoint close is non-blocking; it signals workers to finish draining but does not expose an await-termination API. | **Open:** API/lifecycle choice; tests should verify accepted work drains. |
| G3-06 | Medium | Bus close shuts the injected executor as well as the default executor. Constructor injection does not communicate executor ownership, and executor close behavior can surprise callers. | **Open:** define injected-executor ownership; defer public API change until review of intended contract. |
| G3-07 | Medium | Handler failure completes request futures exceptionally and capacity is released in finally. A subscriber failure is not tied to a request future. | Existing behavior; add regression coverage for worker survival and capacity release. |
| G3-08 | Medium | Caller cancellation does not cancel queued/running work; this is distinct from operation cancellation and is currently implicit. | **Clarify as caller-only cancellation.** No operation-cancellation API introduced. |
| G3-09 | Low | No unsubscribe or endpoint unregister API; dynamic subscription lifecycle is not covered by current flows. | Deferred; not a Gate 4 blocker unless a flow requires it. |
| G3-10 | Low | Endpoint isolation is mailbox/concurrency-budget isolation, not executor, CPU, memory, fairness, or downstream isolation. | Keep claim narrow; add workload measurements in later validation. |

## Fix applied in Gate 3

- Shared admission lock defines the close/admission linearization boundary.
- After capacity is acquired, the final closed check, mailbox offer, and work signal are inside the same lock as close's state transition.
- Therefore close cannot interleave between the final closed check and mailbox offer; accepted work is signalled and drainable.
- A producer that reserved capacity but loses the race to close releases its permit and rejects.

## Gate 3 exit criteria

- [x] Read actual implementation against foundation and flows.
- [x] Record source-grounded gaps and severity.
- [x] Fix admission/close race.
- [x] Add a regression test for post-close and capacity-waiting admission rejection.
- [ ] Remote CI green for the latest commits.
- [ ] Revisit open contracts only when they block a selected flow or accumulated test.

## Next gates

**Gate 4 — Contract decisions and correctness tests:** settle only the decisions necessary for the current API, and convert them into tests: close/drain, blocked admission, failure recovery, cancellation, publish partial admission, subscriber error behavior, concurrency/order, and capacity accounting.

**Gate 5 — Cumulative flow tests:** run the complete test suite and expand from primitive-level tests to cumulative flow coverage (stateful aggregate, fan-out/fan-in, blocking workflow, pipeline transfer, overload, endpoint budgets). Each new gate runs the prior gates together.

**Gate 6 — Benchmark review:** only after correctness gates pass, interpret workload benchmarks against the ownership-first goal; CPU-only stream comparison is a control, not a target claim.

**Ready for user cumulative testing:** all required gates are green on the branch; no unresolved correctness-critical findings; CI reports pass for both tests and benchmark harness build/run; final review notes identify any explicitly deferred capabilities.
