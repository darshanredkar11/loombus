# Gate 5 — Cumulative Flow Tests

**Status:** Flow-level coverage is in place; latest CI runs validate the cumulative suite. Benchmark gate remains separate.

Gate 5 tests the documented supported flow families together with primitive/regression tests. It does not add runtime features or change the ownership-first thesis.

## Flow coverage

| Flow | Automated coverage | Evidence |
|---|---|---|
| Stateful aggregate | Commands mutate one aggregate serially under endpoint concurrency 1 | `statefulAggregateProcessesCommandsSequentially`, `statefulEndpointDoesNotCorruptOwnedStateUnderConcurrentProducers` |
| Parallel decisions | Independent endpoint evaluations and fan-in result correlation | `fanOutFanInCorrelatesIndependentChildResults` |
| Publish/subscribe | Normal delivery, partial admission, capacity recovery, subscriber failure recovery | `publishReachesAllSubscribers`, `publishCanPartiallyAdmitSubscribersAndRecoverCapacity`, `subscriberFailureDoesNotKillEndpointWorkerOrLeakCapacity` |
| Blocking workflow | Blocking stage executes behind endpoint admission and stage order is observed | `blockingWorkflowUsesEndpointBudgetAndCompletesStagesInOrder` |
| Workflow/session owner | Explicit state transitions remain serialized and invalid transitions fail | `workflowSessionOwnerSerializesTransitions` |
| Pipeline / ownership transfer | Stage A returns state to Stage B; same reference crosses sequential handoff | `pipelineTransfersMutableStateBetweenSequentialOwners` |
| Overload protection | REJECT, TIMEOUT, WAIT release/recovery, close-aware waiting producer, capacity after handler failure | `boundedRejectBackpressure`, `timeoutBackpressureEventuallyRejects`, `waitBackpressureAdmitsProducerWhenCapacityIsReleased`, `closeRejectsNewAdmissionAndWakesBlockedWaiters`, `capacityIsRecoveredAfterHandlerFailure` |
| Fan-out/fan-in | Children execute concurrently; aggregate is correlated | `fanOutFanInCorrelatesIndependentChildResults` |
| Endpoint isolation | Saturated endpoint A does not consume endpoint B mailbox budget; peak concurrency bound tested | `endpointAdmissionBudgetsRemainIndependent`, `endpointConcurrencyBudgetIsEnforced` |

## Lifecycle and cross-cutting regression coverage

- Close/drain of already admitted work.
- Admission/close linearization and bounded rejection of WAIT producers.
- Bus close idempotence, injected-executor ownership, and concurrent registration/close.
- Worker survival after handler/subscriber failure and idle interruption.
- Cleanup after partial worker startup rejection.
- Caller-only cancellation semantics.
- Reference identity and safe publication.
- Capacity recovery on terminal handler failure.

The full Maven test suite runs cumulatively on each commit; these tests are not run as isolated gate subsets.

## Exclusions / deferred capabilities

- Keyed tenant/session serialization (future partitioning; not in the current API).
- Structured cancellation and operation cancellation.
- Durable delivery/replay.
- Atomic publish fan-out or delivery receipts.
- Subscriber unregistration/unsubscribe.
- Fairness/priority or hard CPU, memory, and downstream isolation.

## Gate 5 exit

Gate 5 is accepted when the entire test suite passes on the latest code, no required supported flow has a correctness-critical gap, and the latest CI test workflow is green. Gate 6 remains a benchmark review and cannot substitute for correctness evidence.