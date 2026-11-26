# LoomBus Functional Flow Specifications — Gate 2

**Status:** Draft for review  
**Foundation:** [FOUNDATION.md](FOUNDATION.md)  
**Scope:** Original documented use cases expanded into ownership, sequence, concurrency, capacity, outcomes, failure, and lifecycle  
**Version target:** LoomBus 0.1

This document elaborates the use cases in USE-CASES.md and PRODUCTION-SCENARIOS.md. It records current intent and explicitly labels gaps. It does not add runtime capabilities or settle contracts that remain open in FOUNDATION.md.

## Flow-spec rules

Each flow records intent, boundary, inputs/outputs, ownership, sequence, concurrency/ordering, capacity, failure, lifecycle, and acceptance tests.

**Ownership terms:** caller-owned before admission; transferred after successful admission under the API convention; endpoint-owned while handled; parent-owned for aggregation; shared when concurrent access is intentional. Java does not enforce move semantics. Prefer immutable commands and events.

An endpoint is not mandatory for every stage. Direct calls or native Java constructs remain preferable where they express the flow more clearly.

---

# Flow 1 — Stateful domain aggregate

**Existing scenarios:** account, cart, authentication session, workflow, device state, serialized domain commands.

**Intent:** Serialize commands that mutate one logical state and preserve invariants without requiring producers to coordinate through locks.

**Actors/boundary:** multiple producers; one stateful endpoint with concurrency 1; endpoint handler owns mutable aggregate; optional caller awaiting response.

**Input/output:** immutable command preferred; endpoint-owned aggregate; return a result/snapshot or failure.

**Ownership transitions**
1. Producer creates command and owns it.
2. Successful admission transfers command reference under LoomBus convention; failed admission leaves ownership with producer.
3. Single handler worker mutates endpoint-owned aggregate.
4. Handler returns a result; caller receives it.

**Sequence**
1. Producer sends command to endpoint.
2. Admission reserves capacity according to policy.
3. Accepted command enters mailbox.
4. Single worker handles in mailbox acceptance order.
5. State transition occurs.
6. Future completes normally or exceptionally.
7. Capacity is released at processing completion.

**Concurrency/order:** handler concurrency 1; sequential processing by acceptance order; no cross-endpoint global order; concurrent producer invocation order is not promised.

**Capacity:** finite admitted-but-unfinished work; WAIT, REJECT, or TIMEOUT.

**Failure:** admission failure means no handling and no ownership transfer; handler failure reaches request future; process failure is not recoverable/durable; close/admission race is unresolved.

**Lifecycle/cancellation:** caller future cancellation does not imply command cancellation; drain and close outcomes are open.

**Acceptance tests:** multi-field invariant under concurrent producers; acceptance-order processing; rejection causes no mutation; capacity recovers after handler failure; close races deterministic.

---

# Flow 2 — Parallel decision / risk evaluation

**Existing scenarios:** MFA/risk, geo/device/behavior/reputation/policy checks, parallel aggregation.

**Intent:** Run independent evaluators concurrently, with parent-owned aggregation state.

**Actors/boundary:** parent request; named evaluator endpoints with independent budgets; parent aggregation logic.

**Input/output:** request or immutable evaluation input; evaluator-local state; child results; combined parent decision.

**Ownership:** parent owns request and aggregate; children receive immutable inputs or explicitly transferred state; each child returns a result and does not mutate parent aggregate.

**Sequence**
1. Parent selects independent checks.
2. Parent submits each check.
3. Each endpoint applies its own admission policy.
4. Accepted checks run concurrently within endpoint budgets.
5. Child futures complete with values or failures.
6. Parent applies selected outcome policy (all required, partial, fallback, etc.).
7. Parent-owned aggregation produces result.

**Concurrency/order:** checks may overlap; completion order is not guaranteed; aggregation must correlate results by identity and must not depend on completion order.

**Capacity:** separate evaluator capacities; endpoint limits are not an end-to-end request budget; fan-out can amplify load.

**Failure:** individual admission rejection, evaluator exception, partial success, parent aggregation failure, or caller abandonment. The business policy for all-required/partial/fallback/fail-fast remains application-defined and must be explicit.

**Lifecycle/cancellation:** child work may continue after parent cancellation/failure. Structured cancellation is not currently promised.

**Acceptance tests:** overlap; independent budgets; completion order does not affect result; parent aggregate is not child-mutated; failure policy deterministic; cancellation claims truthful.

---

# Flow 3 — Event fan-out / publish-subscribe

**Existing scenarios:** authentication/user events; audit, metrics, email, SMS, notification, local side effects.

**Intent:** Notify independent in-process subscribers without coupling the publisher to each implementation.

**Actors/boundary:** publisher; event endpoint and subscriber registry; subscriber handlers.

**Input/output:** preferably immutable event; publish/delivery status is subject to current API contract; no durable storage or crash recovery.

**Ownership:** publisher owns event before publish; immutable payload is preferred because the same object reference may be delivered to subscribers; subscribers own their side-effect state and should not mutate the shared event.

**Sequence**
1. Publisher creates event.
2. Publisher invokes publish.
3. Runtime identifies current subscribers (registration race semantics still open).
4. Delivery admission is attempted per subscriber.
5. Accepted deliveries are queued.
6. Subscriber handler processes event.
7. Subscriber failure is reported through current draft mechanism.
8. Capacity is released per completed delivery.

**Concurrency/order:** deliveries may overlap; no cross-subscriber completion order; concurrent-publish ordering needs a defined contract.

**Capacity:** subscriber delivery is individually admitted; current implementation does not guarantee all-or-nothing fan-out; partial delivery may occur.

**Failure:** a later subscriber admission can fail after earlier subscribers accepted; subscriber handler failure has weak observability today; no durable retry/replay.

**Lifecycle/cancellation:** no current guarantee that publish can cancel queued deliveries; unsubscribe/unregister contract is not established.

**Acceptance tests:** delivery under normal load; event identity/reference semantics; deterministic registration timing behavior; subscriber failure isolation and observability; partial fan-out outcomes visible.

---

# Flow 4 — Blocking database / HTTP workflow

**Existing scenarios:** blocking I/O; DB/HTTP; risk decision; customer/profile lookup.

**Intent:** Keep ordinary blocking business logic while providing a bounded endpoint boundary when admission, concurrency, or reuse of an operation boundary is valuable.

**Actors/boundary:** caller; named business endpoint; virtual-thread handler; DB, HTTP client, and other downstream resources.

**Input/output:** immutable request; handler-local mutable state; result/failure. Shared clients, pools, caches, and repositories remain independently concurrency-safe.

**Ownership:** caller owns request pre-admission; successful admission transfers under API convention; handler owns invocation-local state; external resources are not owned or protected by LoomBus.

**Sequence**
1. Caller requests endpoint.
2. Admission applies capacity/backpressure.
3. Accepted operation runs in a virtual thread under endpoint concurrency.
4. Handler performs blocking calls and transformations.
5. Handler returns or throws.
6. Request future completes.
7. Capacity is released.

**Concurrency/order:** operations overlap only within configured concurrency; concurrency 1 serializes requests; different endpoint budgets do not imply hard resource isolation.

**Capacity:** tune endpoint concurrency to downstream pool limits; virtual threads do not create DB/API capacity.

**Failure:** downstream exceptions propagate unless transformed; rejection means handler did not run; partial downstream side effects may have occurred; LoomBus provides no transaction/rollback.

**Lifecycle/cancellation:** caller cancellation does not guarantee blocking call cancellation; interruption and client behavior require verification.

**Acceptance tests:** bounded overlap; admission policy; downstream pool safety; exception propagation; partial side-effect behavior; cancellation claims tested.

**Fit check:** if a direct call with virtual threads is sufficient, do not add LoomBus.

---

# Flow 5 — Stateful workflow / session owner

**Existing scenarios:** session/workflow receiving events from HTTP, timers, Kafka, users, internal producers.

**Intent:** Serialize transitions for one logical workflow/session.

**Actors/boundary:** multiple event sources; workflow endpoint; owned state; optional response caller.

**Input/output:** workflow command/event; retained mutable workflow state; acknowledgement, transition result, snapshot, or failure.

**Ownership:** sources own input before admission; workflow endpoint owns state; commands/events should be immutable; state should not escape as mutable reference; return immutable snapshots/results.

**Sequence**
1. Source constructs command/event.
2. Source submits to workflow endpoint.
3. Admission is applied.
4. Accepted item enters mailbox.
5. Single-concurrency handler validates current state and command.
6. Handler applies transition or returns domain rejection.
7. Future completes and capacity is released.

**Concurrency/order:** concurrency 1; acceptance-order transitions; different endpoints may run independently. One endpoint is suitable for a bounded logical domain, not automatically an unbounded set of session IDs.

**Capacity:** finite; upstream must handle WAIT/REJECT/TIMEOUT; avoid endpoint-per-arbitrary-key growth.

**Failure:** invalid transition should not corrupt state; exceptions may occur after partial mutation; admission failure means no transition; in-memory state is lost on process crash unless persisted separately.

**Lifecycle/cancellation:** caller cancellation does not necessarily stop an admitted transition; close/drain semantics remain open.

**Acceptance tests:** concurrent producers preserve invariants; invalid transitions safe; rejection leaves state unchanged; acceptance order respected; mutable state does not escape owner.

---

# Flow 6 — Multi-stage pipeline / ownership transfer

**Existing scenarios:** Create → Validate → Enrich → Decide → Persist; operation context shared across stages.

**Intent:** Pass operation state through transformations rather than allowing several stages to concurrently mutate shared context.

**Actors/boundary:** caller; optional stage endpoints; intermediate operation-state values; final consumer.

**Input/output:** initial operation; intermediate stage results; final result.

**Ownership transitions:** caller transfers to Stage A after admission; Stage A owns during handling; Stage A returns state to Stage B; each stage owns state while processing; final result is returned. Branching requires immutable input or independent ownership; aggregation remains parent-owned.

**Sequence**
1. Submit to Stage A.
2. Stage A validates/transforms and returns next state.
3. Caller/orchestrator passes returned state to Stage B.
4. Stage B enriches and returns state.
5. Continue through declared stages.
6. Each stage applies its own admission/concurrency policy.
7. Final stage returns output.

**Concurrency/order:** stages are sequential for one pipeline invocation unless explicit branching is used; separate operations may overlap; stage concurrency controls overlap across operations; no global ordering implied.

**Capacity:** each stage has separate capacity. Downstream admission can fail after upstream side effects. No automatic rollback or compensation.

**Failure:** validation/domain failure stops pipeline by application rule; handler failure completes stage exceptionally; downstream admission failure leaves prior-stage effects possible; branch failures need parent policy.

**Lifecycle/cancellation:** cancellation propagation is not guaranteed today; endpoints close independently; no durable pipeline recovery.

**Acceptance tests:** stage order; ownership transitions; clear terminal failure; partial side effects accounted for; separate pipeline invocations overlap safely; no mutation after transfer.

---

# Flow 7 — Overload protection / bounded admission

**Existing scenarios:** burst traffic, producer/consumer rate mismatch, WAIT/REJECT/TIMEOUT.

**Intent:** Prevent unbounded backlog and make overload visible.

**Actors/boundary:** producers; endpoint capacity/admission; mailbox; workers; caller observing outcome.

**Input/output:** request/event; outcome is admitted work (eventually completed/failed) or admission rejection/timeout/interruption.

**Ownership:** producer owns before admission; successful admission transfers under convention; rejected admission retains producer ownership; handler owns local mutable state.

**Sequence**
1. Producer submits.
2. Admission attempts capacity reservation.
3. If available, reserve and enqueue.
4. If full, WAIT blocks; REJECT fails immediately; TIMEOUT waits bounded duration then fails.
5. Accepted work is signalled.
6. Handler executes.
7. Completion/failure occurs.
8. Capacity is released.

**Concurrency/order:** capacity and execution concurrency are distinct; capacity can include queued and running work; mailbox acceptance order applies; WAIT fairness is not promised.

**Capacity:** exact accounting must hold across success, failure, cancellation, close, and subscriber delivery.

**Failure:** REJECT immediate; TIMEOUT is admission timeout not handler deadline; WAIT may block indefinitely unless interruption/close is defined; handler failure still releases capacity; worker/runtime failure recovery requires testing.

**Lifecycle/cancellation:** close must unblock waiting producers; producer interruption behavior must be defined; caller future cancellation after admission does not reclaim capacity while processing continues.

**Acceptance tests:** REJECT immediate; TIMEOUT bounded; WAIT admits when capacity frees; close unblocks waiters; interruption no leak; permits released exactly once; capacity never exceeded; rejected messages never processed; stress recovery.

---

# Flow 8 — Parallel aggregation (fan-out/fan-in)

**Existing scenarios:** independent evaluations and parent aggregation.

**Intent:** Execute independent child work concurrently and combine results in parent-owned state.

**Actors/boundary:** parent; child endpoints; result futures; parent aggregator.

**Input/output:** parent request; immutable child inputs; child values/failures; aggregate result.

**Ownership:** parent owns original input and aggregate; children own their local operation state; child results return to parent; children do not mutate shared aggregation.

**Sequence**
1. Parent identifies independent child work.
2. Submit children.
3. Each endpoint applies admission.
4. Children execute concurrently as budgets permit.
5. Parent awaits required outcome rule (all, quorum, first, fallback—application-level).
6. Collect results by identity.
7. Parent combines results.
8. Return aggregate or failure.

**Concurrency/order:** completion order may vary; result correlation must be explicit; parent aggregation is serialized within parent operation.

**Capacity:** separate child endpoint budgets; partial admission is possible; fan-out can amplify load; no automatic parent-to-child budget.

**Failure:** child rejection/failure, partial results, aggregation failure, parent abandonment. No universal fail-fast, fallback, quorum, or sibling cancellation semantics.

**Lifecycle/cancellation:** child work may continue after parent cancellation under current draft.

**Acceptance tests:** children overlap; aggregation deterministic independent of completion order; correct result correlation; chosen failure policy; parent state not child-mutated; partial admission observable.

---

# Flow 9 — Endpoint isolation and resource budgets

**Existing scenarios:** independent endpoint classes (audit, notification, risk, customer operations), each with capacity/concurrency configuration.

**Intent:** Configure separate admission and concurrency budgets for distinct operation classes.

**Actors/boundary:** multiple endpoints; shared bus/executor; possibly shared external resources.

**Input/output:** per-endpoint requests and their results/failures.

**Ownership:** endpoint-local state is confined per handler model; shared downstream resources require their own controls; endpoint separation does not imply dedicated CPU or memory.

**Sequence**
1. Route operation to endpoint.
2. Endpoint applies its admission policy.
3. Accepted work runs via bus execution facility.
4. Handler interacts with shared runtime/downstream resources.
5. Handler completes/fails.
6. Endpoint capacity is released.

**Concurrency/order:** each endpoint configures concurrency; endpoints may share runtime resources; fairness, priority, starvation protection, and hard isolation are not promised.

**Capacity:** per-endpoint admitted-work capacity; shared executor/downstream limits may still couple endpoints.

**Failure:** saturation can be independent at mailbox level; shared resource exhaustion can affect several endpoints; worker recovery remains a test requirement.

**Lifecycle/cancellation:** bus close affects all endpoints and shared execution; per-endpoint close must be distinguished.

**Acceptance tests:** endpoint capacity independently enforced; saturating A does not consume B's mailbox capacity; shared-runtime coupling measured/documented; no fairness claims without tests.

---

# Flow 10 — Keyed tenant/session serialization (future, not current capability)

**Source:** production scenario discusses per-tenant/per-session serialization and future partitioning/sharding.

**Status:** Not a dedicated LoomBus API today. Excluded from Gate 2 core acceptance.

**Intent:** Serialize operations per key while allowing different keys to process concurrently.

**Desired ownership:** one logical owner per key or partition; same-key ordering; cross-key concurrency.

**Current limitation:** a single stateful endpoint serializes all commands; arbitrary endpoint-per-key risks unbounded runtime/lifecycle growth.

**Future decisions:** partition count; key-to-partition mapping; ordering; capacity per partition/global capacity; hot-key overload; partition lifecycle/migration; state recovery; whether LoomBus should own keyed scheduling.

No implementation is authorized by this specification.

---

# Cross-flow invariants

1. Ownership: after transfer, previous owner does not concurrently mutate mutable reference.
2. Admission: every submission has a clear accepted or rejected outcome.
3. Capacity: admitted work consumes capacity exactly once until terminal processing.
4. Completion: each request reaches a terminal result/failure per lifecycle contract.
5. Ordering: endpoint-local; acceptance order for concurrency one; no global order.
6. Concurrency: handler overlap does not exceed configured endpoint concurrency.
7. Failure: handler failure must not leak capacity or silently break worker lifecycle.
8. Overload: endpoint does not create an unbounded backlog.
9. Visibility: safe publication follows the Java Memory Model.
10. Durability: in-memory accepted work is not durable across process failure.
11. Isolation: endpoint budgets do not imply hard CPU, memory, fairness, or downstream isolation.
12. Cancellation: caller cancellation does not prove operation stopped.
13. Fan-out: publish does not promise atomic all-subscriber delivery today.
14. Simplicity: use native Java if it is clearer.

# Gate 2 acceptance criteria

Gate 2 is documented for review when:

- All original use-case families have input, boundary, ownership, sequence, concurrency, capacity, failure, and lifecycle detail.
- Future partitioning/keyed endpoint ideas are separated from current API capabilities.
- Partial failures and side effects are visible.
- No flow claims semantics the API lacks.
- Native Java alternatives remain acknowledged.
- These specs can be used to derive Gate 3 implementation-gap findings and Gate 4 correctness tests.

**Status:** Flow specs are written for review, not approved. Business-level failure policies and open contracts remain unresolved.

## Sources

- [FOUNDATION.md](FOUNDATION.md)
- [FEATURES.md](FEATURES.md)
- [USE-CASES.md](USE-CASES.md)
- [PRODUCTION-SCENARIOS.md](PRODUCTION-SCENARIOS.md)
- [BENCHMARK-PLAN.md](BENCHMARK-PLAN.md)
