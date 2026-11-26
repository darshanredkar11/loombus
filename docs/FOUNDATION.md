# LoomBus Foundation — Gate 0 and Gate 1

**Status:** Design baseline for review  
**Scope:** Technical goal, user/business value, non-goals, principles, primitive inventory, and contract register  
**Applies to:** LoomBus 0.1 development line

This document is the canonical foundation for Gate 0 and Gate 1. It consolidates the intent in the README, feature guide, use-case guide, and production-scenario guide. It deliberately distinguishes what the project already states, what the current implementation does, and what remains an open contract decision.

It is not an implementation proposal. No data structure, worker model, or optimization becomes authoritative merely because it exists in the current code.

## 1. Goal

### 1.1 Technical goal

LoomBus is a small **in-process communication and execution model for Java virtual-thread applications**.

Its central idea is:

> Prefer moving ownership of mutable operation state between operations over allowing multiple concurrent operations to mutate the same state.

LoomBus provides a communication boundary around virtual-thread execution: named endpoints, mailboxes, request/response, publish/subscribe, bounded admission, and explicit endpoint concurrency.

The technical objective is not to replace Java's concurrency primitives. It is to give application code a consistent, higher-level model for coordinating concurrent business operations without repeatedly assembling locks, shared mutable objects, queues, executor plumbing, callback chains, and worker lifecycle logic inside each business flow.

### 1.2 User and business value goal

The user value described by the repository is to make in-process concurrent application architecture easier to reason about and operate:

- Give a business operation a named communication and execution boundary.
- Make ownership of mutable state explicit at that boundary.
- Allow independent operations to execute concurrently under deliberate budgets.
- Let handlers use ordinary blocking Java code, including blocking I/O, on virtual threads.
- Make capacity and overload behavior visible instead of hiding it in ad-hoc queues.
- Provide consistent request completion, failure propagation, and event-delivery semantics.

The repository does **not** currently define a commercial business model, revenue target, pricing, or a validated customer segment. This document therefore records the user value goal, not an invented monetization strategy.

### 1.3 Success criteria

LoomBus earns its place only when it can demonstrate all of the following:

1. Its endpoint and ownership model makes an application boundary clearer than the equivalent native-Java implementation.
2. The semantics of admission, ordering, completion, failure, cancellation, and shutdown are explicit and testable.
3. Stateful endpoints prevent concurrent handler mutation through the endpoint's configured single-concurrency model.
4. Capacity and backpressure behave predictably under load, failure, and lifecycle transitions.
5. Independent endpoint work can be bounded without claiming that external resources become unlimited.
6. The abstraction remains small enough that a direct method call, Java queue, executor, lock, or structured-concurrency scope is still preferred when it expresses the problem more clearly.
7. Performance claims are limited to reproducible target workloads and never substitute for correctness.

## 2. Non-goals

LoomBus is not intended to be:

- A distributed message broker or durable queue.
- A network RPC framework or service-discovery mechanism.
- A replacement for Kafka, Pulsar, REST, HTTP, or gRPC.
- A database concurrency-control or transaction layer.
- A guarantee that synchronization disappears from Java applications.
- A compile-time ownership/type system equivalent to Rust move semantics.
- A universal replacement for locks, atomics, concurrent collections, executors, queues, or structured concurrency.
- A general-purpose CPU parallelism accelerator or a promise to outperform parallel streams in CPU-bound work.
- A mechanism for cross-process or cross-pod coordination.
- A guarantee of durable delivery, replay, exactly-once processing, or recovery after process failure.

A direct method call remains preferable when no meaningful communication boundary exists. Native Java concurrency primitives remain preferable when they express the requirement more simply.

## 3. Foundational principles

These principles govern feature design and implementation decisions, in priority order.

1. **Ownership before scheduling.** First determine who owns mutable operation state. Worker count and queue choice are subordinate implementation questions.
2. **Transfer over shared mutation.** Prefer passing operation state to the next computation rather than allowing multiple concurrent operations to mutate the same object.
3. **Communication is an explicit boundary.** Endpoint and mailbox semantics should be visible to the application rather than emerging accidentally from executor and lock plumbing.
4. **Concurrency is deliberate.** Each endpoint has an explicit concurrency budget. Independent work may run concurrently; stateful work may be serialized.
5. **Admission is bounded.** Virtual threads make waiting inexpensive, not unlimited. CPU, memory, database pools, downstream services, and queues remain finite.
6. **Blocking is a valid programming model.** A handler may use ordinary blocking I/O on a virtual thread. LoomBus is not a reactive programming framework.
7. **The Java Memory Model still applies.** LoomBus must rely on ordinary Java happens-before guarantees. Ownership-oriented design reduces concurrent mutation; it does not bypass memory safety rules.
8. **Failure and lifecycle are part of the model.** They are not optional implementation clean-up. Their behavior must be specified alongside successful execution.
9. **Correctness precedes performance.** Benchmarks evaluate defined semantics under target workloads. They do not determine the architecture.
10. **Use the narrowest useful abstraction.** Do not introduce LoomBus when native Java is clearer or when the requirement is distributed/durable communication.

## 4. Primitive inventory

The inventory separates conceptual primitives from the current Java types and from composed capabilities. This avoids mistaking an implementation class or a benchmark helper for a foundational concept.

### 4.1 Core conceptual primitives

| Primitive | Responsibility | Depends on |
|---|---|---|
| **Bus** | Owns the in-process endpoint registry and provides the application entry point for registration, lookup, request, publish, and subscription | Endpoint lifecycle |
| **Endpoint** | Named communication and execution boundary; owns handler configuration, mailbox admission, concurrency policy, and subscribers | Mailbox, execution, admission |
| **Mailbox** | Holds admitted messages until endpoint workers process them | Message envelope, capacity, work signalling |
| **Message / envelope** | Represents a request or event plus the completion or delivery context needed by the runtime | Reference semantics, completion |
| **Ownership contract** | Defines which operation may mutate a mutable operation-state object at a given time | Message transfer, endpoint execution |
| **Execution** | Runs endpoint handlers on Java virtual threads | Executor/worker lifecycle |
| **Concurrency budget** | Limits the number of handlers that may execute concurrently for an endpoint | Execution |
| **Capacity** | Bounds admitted work that has not yet completed | Admission and completion |
| **Admission** | Decides whether incoming work is accepted, waits, or is rejected | Capacity, backpressure policy |
| **Backpressure policy** | Defines producer behavior when capacity is unavailable | Admission |
| **Completion** | Delivers a request's result or failure to its caller | Message envelope, handler outcome |
| **Subscriber delivery** | Delivers an event to registered subscribers | Endpoint, mailbox, admission |
| **Lifecycle** | Governs endpoint/bus registration, closing, draining, and worker termination | Admission, mailbox, execution, completion |

The ownership contract is a logical/API contract, not a Java-enforced move operation. The envelope and mailbox are conceptual runtime primitives; their current concrete implementation is private to the endpoint.

### 4.2 Cross-cutting semantic contracts

These are not necessarily standalone classes, but every primitive depends on them:

- Acceptance and ordering.
- Reference identity and ownership transfer.
- Safe publication and Java Memory Model guarantees.
- Capacity accounting and backpressure.
- Request completion and handler failure.
- Subscriber delivery and subscriber failure.
- Caller cancellation versus operation cancellation.
- Admission timeout versus operation deadline.
- Endpoint close, bus close, and drain semantics.
- Per-endpoint limits versus actual resource isolation.
- Composition with structured concurrency.

### 4.3 Current implementation inventory

| Current type/mechanism | Current role | Classification |
|---|---|---|
| LoomBus | Endpoint registry and public bus API | Core public API |
| Endpoint | Handler, worker loops, mailbox, admission, subscribers | Core public API/runtime |
| EndpointConfig | Capacity, concurrency, backpressure, admission timeout | Core public API |
| Backpressure | WAIT, REJECT, TIMEOUT policy names | Core public API |
| RejectedExecutionException | Admission rejection signal | Core public API |
| ExecutorService | Shared bus execution facility; default is a virtual-thread-per-task executor | Runtime support |
| ConcurrentLinkedQueue | Current mailbox storage | Internal implementation choice |
| Semaphore | Current capacity accounting and work signalling | Internal implementation choice |
| Envelope record | Current private message/completion/subscriber wrapper | Internal implementation |
| CopyOnWriteArrayList | Current subscriber registry | Internal implementation choice |
| AtomicBoolean | Current endpoint closed flag | Internal implementation choice |
| LoomBusOps | Side-by-side execution helper used by comparison/benchmark work | Benchmark/support helper, not a core primitive |
| ExecutionMode | Selects parallel-stream or LoomBus mode in that helper | Benchmark/support helper, not a core primitive |

The internal data structures above are **not** architectural commitments. Replacing one must not change a documented contract.

### 4.4 Capabilities composed from primitives

| Capability | Composition |
|---|---|
| Stateful endpoint | Endpoint + mailbox + concurrency budget of one + ownership contract |
| Concurrent endpoint | Endpoint + mailbox + concurrency budget greater than one |
| Request/response | Message + admission + handler execution + completion |
| Publish/subscribe | Event + subscriber registry + one delivery per subscriber |
| Fan-out | Parent operation + multiple independent endpoint requests |
| Fan-in | Parent-owned aggregation of child results |
| Pipeline | Sequential stages with explicit state/reference transfer |
| Overload protection | Capacity + admission + WAIT/REJECT/TIMEOUT |
| Blocking I/O workflow | Endpoint execution on virtual threads + downstream-aware concurrency budget |
| Endpoint isolation | Separate endpoint policies and budgets; stronger scheduling isolation is not currently promised |

Fan-out/fan-in and pipelines are application patterns composed from the core API; they are not currently dedicated runtime primitives.

## 5. Primitive dependency order

The conceptual dependency order is:

1. Define message/reference and ownership semantics.
2. Define endpoint identity and endpoint lifecycle.
3. Define mailbox acceptance and ordering.
4. Define execution and endpoint concurrency.
5. Define capacity accounting and admission.
6. Define WAIT, REJECT, and TIMEOUT behavior.
7. Define request completion and handler failure.
8. Define subscriber delivery and fan-out failure.
9. Define cancellation, deadlines, close, and drain.
10. Validate memory visibility, overload, and isolation.
11. Only then evaluate implementation alternatives and tune performance.

This is a reasoning/dependency order, not an instruction to create one Java class per row.

## 6. Contract register

The following register is the Gate 1 baseline. “Established” means the original project documentation already expresses the principle. “Draft behavior” means the current 0.1 API or implementation exhibits the behavior but it must not automatically be treated as a complete production guarantee. “Open” means the contract needs a deliberate decision.

| ID | Contract | Source-backed baseline | Current draft behavior | Status / decision needed |
|---|---|---|---|---|
| SCOPE-01 | Process boundary | LoomBus is in-process; no network, broker, or serialization boundary | Object references are passed within one JVM | **Established** |
| BUS-01 | Bus and endpoint identity | Endpoints are named and registered on a bus | Duplicate names are rejected; lookup of unknown names fails; no unregister API | **Draft** — specify registry and re-registration lifecycle |
| END-01 | Endpoint responsibility | Endpoint is the unit of capacity and execution concurrency | Handler, queue, workers, and subscribers are combined in Endpoint | **Established concept** — keep public semantics separate from implementation shape |
| OWN-01 | Mutable-state ownership | Prefer one logical owner; producer should stop mutating a transferred mutable message | Ownership is advisory; Java does not enforce it | **Established principle / open precision** — define exact transfer point and permitted aliasing |
| MSG-01 | Message reference semantics | No serialization, deep copy, or network hop; immutable messages/events are preferred | Same object reference is enqueued and passed to handlers/subscribers | **Established** — define behavior for producer mutation and shared subscribers |
| ORD-01 | Ordering | Ordering is endpoint-local; stateful concurrency-one processing is sequential in mailbox acceptance order; concurrency greater than one has no completion-order guarantee | Queue is FIFO by successful insertion; one worker polls for concurrency one | **Established baseline / open edge** — define ordering among concurrent producers as acceptance linearization, not invocation-start order |
| EXE-01 | Handler execution | Virtual-thread execution; handlers may use ordinary blocking I/O | Bus creates a virtual-thread-per-task executor by default; each endpoint submits concurrency long-lived worker loops | **Established intent / draft mechanism** — worker lifecycle and injected-executor ownership need specification |
| CON-01 | Concurrency budget | Endpoint concurrency is explicit; stateful is concurrency one; concurrent endpoints permit overlap | EndpointConfig validates positive capacity and concurrency | **Established** — define interaction with a shared bus executor and resource isolation |
| CAP-01 | Capacity meaning | Capacity bounds admitted but not yet completed work | A permit is acquired before enqueue and released after handler/subscriber processing; queued and executing messages both consume capacity | **Established in feature guide** — verify all paths preserve exact accounting |
| ADM-01 | Admission point | Admission is bounded and selected by policy | Capacity is acquired, then a second closed check occurs, then the envelope is offered | **Open** — define the linearization point and close/admission race |
| BP-01 | Backpressure policies | WAIT, REJECT, and TIMEOUT are exposed | WAIT blocks for capacity; REJECT uses immediate try-acquire; TIMEOUT waits for configured admission duration; default policy is WAIT | **Established baseline** — specify interruption and close behavior for blocked producers |
| BP-02 | Timeout meaning | TIMEOUT is a backpressure/admission policy | It limits waiting to acquire capacity; it does not time out handler execution | **Open documentation requirement** — distinguish admission timeout from request deadline clearly |
| REQ-01 | Request/response completion | Request returns a CompletableFuture; handler returns a result or failure | Handler result completes the future; thrown handler error completes it exceptionally | **Established baseline** — define terminal-state guarantees under close/cancel |
| FAIL-01 | Handler failure | Request failures reach the caller through the future | Worker catches handler throwable, completes request future exceptionally, and releases capacity in finally | **Established baseline / open edges** — define fatal errors and worker survival |
| PUB-01 | Publish/subscribe delivery | Multiple subscribers receive the event reference; immutable event payloads are preferred | One envelope is enqueued per subscriber, through the endpoint mailbox and capacity budget | **Draft** — define delivery ordering, completion visibility, and subscriber registration timing |
| PUB-02 | Fan-out admission | Atomic all-or-nothing fan-out is not currently promised; original feature guide marks it for hardening | Subscriber admission is performed serially; an admission failure can occur after earlier subscribers were enqueued | **Open** — choose best-effort or atomic admission and define how partial outcomes are reported |
| PUB-03 | Subscriber failure | Subscriber failures need isolation and observability | No response future exists; errors are sent to the current thread's uncaught-exception handler | **Open** — define error reporting, retry policy (if any), and whether one subscriber can affect others |
| CAN-01 | Caller cancellation | Caller abandonment and actual operation cancellation are distinct concepts | Cancelling the returned future does not remove queued work or stop a running handler | **Open** — explicitly define future cancellation as caller-only or introduce a separate operation-cancellation contract |
| CAN-02 | Operation interruption | Interruption behavior is listed as hardening work | Worker interruption while open causes that worker loop to exit; admission interruption restores interrupt status and throws CancellationException | **Open** — define supported interruption semantics and worker replacement |
| LIFE-01 | Endpoint close | Close must account for queued work, in-flight handlers, pending futures, subscriber deliveries, and worker termination | Endpoint.close sets a flag; worker loops attempt to drain already-signalled work; method does not itself await worker termination | **Open** — define graceful close, immediate close, and blocking/non-blocking behavior |
| LIFE-02 | Bus close | Bus lifecycle must close endpoints and execution resources | LoomBus.close closes endpoints and then closes its executor | **Open** — define ordering, concurrent registration, idempotence, and ownership of an injected executor |
| LIFE-03 | Close/admission race | Work accepted before close must have a defined outcome; new work after close must not be stranded | A race exists between the final closed check and mailbox offer | **Open / correctness-critical** — admission and close need a single, testable contract |
| MEM-01 | Memory visibility | Ordinary Java Memory Model and happens-before rules apply | Concurrent queue/semaphore/executor primitives provide publication mechanisms | **Established principle** — retain explicit safe-publication tests |
| ISO-01 | Endpoint isolation | Each endpoint has its own configured capacity and concurrency policy | Endpoints submit workers to the same bus executor; no priority/fairness or hard resource partition is exposed | **Open** — state exactly what isolation means and what it does not guarantee |
| STR-01 | Structured concurrency | Related child-operation lifetime and failure should compose cleanly with structured concurrency | No current integration | **Future capability / non-blocking for core draft** — do not claim structured cancellation today |
| PART-01 | Keyed partitioning | Production scenarios describe partitioning/sharding as a possible approach for dynamic key spaces | No partitioned endpoint API exists | **Future candidate, not a current primitive** |
| API-01 | Subscription and endpoint removal | Current docs do not establish unsubscribe or unregister semantics | No public unsubscribe/unregister method exists | **Open only if required by selected user flows** |

## 7. Proposed defaults for unresolved contracts

These are recommendations for discussion, not silently accepted guarantees. They follow the ownership-first thesis and preserve the current narrow scope.

1. **Admission linearization:** a message becomes accepted at one atomic, unambiguous point after capacity is reserved and before a worker can process it. Ownership transfer should coincide with successful admission. A producer must retain ownership when admission fails.
2. **Close versus admission:** once an endpoint enters closing, new admissions and producers still waiting for capacity should fail predictably. Already accepted work should follow the endpoint's explicitly selected drain policy.
3. **Caller cancellation:** cancelling a CompletableFuture should be documented as abandoning the caller's interest, not as proof that the operation stopped. Actual operation cancellation should be a separate, explicit capability.
4. **Admission timeout:** keep admission timeout distinct from an end-to-end operation deadline. A timeout waiting for capacity must not imply that already-running work is interrupted.
5. **Publish semantics:** do not claim atomic fan-out unless the implementation can reserve/admit all subscriber deliveries as one defined operation. Otherwise explicitly expose best-effort/per-subscriber outcomes.
6. **Subscriber failure:** isolate one subscriber's failure from unrelated subscribers and make the failure observable through an explicit mechanism; do not rely solely on a process-wide uncaught-exception handler as the long-term contract.
7. **Ordering:** define sequential processing for concurrency one in successful mailbox-acceptance order. Do not promise an ordering between concurrent producer method invocations unless an ordering mechanism establishes it.
8. **Isolation:** initially promise per-endpoint admission and execution limits only. Do not imply CPU, memory, downstream connection, fairness, or tenant isolation unless those are independently enforced.
9. **Injected executor ownership:** document whether the bus owns and closes an executor supplied by the caller. The current implementation closes it, but constructor injection alone does not make that ownership obvious.
10. **Structured concurrency:** keep it as a composition/integration concern. Do not expand the core primitive set until an actual flow requires it.

## 8. Design flow map

The existing use-case documentation maps to the following capability flows:

| Flow | Input and ownership | Execution pattern | Result |
|---|---|---|---|
| Stateful domain component | Commands enter; endpoint owns mutable aggregate/session/workflow state | One logical owner; sequential processing | Snapshot/result or failure |
| Parallel decision/risk evaluation | Parent operation owns aggregation state; child requests receive their input references | Independent evaluator endpoints run concurrently | Child results return to parent-owned aggregation |
| Event fan-out | Publisher supplies an event reference; immutable payload preferred | Subscriber deliveries are independently processed | Side effects; delivery/failure visibility still needs contract |
| Blocking DB/HTTP workflow | Request enters an endpoint; handler owns its local operation state | Blocking I/O on virtual thread under endpoint budget | Result or failure |
| Pipeline/state transfer | Operation state passes stage to stage | Stage completes before next ownership transfer, unless a branch is explicitly independent | Transformed state/result |
| Overload protection | Producers submit work to a finite endpoint | WAIT, REJECT, or TIMEOUT admission | Accepted work or explicit rejection/timeout |
| Keyed tenant/session serialization | Commands carry a key identifying a logical owner | Would require partitioning/sharding for large dynamic key spaces | **Future scenario; not supported as a dedicated API today** |

Detailed flow specifications belong in Gate 2. This table identifies the current flow families without prematurely specifying every input, transition, and error path.

## 9. Gate acceptance criteria

### Gate 0 — thesis and goal lock

Gate 0 is ready for sign-off when the project owner accepts:

- The technical goal in Section 1.1.
- The user/business value goal and the fact that no commercial business model is currently specified.
- The non-goals in Section 2.
- The foundational principles in Section 3.
- The rule that ownership and communication semantics outrank implementation-level performance choices.

### Gate 1 — primitive and contract inventory

Gate 1 is ready for sign-off when:

- Every conceptual primitive is classified in Section 4.
- Public API types are separated from internal implementation mechanisms and benchmark helpers.
- Each contract in Section 6 has an explicit status.
- Open contracts that could change observable behavior are deliberately decided before implementation work depends on them.
- Future candidates such as partitioning and structured-concurrency integration are not misrepresented as existing capabilities.

### Current gate status

- **Gate 0:** documented as a reviewable baseline; not represented as user-approved until accepted.
- **Gate 1:** primitive inventory and contract register are documented; not closed because several externally observable semantics remain open.
- **Implementation:** no code changes are authorized by this document. Gate 2 flow specifications and subsequent implementation work must use this foundation.

## 10. Source documents

- [README — LoomBus thesis and overview](../README.md)
- [Feature and Semantic Guide](FEATURES.md)
- [Use Cases and Patterns](USE-CASES.md)
- [Production Scenarios](PRODUCTION-SCENARIOS.md)
- [Benchmark Plan](BENCHMARK-PLAN.md)

The README states the core thesis. This document makes its goals, primitive inventory, and contract status explicit. The feature and use-case documents remain the detailed descriptions of draft capabilities and scenarios; they must not override the contract decisions recorded here.
