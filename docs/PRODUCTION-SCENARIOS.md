# LoomBus in Production: Scenario-Based Comparisons

## Purpose

This document answers a different question from `USE-CASES.md`:

> **When should a production Java system use LoomBus, and what problem does it solve compared with native Java?**

LoomBus is not intended to replace normal Java method calls, `CompletableFuture`, executors, locks, Kafka, Pulsar, REST, or gRPC. It is an in-process concurrency and communication abstraction for cases where concurrent business operations need explicit ownership, controlled execution, mailbox semantics, ordering, failure handling, and backpressure.

The comparisons below are architectural scenarios, not performance claims. Any claim about throughput or latency should be validated with benchmarks for the actual workload.

---

## 1. Stateful object receiving concurrent commands

### Production scenario

A single logical business entity receives many concurrent commands:

- account balance updates
- shopping-cart mutations
- session state
- workflow state
- device state
- tenant configuration
- rate-limit state

The important requirement is that mutations for one logical owner must not corrupt each other.

### Native Java approach

A common implementation is a mutable object protected by `synchronized`, `Lock`, or atomics.

```java
class Account {
    private final Object lock = new Object();
    private BigDecimal balance;

    void debit(BigDecimal amount) {
        synchronized (lock) {
            balance = balance.subtract(amount);
        }
    }
}
```

This is valid Java. The difficulty is that the concurrency protocol becomes part of every mutable component. As the system grows, developers must reason about lock ownership, lock ordering, deadlocks, contention, queues, and executor behavior.

### LoomBus model

Treat the endpoint as the logical owner of the mutable state.

```text
Concurrent callers
      │
      ▼
   LoomBus
      │
      ▼
 Stateful endpoint
      │
      ▼
 Single logical owner
      │
      ▼
 Mutable state
```

With `concurrency = 1`, commands are processed sequentially by that endpoint.

### What LoomBus changes

The key change is **where concurrency is controlled**. Instead of allowing many operations to enter the state concurrently and protecting every mutation with a lock, the architecture can serialize commands at the endpoint boundary.

This does not make Java synchronization disappear everywhere. Shared resources outside the endpoint still require their own concurrency controls.

### Good fit when

- state has a clear logical owner
- commands can be processed sequentially
- ordering matters
- the state lives in one JVM
- callers should not mutate the state directly

### Not a fit when

The state must be concurrently updated by many independent workers and a lock-free/atomic data structure is the simpler and more appropriate abstraction.

---

## 2. High-volume commands targeting the same logical entity

### Production scenario

A service receives thousands of operations for the same customer, session, device, or workflow.

```text
VT1 ─┐
VT2 ─┤
VT3 ─┤──> same logical entity
VT4 ─┤
VT5 ─┘
```

### Native Java approach

Typical designs use a map of objects plus per-object locks, striped locks, actor-like queues, or custom executors.

This can work well, but the application now owns several pieces of infrastructure:

```text
entity lookup
   +
lock selection
   +
queueing
   +
execution
   +
rejection
   +
shutdown
```

### LoomBus model

The endpoint/mailbox becomes the concurrency boundary.

```text
Commands
   │
   ▼
Endpoint mailbox
   │
   ├── command 1
   ├── command 2
   ├── command 3
   └── command 4
          │
          ▼
   controlled execution
```

The useful abstraction is not simply “a queue.” It is the combination of **message admission + ownership + execution + completion**.

### Design consideration

If there are many independent entities, one endpoint per entity may be excessive. A future LoomBus design may need sharding/partitioning semantics so that many logical owners can share a bounded number of execution domains.

---

## 3. Parallel risk or decision checks

### Production scenario

An authentication or authorization request needs several independent checks:

```text
                 Request
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
         Geo      Device   Behaviour
          │         │         │
          └─────────┼─────────┘
                    ▼
                 Decision
```

### Native Java

A common solution is `CompletableFuture` with an executor:

```java
var geo = CompletableFuture.supplyAsync(() -> geoCheck(request), executor);
var device = CompletableFuture.supplyAsync(() -> deviceCheck(request), executor);
var behaviour = CompletableFuture.supplyAsync(() -> behaviourCheck(request), executor);
```

This is powerful, but larger workflows can accumulate future composition, timeout, cancellation, exception, and executor-management code.

### LoomBus

Each independent operation can be represented as an in-process endpoint operation, with virtual threads handling blocking work naturally.

The intended benefit is **simpler orchestration and explicit concurrency boundaries**, not an assertion that LoomBus is inherently faster than `CompletableFuture`.

### Good fit when

- checks are independent
- operations belong to one JVM
- each operation has a meaningful endpoint boundary
- failures and admission limits need explicit semantics

### Native Java may be simpler when

There are only two small asynchronous calls and no need for endpoint-level lifecycle, backpressure, ownership, or communication semantics.

---

## 4. Blocking database and HTTP workflow

### Production scenario

A request performs several blocking operations:

```text
Request
  │
  ▼
DB lookup
  │
  ▼
HTTP service
  │
  ▼
DB update
  │
  ▼
Response
```

### Traditional Java

Historically, developers had to carefully size platform-thread pools because blocking operations occupied threads.

With modern Java virtual threads, this concern changes substantially. Native Java can already express this workflow directly:

```java
var user = repository.findUser(id);
var risk = riskClient.check(user);
repository.updateDecision(id, risk);
return risk;
```

### Important conclusion

**LoomBus is not necessary merely because the code uses blocking I/O.** Java virtual threads already solve much of the thread-efficiency problem.

LoomBus becomes relevant when this workflow also needs explicit endpoint ownership, admission control, communication, lifecycle, or backpressure.

This distinction is important to the project's positioning.

---

## 5. In-process event fan-out

### Production scenario

An event needs to reach multiple independent components in the same JVM.

```text
                 Event
                   │
              LoomBus
          ┌────────┼────────┐
          ▼        ▼        ▼
       Audit    Metrics   Notification
```

### Native Java

Common approaches include direct callbacks, listener lists, application events, queues, or custom executor dispatch.

These approaches are perfectly reasonable for small systems.

### LoomBus

Pub/sub provides an explicit communication boundary with endpoint admission and subscriber execution.

### Important limitation

LoomBus is **not a durable event broker**.

If the event must survive process failure, be replayed later, or be consumed by another service, use Kafka, Pulsar, or another durable messaging system.

---

## 6. Burst traffic and backpressure

### Production scenario

A producer suddenly generates work faster than a component can process it.

```text
100,000 incoming operations
             │
             ▼
       processing rate
          1,000/sec
```

### Dangerous native design

```java
executor.submit(() -> process(request));
```

If admission is effectively unbounded, work can accumulate faster than it is completed, increasing memory use and latency and potentially contributing to cascading failure.

### Native Java can solve this

A bounded `BlockingQueue`, semaphore, rate limiter, or bounded executor can provide admission control.

### LoomBus

The mailbox capacity and backpressure policy are part of the endpoint configuration.

```text
Producer
   │
   ▼
bounded mailbox
   │
   ├── WAIT
   ├── REJECT
   └── TIMEOUT
   │
   ▼
controlled workers
```

### What LoomBus contributes

It makes backpressure part of the communication abstraction rather than an unrelated executor detail.

The application still chooses its concurrency and capacity budget.

---

## 7. Stateful multi-stage workflow

### Production scenario

A workflow accumulates mutable state through several stages:

```text
Create
  │
  ▼
Validate
  │
  ▼
Enrich
  │
  ▼
Decide
  │
  ▼
Persist
```

### Shared-state native approach

A workflow object can be passed between concurrent operations while multiple threads mutate it. That requires synchronization or carefully designed immutable snapshots.

### LoomBus approach

Prefer moving ownership of the workflow state between stages.

```text
Worker A
   │ owns state
   ▼
Worker B
   │ owns state
   ▼
Worker C
   │ owns state
   ▼
Result
```

The architectural principle is:

> **Move the state to the computation instead of moving the computation to shared state.**

Java does not enforce Rust-style move semantics, so ownership transfer remains an API and coding contract.

---

## 8. Failure in parallel work

### Production scenario

Several operations execute in parallel and one fails.

```text
             Parent
          /    |     \
        Geo  Device  Behaviour
         ✓      ✓        X
                        │
                     failure
```

### Native Java

Using futures directly, the application has to define what failure means:

- should siblings continue?
- should they be cancelled?
- what exception reaches the caller?
- what happens to late results?
- how are timeouts handled?

Java's structured concurrency APIs are also relevant here and should be considered before building custom semantics.

### LoomBus

A future version should integrate clearly with structured concurrency so that the communication abstraction does not create detached work accidentally.

The intended semantic model is:

```text
failure
   ↓
operation/scope failure
   ↓
cancel remaining related work
   ↓
propagate failure
```

The exact cancellation semantics must be explicit and tested.

---

## 9. Per-session or per-tenant serialization

### Production scenario

Different tenants can process concurrently, but operations belonging to the same tenant must be serialized.

```text
Tenant A ──> A1 ──> A2 ──> A3

Tenant B ──> B1 ──> B2 ──> B3

A and B can run concurrently.
```

### Native Java

A common implementation uses maps of locks, keyed executors, partitions, or queues.

### LoomBus opportunity

This is a natural extension of the ownership model: each logical owner gets a serialized execution domain while unrelated owners remain concurrent.

A production-ready implementation would need careful lifecycle management so that millions of transient keys do not create unbounded endpoint objects.

---

## 10. In-process command processing

### Production scenario

A subsystem needs asynchronous command delivery but does not need durability or network communication.

Examples:

- notification preparation
- cache invalidation work
- local indexing
- internal workflow commands
- CPU-bound or blocking business operations with bounded admission

### Native Java

Possible building blocks:

```text
BlockingQueue
ExecutorService
worker threads
Future
locks
semaphores
```

The developer assembles the protocol.

### LoomBus

LoomBus packages the communication model around an endpoint:

```text
request
  ↓
mailbox
  ↓
execution
  ↓
response/failure
```

The value is a consistent abstraction for this class of in-process communication.

---

# LoomBus vs Native Java: Architectural Comparison

| Concern | Native Java building blocks | LoomBus model |
|---|---|---|
| Simple synchronous operation | Method call | Method call is still preferred |
| Async result | `Future` / `CompletableFuture` | Request returns a future |
| Blocking I/O | Virtual threads | Virtual threads + endpoint semantics |
| Stateful serialization | Locks / synchronized / custom queues | Stateful endpoint ownership |
| In-process messages | Queue / callback / custom protocol | Endpoint mailbox |
| Backpressure | Semaphore / bounded queue / executor | Endpoint admission policy |
| Fan-out | Listeners / callbacks / futures | Pub/sub |
| Concurrent workers | ExecutorService | Endpoint concurrency |
| Failure propagation | Application-defined | Endpoint/request semantics |
| Durable messaging | Kafka/Pulsar/etc. | **Not LoomBus** |
| Cross-process communication | REST/gRPC/messaging | **Not LoomBus** |
| Distributed coordination | Distributed systems tooling | **Not LoomBus** |

---

# The Core Architectural Difference

Native Java gives you excellent primitives:

```text
Thread / Virtual Thread
Executor
Future
Queue
Lock
Semaphore
Atomic
CompletableFuture
StructuredTaskScope
```

LoomBus should not attempt to replace these primitives.

Its purpose is to provide a **higher-level communication and ownership model** on top of them.

```text
              Native Java
                   │
      ┌────────────┼────────────┐
      │            │            │
   Virtual       Queue        Future
   threads       Lock       Semaphore
      │            │            │
      └────────────┼────────────┘
                   ▼
                LoomBus
                   │
      ┌────────────┼────────────┐
      ▼            ▼            ▼
   Ownership    Mailbox    Backpressure
      │            │            │
      └────────────┼────────────┘
                   ▼
          Business operation
```

---

# When LoomBus Is a Strong Candidate

Consider LoomBus when most of these are true:

- communication happens **inside one JVM**
- multiple concurrent callers interact with the same logical business boundary
- mutable state has a clear owner
- commands/events need controlled admission
- ordering matters for at least some endpoints
- the system needs bounded concurrency or mailbox capacity
- asynchronous work needs a consistent request/response or event abstraction
- the application would otherwise build several pieces of custom concurrency infrastructure

# When Native Java Is the Better Choice

Prefer ordinary Java when:

- a direct method call is enough
- a simple `CompletableFuture` solves the problem
- a standard concurrent collection is sufficient
- an atomic variable is the natural model
- structured concurrency alone expresses the workflow cleanly
- introducing an endpoint/message abstraction would add more complexity than value

# When LoomBus Is the Wrong Boundary

Do not use LoomBus as a replacement for distributed infrastructure.

```text
Different JVM / Pod
        │
        ├── REST / HTTP
        ├── gRPC
        ├── Kafka
        ├── Pulsar
        └── durable queue
```

LoomBus is intentionally **in-process**.

It does not provide:

- durable storage
- cross-process delivery
- message replay
- distributed consensus
- distributed transactions
- guaranteed delivery after process failure
- cross-service discovery

---

# Production Decision Checklist

Before introducing LoomBus, ask:

### 1. Is this communication inside one JVM?

If no, LoomBus is not the right boundary.

### 2. Is there a meaningful business operation or endpoint boundary?

If no, a method call may be simpler.

### 3. Is there mutable state with a clear logical owner?

If yes, a stateful endpoint may be useful.

### 4. Do concurrent producers need controlled admission?

If yes, mailbox capacity and backpressure may justify LoomBus.

### 5. Does the operation need independent concurrent work?

If yes, compare LoomBus with structured concurrency and `CompletableFuture` for the specific workflow.

### 6. Does the message need to survive a process crash?

If yes, use durable messaging instead.

### 7. Does the operation cross a service boundary?

If yes, use HTTP/gRPC/messaging rather than LoomBus.

### 8. Can ownership be made explicit?

If yes, LoomBus's ownership model may simplify the concurrency design.

---

# The Principle Behind LoomBus

The project's central idea can be summarized as:

> **Concurrency should disappear from business logic without disappearing from the architecture.**

And more specifically:

> **Prefer moving ownership of mutable state between operations over allowing multiple concurrent operations to mutate the same state.**

LoomBus is useful when those principles make a production concurrency boundary easier to understand, control, test, and operate than a collection of lower-level Java concurrency primitives.

It should not be adopted simply because virtual threads exist.
