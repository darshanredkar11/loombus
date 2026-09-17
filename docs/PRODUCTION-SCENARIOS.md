# LoomBus in Production: Scenario-Based Comparisons

## Purpose

This document answers:

> **When should a production Java system use LoomBus, and what does the LoomBus version look like compared with native Java?**

The goal is not to claim that LoomBus is universally better than Java's concurrency primitives. Java already provides excellent building blocks. LoomBus is useful when an application needs a higher-level **in-process communication boundary** combining messages, mailboxing, ownership, controlled concurrency, completion, and backpressure.

Every scenario below shows:

1. The production problem.
2. A representative native-Java solution.
3. The same scenario using LoomBus.
4. What LoomBus changes architecturally.
5. When the native solution may still be preferable.

These are architectural comparisons, not performance claims. Throughput, latency, allocation, CPU, and memory must be benchmarked for the real workload.

---

## 1. Stateful account/session/workflow receiving concurrent commands

### Production problem

Many virtual threads can concurrently issue commands against one logical state owner:

- account balance
- shopping cart
- authentication session
- workflow state
- device state
- tenant configuration

The desired invariant is simple:

> Operations for one logical owner must not concurrently corrupt that owner's mutable state.

### Native Java

A straightforward solution is a lock around the mutable state.

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

This is valid and often perfectly appropriate. But the lock becomes part of the business object's concurrency protocol.

As the application evolves, it may additionally need:

```text
lock ownership
lock ordering
queueing
executor management
failure handling
admission limits
```

### LoomBus

Make the endpoint the logical owner of the mutable state.

```java
var balance = new BigDecimal[] { new BigDecimal("1000") };

var account = bus.register(
        "account",
        EndpointConfig.stateful(1_000),
        (Debit command) -> {
            balance[0] = balance[0].subtract(command.amount());
            return balance[0];
        });

CompletableFuture<BigDecimal> result =
        bus.request("account", new Debit(new BigDecimal("50")));
```

With `EndpointConfig.stateful(...)`, the endpoint is configured with concurrency `1`. The mutable `balance` is therefore accessed by one endpoint worker at a time.

A cleaner production implementation would encapsulate the state in an endpoint-owned object rather than expose the array directly; the array here simply demonstrates that the handler can retain mutable state owned by that endpoint.

### What LoomBus changes

Instead of:

```text
many operations
      ↓
shared mutable state
      ↓
lock
```

we get:

```text
many operations
      ↓
endpoint mailbox
      ↓
one logical owner
      ↓
state mutation
```

The important change is **where concurrency is controlled**.

### Good fit

Use this model when a business object has a clear owner and commands can be serialized.

### Native Java may be better

If the object has only one caller, a direct method call is simpler. If highly concurrent atomic updates are the natural model, `Atomic*`, `LongAdder`, concurrent collections, or a lock may be more appropriate.

---

## 2. Thousands of commands targeting the same logical entity

### Production problem

Imagine an IDP receiving many simultaneous operations for one authentication session:

```text
VT1 ─┐
VT2 ─┤
VT3 ─┤
VT4 ─┼──> session state
VT5 ─┤
...  ┘
```

For example:

- MFA challenge update
- risk score update
- device registration
- authentication attempt
- session transition

### Native Java

A typical design might maintain a map of state and a map of locks:

```java
ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();
ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

void update(String sessionId, Consumer<Session> operation) {
    var lock = locks.computeIfAbsent(sessionId, id -> new ReentrantLock());
    lock.lock();
    try {
        operation.accept(sessions.get(sessionId));
    } finally {
        lock.unlock();
    }
}
```

This is a legitimate design, but lifecycle and cleanup of per-key locks now become another production concern.

### LoomBus

For a bounded set of logical domains, the endpoint itself can represent the serialized domain:

```java
bus.register(
        "authentication-session",
        EndpointConfig.stateful(10_000),
        (SessionCommand command) -> sessionState.apply(command));

var result = bus.request(
        "authentication-session",
        new SessionCommand(sessionId, AUTHENTICATE));
```

The endpoint establishes the serialization boundary and mailbox admission.

For very large or dynamic key spaces, do **not** create an unlimited endpoint per key. A production LoomBus design should eventually provide sharding/partitioning semantics:

```text
session key
    ↓
partition(key)
    ↓
bounded endpoint/owner
```

### What LoomBus changes

The concurrency protocol becomes a named architectural boundary rather than a collection of per-object locks.

### Native Java may be better

If there are millions of dynamic keys, a carefully designed keyed executor or partitioned concurrent structure may be simpler until LoomBus provides native sharding semantics.

---

## 3. Parallel MFA/risk decision checks

### Production problem

An authentication request needs independent checks:

```text
                    Risk request
                         │
            ┌────────────┼────────────┐
            ▼            ▼            ▼
           Geo         Device     Behaviour
            │            │            │
            └────────────┼────────────┘
                         ▼
                      Decision
```

### Native Java

A common implementation uses `CompletableFuture`:

```java
var geo = CompletableFuture.supplyAsync(
        () -> geoCheck(request), executor);

var device = CompletableFuture.supplyAsync(
        () -> deviceCheck(request), executor);

var behaviour = CompletableFuture.supplyAsync(
        () -> behaviourCheck(request), executor);

return CompletableFuture.allOf(geo, device, behaviour)
        .thenApply(ignored -> decide(
                geo.join(), device.join(), behaviour.join()));
```

This is already a strong solution.

### LoomBus

Represent meaningful checks as endpoints and use the bus for the in-process request boundary:

```java
bus.register("geo", EndpointConfig.concurrent(1_000, 100),
        this::geoCheck);

bus.register("device", EndpointConfig.concurrent(1_000, 100),
        this::deviceCheck);

bus.register("behaviour", EndpointConfig.concurrent(1_000, 100),
        this::behaviourCheck);

var geo = bus.request("geo", request);
var device = bus.request("device", request);
var behaviour = bus.request("behaviour", request);

return CompletableFuture.allOf(geo, device, behaviour)
        .thenApply(ignored -> decide(
                geo.join(), device.join(), behaviour.join()));
```

The important point is that LoomBus does **not** magically eliminate orchestration code. The current API still returns `CompletableFuture`, so `CompletableFuture` remains a natural aggregation primitive.

The LoomBus value is that each check now has an explicit endpoint boundary with its own concurrency and mailbox capacity.

### What LoomBus changes

```text
Native:
workflow → futures → executor → functions

LoomBus:
workflow → endpoint messages → controlled execution
```

### Native Java may be better

If these are just three small functions and no independent endpoint semantics are needed, plain structured concurrency or `CompletableFuture` is likely simpler.

---

## 4. Blocking database + HTTP workflow

### Production problem

A business operation performs blocking I/O:

```text
Request
  ↓
DB lookup
  ↓
HTTP risk service
  ↓
DB update
  ↓
Response
```

### Native Java with virtual threads

Modern Java can already make this straightforward:

```java
var user = repository.findUser(id);
var risk = riskClient.check(user);
repository.updateDecision(id, risk);
return risk;
```

There is no need to introduce LoomBus merely because the work blocks.

### LoomBus

If this operation is also a bounded internal business endpoint:

```java
bus.register(
        "risk-decision",
        EndpointConfig.concurrent(2_000, 100),
        request -> {
            var user = repository.findUser(request.userId());
            var risk = riskClient.check(user);
            repository.updateDecision(request.userId(), risk);
            return risk;
        });

var result = bus.request("risk-decision", request);
```

The handler can perform blocking I/O while running on a virtual thread. The endpoint adds an explicit admission and concurrency boundary around the business operation.

### What LoomBus changes

Not thread efficiency. **Execution policy and communication semantics.**

Virtual threads solve the former. LoomBus can address the latter.

### Native Java may be better

If the operation is directly called from the request handler and needs no asynchronous communication or bounded endpoint semantics, keep the direct method call.

---

## 5. In-process event fan-out

### Production problem

An authentication event needs several independent consumers in the same JVM:

```text
             AuthenticationEvent
                     │
                  Bus
          ┌──────────┼──────────┐
          ▼          ▼          ▼
        Audit      Metrics    Notification
```

### Native Java

A simple listener design might be:

```java
List<Consumer<AuthEvent>> listeners = new CopyOnWriteArrayList<>();

void publish(AuthEvent event) {
    for (var listener : listeners) {
        listener.accept(event);
    }
}
```

If consumers need asynchronous execution, the application adds executors, queues, error handling, and admission rules.

### LoomBus

```java
bus.register(
        "audit",
        EndpointConfig.concurrent(1_000, 4),
        event -> auditService.record((AuthEvent) event));

bus.register(
        "metrics",
        EndpointConfig.concurrent(1_000, 2),
        event -> metricsService.record((AuthEvent) event));

bus.register(
        "notification",
        EndpointConfig.concurrent(500, 20),
        event -> notificationService.send((AuthEvent) event));

bus.subscribe("audit", (AuthEvent event) -> auditService.record(event));
```

Or, using the current API's endpoint pub/sub model:

```java
bus.register(
        "auth-events",
        EndpointConfig.concurrent(1_000, 20),
        ignored -> null);

bus.subscribe("auth-events", (AuthEvent event) -> auditService.record(event));
bus.subscribe("auth-events", (AuthEvent event) -> metricsService.record(event));
bus.subscribe("auth-events", (AuthEvent event) -> notificationService.send(event));

bus.publish("auth-events", new AuthEvent(userId));
```

### Important semantic issue

The current implementation enqueues separately for each subscriber. If one subscriber rejects after another has accepted, fan-out can be partially delivered. This must be addressed before claiming atomic broadcast semantics.

### What LoomBus changes

It provides a common in-process event boundary with endpoint capacity and subscriber execution.

### Native Java may be better

For two local listeners, direct callbacks are simpler.

### Do not use LoomBus when

The event must survive process failure, be replayed, or cross service boundaries. Use a durable broker such as Kafka or Pulsar.

---

## 6. Burst traffic and backpressure

### Production problem

A producer can generate work much faster than a subsystem can process it.

```text
100,000 operations
       │
       ▼
 processing capacity
       │
       ▼
   1,000/sec
```

### Native Java

A bounded executor/queue can solve this:

```java
var queue = new ArrayBlockingQueue<Request>(1_000);
var executor = new ThreadPoolExecutor(
        50,
        50,
        0,
        TimeUnit.MILLISECONDS,
        queue,
        new ThreadPoolExecutor.AbortPolicy());

executor.submit(() -> process(request));
```

This is a good and established solution.

### LoomBus

Capacity and admission are configured on the endpoint:

```java
bus.register(
        "notification",
        EndpointConfig.concurrent(1_000, 50)
                .withBackpressure(Backpressure.REJECT),
        this::processNotification);

bus.request("notification", request)
        .whenComplete((result, error) -> {
            if (error != null) {
                // admission or processing failure
            }
        });
```

Or with bounded waiting:

```java
var config = EndpointConfig.concurrent(1_000, 50)
        .withBackpressure(Backpressure.TIMEOUT, Duration.ofMillis(100));
```

### What LoomBus changes

The important difference is that backpressure is part of the **message endpoint contract** rather than something separately assembled around an executor.

### Important distinction

Virtual threads are cheap, but the work they perform is not free. Database connections, downstream APIs, CPU, memory, and queues remain bounded resources.

---

## 7. Stateful multi-stage workflow

### Production problem

A workflow has mutable state that progresses through stages:

```text
Create → Validate → Enrich → Decide → Persist
```

A dangerous design lets several workers mutate the same workflow object concurrently.

### Native Java

One approach is a shared object protected by a lock:

```java
synchronized (workflow) {
    workflow.setCustomer(customer);
    workflow.setRisk(risk);
}
```

Another is to make every stage immutable and create a new state object.

### LoomBus

Use messages to move ownership between endpoint operations:

```java
bus.register("validate", EndpointConfig.stateful(500),
        (Workflow workflow) -> validate(workflow));

bus.register("enrich", EndpointConfig.stateful(500),
        (Workflow workflow) -> enrich(workflow));

bus.register("decide", EndpointConfig.stateful(500),
        (Workflow workflow) -> decide(workflow));

var validated = bus.request("validate", workflow);
var enriched = validated.thenCompose(
        state -> bus.request("enrich", state));
var decision = enriched.thenCompose(
        state -> bus.request("decide", state));
```

The conceptual model is:

```text
Stage A owns state
       ↓
ownership transfer
       ↓
Stage B owns state
       ↓
ownership transfer
       ↓
Stage C owns state
```

The object is transferred by reference inside the JVM; LoomBus does not serialize or deep-copy the message.

### What LoomBus changes

It encourages ownership transfer instead of shared mutation.

Java does not enforce move semantics, so this remains a programming contract: once ownership has transferred, previous code should not mutate the object concurrently.

---

## 8. Failure in parallel operations

### Production problem

Several checks execute in parallel and one fails:

```text
Parent
 ├── Geo       ✓
 ├── Device    ✓
 └── Behaviour X
```

The application must decide whether siblings continue, stop, or are cancelled.

### Native Java

With futures, developers explicitly compose the policy:

```java
var a = CompletableFuture.supplyAsync(this::a, executor);
var b = CompletableFuture.supplyAsync(this::b, executor);
var c = CompletableFuture.supplyAsync(this::c, executor);

return CompletableFuture.allOf(a, b, c);
```

Cancellation and timeout policy then has to be defined separately.

### LoomBus

Current LoomBus requests naturally expose failure through their returned futures:

```java
var geo = bus.request("geo", request);
var device = bus.request("device", request);
var behaviour = bus.request("behaviour", request);

return CompletableFuture.allOf(geo, device, behaviour)
        .thenApply(ignored -> decide(
                geo.join(), device.join(), behaviour.join()));
```

A production version of LoomBus should integrate explicitly with structured concurrency so that related work has a well-defined lifetime and fail-fast/cancellation behavior.

### Current status

Do not document current LoomBus as providing complete structured cancellation semantics yet. This is an area for implementation and testing.

---

## 9. Per-tenant or per-session serialization

### Production problem

Different tenants may process concurrently, but commands for the same tenant must be serialized:

```text
Tenant A → A1 → A2 → A3

Tenant B → B1 → B2 → B3

A and B execute concurrently.
```

### Native Java

Typical solutions include keyed locks, striped locks, keyed executors, partitions, or custom actor-like queues.

### LoomBus today

For a fixed set of logical domains, an endpoint can provide the serialization boundary:

```java
bus.register(
        "tenant-command",
        EndpointConfig.stateful(10_000),
        command -> tenantState.apply(command));
```

For dynamic tenants, however, the application must not blindly create one endpoint per tenant.

### Required future capability

A stronger LoomBus API could eventually express:

```java
bus.registerPartitioned(
        "tenant-command",
        keyExtractor = Command::tenantId,
        partitions = 64,
        ...);
```

Conceptually:

```text
Tenant ID
    ↓
hash / partition
    ↓
64 bounded owners
    ↓
commands for same partition serialized
```

This should be treated as a future design, not as a current API claim.

---

## 10. In-process command processing

### Production problem

A subsystem needs asynchronous command processing but does not need network communication or durable messaging.

Examples:

- notification preparation
- local indexing
- cache invalidation
- internal workflow commands
- bounded background processing

### Native Java

The application might assemble:

```text
BlockingQueue
     +
ExecutorService
     +
Future
     +
Semaphore
     +
shutdown handling
```

For example:

```java
BlockingQueue<Command> queue = new ArrayBlockingQueue<>(1_000);
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

executor.submit(() -> {
    while (!Thread.currentThread().isInterrupted()) {
        var command = queue.take();
        process(command);
    }
});
```

### LoomBus

The same architectural boundary becomes an endpoint:

```java
bus.register(
        "local-indexer",
        EndpointConfig.concurrent(1_000, 20)
                .withBackpressure(Backpressure.REJECT),
        command -> {
            index((IndexCommand) command);
            return null;
        });

bus.request("local-indexer", command);
```

### What LoomBus changes

Instead of each subsystem inventing its own queue/executor/admission protocol, the endpoint provides a common vocabulary:

```text
request
  ↓
mailbox
  ↓
controlled execution
  ↓
completion / failure
```

### Native Java may be better

If the subsystem only needs a single queue consumer and has no need for request/response, endpoint ownership, or standardized admission semantics, a plain `BlockingQueue` can be clearer.

---

# Scenario 11: Request-scoped fan-out with blocking operations

### Production problem

One request performs three independent downstream calls and then combines them.

### Native Java

```java
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    var geo = executor.submit(() -> geoClient.lookup(request));
    var device = executor.submit(() -> deviceClient.lookup(request));
    var reputation = executor.submit(() -> reputationClient.lookup(request));

    return decide(geo.get(), device.get(), reputation.get());
}
```

This is already concise with virtual threads.

### LoomBus

If the checks are reusable application endpoints with independent capacity policies:

```java
bus.register("geo", EndpointConfig.concurrent(500, 50),
        geoClient::lookup);

bus.register("device", EndpointConfig.concurrent(500, 50),
        deviceClient::lookup);

bus.register("reputation", EndpointConfig.concurrent(500, 20),
        reputationClient::lookup);

var geo = bus.request("geo", request);
var device = bus.request("device", request);
var reputation = bus.request("reputation", request);

return CompletableFuture.allOf(geo, device, reputation)
        .thenApply(v -> decide(
                geo.join(), device.join(), reputation.join()));
```

### Why use LoomBus here?

Not because the native virtual-thread solution is bad. The reason would be that **geo, device, and reputation are independently managed internal capabilities**, each with its own capacity, concurrency, failure, and communication boundary.

If that distinction does not matter, native structured concurrency is simpler.

---

# Scenario 12: When LoomBus should not be used

## Cross-service request

```text
Service A ───── HTTP/gRPC ─────> Service B
```

Use REST, HTTP, gRPC, or another network protocol.

LoomBus is in-process.

## Durable event

```text
Service A → Kafka/Pulsar → Service B
```

Use a durable broker when messages need persistence, replay, independent consumers, or cross-process delivery.

## Simple synchronous method

```java
var result = service.calculate(input);
```

Do not add a message bus merely to avoid a method call.

## Simple atomic state

```java
counter.incrementAndGet();
```

Use the appropriate Java concurrency primitive when that is the natural model.

---

# Native Java vs LoomBus: Production Comparison

| Production concern | Native Java | LoomBus |
|---|---|---|
| Simple synchronous business operation | Method call | Method call is still preferred |
| Async result | `Future` / `CompletableFuture` | `request()` returns `CompletableFuture` |
| Blocking I/O | Virtual threads | Virtual threads + endpoint boundary |
| Stateful serialization | `synchronized` / `Lock` / actor-like queue | Stateful endpoint with concurrency `1` |
| In-process command delivery | Queue + executor | Endpoint mailbox |
| Backpressure | Bounded queue / semaphore / executor policy | Endpoint capacity + backpressure policy |
| Event fan-out | Callbacks/listeners + dispatch | `publish()` / `subscribe()` |
| Independent worker concurrency | Executor configuration | Endpoint configuration |
| Message ownership | Application convention | Explicit architectural convention |
| Failure propagation | `Future` / scope semantics | Request future + endpoint semantics |
| Structured cancellation | Java structured concurrency | Must integrate carefully; not fully solved yet |
| Durable messaging | Kafka/Pulsar/etc. | **Not LoomBus** |
| Cross-process communication | HTTP/gRPC/messaging | **Not LoomBus** |
| Distributed coordination | Distributed systems primitives | **Not LoomBus** |

---

# What LoomBus Actually Adds

Native Java already gives us:

```text
Virtual Threads
Executors
CompletableFuture
Locks
Atomics
Queues
Semaphores
Structured Concurrency
```

LoomBus should sit above those primitives and provide a consistent application-level model:

```text
              Native Java primitives
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
       Threads        Queue        Future
          │            │            │
          └────────────┼────────────┘
                       ▼
                    LoomBus
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
      Endpoint       Mailbox     Admission
          │            │            │
          ▼            ▼            ▼
      Ownership     Ordering    Backpressure
          │            │            │
          └────────────┼────────────┘
                       ▼
                Business operation
```

The proposed value proposition is therefore:

> **LoomBus turns a collection of Java concurrency primitives into an explicit in-process communication boundary for business operations.**

---

# The Core Ownership Model

The strongest LoomBus scenario is one where mutable state has a clear logical owner.

Instead of:

```text
many workers
     │
     ├── mutate ──┐
     ├── mutate ──┼──> shared state
     └── mutate ──┘
             protected by locks
```

prefer:

```text
Command
   ↓
Mailbox
   ↓
Single owner
   ↓
Mutate state
   ↓
Transfer result/state
   ↓
Next operation
```

The principle is:

> **Prefer moving ownership of mutable state between operations over allowing multiple concurrent operations to mutate the same state.**

This is an architectural convention, not a Java language guarantee. Messages should generally be immutable; if mutable objects are transferred by reference, previous owners must stop mutating them.

---

# When to Introduce LoomBus

LoomBus is a strong candidate when most of the following are true:

- the communication is inside one JVM
- there is a meaningful business endpoint or operation boundary
- multiple callers can operate concurrently
- state has a clear logical owner
- ordering for an endpoint matters
- bounded admission is important
- different operations need different concurrency budgets
- the application is repeatedly building queues, executors, locks, callbacks, and failure protocols
- asynchronous communication should have one consistent abstraction

# When to Stay With Native Java

Prefer native Java when:

- a method call is enough
- `CompletableFuture` or structured concurrency directly expresses the workflow
- a standard concurrent collection solves the problem
- an atomic variable is the natural model
- a simple bounded executor/queue is clearer
- LoomBus would introduce an abstraction without providing a meaningful architectural boundary

# When LoomBus Is the Wrong Boundary

Do not use LoomBus for:

- cross-process communication
- cross-pod communication
- durable messaging
- replayable event streams
- distributed coordination
- distributed transactions
- guaranteed delivery after process failure
- service-to-service discovery

Use the appropriate distributed-system mechanism instead.

---

# Production Decision Checklist

### 1. Is the communication inside one JVM?

If no → use HTTP/gRPC/messaging.

### 2. Is there a meaningful endpoint boundary?

If no → a direct method call may be better.

### 3. Does mutable state have a clear owner?

If yes → a stateful endpoint may be a good fit.

### 4. Do producers need bounded admission?

If yes → LoomBus mailbox capacity/backpressure may be useful.

### 5. Does each operation need an independent concurrency budget?

If yes → endpoint-level configuration may be useful.

### 6. Is the work merely blocking I/O?

If yes → virtual threads may already solve the problem. LoomBus is optional.

### 7. Does the message need to survive process failure?

If yes → use durable messaging.

### 8. Does the application need structured cancellation?

If yes → compare against Java structured concurrency and ensure LoomBus does not detach work from the parent scope.

### 9. Would a queue + executor be simpler?

If yes → use the simpler solution unless LoomBus provides additional architectural value.

---

# The Testable Production Hypothesis

LoomBus should eventually be evaluated against native Java implementations of the **same scenarios**, not artificial microbenchmarks alone.

For each scenario, compare:

```text
Native Java
     vs
LoomBus
```

Measure:

- throughput
- p50 latency
- p95 latency
- p99 latency
- CPU utilization
- heap allocation
- memory usage
- queue depth
- rejected work
- cancellation latency
- failure propagation latency
- correctness under contention

The goal is not to prove that LoomBus is faster in every case.

The goal is to determine whether the higher-level abstraction provides measurable operational or engineering benefits for the production scenarios it targets.

---

# Summary

LoomBus should not be positioned as:

> “A better executor.”

or:

> “A replacement for virtual threads.”

or:

> “A replacement for Kafka.”

The production-oriented positioning is narrower:

> **LoomBus is an in-process concurrency and communication model for business operations that need explicit ownership, mailboxing, controlled execution, completion, ordering, and backpressure.**

The strongest use case is where concurrent business operations would otherwise require developers to assemble and maintain several lower-level Java concurrency mechanisms around the same logical boundary.

If native Java expresses the problem more clearly, use native Java. LoomBus earns its place when the endpoint/ownership model makes the production architecture easier to reason about and operate.