# LoomBus

A small in-process communication and execution model for Java virtual threads.

LoomBus is an experiment around one idea:

> **Prefer moving ownership of mutable operation state between operations over allowing multiple concurrent operations to mutate the same state.**

Virtual threads make blocking concurrency inexpensive. LoomBus provides the communication boundary around that concurrency: endpoints, mailboxes, request/response, pub/sub, bounded admission, and explicit concurrency.

## What LoomBus is

LoomBus is an **in-process** communication model for concurrent Java code.

It is designed for applications where many virtual threads need to coordinate without turning business logic into a collection of shared mutable objects, locks, and ad-hoc queues.

```text
Caller VT(s)
     │
     │ request / publish
     ▼
  LoomBus
     │
     ├── admission / backpressure
     │
     ▼
 Bounded Mailbox
     │
     ▼
 Virtual Thread execution
     │
     ▼
 Endpoint Handler
     │
     ├── mutate owned state
     ├── perform blocking I/O
     └── return response / emit event
```

## Why it exists

Traditional concurrent Java code often makes the communication mechanism part of the business logic:

- `synchronized` blocks around state
- `Lock` / `ReentrantLock`
- shared `ConcurrentHashMap` state
- ad-hoc `BlockingQueue`s
- executor plumbing
- callback chains
- manual worker lifecycle

LoomBus explores a different boundary:

```text
Instead of:

many threads → shared mutable state → synchronization

Prefer:

many threads → mailbox → one logical owner → state mutation
```

This does **not** eliminate synchronization. It reduces the amount of application state that needs to be concurrently mutated.

## Features

| Feature | First-draft behavior |
|---|---|
| Request/response | `CompletableFuture` response |
| Publish/subscribe | Multiple subscribers per endpoint |
| Stateful endpoint | Sequential processing with `concurrency = 1` |
| Concurrent endpoint | Multiple handlers may execute concurrently |
| Bounded mailbox | Capacity limits admission |
| Backpressure | `WAIT`, `REJECT`, `TIMEOUT` |
| Virtual threads | Java virtual-thread execution |
| Work-triggered execution | Work wakes execution instead of periodic application polling |
| Message transport | Object reference; no serialization/deep copy |
| Error propagation | Request failures reach the caller through the response future |
| In-process | No network, broker, or pod communication |

## Quick start

### 1. Request / response

```java
try (var bus = new LoomBus()) {
    bus.register(
        "math",
        EndpointConfig.stateful(1024),
        (Integer value) -> value * 2
    );

    var result = bus.request("math", 21).join();
    System.out.println(result); // 42
}
```

The caller sends an object reference to the endpoint. The handler processes it and completes the response future.

### 2. Stateful endpoint

Use a stateful endpoint when a piece of mutable application state should have one logical owner.

```java
var account = new AccountState();

bus.register(
    "account",
    EndpointConfig.stateful(10_000),
    command -> {
        account.apply(command);
        return account.snapshot();
    }
);
```

Multiple virtual threads can submit commands, while the endpoint processes them sequentially.

The important boundary is:

```text
VT1 ──┐
VT2 ──┤
VT3 ──┼──► mailbox ──► one logical owner ──► mutable state
VT4 ──┤
VT5 ──┘
```

### 3. Concurrent endpoint

Use a concurrent endpoint when independent messages can safely execute in parallel.

```java
bus.register(
    "payment-api",
    EndpointConfig.concurrent(10_000, 100),
    payment -> paymentService.call(payment)
);
```

Here `100` is the application's concurrency budget for that endpoint. Messages may complete out of order.

### 4. Publish / subscribe

```java
bus.register("events", EndpointConfig.concurrent(1_000, 8), ignored -> null);

bus.subscribe("events", event -> auditService.record(event));
bus.subscribe("events", event -> metricsService.record(event));
bus.subscribe("events", event -> notificationService.process(event));

bus.publish("events", new UserCreated("u-123"));
```

For published events, prefer immutable objects because the same reference can be delivered to multiple subscribers.

### 5. Backpressure

Bounded admission prevents an application from accepting unlimited work simply because virtual threads are cheap.

Reject immediately:

```java
var config = EndpointConfig.stateful(1)
        .withBackpressure(Backpressure.REJECT);
```

Wait for capacity:

```java
var config = EndpointConfig.stateful(1)
        .withBackpressure(Backpressure.WAIT);
```

Wait only for a bounded period:

```java
var config = EndpointConfig.stateful(1)
        .withBackpressure(
            Backpressure.TIMEOUT,
            Duration.ofMillis(100)
        );
```

The application chooses its capacity and concurrency budget; LoomBus enforces admission against that budget.

## Core semantics

### Ordering

Ordering is **endpoint-local**.

For a stateful endpoint (`concurrency = 1`), accepted messages are processed sequentially in mailbox acceptance order.

```text
submit A ─┐
submit B ─┼──► [A][B][C] ──► owner ──► A → B → C
submit C ─┘
```

For `concurrency > 1`, there is no completion-order guarantee.

Do not confuse:

- submission order
- mailbox acceptance order
- processing order
- completion order

They are only equivalent when the endpoint's configured concurrency and workload make them equivalent.

### Ownership and references

LoomBus is in-process, so messages are passed as Java object references.

There is no JSON serialization, network hop, or deep copy.

```text
Producer
   │
   │ object reference
   ▼
Mailbox
   │
   │ same reference
   ▼
Handler
```

The intended contract is:

> After transferring a mutable message into LoomBus, the producer should no longer mutate it unless the application has explicitly designed for shared mutation.

Prefer immutable records/classes for messages.

Java does not enforce ownership transfer like Rust's type system. Ownership in LoomBus is therefore an API and programming-model contract.

### Memory model

Virtual threads do **not** create a new Java Memory Model.

LoomBus relies on the normal Java Memory Model and the happens-before guarantees of its concurrency primitives. The goal is to make safe ownership boundaries easier to reason about, not to bypass Java memory semantics.

Genuinely shared resources still require appropriate synchronization or concurrency-safe APIs:

- databases
- caches
- connection pools
- metrics
- files
- rate limiters
- shared collections
- external services with shared state

### Failure

Request handlers can complete normally or exceptionally:

```java
var future = bus.request("payment", command);

try {
    var result = future.get();
} catch (ExecutionException e) {
    // handler failure is available as e.getCause()
}
```

Failure semantics, cancellation, endpoint shutdown, and structured concurrency integration are still being hardened in the 0.1 development line.

## Use cases

### Stateful domain component

**Problem:** Many requests need to update the same logical state.

**Typical approach:** Shared object + locks/atomics.

**LoomBus approach:** Commands enter a bounded mailbox and one endpoint owner mutates the state.

```text
requests → mailbox → owner → state
```

Useful for aggregates, session state, workflow state, counters with richer invariants, and serialized domain commands.

### Parallel decision / risk evaluation

**Problem:** A request needs several independent evaluations.

**LoomBus approach:** Fan out work to independent concurrent endpoints and aggregate the results.

```text
                         Parent
                    /      |       \
                   ▼       ▼        ▼
                 Geo    Device   Behaviour
                   \       |        /
                    \      |       /
                     ▼     ▼      ▼
                       Decision
```

This is useful for authentication, MFA/risk evaluation, fraud signals, policy checks, and other independent computations.

### Blocking I/O with virtual threads

**Problem:** Business logic performs blocking database or HTTP calls and becomes tangled with executor management.

**LoomBus approach:** A virtual-thread handler can perform ordinary blocking I/O. The virtual thread can park while the operation waits, without requiring reactive callback plumbing for the business logic.

```text
Endpoint VT
    │
    ├── DB call ──► park ──► resume
    │
    └── HTTP call ─► park ──► resume
```

### Event fan-out

**Problem:** One business event needs several independent consumers.

**LoomBus approach:** Publish one event reference to multiple subscribers.

```text
                 event
                   │
             ┌─────┼─────┐
             ▼     ▼     ▼
           audit metrics notification
```

Prefer immutable event payloads.

### Pipeline / ownership transfer

**Problem:** Several processing stages need to transform the same logical operation state.

**LoomBus approach:** Pass the operation state from one stage to another instead of allowing every stage to concurrently mutate it.

```text
Operation
   │ owns state
   ▼
Stage A
   │ transfer
   ▼
Stage B
   │ transfer
   ▼
Stage C
   ▼
Result
```

This is the central LoomBus programming model.

### Overload protection

**Problem:** A traffic spike creates more work than the application can safely process.

**LoomBus approach:** Bound endpoint capacity and select an admission policy.

```text
incoming work
     │
     ▼
bounded capacity
  ┌──┴───────────┐
  │              │
 WAIT         REJECT/TIMEOUT
  │
  ▼
process safely
```

Virtual threads make waiting cheap, but they do not make CPU, database connections, downstream services, memory, or queues unlimited.

## LoomBus vs the alternatives

LoomBus is intentionally narrower than a distributed messaging system.

| Need | LoomBus | Kafka / Pulsar | REST / HTTP |
|---|---|---|---|
| Same JVM | Yes | Usually external broker | Usually network boundary |
| Object reference transfer | Yes | No | No |
| Serialization required | No | Yes | Usually yes |
| Durable messaging | No | Yes | No by itself |
| Network communication | No | Yes | Yes |
| Local mailbox/backpressure | Yes | Broker/topic semantics | Application-specific |
| Virtual-thread integration | Native design target | Consumer-side | Server/client-side |

Use a distributed broker when you need durable, distributed, replayable communication. LoomBus is for communication **inside one process**.

## Project structure

```text
src/main/java/io/loombus/
├── Backpressure.java
├── Endpoint.java
├── EndpointConfig.java
├── LoomBus.java
└── RejectedExecutionException.java

src/test/java/io/loombus/
└── LoomBusTest.java
```

Detailed design and use-case documentation lives under [`docs/`](docs/).

## Current status

**0.1 functional draft.**

The API is intentionally small while we validate semantics, failure behavior, memory visibility, overload behavior, and performance.

Current validation includes request/response, endpoint ordering, bounded backpressure, parallel execution, exception propagation, pub/sub, reference identity, safe-publication stress, and state ownership under concurrent producers.

Planned hardening includes:

- endpoint close/drain semantics
- request timeout and cancellation semantics
- interruption behavior
- stronger publish/fan-out semantics
- mailbox accounting and activation races
- structured concurrency integration
- Java Memory Model stress testing
- performance and throughput benchmarks
- API refinement based on benchmark and failure results

## Documentation

- [Feature and semantic guide](docs/FEATURES.md)
- [Use cases and patterns](docs/USE-CASES.md)

## License

The project is currently an experimental development repository; licensing will be formalized before a stable release.
