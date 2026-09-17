# LoomBus Feature and Semantic Guide

This document describes the intended semantics of the 0.1 LoomBus API. The implementation is still experimental, so behavior explicitly marked as under development may change.

## 1. Endpoint model

An endpoint is a named in-process execution boundary.

```java
bus.register(
    "account",
    EndpointConfig.stateful(10_000),
    command -> account.apply(command)
);
```

An endpoint has:

- a name
- a bounded mailbox
- a configured concurrency level
- an admission/backpressure policy
- a handler
- optional subscribers

The endpoint is the unit at which mailbox capacity and execution concurrency are configured.

## 2. Stateful endpoints

`EndpointConfig.stateful(capacity)` configures `concurrency = 1`.

This is intended for state that has one logical owner.

```text
many producers
     │
     ▼
 bounded mailbox
     │
     ▼
 one owner
     │
     ▼
 mutable state
```

This lets the handler use ordinary mutable state without concurrent mutation from multiple endpoint workers.

This is a programming-model guarantee created by the endpoint's single worker, not a Java language ownership guarantee.

## 3. Concurrent endpoints

`EndpointConfig.concurrent(capacity, concurrency)` allows multiple messages to be processed at the same time.

Use it when operations are independent or the underlying state is already concurrency-safe.

```text
mailbox
 ├──► VT 1 ──► message A
 ├──► VT 2 ──► message B
 ├──► VT 3 ──► message C
 └──► ...
```

There is no completion-order guarantee when concurrency is greater than one.

## 4. Mailbox

The mailbox is bounded by endpoint capacity.

Capacity represents admitted but not yet completed work. The current implementation releases capacity after a message has finished processing.

A bounded mailbox is important because virtual threads are cheap but application resources are not unlimited.

## 5. Backpressure

Three policies are currently exposed.

### WAIT

The producer waits for endpoint capacity.

```java
EndpointConfig.stateful(100)
    .withBackpressure(Backpressure.WAIT);
```

This is useful when the caller can naturally tolerate waiting and dropping work is unacceptable.

### REJECT

Admission fails immediately when capacity is unavailable.

```java
EndpointConfig.stateful(100)
    .withBackpressure(Backpressure.REJECT);
```

This is useful when overload must be visible to the caller instead of being hidden behind an unbounded queue.

### TIMEOUT

Admission waits for a bounded duration and then rejects if capacity is still unavailable.

```java
EndpointConfig.stateful(100)
    .withBackpressure(
        Backpressure.TIMEOUT,
        Duration.ofMillis(100)
    );
```

Backpressure is intentionally an admission concern. The application chooses its capacity and concurrency budget; LoomBus enforces it.

## 6. Work-triggered execution

The execution model is intended to respond to actual mailbox work rather than asking application code to poll for work on a fixed timer.

Conceptually:

```text
message arrives
     │
     ▼
mailbox + work signal
     │
     ▼
worker executes
     │
     ▼
worker waits again
```

Time-based scheduling is a different concern and should be introduced only when the business requirement is actually time-based.

## 7. Request / response

`request()` returns a `CompletableFuture` representing the endpoint result.

```java
CompletableFuture<Result> result =
    bus.request("risk", request);
```

The message is submitted once. The handler produces one response or one failure.

The caller can compose the result using normal `CompletableFuture` APIs or wait for it.

## 8. Failure propagation

A request handler that throws causes its response future to complete exceptionally.

```text
handler
   │
   ├── return ──► successful future
   │
   └── throw ───► exceptional future
```

Subscriber failures have different semantics because there is no response future for a published event. Subscriber failure isolation and observability remain areas for API hardening.

## 9. Publish / subscribe

An endpoint may have multiple subscribers.

```java
bus.subscribe("events", event -> audit.record(event));
bus.subscribe("events", event -> metrics.record(event));
bus.publish("events", new UserCreated("u-123"));
```

The same in-memory event reference can be delivered to multiple subscribers. Therefore immutable events are strongly preferred.

The current 0.1 implementation enqueues delivery independently for each subscriber. Atomic all-or-nothing fan-out is a planned hardening area.

## 10. Ordering

Ordering is scoped to an endpoint.

For a stateful endpoint, accepted messages are processed sequentially in mailbox acceptance order.

```text
accept A → accept B → accept C
       │
       ▼
     A → B → C
```

For concurrent endpoints, messages may overlap and complete in a different order.

The API should not imply a global ordering guarantee across endpoints.

## 11. Ownership transfer

The central LoomBus idea is to move operation state between execution stages instead of making the state globally shared.

```text
Stage A owns state
       │
       │ transfer reference
       ▼
Stage B owns state
       │
       │ transfer reference
       ▼
Stage C owns state
```

Java does not provide Rust-style compile-time move semantics. LoomBus therefore treats ownership as an architectural/API contract.

## 12. Message reference semantics

Messages remain in-process Java objects.

LoomBus does not serialize, deserialize, or deep-copy them.

```text
Object X
  │
  ├── producer reference
  │
  ▼
LoomBus mailbox
  │
  ▼
handler reference ──► Object X
```

The producer should treat a mutable message as transferred after submission. If multiple parties need to mutate it concurrently, that is shared mutable state and requires explicit concurrency design.

## 13. Java Memory Model

Virtual threads do not alter the Java Memory Model.

LoomBus must therefore establish safe publication through ordinary Java concurrency mechanisms. The architectural objective is to reduce the number of mutable objects simultaneously accessed by multiple threads.

This distinction matters:

> Ownership-oriented design reduces races; it does not repeal the Java Memory Model.

Shared resources such as caches, databases, pools, files, metrics, and concurrent collections still need appropriate synchronization or concurrency-safe APIs.

## 14. Cancellation and timeout

Cancellation is intentionally being hardened separately from simple future completion.

A caller timing out does not automatically prove that endpoint work has stopped. Virtual-thread interruption is a cancellation signal; application code and blocking libraries determine how promptly that signal is observed.

Future versions should make these states explicit:

```text
caller timeout
     │
     ▼
cancellation requested
     │
     ├── work stops
     └── work continues / cannot stop immediately
```

## 15. Endpoint lifecycle

Endpoint shutdown must account for:

- queued messages
- in-flight handlers
- pending response futures
- subscriber delivery
- worker termination

The 0.1 lifecycle is intentionally minimal and is subject to change as shutdown semantics are formalized.

## 16. Structured concurrency

LoomBus and structured concurrency solve related but different problems.

Structured concurrency is useful for defining the lifetime and failure relationship of child tasks.

LoomBus is useful for defining communication boundaries and endpoint ownership.

A future integration should make these compose cleanly:

```text
parent operation
      │
 structured scope
   /      |      \
endpoint endpoint endpoint
   \      |      /
       results
```

## 17. What LoomBus is not

LoomBus is not:

- a distributed message broker
- a durable queue
- a replacement for Kafka or Pulsar
- a network RPC framework
- a serialization protocol
- a database concurrency layer
- a guarantee that all synchronization disappears

Its scope is intentionally smaller: **in-process communication and execution coordination for Java virtual-thread applications.**

## 18. Design rule of thumb

When designing an operation, ask:

1. Who owns this mutable state?
2. Can that state be confined to one endpoint?
3. Can another stage receive the state instead of sharing it?
4. Is the message immutable?
5. Is this resource genuinely shared?
6. What is the endpoint's capacity?
7. What should happen under overload?
8. Does ordering actually matter?
9. What happens when the handler fails?
10. What happens when the caller cancels or times out?

The answers should be explicit rather than emerging accidentally from executor and lock usage.
