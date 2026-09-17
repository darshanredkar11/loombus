# LoomBus

A small in-process communication and execution model for Java virtual threads.

LoomBus is an experiment around one idea:

> Prefer ownership transfer of operation state over shared mutable state and synchronization.

It provides request/response, publish/subscribe, bounded mailboxes, application-defined concurrency, and work-triggered virtual-thread execution.

## First-draft semantics

- **In-process only.** No network or pod communication.
- **One message object.** LoomBus transfers references; it does not serialize or deep-copy message payloads.
- **Immutable messages by default.** After sending, the producer should not mutate the message.
- **Endpoint ordering.** A stateful endpoint (`concurrency = 1`) processes its mailbox sequentially in enqueue/acceptance order.
- **Concurrency is explicit.** The application chooses endpoint capacity and concurrency; LoomBus enforces the limit.
- **Bounded admission.** Mailboxes are bounded. Overload is handled by `WAIT`, `REJECT`, or `TIMEOUT`.
- **Work-triggered execution.** Workers wait for actual mailbox work rather than polling on a fixed timer.
- **Failure propagation.** Request handlers complete their response future normally or exceptionally.
- **No global ordering.** Ordering is scoped to an endpoint/mailbox.

## Quick start

```java
try (var bus = new LoomBus()) {
    bus.register("math", EndpointConfig.stateful(1024),
            (Integer value) -> value * 2);

    var result = bus.request("math", 21).join();
    System.out.println(result); // 42
}
```

Concurrent endpoint:

```java
bus.register(
    "payment-api",
    EndpointConfig.concurrent(10_000, 100),
    payment -> paymentService.call(payment)
);
```

Reject when the mailbox is full:

```java
var config = EndpointConfig.stateful(1)
        .withBackpressure(Backpressure.REJECT);
```

## Architecture

```text
Caller VT
   │
   │ request / publish
   ▼
 LoomBus
   │
   ▼
Admission Control
   │
   ▼
Bounded Mailbox
   │
   ▼
Work Signal
   │
   ▼
Virtual Thread(s)
   │
   ▼
Endpoint Handler
   │
   ├── success → response
   └── failure → exception
```

## Memory and ownership model

Virtual threads do **not** change the Java Memory Model. LoomBus therefore relies on normal Java safe-publication and happens-before guarantees from its concurrency primitives.

The intended programming model is:

```text
Producer owns message
        │
        │ enqueue reference
        ▼
     LoomBus
        │
        │ safe publication
        ▼
Consumer owns message
```

For mutable endpoint state, prefer one logical owner:

```text
VT1 ──┐
VT2 ──┼──► bounded mailbox ──► owner VT ──► mutable state
VT3 ──┘
```

LoomBus does not claim that synchronization disappears. Databases, caches, pools, metrics, and other genuinely shared resources still require their own concurrency controls.

## Status

This is a **0.1 functional draft**. The API is intentionally small and will change while we validate semantics and benchmarks.

The next work areas are memory-model stress tests, cancellation, endpoint lifecycle, structured concurrency integration, stronger publish semantics, and performance benchmarks.
