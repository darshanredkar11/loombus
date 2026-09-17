# LoomBus Use Cases and Patterns

LoomBus is most useful when the application has concurrent operations that need a clear communication boundary and some form of state ownership.

The examples below are architectural patterns, not requirements that every application should adopt.

## 1. Stateful domain aggregate

### Problem

Many requests need to update one logical domain object while preserving its invariants.

A shared object protected by locks can work, but the concurrency policy becomes scattered through the business code.

### LoomBus approach

Send commands to a stateful endpoint.

```text
Command A ─┐
Command B ─┼──► mailbox ──► aggregate owner ──► state
Command C ─┘
```

### Example

```java
record Deposit(long accountId, long cents) {}

var balance = new AtomicLong(); // replace with richer owned state in a real domain model

bus.register(
    "account-123",
    EndpointConfig.stateful(10_000),
    (Deposit command) -> {
        balance.addAndGet(command.cents());
        return balance.get();
    }
);
```

The more interesting case is state that contains multiple related fields and invariants. The endpoint gives that state one logical mutation owner.

### Why use it

- serialized domain commands
- easier reasoning about invariants
- bounded admission
- no need for every caller to coordinate on the same lock

---

## 2. Authentication / MFA risk decision

### Problem

An authentication request may require several independent signals:

- location / geovelocity
- device posture
- behavioral signal
- account history
- policy evaluation

Waiting for each signal sequentially increases latency.

### LoomBus approach

Fan out independent evaluations concurrently and aggregate the results.

```text
                         Authentication
                              │
                         parallel work
                       /      |       \
                      ▼       ▼        ▼
                    Geo    Device   Behaviour
                      │       │        │
                      ▼       ▼        ▼
                    GeoR   DeviceR  BehaviourR
                       \      |       /
                        \     |      /
                         ▼    ▼     ▼
                          Risk decision
```

### Example shape

```java
var geo = bus.request("geo-risk", request);
var device = bus.request("device-risk", request);
var behaviour = bus.request("behaviour-risk", request);

var decision = combine(
    geo.join(),
    device.join(),
    behaviour.join()
);
```

A structured-concurrency layer can later provide stronger lifetime and fail-fast semantics around these child operations.

### Why use it

- independent work executes concurrently
- each evaluator has its own concurrency policy
- business logic can remain ordinary Java code
- stateful evaluators can still use single-owner endpoints

---

## 3. Notification fan-out

### Problem

One business action needs multiple independent side effects:

- audit
- metrics
- email
- SMS
- downstream notification

### LoomBus approach

Publish one immutable event.

```text
                    UserCreated
                        │
             ┌──────────┼──────────┐
             ▼          ▼          ▼
           Audit      Metrics   Notification
```

### Example

```java
record UserCreated(String userId) {}

bus.subscribe("user-events", event -> audit.record(event));
bus.subscribe("user-events", event -> metrics.record(event));
bus.subscribe("user-events", event -> email.send(event));

bus.publish("user-events", new UserCreated("u-123"));
```

### Important contract

The event should normally be immutable because subscribers may receive the same object reference.

### When not to use it

If delivery must survive process failure or requires durable replay, use a durable messaging system instead.

---

## 4. Blocking database / HTTP workflow

### Problem

A service performs blocking I/O and wants straightforward sequential business logic without building an asynchronous callback graph.

### LoomBus approach

Run the handler on a virtual thread and use ordinary blocking APIs.

```text
request
   │
   ▼
endpoint virtual thread
   │
   ├── DB query ──► park ──► resume
   │
   ├── HTTP call ─► park ──► resume
   │
   ▼
result
```

### Example

```java
bus.register(
    "customer",
    EndpointConfig.concurrent(5_000, 200),
    customerId -> {
        var customer = customerRepository.find(customerId);
        var profile = profileClient.fetch(customer.profileId());
        return combine(customer, profile);
    }
);
```

Virtual threads make the blocking style practical; LoomBus supplies the endpoint boundary and admission control.

### Important limitation

The endpoint concurrency budget should account for downstream resources. If a database pool has 50 connections, configuring thousands of simultaneous database operations does not make the database capable of thousands of concurrent queries.

---

## 5. Stateful workflow / session owner

### Problem

A workflow has mutable state that receives events from many concurrent sources.

### LoomBus approach

Treat the workflow state as owned by one endpoint.

```text
HTTP ──┐
Timer ─┼──► workflow mailbox ──► workflow owner
Kafka ─┤                              │
User ──┘                              ▼
                                  state
```

Events become commands to the workflow owner rather than direct mutations of shared state.

### Benefits

- explicit ownership
- serialized transitions
- bounded queue
- easier event ordering
- simpler invariant reasoning

---

## 6. Multi-stage processing pipeline

### Problem

An operation passes through several transformations and every stage needs access to the same mutable context.

A common implementation puts the context in a shared object and lets multiple components modify it.

### LoomBus approach

Pass the operation state from stage to stage.

```text
             owns
Operation ───────► Stage A
                     │
                  transfer
                     ▼
                   Stage B
                     │
                  transfer
                     ▼
                   Stage C
                     │
                     ▼
                   Result
```

### Design rule

> Move the state to the computation instead of moving the computation to shared state.

This does not mean every pipeline must be sequential. Independent branches can still fan out and later aggregate into a new result.

---

## 7. Overload protection

### Problem

Traffic can arrive faster than an endpoint can safely process it.

An unbounded queue can turn a temporary throughput mismatch into memory pressure and eventually process instability.

### LoomBus approach

Give the endpoint a finite capacity and define what the producer should do when capacity is unavailable.

```text
incoming
   │
   ▼
┌───────────────┐
│ capacity = N  │
└───────┬───────┘
        │
   ┌────┼────────────┐
   ▼    ▼            ▼
 WAIT REJECT      TIMEOUT
```

### Choosing a policy

**WAIT** — useful when upstream can naturally slow down.

**REJECT** — useful when overload should immediately propagate to the caller.

**TIMEOUT** — useful when a bounded wait is acceptable but indefinite waiting is not.

The correct policy is application-specific.

---

## 8. Parallel aggregation

### Problem

One operation requires several independent calculations and then a combined result.

### Pattern

```text
                         Parent
                       /   |   \
                      /    |    \
                     ▼     ▼     ▼
                    A      B      C
                     \     |     /
                      \    |    /
                       ▼   ▼   ▼
                         Join
                           │
                           ▼
                         Result
```

The key distinction is that the parent owns the aggregation state. Workers return results rather than concurrently mutating the parent's result object.

That is a direct application of the ownership-transfer principle.

---

## 9. When LoomBus is the wrong abstraction

Do not use LoomBus merely because the application uses Java 21+.

A direct method call is preferable when there is no meaningful concurrency boundary.

A normal concurrent collection or lock may be preferable when the state is genuinely shared and concurrent mutation is intentional.

Kafka, Pulsar, or another durable broker is appropriate when you need:

- process-independent communication
- durable storage
- replay
- consumer offsets
- distributed delivery
- cross-service communication

REST/HTTP is appropriate when you need a network API boundary.

LoomBus is specifically for **in-process communication and execution coordination**.

---

## 10. Design checklist

Before introducing an endpoint, answer these questions:

### Ownership

- Who owns the mutable state?
- Can it be confined to one endpoint?
- Can state be transferred instead of shared?
- Are messages immutable?

### Concurrency

- Can messages execute independently?
- Does ordering matter?
- What concurrency budget is safe for downstream resources?

### Capacity

- What is the maximum admitted work?
- Should producers wait, reject, or time out?
- Does backpressure propagate to the caller?

### Failure

- What happens when one operation fails?
- What happens to sibling work?
- What does cancellation mean for the actual running operation?

### Lifecycle

- What happens to queued work on shutdown?
- What happens to in-flight work?
- What happens to pending response futures?

These questions are part of the architecture, not implementation details.
