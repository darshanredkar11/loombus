# Gate 4 — Contract Decisions

**Status:** Decisions for the 0.1 experimental API; subject to cumulative CI validation.

This gate closes the observable semantics needed by the currently documented flows. It does not add new public capabilities.

## Decisions

| Area | Contract for this branch |
|---|---|
| Admission / ownership transfer | Work is admitted only after capacity is reserved and the envelope is inserted while synchronized with endpoint close. If admission throws, caller retains ownership. After admission, caller must not mutate transferred mutable message state. |
| Endpoint close | Non-blocking close signal. New admissions reject. Work admitted before the close linearization point is drainable and should complete unless the handler itself fails or the bus/runtime is interrupted. |
| WAIT producer at close | WAIT does not receive a separate close wakeup. When capacity becomes available, the producer rechecks closed under the admission lock and rejects rather than enqueueing. |
| Bus close | Closes endpoints, then closes the bus executor. Executor termination provides the bus-level wait behavior of the current Java ExecutorService implementation. |
| Injected executor | Ownership is transferred to LoomBus; bus.close() closes it. This is the current constructor behavior and is made explicit, not changed. |
| Caller future cancellation | Cancellation means caller abandonment only. It does not remove queued work or interrupt a running handler. |
| TIMEOUT | Admission timeout only. It is not a handler deadline or automatic cancellation timeout. |
| Publish | Best-effort sequential admission per subscriber. A rejection can occur after earlier subscriber deliveries were accepted. No atomic fan-out or receipt is promised. |
| Subscriber error | Isolated at worker level; current observability is through the worker thread's uncaught-exception handler. Delivery is not retried. |
| Ordering | Concurrency-one endpoint processes FIFO in mailbox acceptance order. With concurrency greater than one, completion order is unspecified. Concurrent producers have no invocation-order promise. |
| Isolation | Endpoint-specific capacity/concurrency only. No priority, fairness, CPU, memory, or downstream-resource isolation promise. |
| Subscription lifetime | Subscription is endpoint-lifetime for 0.1; unsubscribe is deferred. |
| Durability | In-memory only; process failure may lose admitted work. No persistence/replay/transaction guarantee. |

## Gate 4 test matrix

| Test | Expected evidence |
|---|---|
| Stateful sequential command flow | State updates remain serialized and each future sees its result |
| Concurrent fan-out/fan-in | Child work is independently scheduled; parent combines correlated outcomes |
| Pipeline state transfer | Stage B runs after Stage A result and receives the returned reference/value |
| Blocking work | Handler can block without blocking the calling thread; completion is correct |
| Concurrency budget | Peak active handler count does not exceed configured endpoint concurrency |
| REJECT / TIMEOUT / WAIT | Admission outcomes match policy and capacity is recovered |
| Handler failure | Exceptional completion and capacity release; worker remains usable |
| Caller cancellation | Cancelled future does not prevent processing or leak capacity |
| Endpoint close/drain | Previously admitted work drains; post-close admissions reject |
| Close/admission race | No accepted work can be inserted after close linearization; losing producer releases permit |
| Publish | Subscriber delivery and partial admission semantics are not falsely treated as atomic |
| Subscriber exception | Worker survives handler/subscriber failure and capacity is released |
| Safe publication | Message values are visible as expected through the mailbox/executor boundary |
| Endpoint budgets | Independent mailbox budgets and configured concurrency are honored |

## Exit

Gate 4 closes once every row above has an automated test, all previous tests still pass, and CI is green. Any test that depends on timing must use latches or bounded assertions rather than unbounded sleeps.
