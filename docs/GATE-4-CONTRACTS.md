# Gate 4 — Contract Decisions

**Status:** Contract decisions for the 0.1 experimental API are finalized for the current flows; the latest cumulative CI run remains the validation authority.

This gate closes the observable semantics needed by the currently documented flows. It does not add new public capabilities.

## Decisions

| Area | Contract for this branch |
|---|---|
| Admission / ownership transfer | Work is admitted only after capacity is reserved and the envelope is inserted while synchronized with endpoint close. If admission throws, caller retains ownership. After admission, caller must not mutate transferred mutable message state. |
| Endpoint close | Non-blocking close signal. New request, publish, and subscription admissions reject. Work admitted before the close linearization point is drainable and should complete unless the handler itself fails or the bus/runtime is interrupted. |
| WAIT producer at close | A producer blocked in WAIT periodically rechecks endpoint closure (50 ms interval in this implementation). Closing causes it to reject within a bounded check interval, even if capacity is still occupied; no capacity permit is fabricated. |
| Bus close | Idempotent. Registration and the close transition are serialized by a lifecycle lock. Close marks the bus closed and closes endpoints under the lock, then closes/waits for the executor outside the lock. Registration after close rejects; concurrent registration either completes before close and is closed, or rejects. |
| Injected executor | Ownership is transferred to LoomBus; bus.close() closes it. This is the current constructor behavior and is made explicit, not changed. |
| Caller future cancellation | Cancellation means caller abandonment only. It does not remove queued work or interrupt a running handler. |
| TIMEOUT | Admission timeout only. It is not a handler deadline or automatic cancellation timeout. |
| Publish | Best-effort sequential admission per subscriber. A rejection can occur after earlier subscriber deliveries were accepted. No atomic fan-out or receipt is promised. |
| Subscriber error | Isolated at worker level; current observability is through the worker thread's uncaught-exception handler. Delivery is not retried. Worker capacity is released, and the worker remains usable after the exception. |
| Ordering | Concurrency-one endpoint processes FIFO in mailbox acceptance order. With concurrency greater than one, completion order is unspecified. Concurrent producers have no invocation-order promise. |
| Isolation | Endpoint-specific capacity/concurrency only. No priority, fairness, CPU, memory, or downstream-resource isolation promise. |
| Subscription lifetime | Subscription is endpoint-lifetime for 0.1; adding a subscription after endpoint close rejects; unsubscribe is deferred. |
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
| Safe publication | Message values are visible as expected through the mailbox/executor boundary || Bus close and registration race | Registration either completes before bus close and is then closed, or rejects after close; injected executor is closed by LoomBus. |
| Worker interruption / partial startup | An idle worker recovers from interruption while endpoint is open; partial worker startup failure closes earlier workers. |
| Bus close and registration race | Registration is either completed before bus close and then closed, or rejected after close. Executor ownership and idempotent close are verified. |
| Endpoint budgets | Independent mailbox budgets and configured concurrency are honored |

## Exit

Gate 4 closes once every row above has an automated test, all previous tests still pass, and CI is green. Any test that depends on timing must use latches or bounded assertions rather than unbounded sleeps.
