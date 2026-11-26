package io.loombus;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class LoomBusTest {
    @Test
    void requestResponse() throws Exception {
        try (var bus = new LoomBus()) {
            bus.register("math", EndpointConfig.stateful(32), (Integer x) -> x * 2);
            assertEquals(42, bus.request("math", 21).get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void statefulEndpointPreservesProcessingOrder() throws Exception {
        try (var bus = new LoomBus()) {
            var seen = new ArrayList<Integer>();
            bus.register("ordered", EndpointConfig.stateful(100), (Integer x) -> {
                seen.add(x);
                return x;
            });

            var futures = new ArrayList<CompletableFuture<Integer>>();
            for (int i = 0; i < 100; i++) futures.add(bus.request("ordered", i));
            for (var future : futures) future.get(1, TimeUnit.SECONDS);

            assertEquals(100, seen.size());
            for (int i = 0; i < 100; i++) assertEquals(i, seen.get(i));
        }
    }

    @Test
    void boundedRejectBackpressure() throws Exception {
        try (var bus = new LoomBus()) {
            var gate = new CountDownLatch(1);
            bus.register("slow", EndpointConfig.stateful(1)
                    .withBackpressure(Backpressure.REJECT), (Integer x) -> {
                        try {
                            gate.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new java.util.concurrent.CancellationException();
                        }
                        return x;
                    });

            var first = bus.request("slow", 1);
            awaitUntil(() -> bus.request("slow", 2), 500);
            gate.countDown();
            assertEquals(1, first.get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void concurrentEndpointRunsInParallel() throws Exception {
        try (var bus = new LoomBus()) {
            var started = new CountDownLatch(2);
            var release = new CountDownLatch(1);
            bus.register("parallel", EndpointConfig.concurrent(2, 2), (Integer x) -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.util.concurrent.CancellationException();
                }
                return x;
            });

            var a = bus.request("parallel", 1);
            var b = bus.request("parallel", 2);
            assertTrue(started.await(1, TimeUnit.SECONDS), "both handlers should run concurrently");
            release.countDown();
            assertEquals(1, a.get(1, TimeUnit.SECONDS));
            assertEquals(2, b.get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void exceptionsReachRequestCaller() throws Exception {
        try (var bus = new LoomBus()) {
            bus.register("failure", EndpointConfig.stateful(8), (Integer ignored) -> {
                throw new IllegalStateException("boom");
            });

            var future = bus.request("failure", 1);
            var error = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> future.get(1, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, error.getCause());
            assertEquals("boom", error.getCause().getMessage());
        }
    }

    @Test
    void publishReachesAllSubscribers() throws Exception {
        try (var bus = new LoomBus()) {
            bus.register("events", EndpointConfig.concurrent(32, 4), (Object ignored) -> null);
            var received = new AtomicInteger();
            var done = new CountDownLatch(2);
            bus.subscribe("events", event -> {
                assertEquals("hello", event);
                received.incrementAndGet();
                done.countDown();
            });
            bus.subscribe("events", event -> {
                assertEquals("hello", event);
                received.incrementAndGet();
                done.countDown();
            });

            bus.publish("events", "hello");
            assertTrue(done.await(1, TimeUnit.SECONDS));
            assertEquals(2, received.get());
        }
    }

    @Test
    void publishCanPartiallyAdmitSubscribersAndRecoverCapacity() throws Exception {
        try (var bus = new LoomBus()) {
            var blockerStarted = new CountDownLatch(1);
            var releaseBlocker = new CountDownLatch(1);
            var firstSubscriberEntered = new CountDownLatch(1);
            var releaseFirstSubscriber = new CountDownLatch(1);
            var firstSubscriberDone = new CountDownLatch(1);
            var recoveredSecondSubscriber = new CountDownLatch(1);
            var secondDelivered = new AtomicInteger();

            bus.register("partial-publish", EndpointConfig.concurrent(2, 1)
                    .withBackpressure(Backpressure.REJECT), (Object event) -> {
                if ("hold".equals(event)) {
                    blockerStarted.countDown();
                    try {
                        releaseBlocker.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new java.util.concurrent.CancellationException();
                    }
                }
                return null;
            });
            bus.subscribe("partial-publish", event -> {
                if ("partial".equals(event)) {
                    firstSubscriberEntered.countDown();
                    try {
                        releaseFirstSubscriber.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new java.util.concurrent.CancellationException();
                    } finally {
                        firstSubscriberDone.countDown();
                    }
                }
            });
            bus.subscribe("partial-publish", event -> {
                if ("recovered".equals(event)) {
                    secondDelivered.incrementAndGet();
                    recoveredSecondSubscriber.countDown();
                }
            });

            var blocker = bus.request("partial-publish", "hold");
            assertTrue(blockerStarted.await(1, TimeUnit.SECONDS));
            assertThrows(RejectedExecutionException.class, () -> bus.publish("partial-publish", "partial"));

            releaseBlocker.countDown();
            assertNull(blocker.get(1, TimeUnit.SECONDS));
            assertTrue(firstSubscriberEntered.await(1, TimeUnit.SECONDS));
            releaseFirstSubscriber.countDown();
            assertTrue(firstSubscriberDone.await(1, TimeUnit.SECONDS));
            assertEquals(0, secondDelivered.get());

            publishUntilDelivered(bus, "partial-publish", "recovered", recoveredSecondSubscriber, 1_000);
            assertEquals(1, secondDelivered.get());
        }
    }

    @Test
    void subscriberFailureDoesNotKillEndpointWorkerOrLeakCapacity() throws Exception {
        try (var bus = new LoomBus()) {
            var observed = new AtomicInteger();
            var badDeliveryDone = new CountDownLatch(1);
            var goodDeliveryDone = new CountDownLatch(1);
            bus.register("subscriber-failure", EndpointConfig.concurrent(2, 1), (Object ignored) -> null);
            bus.subscribe("subscriber-failure", event -> {
                if ("bad".equals(event)) throw new IllegalStateException("subscriber boom");
            });
            bus.subscribe("subscriber-failure", event -> {
                if ("bad".equals(event)) badDeliveryDone.countDown();
                if ("good".equals(event)) {
                    observed.incrementAndGet();
                    goodDeliveryDone.countDown();
                }
            });
            bus.publish("subscriber-failure", "bad");
            assertTrue(badDeliveryDone.await(1, TimeUnit.SECONDS));
            publishUntilDelivered(bus, "subscriber-failure", "good", goodDeliveryDone, 2_000);
            assertEquals(1, observed.get());
        }
    }

    @Test
    void messageReferenceIsNotCopied() throws Exception {
        try (var bus = new LoomBus()) {
            var message = new Object();
            var seen = new AtomicReference<>();
            bus.register("identity", EndpointConfig.stateful(4), (Object value) -> {
                seen.set(value);
                return value;
            });

            assertSame(message, bus.request("identity", message).get(1, TimeUnit.SECONDS));
            assertSame(message, seen.get());
        }
    }

    @Test
    void safePublicationStressWithFinalMessageState() throws Exception {
        try (var bus = new LoomBus()) {
            bus.register("visibility", EndpointConfig.concurrent(20_000, 100),
                    (Message value) -> value.value() == 42);

            var futures = IntStream.range(0, 10_000)
                    .mapToObj(i -> bus.request("visibility", new Message(42)))
                    .toList();

            for (var future : futures) {
                assertTrue((Boolean) future.get(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void statefulEndpointDoesNotCorruptOwnedStateUnderConcurrentProducers() throws Exception {
        try (var bus = new LoomBus()) {
            var state = new Counter();
            bus.register("counter", EndpointConfig.stateful(20_000), (Integer ignored) -> {
                state.value++;
                return state.value;
            });

            var futures = IntStream.range(0, 10_000)
                    .mapToObj(i -> bus.request("counter", i))
                    .toList();

            for (var future : futures) future.get(5, TimeUnit.SECONDS);
            assertEquals(10_000, state.value);
        }
    }

    @Test
    void timeoutBackpressureEventuallyRejects() throws Exception {
        try (var bus = new LoomBus()) {
            var gate = new CountDownLatch(1);
            bus.register("timeout", EndpointConfig.stateful(1)
                    .withBackpressure(Backpressure.TIMEOUT, java.time.Duration.ofMillis(50)),
                    (Integer x) -> {
                        try {
                            gate.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new java.util.concurrent.CancellationException();
                        }
                        return x;
                    });

            var first = bus.request("timeout", 1);
            assertThrows(RejectedExecutionException.class, () -> bus.request("timeout", 2));
            gate.countDown();
            assertEquals(1, first.get(1, TimeUnit.SECONDS));
        }
    }


    @Test
    void handlerInterruptDoesNotPermanentlyLoseWorker() throws Exception {
        try (var bus = new LoomBus()) {
            bus.register("interrupt-recovery", EndpointConfig.stateful(8), (Integer value) -> {
                if (value == 1) Thread.currentThread().interrupt();
                return value;
            });
            assertEquals(1, bus.request("interrupt-recovery", 1).get(1, TimeUnit.SECONDS));
            assertEquals(2, bus.request("interrupt-recovery", 2).get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void capacityIsRecoveredAfterHandlerFailure() throws Exception {
        try (var bus = new LoomBus()) {
            var attempts = new AtomicInteger();
            bus.register("recover", EndpointConfig.concurrent(1, 1), (Integer value) -> {
                if (attempts.incrementAndGet() == 1) throw new IllegalStateException("first failure");
                return value;
            });
            assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> bus.request("recover", 1).get(1, TimeUnit.SECONDS));
            assertEquals(2, bus.request("recover", 2).get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void endpointCloseDrainsAdmittedWork() throws Exception {
        try (var bus = new LoomBus()) {
            var gate = new CountDownLatch(1);
            var endpoint = bus.register("drain", EndpointConfig.concurrent(8, 1), (Integer value) -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.util.concurrent.CancellationException();
                }
                return value;
            });
            var futures = IntStream.range(0, 8).mapToObj(i -> bus.request("drain", i)).toList();
            endpoint.close();
            assertThrows(IllegalStateException.class, () -> bus.publish("drain", "after-close"));
            assertThrows(IllegalStateException.class, () -> bus.subscribe("drain", event -> {}));
            gate.countDown();
            for (int i = 0; i < futures.size(); i++) {
                assertEquals(i, futures.get(i).get(2, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void closeRejectsNewAdmissionAndWakesBlockedWaiters() throws Exception {
        try (var bus = new LoomBus()) {
            var gate = new CountDownLatch(1);
            var started = new CountDownLatch(1);
            var endpoint = bus.register("closing", EndpointConfig.stateful(1), (Integer value) -> {
                started.countDown();
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.util.concurrent.CancellationException();
                }
                return value;
            });

            var first = bus.request("closing", 1);
            assertTrue(started.await(1, TimeUnit.SECONDS));

            var producerThread = new AtomicReference<Thread>();
            var producerStarted = new CountDownLatch(1);
            var producer = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                producerThread.set(Thread.currentThread());
                producerStarted.countDown();
                try {
                    bus.request("closing", 2);
                    return "admitted";
                } catch (IllegalStateException expected) {
                    return "closed";
                }
            });

            assertTrue(producerStarted.await(1, TimeUnit.SECONDS));
            awaitBlockedProducer(producerThread.get(), 1_000);
            endpoint.close();
            try {
                assertEquals("closed", producer.get(500, TimeUnit.MILLISECONDS),
                        "close must wake a producer waiting for capacity");
            } finally {
                gate.countDown();
            }
            assertEquals(1, first.get(1, TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, () -> bus.request("closing", 3));
        }
    }

    @Test
    void partialWorkerStartupFailureDoesNotLeakEarlierWorker() throws Exception {
        var rejectingExecutor = new RejectSecondWorkerExecutor();
        try (var bus = new LoomBus(rejectingExecutor)) {
            assertThrows(RejectedExecutionException.class,
                    () -> bus.register("partial-workers", EndpointConfig.concurrent(4, 2),
                            (Integer value) -> value));
            assertTrue(rejectingExecutor.firstWorkerExited.await(1, TimeUnit.SECONDS),
                    "worker started before executor rejection must exit");
        }
    }

    @Test
    void busCloseIsIdempotentAndOwnsInjectedExecutor() throws Exception {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var bus = new LoomBus(executor);
        bus.register("owned-executor", EndpointConfig.stateful(4), (Integer value) -> value);
        bus.close();
        assertTrue(executor.isShutdown());
        assertThrows(IllegalStateException.class,
                () -> bus.register("after-close", EndpointConfig.stateful(1), (Integer value) -> value));
        bus.close();
    }

    @Test
    void busCloseAndRegistrationRaceCannotLeaveLiveEndpoint() throws Exception {
        var bus = new LoomBus();
        var start = new CountDownLatch(1);
        var registered = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                start.await();
                return bus.register("race", EndpointConfig.stateful(4), (Integer value) -> value);
            } catch (IllegalStateException closed) {
                return null;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new java.util.concurrent.CancellationException();
            }
        });
        var closing = java.util.concurrent.CompletableFuture.runAsync(() -> {
            try {
                start.await();
                bus.close();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new java.util.concurrent.CancellationException();
            }
        });
        start.countDown();
        var endpoint = registered.get(2, TimeUnit.SECONDS);
        closing.get(2, TimeUnit.SECONDS);
        assertThrows(IllegalStateException.class,
                () -> bus.register("after-race", EndpointConfig.stateful(1), (Integer value) -> value));
        if (endpoint != null) {
            assertThrows(IllegalStateException.class, () -> bus.request("race", 1));
        }
        bus.close();
    }

    @Test
    void cancelledCallerFutureDoesNotLoseCapacity() throws Exception {
        try (var bus = new LoomBus()) {
            var started = new CountDownLatch(1);
            var gate = new CountDownLatch(1);
            bus.register("cancel", EndpointConfig.stateful(1), (Integer value) -> {
                started.countDown();
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.util.concurrent.CancellationException();
                }
                return value;
            });
            var abandoned = bus.request("cancel", 1);
            assertTrue(started.await(1, TimeUnit.SECONDS));
            assertTrue(abandoned.cancel(false));
            gate.countDown();
            assertEquals(2, bus.request("cancel", 2).get(1, TimeUnit.SECONDS));
        }
    }

    private static final class RejectSecondWorkerExecutor extends AbstractExecutorService {
        private final AtomicInteger submissions = new AtomicInteger();
        private final CountDownLatch firstWorkerExited = new CountDownLatch(1);
        private volatile Thread worker;
        private volatile boolean shutdown;

        @Override
        public void execute(Runnable command) {
            if (submissions.incrementAndGet() > 1) {
                throw new RejectedExecutionException("reject second worker");
            }
            worker = new Thread(() -> {
                try {
                    command.run();
                } finally {
                    firstWorkerExited.countDown();
                }
            }, "loombus-test-worker");
            worker.setDaemon(true);
            worker.start();
        }

        @Override public void shutdown() { shutdown = true; }
        @Override public java.util.List<Runnable> shutdownNow() { shutdown = true; return java.util.List.of(); }
        @Override public boolean isShutdown() { return shutdown; }
        @Override public boolean isTerminated() { return shutdown && (worker == null || !worker.isAlive()); }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            if (worker != null) worker.join(unit.toMillis(timeout));
            return isTerminated();
        }
    }

    private static void publishUntilDelivered(
            LoomBus bus, String endpoint, Object event, CountDownLatch delivered, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            try {
                bus.publish(endpoint, event);
                long remaining = deadline - System.nanoTime();
                assertTrue(remaining > 0 && delivered.await(remaining, TimeUnit.NANOSECONDS),
                        "accepted publish did not reach its target subscriber");
                return;
            } catch (RejectedExecutionException partialOrFull) {
                if (System.nanoTime() >= deadline) {
                    fail("publish did not reach target subscriber before timeout");
                }
                Thread.sleep(1);
            }
        }
    }

    private static void awaitBlockedProducer(Thread thread, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (thread.getState() != Thread.State.WAITING
                && thread.getState() != Thread.State.TIMED_WAITING) {
            if (System.nanoTime() >= deadline) {
                fail("producer did not block on admission");
            }
            Thread.yield();
        }
    }

    private static void awaitUntil(ThrowingRunnable action, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            try {
                action.run();
                fail("expected rejection");
            } catch (RejectedExecutionException expected) {
                return;
            }
            if (System.nanoTime() >= deadline) fail("endpoint did not reject within timeout");
            Thread.yield();
        }
    }

    private record Message(int value) {}

    private static final class Counter {
        int value;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
