package io.loombus;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    void publishCanPartiallyAdmitSubscribersWhenCapacityRejects() throws Exception {
        try (var bus = new LoomBus()) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var secondDelivered = new AtomicInteger();
            var nextReceived = new CountDownLatch(1);
            bus.register("partial-publish", EndpointConfig.stateful(1)
                    .withBackpressure(Backpressure.REJECT), (Object ignored) -> null);
            bus.subscribe("partial-publish", event -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.util.concurrent.CancellationException();
                }
            });
            bus.subscribe("partial-publish", event -> {
                secondDelivered.incrementAndGet();
                if ("next".equals(event)) nextReceived.countDown();
            });

            assertThrows(RejectedExecutionException.class, () -> bus.publish("partial-publish", "event"));
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            release.countDown();
            assertEquals(0, secondDelivered.get());
            bus.publish("partial-publish", "next");
            assertTrue(nextReceived.await(1, TimeUnit.SECONDS));
            assertEquals(1, secondDelivered.get());
        }
    }

    @Test
    void subscriberFailureDoesNotKillEndpointWorkerOrLeakCapacity() throws Exception {
        try (var bus = new LoomBus()) {
            var observed = new AtomicInteger();
            var done = new CountDownLatch(1);
            bus.register("subscriber-failure", EndpointConfig.stateful(1), (Object ignored) -> null);
            bus.subscribe("subscriber-failure", event -> {
                if ("bad".equals(event)) throw new IllegalStateException("subscriber boom");
            });
            bus.subscribe("subscriber-failure", event -> {
                if ("good".equals(event)) {
                    observed.incrementAndGet();
                    done.countDown();
                }
            });
            bus.publish("subscriber-failure", "bad");
            bus.publish("subscriber-failure", "good");
            assertTrue(done.await(2, TimeUnit.SECONDS));
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
            gate.countDown();
            for (int i = 0; i < futures.size(); i++) {
                assertEquals(i, futures.get(i).get(2, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void closeRejectsNewAdmissionAndBlockedWaitersAfterDrain() throws Exception {
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

            var producer = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try {
                    bus.request("closing", 2);
                    return "admitted";
                } catch (IllegalStateException expected) {
                    return "closed";
                }
            });

            endpoint.close();
            gate.countDown();
            assertEquals(1, first.get(1, TimeUnit.SECONDS));
            assertEquals("closed", producer.get(2, TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, () -> bus.request("closing", 3));
        }
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
