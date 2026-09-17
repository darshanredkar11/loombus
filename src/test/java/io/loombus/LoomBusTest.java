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
                assertTrue(future.get(5, TimeUnit.SECONDS));
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
