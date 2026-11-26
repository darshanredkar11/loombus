package io.loombus;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class AccumulatedFlowTest {
    @Test
    void statefulAggregateProcessesCommandsSequentially() throws Exception {
        try (var bus = new LoomBus()) {
            var aggregate = new Account();
            bus.register("account", EndpointConfig.stateful(32), (Command command) -> {
                aggregate.balance += command.delta();
                return aggregate.balance;
            });
            var a = bus.request("account", new Command(10));
            var b = bus.request("account", new Command(-3));
            var c = bus.request("account", new Command(8));
            assertEquals(10, a.get(1, TimeUnit.SECONDS));
            assertEquals(7, b.get(1, TimeUnit.SECONDS));
            assertEquals(15, c.get(1, TimeUnit.SECONDS));
            assertEquals(15, aggregate.balance);
        }
    }

    @Test
    void fanOutFanInCorrelatesIndependentChildResults() throws Exception {
        try (var bus = new LoomBus()) {
            bus.register("risk", EndpointConfig.concurrent(8, 2), (Integer x) -> x * 2);
            bus.register("price", EndpointConfig.concurrent(8, 2), (Integer x) -> x + 100);
            var input = 25;
            var risk = bus.<Integer, Integer>request("risk", input);
            var price = bus.<Integer, Integer>request("price", input);
            var combined = CompletableFuture.allOf(risk, price)
                    .thenApply(ignored -> risk.join() + price.join());
            assertEquals(175, combined.get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void blockingWorkflowUsesEndpointBudgetAndCompletesStagesInOrder() throws Exception {
        try (var bus = new LoomBus()) {
            var order = new StringBuilder();
            bus.register("validate", EndpointConfig.stateful(8), (String value) -> {
                order.append("V");
                return value.trim();
            });
            bus.register("persist", EndpointConfig.stateful(8), (String value) -> {
                order.append("P");
                try {
                    Thread.sleep(Duration.ofMillis(10));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.util.concurrent.CancellationException();
                }
                return value.toUpperCase();
            });
            var validated = bus.request("validate", " loombus ").get(1, TimeUnit.SECONDS);
            var persisted = bus.request("persist", validated).get(1, TimeUnit.SECONDS);
            assertEquals("LOOMBUS", persisted);
            assertEquals("VP", order.toString());
        }
    }

    @Test
    void endpointConcurrencyBudgetIsEnforced() throws Exception {
        try (var bus = new LoomBus()) {
            var active = new java.util.concurrent.atomic.AtomicInteger();
            var peak = new java.util.concurrent.atomic.AtomicInteger();
            var started = new CountDownLatch(2);
            var release = new CountDownLatch(1);
            bus.register("budget", EndpointConfig.concurrent(6, 2), (Integer x) -> {
                int now = active.incrementAndGet();
                peak.accumulateAndGet(now, Math::max);
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.util.concurrent.CancellationException();
                } finally {
                    active.decrementAndGet();
                }
                return x;
            });
            var futures = java.util.stream.IntStream.range(0, 3)
                    .mapToObj(i -> bus.request("budget", i)).toList();
            assertTrue(started.await(1, TimeUnit.SECONDS));
            assertEquals(2, peak.get());
            release.countDown();
            for (var future : futures) future.get(1, TimeUnit.SECONDS);
            assertEquals(2, peak.get());
        }
    }

    @Test
    void pipelineTransfersMutableStateBetweenSequentialOwners() throws Exception {
        try (var bus = new LoomBus()) {
            bus.register("stage-a", EndpointConfig.stateful(8), (MutableState state) -> {
                state.value++;
                state.trace.append("A");
                return state;
            });
            bus.register("stage-b", EndpointConfig.stateful(8), (MutableState state) -> {
                state.value += 2;
                state.trace.append("B");
                return state;
            });

            var original = new MutableState();
            var result = bus.<MutableState, MutableState>request("stage-a", original)
                    .thenCompose(state -> bus.<MutableState, MutableState>request("stage-b", state))
                    .get(1, TimeUnit.SECONDS);

            assertSame(original, result);
            assertEquals(3, result.value);
            assertEquals("AB", result.trace.toString());
        }
    }

    @Test
    void waitBackpressureAdmitsProducerWhenCapacityIsReleased() throws Exception {
        try (var bus = new LoomBus()) {
            var firstStarted = new CountDownLatch(1);
            var releaseFirst = new CountDownLatch(1);
            var secondStarted = new CountDownLatch(1);
            var producerThread = new AtomicReference<Thread>();
            var producerReady = new CountDownLatch(1);
            bus.register("wait", EndpointConfig.stateful(1), (Integer value) -> {
                if (value == 1) {
                    firstStarted.countDown();
                    try {
                        releaseFirst.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new java.util.concurrent.CancellationException();
                    }
                } else {
                    secondStarted.countDown();
                }
                return value;
            });

            var first = bus.request("wait", 1);
            assertTrue(firstStarted.await(1, TimeUnit.SECONDS));
            var second = CompletableFuture.supplyAsync(() -> {
                producerThread.set(Thread.currentThread());
                producerReady.countDown();
                return bus.request("wait", 2);
            }).thenCompose(Function.identity());
            assertTrue(producerReady.await(1, TimeUnit.SECONDS));
            awaitBlockedProducer(producerThread.get(), 1_000);
            releaseFirst.countDown();
            assertEquals(1, first.get(1, TimeUnit.SECONDS));
            assertEquals(2, second.get(1, TimeUnit.SECONDS));
            assertTrue(secondStarted.await(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void endpointAdmissionBudgetsRemainIndependent() throws Exception {
        try (var bus = new LoomBus()) {
            var aStarted = new CountDownLatch(1);
            var releaseA = new CountDownLatch(1);
            bus.register("isolated-a", EndpointConfig.concurrent(1, 1)
                    .withBackpressure(Backpressure.REJECT), (Integer value) -> {
                aStarted.countDown();
                try {
                    releaseA.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.util.concurrent.CancellationException();
                }
                return value;
            });
            bus.register("isolated-b", EndpointConfig.stateful(2), (Integer value) -> value * 10);

            var heldA = bus.request("isolated-a", 1);
            assertTrue(aStarted.await(1, TimeUnit.SECONDS));
            assertThrows(RejectedExecutionException.class, () -> bus.request("isolated-a", 2));
            assertEquals(30, bus.request("isolated-b", 3).get(1, TimeUnit.SECONDS));
            releaseA.countDown();
            assertEquals(1, heldA.get(1, TimeUnit.SECONDS));
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

    @Test
    void workflowSessionOwnerSerializesTransitions() throws Exception {
        try (var bus = new LoomBus()) {
            var session = new WorkflowSession();
            bus.register("session", EndpointConfig.stateful(8), (WorkflowCommand command) -> {
                if (command.next() == Phase.VALIDATED && session.phase == Phase.NEW) {
                    session.phase = Phase.VALIDATED;
                } else if (command.next() == Phase.COMPLETE && session.phase == Phase.VALIDATED) {
                    session.phase = Phase.COMPLETE;
                } else {
                    throw new IllegalStateException("invalid workflow transition");
                }
                return session.phase;
            });
            assertEquals(Phase.VALIDATED,
                    bus.<WorkflowCommand, Phase>request("session", new WorkflowCommand(Phase.VALIDATED))
                            .get(1, TimeUnit.SECONDS));
            assertEquals(Phase.COMPLETE,
                    bus.<WorkflowCommand, Phase>request("session", new WorkflowCommand(Phase.COMPLETE))
                            .get(1, TimeUnit.SECONDS));
            assertEquals(Phase.COMPLETE, session.phase);
        }
    }

    private record Command(int delta) {}
    private static final class Account { int balance; }
    private static final class MutableState { int value; final StringBuilder trace = new StringBuilder(); }
    private enum Phase { NEW, VALIDATED, COMPLETE }
    private record WorkflowCommand(Phase next) {}
    private static final class WorkflowSession { Phase phase = Phase.NEW; }
}
