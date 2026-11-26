package io.loombus;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

    private record Command(int delta) {}
    private static final class Account { int balance; }
}
