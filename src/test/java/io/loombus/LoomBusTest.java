package io.loombus;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

            var futures = new ArrayList<java.util.concurrent.CompletableFuture<Integer>>();
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
            Thread.sleep(50);
            assertThrows(RejectedExecutionException.class, () -> bus.request("slow", 2));
            gate.countDown();
            assertEquals(1, first.get(1, TimeUnit.SECONDS));
        }
    }
}
