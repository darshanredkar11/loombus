package io.loombus;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LoomBusOpsTest {
    @Test
    void reusableEndpointSupportsRepeatedAndConcurrentMapCallsWithoutMapperMixing() throws Exception {
        try (var bus = new LoomBus();
             var ops = new LoomBusOps(bus, new LoomBusOps.Defaults(
                     64, 4, Backpressure.WAIT, java.time.Duration.ofMillis(100)))) {
            var items = List.of(1, 2, 3, 4, 5, 6, 7, 8);
            var doubled = CompletableFuture.supplyAsync(
                    () -> ops.mapAll(ExecutionMode.LOOMBUS, items, value -> value * 2));
            var offset = CompletableFuture.supplyAsync(
                    () -> ops.mapAll(ExecutionMode.LOOMBUS, items, value -> value + 100));

            assertEquals(List.of(2, 4, 6, 8, 10, 12, 14, 16),
                    doubled.get(2, TimeUnit.SECONDS));
            assertEquals(List.of(101, 102, 103, 104, 105, 106, 107, 108),
                    offset.get(2, TimeUnit.SECONDS));
            assertEquals(List.of(2, 4, 6, 8, 10, 12, 14, 16),
                    ops.mapAll(ExecutionMode.LOOMBUS, items, value -> value * 2));
        }
    }

    @Test
    void sameHelperRejectsReentrantLoomBusMappingInsteadOfDeadlocking() {
        try (var bus = new LoomBus();
             var ops = new LoomBusOps(bus, new LoomBusOps.Defaults(
                     16, 1, Backpressure.WAIT, java.time.Duration.ofMillis(100)))) {
            var error = assertThrows(java.util.concurrent.CompletionException.class,
                    () -> ops.mapAll(ExecutionMode.LOOMBUS, List.of(1),
                            value -> ops.mapAll(ExecutionMode.LOOMBUS, List.of(value), nested -> nested).get(0)));
            assertInstanceOf(IllegalStateException.class, error.getCause());
            assertTrue(error.getCause().getMessage().contains("Nested LoomBus mapAll"));
        }
    }

    @Test
    void closingOpsOnSharedBusClosesOnlyItsEndpoint() throws Exception {
        try (var bus = new LoomBus()) {
            var ops = new LoomBusOps(bus, new LoomBusOps.Defaults(
                    16, 2, Backpressure.WAIT, java.time.Duration.ofMillis(100)));
            assertEquals(List.of(3, 4), ops.mapAll(ExecutionMode.LOOMBUS, List.of(1, 2), value -> value + 2));
            ops.close();
            assertThrows(IllegalStateException.class,
                    () -> ops.mapAll(ExecutionMode.LOOMBUS, List.of(1), value -> value));
            assertEquals(42, bus.register("still-open", EndpointConfig.stateful(2),
                    (Integer value) -> value).request(42).get(1, TimeUnit.SECONDS));
        }
    }
}