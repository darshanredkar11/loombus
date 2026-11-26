package io.loombus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Side-by-side execution helper for Java parallel streams and LoomBus.
 * LoomBus uses one reusable endpoint per helper instance, not a new endpoint
 * for every mapAll invocation.
 */
public final class LoomBusOps implements AutoCloseable {
    public record Defaults(
            int capacity,
            int concurrency,
            Backpressure backpressure,
            Duration admissionTimeout) {
        public Defaults {
            if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
            if (concurrency <= 0) throw new IllegalArgumentException("concurrency must be > 0");
            Objects.requireNonNull(backpressure, "backpressure");
            if (backpressure == Backpressure.TIMEOUT
                    && (admissionTimeout == null || admissionTimeout.isZero() || admissionTimeout.isNegative())) {
                throw new IllegalArgumentException("admissionTimeout must be > 0 for TIMEOUT");
            }
        }

        public static Defaults standard() {
            int cores = Math.max(4, Runtime.getRuntime().availableProcessors());
            return new Defaults(4_096, cores * 8, Backpressure.WAIT, Duration.ofMillis(250));
        }

        public EndpointConfig toEndpointConfig() {
            EndpointConfig config = EndpointConfig.concurrent(capacity, concurrency);
            return backpressure == Backpressure.TIMEOUT
                    ? config.withBackpressure(backpressure, admissionTimeout)
                    : config.withBackpressure(backpressure);
        }
    }

    private final LoomBus bus;
    private final Defaults defaults;
    private final boolean ownsBus;
    private final ThreadLocal<Boolean> executingMapper = ThreadLocal.withInitial(() -> false);
    private final Endpoint<WorkItem, Object> endpoint;

    public LoomBusOps() {
        this(new LoomBus(), Defaults.standard(), true);
    }

    public LoomBusOps(LoomBus bus) {
        this(bus, Defaults.standard(), false);
    }

    public LoomBusOps(LoomBus bus, Defaults defaults) {
        this(bus, defaults, false);
    }

    private LoomBusOps(LoomBus bus, Defaults defaults, boolean ownsBus) {
        this.bus = Objects.requireNonNull(bus, "bus");
        this.defaults = Objects.requireNonNull(defaults, "defaults");
        this.ownsBus = ownsBus;
        this.endpoint = bus.register("ops." + UUID.randomUUID(), defaults.toEndpointConfig(),
                work -> work.mapper().apply(work.item()));
    }

    public Defaults defaults() {
        return defaults;
    }

    public <T, R> List<R> mapAll(
            ExecutionMode mode,
            List<T> items,
            Function<? super T, ? extends R> mapper) {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(items, "items");
        Objects.requireNonNull(mapper, "mapper");
        if (items.isEmpty()) return List.of();

        return switch (mode) {
            case PARALLEL_STREAM -> {
                List<R> mapped = new ArrayList<>(items.size());
                items.parallelStream().map(mapper::apply).forEachOrdered(mapped::add);
                yield mapped;
            }
            case LOOMBUS -> mapViaEndpoint(items, mapper);
        };
    }

    public <T> void forEachAll(ExecutionMode mode, List<T> items, Consumer<? super T> action) {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(items, "items");
        Objects.requireNonNull(action, "action");
        if (items.isEmpty()) return;

        switch (mode) {
            case PARALLEL_STREAM -> items.parallelStream().forEach(action);
            case LOOMBUS -> mapViaEndpoint(items, item -> {
                action.accept(item);
                return null;
            });
        }
    }

    @SuppressWarnings("unchecked")
    private <T, R> List<R> mapViaEndpoint(
            List<T> items,
            Function<? super T, ? extends R> mapper) {
        if (executingMapper.get()) {
            throw new IllegalStateException("Nested LoomBus mapAll on the same LoomBusOps instance is not supported");
        }
        Function<Object, Object> erasedMapper = value -> {
            executingMapper.set(true);
            try {
                return mapper.apply((T) value);
            } finally {
                executingMapper.remove();
            }
        };
        List<CompletableFuture<Object>> futures = new ArrayList<>(items.size());
        Throwable admissionFailure = null;

        for (T item : items) {
            try {
                futures.add(endpoint.request(new WorkItem(item, erasedMapper)));
            } catch (RuntimeException | Error error) {
                admissionFailure = error;
                break;
            }
        }

        if (admissionFailure != null) {
            // Already admitted work is not cancelled by caller abandonment.
            // Wait for its terminal processing before returning an admission error.
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                    .handle((ignored, failure) -> null)
                    .join();
            if (admissionFailure instanceof RuntimeException runtime) throw runtime;
            throw (Error) admissionFailure;
        }

        CompletableFuture<Void> allDone = CompletableFuture.allOf(
                futures.toArray(CompletableFuture[]::new));
        try {
            allDone.join();
        } catch (CompletionException failure) {
            throw failure;
        }

        List<R> results = new ArrayList<>(futures.size());
        for (var future : futures) results.add((R) future.join());
        return results;
    }

    @Override
    public void close() {
        if (ownsBus) {
            bus.close();
        } else {
            endpoint.close();
        }
    }

    private record WorkItem(Object item, Function<Object, Object> mapper) {}
}
