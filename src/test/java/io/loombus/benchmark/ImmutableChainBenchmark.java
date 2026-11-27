package io.loombus.benchmark;

import io.loombus.Backpressure;
import io.loombus.ExecutionMode;
import io.loombus.LoomBus;
import io.loombus.LoomBusOps;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

/** Benchmark of immutable inputs through a three-stage independent chain. Not JMH. */
public final class ImmutableChainBenchmark {
    private static final int WARMUP = 2;
    private static final int MEASURED = 5;
    private static final int ITEMS = 3_000;

    private ImmutableChainBenchmark() {}

    public static void main(String[] args) {
        System.out.println("=== Immutable-state chained multiplexing benchmark ===");
        System.out.println("Runtime: " + Runtime.version());
        System.out.println("Processors: " + Runtime.getRuntime().availableProcessors());
        System.out.printf("Items: %d | warmup rounds: %d | measured rounds: %d%n",
                ITEMS, WARMUP, MEASURED);
        System.out.println("Input/state: immutable records; independent items");
        var inputs = java.util.stream.IntStream.range(0, ITEMS)
                .mapToObj(i -> new State(i, i + 17L)).toList();

        var expected = sequential(inputs);
        requireEqual(expected, parallelStreams(inputs), "parallel stream preflight");
        requireEqual(expected, loomBus(inputs), "LoomBus preflight");
        System.out.println("Correctness preflight: identical outputs across all strategies.");

        benchmark(inputs);
    }

    private static void benchmark(List<State> inputs) {
        var results = new EnumMap<Strategy, List<Long>>(Strategy.class);
        for (var strategy : Strategy.values()) results.put(strategy, new ArrayList<>());
        try (var bus = new LoomBus();
             var ops = new LoomBusOps(bus, defaults())) {
            for (int round = 0; round < WARMUP + MEASURED; round++) {
                var order = switch (round % 3) {
                    case 0 -> List.of(Strategy.SEQUENTIAL, Strategy.PARALLEL_STREAM, Strategy.LOOMBUS);
                    case 1 -> List.of(Strategy.PARALLEL_STREAM, Strategy.LOOMBUS, Strategy.SEQUENTIAL);
                    default -> List.of(Strategy.LOOMBUS, Strategy.SEQUENTIAL, Strategy.PARALLEL_STREAM);
                };
                for (var strategy : order) {
                    long start = System.nanoTime();
                    var output = switch (strategy) {
                        case SEQUENTIAL -> sequential(inputs);
                        case PARALLEL_STREAM -> parallelStreams(inputs);
                        case LOOMBUS -> loomBus(ops, inputs);
                    };
                    long elapsed = System.nanoTime() - start;
                    if (round >= WARMUP) {
                        results.get(strategy).add(elapsed);
                        System.out.printf("round=%d strategy=%s elapsed=%.3fms throughput=%.0f items/s checksum=%d%n",
                                round - WARMUP + 1, strategy, elapsed / 1e6,
                                inputs.size() / (elapsed / 1e9), checksum(output));
                    } else {
                        System.out.printf("warmup=%d strategy=%s elapsed=%.3fms%n",
                                round + 1, strategy, elapsed / 1e6);
                    }
                }
            }
        }
        System.out.println("--- Median of measured rounds ---");
        for (var strategy : Strategy.values()) {
            double medianMs = median(results.get(strategy)) / 1e6;
            System.out.printf("%s median=%.3fms throughput=%.0f items/s%n",
                    strategy, medianMs, inputs.size() / (medianMs / 1_000.0));
        }
    }

    private static LoomBusOps.Defaults defaults() {
        return new LoomBusOps.Defaults(8_192,
                Math.max(4, Math.min(32, Runtime.getRuntime().availableProcessors() * 2)),
                Backpressure.WAIT, Duration.ofSeconds(5));
    }

    private static List<State> sequential(List<State> inputs) {
        var output = new ArrayList<State>(inputs.size());
        for (State s : inputs) output.add(stage3(stage2(stage1(s))));
        return List.copyOf(output);
    }

    private static List<State> parallelStreams(List<State> inputs) {
        return inputs.parallelStream().map(ImmutableChainBenchmark::stage1)
                .map(ImmutableChainBenchmark::stage2)
                .map(ImmutableChainBenchmark::stage3).toList();
    }

    private static List<State> loomBus(List<State> inputs) {
        try (var bus = new LoomBus(); var ops = new LoomBusOps(bus, defaults())) {
            return loomBus(ops, inputs);
        }
    }

    private static List<State> loomBus(LoomBusOps ops, List<State> inputs) {
        var a = ops.mapAll(ExecutionMode.LOOMBUS, inputs, ImmutableChainBenchmark::stage1);
        var b = ops.mapAll(ExecutionMode.LOOMBUS, a, ImmutableChainBenchmark::stage2);
        return ops.mapAll(ExecutionMode.LOOMBUS, b, ImmutableChainBenchmark::stage3);
    }

    private static State stage1(State input) {
        return new State(input.id(), compute(input.value(), 90, 13));
    }

    private static State stage2(State input) {
        return new State(input.id(), compute(input.value(), 70, 7));
    }

    private static State stage3(State input) {
        return new State(input.id(), compute(input.value(), 50, 3));
    }

    private static long compute(long value, int rounds, int salt) {
        long x = value ^ salt;
        for (int i = 0; i < rounds; i++)
            x = (x * 0x9E3779B97F4A7C15L + i + salt) ^ (x >>> 11);
        return x;
    }

    private static void requireEqual(List<State> expected, List<State> actual, String label) {
        if (!expected.equals(actual)) throw new IllegalStateException(label + " mismatch");
    }

    private static long checksum(List<State> states) {
        long sum = 0;
        for (State s : states) sum = sum * 31 + s.value();
        return sum;
    }

    private static long median(List<Long> values) {
        var sorted = values.stream().mapToLong(Long::longValue).sorted().toArray();
        if (sorted.length % 2 == 1) return sorted[sorted.length / 2];
        return (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2;
    }

    private enum Strategy { SEQUENTIAL, PARALLEL_STREAM, LOOMBUS }
    private record State(int id, long value) {}
}
