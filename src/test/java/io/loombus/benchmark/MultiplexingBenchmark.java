package io.loombus.benchmark;

import io.loombus.Backpressure;
import io.loombus.ExecutionMode;
import io.loombus.LoomBus;
import io.loombus.LoomBusOps;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

/**
 * Repeatable, lightweight workload comparison (not a substitute for JMH).
 * Target workloads are blocking fan-out and a scarce downstream pool;
 * pure CPU is a control case only.
 */
public final class MultiplexingBenchmark {
    private static final int WARMUP_ROUNDS = 1;
    private static final int MEASURED_ROUNDS = 3;

    private MultiplexingBenchmark() {}

    public static void main(String[] args) {
        System.out.println("=== LoomBus vs ParallelStream Workload Benchmark ===");
        System.out.println("Runtime: " + Runtime.version());
        System.out.println("Available processors: " + Runtime.getRuntime().availableProcessors());
        System.out.println("Warmups: " + WARMUP_ROUNDS + ", measured rounds: " + MEASURED_ROUNDS);
        benchmarkCpuBound();
        benchmarkBlockingIo();
        benchmarkScarceDownstreamResource();
    }

    private static void benchmarkCpuBound() {
        System.out.println("\n--- CPU control (100,000 transforms) ---");
        benchmark("CPU control", 100_000, () -> MultiplexingBenchmark::cpuWork);
    }

    private static void benchmarkBlockingIo() {
        System.out.println("\n--- Blocking I/O fan-out (1,000 items, 15 ms simulated I/O) ---");
        benchmark("Blocking I/O", 1_000, () -> value -> {
            sleepQuietly(15);
            return value * 2L;
        });
    }

    private static void benchmarkScarceDownstreamResource() {
        System.out.println("\n--- Scarce downstream pool (500 items, 16 slots, 10 ms service) ---");
        benchmark("Downstream pool", 500, () -> {
            var dbSlots = new Semaphore(16);
            return value -> {
                dbSlots.acquireUninterruptibly();
                try {
                    sleepQuietly(10);
                    return value + 1L;
                } finally {
                    dbSlots.release();
                }
            };
        });
    }

    private static void benchmark(String label, int count, WorkFactory workFactory) {
        var measured = new java.util.EnumMap<ExecutionMode, List<Round>>(ExecutionMode.class);
        for (var mode : ExecutionMode.values()) measured.put(mode, new ArrayList<>());

        try (var bus = new LoomBus();
             var ops = new LoomBusOps(bus, new LoomBusOps.Defaults(
                     4_096, Math.max(8, Runtime.getRuntime().availableProcessors() * 8),
                     Backpressure.WAIT, Duration.ofSeconds(5)))) {
            for (int round = 0; round < WARMUP_ROUNDS + MEASURED_ROUNDS; round++) {
                var order = round % 2 == 0
                        ? List.of(ExecutionMode.PARALLEL_STREAM, ExecutionMode.LOOMBUS)
                        : List.of(ExecutionMode.LOOMBUS, ExecutionMode.PARALLEL_STREAM);
                for (var mode : order) {
                    var result = runRound(ops, mode, count, workFactory.create());
                    if (round < WARMUP_ROUNDS) {
                        System.out.printf("  warmup %-16s count=%d%n", mode, result.count());
                    } else {
                        measured.get(mode).add(result);
                        System.out.printf("  round %d %-16s count=%d elapsed=%.3f ms throughput=%.1f ops/s "
                                        + "latency p50/p95/p99=%.3f/%.3f/%.3f ms%n",
                                round - WARMUP_ROUNDS + 1, mode, result.count(),
                                result.elapsedNanos() / 1_000_000.0, result.opsPerSecond(),
                                result.p50Millis(), result.p95Millis(), result.p99Millis());
                    }
                }
            }
        }

        System.out.println("  " + label + " summary (median of " + MEASURED_ROUNDS + " measured rounds):");
        for (var mode : ExecutionMode.values()) printSummary(mode, measured.get(mode));
    }

    private static Round runRound(LoomBusOps ops, ExecutionMode mode, int count, IntFunction<Long> work) {
        long submittedAt = System.nanoTime();
        var inputs = IntStream.range(0, count)
                .mapToObj(value -> new TimedInput(value, submittedAt))
                .toList();
        var latencies = new ConcurrentLinkedQueue<Long>();
        long start = System.nanoTime();
        var output = ops.mapAll(mode, inputs, input -> {
            long value = work.apply(input.value());
            latencies.add(System.nanoTime() - input.submittedAtNanos());
            return value;
        });
        long elapsed = System.nanoTime() - start;
        if (output.size() != count) throw new IllegalStateException("Incomplete round: " + output.size() + "/" + count);
        var samples = latencies.stream().mapToLong(Long::longValue).toArray();
        if (samples.length != count) throw new IllegalStateException("Missing latency samples: " + samples.length + "/" + count);
        Arrays.sort(samples);
        double opsPerSecond = count / (elapsed / 1_000_000_000.0);
        return new Round(elapsed, count, opsPerSecond,
                percentileMillis(samples, 0.50), percentileMillis(samples, 0.95), percentileMillis(samples, 0.99));
    }

    private static void printSummary(ExecutionMode mode, List<Round> rounds) {
        System.out.printf("  %-16s median elapsed=%.3f ms throughput=%.1f ops/s latency p50/p95/p99=%.3f/%.3f/%.3f ms%n",
                mode, median(rounds.stream().mapToLong(Round::elapsedNanos).toArray()) / 1_000_000.0,
                median(rounds.stream().mapToDouble(Round::opsPerSecond).toArray()),
                median(rounds.stream().mapToDouble(Round::p50Millis).toArray()),
                median(rounds.stream().mapToDouble(Round::p95Millis).toArray()),
                median(rounds.stream().mapToDouble(Round::p99Millis).toArray()));
    }

    private static double percentileMillis(long[] sortedSamples, double percentile) {
        int index = Math.max(0, Math.min(sortedSamples.length - 1,
                (int) Math.ceil(percentile * sortedSamples.length) - 1));
        return sortedSamples[index] / 1_000_000.0;
    }

    private static double median(long[] values) {
        Arrays.sort(values);
        if (values.length % 2 == 1) return values[values.length / 2];
        return (values[values.length / 2 - 1] + values[values.length / 2]) / 2.0;
    }

    private static double median(double[] values) {
        Arrays.sort(values);
        if (values.length % 2 == 1) return values[values.length / 2];
        return (values[values.length / 2 - 1] + values[values.length / 2]) / 2.0;
    }

    private static long cpuWork(int input) {
        long value = input;
        for (int i = 0; i < 200; i++) value = (value * 31L + i) ^ (value >>> 3);
        return value;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface WorkFactory {
        IntFunction<Long> create();
    }

    private record TimedInput(int value, long submittedAtNanos) {}
    private record Round(long elapsedNanos, int count, double opsPerSecond,
                        double p50Millis, double p95Millis, double p99Millis) {}
}
