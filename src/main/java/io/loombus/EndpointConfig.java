package io.loombus;

import java.time.Duration;

public record EndpointConfig(
        int capacity,
        int concurrency,
        Backpressure backpressure,
        Duration admissionTimeout) {

    public EndpointConfig {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        if (concurrency <= 0) throw new IllegalArgumentException("concurrency must be > 0");
        if (backpressure == null) throw new NullPointerException("backpressure");
        if (backpressure == Backpressure.TIMEOUT
                && (admissionTimeout == null || admissionTimeout.isZero() || admissionTimeout.isNegative())) {
            throw new IllegalArgumentException("admissionTimeout must be > 0 for TIMEOUT");
        }
    }

    public static EndpointConfig stateful(int capacity) {
        return new EndpointConfig(capacity, 1, Backpressure.WAIT, Duration.ZERO);
    }

    public static EndpointConfig concurrent(int capacity, int concurrency) {
        return new EndpointConfig(capacity, concurrency, Backpressure.WAIT, Duration.ZERO);
    }

    public EndpointConfig withBackpressure(Backpressure policy) {
        return new EndpointConfig(capacity, concurrency, policy,
                policy == Backpressure.TIMEOUT ? Duration.ofSeconds(1) : Duration.ZERO);
    }

    public EndpointConfig withBackpressure(Backpressure policy, Duration timeout) {
        return new EndpointConfig(capacity, concurrency, policy, timeout);
    }
}
