package io.loombus;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

public final class LoomBus implements AutoCloseable {
    private final ExecutorService executor;
    private final ConcurrentMap<String, Endpoint<?, ?>> endpoints = new ConcurrentHashMap<>();

    public LoomBus() {
        this(Executors.newVirtualThreadPerTaskExecutor());
    }

    public LoomBus(ExecutorService executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public <M, R> Endpoint<M, R> register(
            String name,
            EndpointConfig config,
            Function<? super M, ? extends R> handler) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(handler, "handler");

        var endpoint = new Endpoint<M, R>(name, config, executor, handler);
        if (endpoints.putIfAbsent(name, endpoint) != null) {
            endpoint.close();
            throw new IllegalArgumentException("Endpoint already exists: " + name);
        }
        return endpoint;
    }

    @SuppressWarnings("unchecked")
    public <M, R> CompletableFuture<R> request(String endpointName, M message) {
        var endpoint = (Endpoint<M, R>) requireEndpoint(endpointName);
        return endpoint.request(message);
    }

    public <M, R> R requestAndWait(String endpointName, M message, Duration timeout) throws Exception {
        return this.<M, R>request(endpointName, message)
                .get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public void publish(String endpointName, Object event) {
        requireEndpoint(endpointName).publish(event);
    }

    @SuppressWarnings("unchecked")
    public <E> void subscribe(String endpointName, Consumer<? super E> consumer) {
        var endpoint = (Endpoint<Object, Object>) requireEndpoint(endpointName);
        endpoint.addSubscriber((Consumer<Object>) consumer);
    }

    private Endpoint<?, ?> requireEndpoint(String name) {
        var endpoint = endpoints.get(name);
        if (endpoint == null) throw new IllegalArgumentException("Unknown endpoint: " + name);
        return endpoint;
    }

    @Override
    public void close() {
        endpoints.values().forEach(Endpoint::close);
        executor.close();
    }
}
