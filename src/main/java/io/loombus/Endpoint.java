package io.loombus;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

public final class Endpoint<M, R> implements AutoCloseable {
    private final String name;
    private final EndpointConfig config;
    private final ExecutorService executor;
    private final Function<? super M, ? extends R> handler;
    private final ConcurrentLinkedQueue<Envelope> mailbox = new ConcurrentLinkedQueue<>();
    private final Semaphore capacity;
    private final Semaphore work;
    private final CopyOnWriteArrayList<Consumer<Object>> subscribers = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    Endpoint(String name, EndpointConfig config, ExecutorService executor,
             Function<? super M, ? extends R> handler) {
        this.name = name;
        this.config = config;
        this.executor = executor;
        this.handler = handler;
        this.capacity = new Semaphore(config.capacity());
        this.work = new Semaphore(0);
        for (int i = 0; i < config.concurrency(); i++) {
            executor.submit(this::workerLoop);
        }
    }

    public String name() {
        return name;
    }

    public CompletableFuture<R> request(M message) {
        var future = new CompletableFuture<R>();
        enqueue(new Envelope(message, future, null));
        return future;
    }

    public void publish(Object event) {
        for (var subscriber : subscribers) {
            enqueue(new Envelope(event, null, subscriber));
        }
    }

    public void addSubscriber(Consumer<Object> subscriber) {
        subscribers.add(Objects.requireNonNull(subscriber, "subscriber"));
    }

    private void enqueue(Envelope envelope) {
        if (closed.get()) throw new IllegalStateException("Endpoint is closed: " + name);

        boolean acquired;
        try {
            acquired = switch (config.backpressure()) {
                case WAIT -> {
                    capacity.acquire();
                    yield true;
                }
                case REJECT -> capacity.tryAcquire();
                case TIMEOUT -> capacity.tryAcquire(
                        config.admissionTimeout().toNanos(), TimeUnit.NANOSECONDS);
            };
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.util.concurrent.CancellationException(
                    "Interrupted while waiting for endpoint capacity: " + name);
        }

        if (!acquired) {
            throw new RejectedExecutionException("Endpoint mailbox is full: " + name);
        }
        if (closed.get()) {
            capacity.release();
            throw new IllegalStateException("Endpoint is closed: " + name);
        }

        mailbox.offer(envelope);
        work.release();
    }

    private void workerLoop() {
        while (!closed.get() || work.availablePermits() > 0) {
            try {
                if (!work.tryAcquire(100, TimeUnit.MILLISECONDS)) continue;
                var envelope = mailbox.poll();
                if (envelope == null) continue;

                try {
                    if (envelope.subscriber != null) {
                        envelope.subscriber.accept(envelope.message);
                    } else {
                        @SuppressWarnings("unchecked")
                        M message = (M) envelope.message;
                        @SuppressWarnings("unchecked")
                        CompletableFuture<R> future = (CompletableFuture<R>) envelope.future;
                        future.complete(handler.apply(message));
                    }
                } catch (Throwable error) {
                    if (envelope.future != null) {
                        envelope.future.completeExceptionally(error);
                    } else {
                        Thread.currentThread().getUncaughtExceptionHandler()
                                .uncaughtException(Thread.currentThread(), error);
                    }
                } finally {
                    capacity.release();
                }
            } catch (InterruptedException e) {
                if (closed.get()) return;
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private record Envelope(
            Object message,
            CompletableFuture<?> future,
            Consumer<Object> subscriber) {
    }
}
