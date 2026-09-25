package io.tiercache.internal;

import io.tiercache.AsyncTierCache;
import io.tiercache.LookupResult;
import io.tiercache.TierCache;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Default {@link AsyncTierCache}: cache work runs on the factory's shared
 * daemon executor. A coalesced follower subscribes to the engine's existing
 * claim and releases its worker; load owners remain blocking. Retained tasks
 * and attachments share one bounded factory admission budget.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @param <K> key type
 * @param <V> value type
 * @since 0.3.0
 */
public final class DefaultAsyncTierCache<K, V> implements AsyncTierCache<K, V> {

    private final TierCache<K, V> delegate;
    private final Executor executor;
    private final AsyncAdmission admission;
    private final java.util.function.BooleanSupplier factoryOpen;
    /**
     * Single ordering point for the submission path and {@link
     * #closeOutstanding()}: the closed check, registry insertion and task
     * submission happen inside it, so a close landing anywhere in the
     * submit path either rejects the submission or drains the registered
     * stage — a stage is never abandoned. Stages are completed outside
     * this lock because their callbacks may run user code.
     */
    private final Object lifecycleLock = new Object();
    /** Stages handed to callers and not yet completed; drained on factory close. */
    private final java.util.Set<CompletableFuture<?>> outstanding =
            java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** Set under {@link #lifecycleLock} by {@link #closeOutstanding()}. */
    private boolean closed;

    /**
     * Creates the async view over a synchronous cache.
     *
     * @param delegate the synchronous engine; must not be {@code null}
     * @param executor the shared executor cache work is offloaded to; must
     *                 not be {@code null}
     * @since 0.3.0
     */
    public DefaultAsyncTierCache(TierCache<K, V> delegate, Executor executor) {
        this(delegate, executor, new AsyncAdmission(Math.max(4,
                Runtime.getRuntime().availableProcessors())), () -> true);
    }

    /** Internal factory wiring: all views share physical resource accounting. */
    public DefaultAsyncTierCache(TierCache<K, V> delegate, Executor executor,
            AsyncAdmission admission, java.util.function.BooleanSupplier factoryOpen) {
        this.admission = Objects.requireNonNull(admission, "admission");
        this.factoryOpen = Objects.requireNonNull(factoryOpen, "factoryOpen");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    /**
     * Fails every stage not yet completed with
     * {@link java.util.concurrent.CancellationException}, so callers never
     * hang on a factory close. Called by the owning factory during
     * {@code close()}; already-completed stages are unaffected.
     *
     * <p>The closed transition and the registry snapshot happen under
     * {@link #lifecycleLock} — after it, no new stage can register, so the
     * snapshot is complete. Stages are failed outside the lock because
     * their callbacks may run user code.
     *
     * <p>Internal lifecycle hook — not for application use.
     */
    public void closeOutstanding() {
        java.util.List<CompletableFuture<?>> drain;
        synchronized (lifecycleLock) {
            closed = true;
            drain = new java.util.ArrayList<>(outstanding);
            outstanding.clear();
        }
        java.util.concurrent.CancellationException cancellation =
                new java.util.concurrent.CancellationException("TierCacheFactory closed");
        // A stage completed between the snapshot and this loop is
        // unaffected: completeExceptionally is a no-op on it.
        drain.forEach(future -> future.completeExceptionally(cancellation));
    }

    @Override
    public CompletionStage<V> getAsync(K key) {
        return supply(() -> delegate.get(key));
    }

    @Override
    public CompletionStage<LookupResult<V>> lookupAsync(K key) {
        return supply(() -> delegate.lookup(key));
    }

    @Override
    public CompletionStage<V> getOrComputeAsync(K key, Function<? super K, ? extends V> loader) {
        Objects.requireNonNull(loader, "loader");
        return compute(key, loader);
    }

    @Override
    public CompletionStage<V> getOrComputeAsyncStage(K key, AsyncLoader<? super K, ? extends V> loader) {
        Objects.requireNonNull(loader, "loader");
        return compute(key, k -> joinLoader(loader, k));
    }

    @Override
    public CompletionStage<Void> putAsync(K key, V value) {
        return run(() -> delegate.put(key, value));
    }

    @Override
    public CompletionStage<Void> putAsync(K key, V value, String... tags) {
        return run(() -> delegate.put(key, value, tags));
    }

    @Override
    public CompletionStage<Boolean> putIfAbsentAsync(K key, V value) {
        return supply(() -> delegate.putIfAbsent(key, value));
    }

    @Override
    public CompletionStage<Void> putNullAsync(K key) {
        return run(() -> delegate.putNull(key));
    }

    @Override
    public CompletionStage<Void> evictAsync(K key) {
        return run(() -> delegate.evict(key));
    }

    @Override
    public CompletionStage<Void> evictAllAsync() {
        return run(delegate::evictAll);
    }

    @Override
    public CompletionStage<Void> evictAllAsync(Collection<K> keys) {
        return run(() -> delegate.evictAll(keys));
    }

    @Override
    public CompletionStage<Void> evictByTagAsync(String tag) {
        return run(() -> delegate.evictByTag(tag));
    }

    /**
     * Offloads a value-producing task to the shared executor. A saturated
     * executor surfaces as a failed stage ({@link
     * java.util.concurrent.RejectedExecutionException}) instead of a
     * synchronous throw on the caller's thread; a submission after close
     * surfaces as a stage failed with
     * {@link java.util.concurrent.CancellationException}.
     *
     * <p>The closed check, registry insertion and the executor hand-off
     * are one atomic step under {@link #lifecycleLock}, so a close landing
     * anywhere in this path either rejects the submission here or completes
     * the registered stage in its drain — never neither.
     */
    private <T> CompletionStage<T> supply(java.util.function.Supplier<T> task) {
        return submit(operation -> task.get());
    }

    private CompletionStage<V> compute(K key, Function<? super K, ? extends V> loader) {
        return submit(operation -> {
            if (delegate instanceof DefaultTierCache<K, V> engine) {
                return engine.getOrComputeDispatch(key, loader, operation::attach);
            }
            return delegate.getOrCompute(key, loader);
        });
    }

    private <T> CompletionStage<T> submit(Function<Operation<T>, T> task) {
        Operation<T> operation;
        java.util.concurrent.RejectedExecutionException rejected = null;
        synchronized (lifecycleLock) {
            if (closed) {
                return CompletableFuture.failedFuture(
                        new java.util.concurrent.CancellationException("TierCacheFactory closed"));
            }
            if (!admission.acquire()) {
                return CompletableFuture.failedFuture(new java.util.concurrent.RejectedExecutionException(
                        "TierCacheFactory retained async resource capacity exhausted"));
            }
            operation = new Operation<>(task);
            outstanding.add(operation.future);
            operation.future.whenComplete((value, error) -> outstanding.remove(operation.future));
            try {
                executor.execute(operation);
            } catch (java.util.concurrent.RejectedExecutionException e) {
                rejected = e;
            }
        }
        if (rejected != null) {
            try { operation.future.completeExceptionally(rejected); }
            finally { operation.discard(); }
        }
        return operation.future;
    }

    /** Task and attachment own independent references; outer completion owns none. */
    private final class Operation<T> implements DefaultTierCache.DiscardableTask {
        private final CompletableFuture<T> future = new CompletableFuture<>();
        private final java.util.concurrent.atomic.AtomicInteger owners =
                new java.util.concurrent.atomic.AtomicInteger(1);
        private final java.util.concurrent.atomic.AtomicBoolean claimed =
                new java.util.concurrent.atomic.AtomicBoolean();
        private Function<Operation<T>, T> task;
        private boolean attached;

        Operation(Function<Operation<T>, T> task) { this.task = task; }

        @Override public void run() {
            if (!claimed.compareAndSet(false, true)) return;
            try {
                T value = task.apply(this);
                if (!attached) future.complete(value);
            } catch (Throwable error) {
                future.completeExceptionally(error);
            } finally {
                task = null;
                retire();
            }
        }

        void attach(LoadClaim<K, T> claim) {
            attached = true;
            owners.incrementAndGet(); // Before registration: completion can be inline.
            claim.result.whenComplete((outcome, error) -> {
                try {
                    if (error != null) {
                        // Match CompletableFuture.join on the synchronous follower path.
                        future.completeExceptionally(error instanceof CompletionException
                                || error instanceof java.util.concurrent.CancellationException
                                ? error : new CompletionException(error));
                    } else if (outcome.isSkipped() && !factoryOpen.getAsBoolean()) {
                        future.completeExceptionally(new java.util.concurrent.CancellationException(
                                "TierCacheFactory closed"));
                    } else {
                        var entry = outcome.resultEntry();
                        future.complete(entry == null || entry.isNullMarker() ? null : entry.value());
                    }
                } catch (Throwable failure) {
                    future.completeExceptionally(failure);
                } finally { retire(); }
            });
        }

        @Override public void discard() {
            if (claimed.compareAndSet(false, true)) {
                task = null;
                retire();
            }
        }

        private void retire() {
            if (owners.decrementAndGet() == 0) admission.release();
        }
    }

    /**
     * Offloads a void task to the shared executor; rejection semantics
     * match {@link #supply(java.util.function.Supplier)}.
     */
    private CompletionStage<Void> run(Runnable task) {
        return supply(() -> {
            task.run();
            return null;
        });
    }

    /**
     * Runs the async loader and blocks the executor thread on its stage
     * (the documented D2 contract), unwrapping the failure so a stage
     * completed exceptionally reaches callers exactly like a synchronous
     * loader throwing the same cause.
     */
    private static <K, V> V joinLoader(AsyncLoader<? super K, ? extends V> loader, K key) {
        try {
            return loader.load(key).toCompletableFuture().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            if (e.getCause() instanceof Error cause) {
                throw cause;
            }
            throw e;
        }
    }
}
