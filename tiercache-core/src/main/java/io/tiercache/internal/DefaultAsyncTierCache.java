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
 * Default {@link AsyncTierCache}: a thin view delegating every operation
 * to the synchronous engine on the factory's shared daemon executor
 * (supplyAsync-style). Adds no coalescing of its own — concurrent calls
 * for one key join the engine's inflight singleflight entry.
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

    /**
     * Creates the async view over a synchronous cache.
     *
     * @param delegate the synchronous engine; must not be {@code null}
     * @param executor the shared executor cache work is offloaded to; must
     *                 not be {@code null}
     * @since 0.3.0
     */
    public DefaultAsyncTierCache(TierCache<K, V> delegate, Executor executor) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public CompletionStage<V> getAsync(K key) {
        return CompletableFuture.supplyAsync(() -> delegate.get(key), executor);
    }

    @Override
    public CompletionStage<LookupResult<V>> lookupAsync(K key) {
        return CompletableFuture.supplyAsync(() -> delegate.lookup(key), executor);
    }

    @Override
    public CompletionStage<V> getOrComputeAsync(K key, Function<? super K, ? extends V> loader) {
        Objects.requireNonNull(loader, "loader");
        return CompletableFuture.supplyAsync(() -> delegate.getOrCompute(key, loader), executor);
    }

    @Override
    public CompletionStage<V> getOrComputeAsyncStage(K key, AsyncLoader<? super K, ? extends V> loader) {
        Objects.requireNonNull(loader, "loader");
        return CompletableFuture.supplyAsync(
                () -> delegate.getOrCompute(key, k -> joinLoader(loader, k)), executor);
    }

    @Override
    public CompletionStage<Void> putAsync(K key, V value) {
        return CompletableFuture.runAsync(() -> delegate.put(key, value), executor);
    }

    @Override
    public CompletionStage<Void> putAsync(K key, V value, String... tags) {
        return CompletableFuture.runAsync(() -> delegate.put(key, value, tags), executor);
    }

    @Override
    public CompletionStage<Boolean> putIfAbsentAsync(K key, V value) {
        return CompletableFuture.supplyAsync(() -> delegate.putIfAbsent(key, value), executor);
    }

    @Override
    public CompletionStage<Void> putNullAsync(K key) {
        return CompletableFuture.runAsync(() -> delegate.putNull(key), executor);
    }

    @Override
    public CompletionStage<Void> evictAsync(K key) {
        return CompletableFuture.runAsync(() -> delegate.evict(key), executor);
    }

    @Override
    public CompletionStage<Void> evictAllAsync() {
        return CompletableFuture.runAsync(delegate::evictAll, executor);
    }

    @Override
    public CompletionStage<Void> evictAllAsync(Collection<K> keys) {
        return CompletableFuture.runAsync(() -> delegate.evictAll(keys), executor);
    }

    @Override
    public CompletionStage<Void> evictByTagAsync(String tag) {
        return CompletableFuture.runAsync(() -> delegate.evictByTag(tag), executor);
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
