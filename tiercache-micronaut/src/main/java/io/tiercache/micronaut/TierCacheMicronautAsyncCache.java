package io.tiercache.micronaut;

import io.micronaut.cache.AsyncCache;
import io.micronaut.core.type.Argument;
import io.tiercache.AsyncTierCache;
import io.tiercache.LookupResult;
import io.tiercache.TierCache;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Micronaut {@link AsyncCache} adapter over a core {@link AsyncTierCache}:
 * every operation returns immediately and performs its cache I/O on the
 * factory's shared executor, never on the calling thread.
 *
 * <p>Null mapping matches the sync adapter (design D3): a miss AND a
 * cached-null marker both complete with {@link Optional#empty()}. Note the
 * loader form {@link #get(Object, Argument, Supplier)} takes a synchronous
 * supplier per Micronaut's SPI; it is executed off the calling thread
 * through the engine's singleflight, so concurrent calls for one absent key
 * still share one supplier execution.
 *
 * <p><strong>Internal:</strong> not part of the supported public API.
 * Instances are created by {@link TierCacheMicronautCache#async()};
 * applications interact with the cache through Micronaut's
 * {@link io.micronaut.cache.AsyncCache} abstraction. It may change in any
 * release without notice.
 *
 * @since 1.1.0
 */
@SuppressWarnings("unchecked")
public class TierCacheMicronautAsyncCache implements AsyncCache<TierCache<Object, Object>> {

    private final String name;
    private final TierCache<Object, Object> nativeCache;
    private final AsyncTierCache<Object, Object> delegate;

    /**
     * Creates an adapter over the given async view.
     *
     * @param name        the cache name exposed to Micronaut's cache
     *                    abstraction
     * @param nativeCache the sync core cache, returned by
     *                    {@link #getNativeCache()} for parity with the sync
     *                    adapter
     * @param delegate    the async view of the same underlying cache
     */
    public TierCacheMicronautAsyncCache(String name, TierCache<?, ?> nativeCache,
            AsyncTierCache<?, ?> delegate) {
        this.name = name;
        this.nativeCache = (TierCache<Object, Object>) nativeCache;
        this.delegate = (AsyncTierCache<Object, Object>) delegate;
    }

    /**
     * Returns the cache name.
     *
     * @return the name passed to the constructor
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns the underlying core cache (the same instance the sync adapter
     * exposes); its {@link TierCache#lookup} exposes the tri-state the
     * Micronaut {@code Optional} API cannot express.
     *
     * @return the core {@link TierCache} backing this adapter
     */
    @Override
    public TierCache<Object, Object> getNativeCache() {
        return nativeCache;
    }

    /**
     * Performs the honest multilevel lookup asynchronously: L1, then L2
     * (warming L1 on a hit). A cached null completes as
     * {@link Optional#empty()}, exactly like a miss.
     *
     * @param key          the key to look up
     * @param requiredType the required value type (ignored; values are stored
     *                     and returned as-is)
     * @param <T>          the value type
     * @return a future producing the cached value, or
     *         {@link Optional#empty()} on a miss or a cached null
     */
    @Override
    public <T> CompletableFuture<Optional<T>> get(Object key, Argument<T> requiredType) {
        return delegate.lookupAsync(key)
                .thenApply(result -> result instanceof LookupResult.Hit<Object> hit
                        ? Optional.of((T) hit.value())
                        : Optional.<T>empty())
                .toCompletableFuture();
    }

    /**
     * Returns the value for the key, loading and caching it on a miss
     * through the engine's singleflight on the shared executor.
     *
     * @param key          the key whose value is requested
     * @param requiredType the required value type (ignored)
     * @param supplier     the supplier invoked on a cache miss, off the
     *                     calling thread
     * @param <T>          the value type
     * @return a future producing the cached or freshly loaded value;
     *         {@code null} when the supplier signalled absence
     */
    @Override
    public <T> CompletableFuture<T> get(Object key, Argument<T> requiredType, Supplier<T> supplier) {
        return (CompletableFuture<T>) delegate.getOrComputeAsync(key, k -> supplier.get())
                .toCompletableFuture();
    }

    /**
     * Atomically stores the value only if the key is absent, backed by the
     * L2 {@code SET NX PX} primitive.
     *
     * @param key   the key to store the value under
     * @param value the value to store if the key is absent
     * @param <T>   the value type
     * @return a future producing {@link Optional#empty()} if the value was
     *         stored, or the existing value if the key was already present
     */
    @Override
    public <T> CompletableFuture<Optional<T>> putIfAbsent(Object key, T value) {
        if (value == null) {
            return delegate.putNullAsync(key)
                    .thenApply(done -> Optional.<T>empty())
                    .toCompletableFuture();
        }
        return delegate.putIfAbsentAsync(key, value)
                .thenCompose(won -> won
                        ? CompletableFuture.completedFuture(Optional.<T>empty())
                        : delegate.lookupAsync(key).thenApply(result ->
                                result instanceof LookupResult.Hit<Object> hit
                                        ? Optional.of((T) hit.value())
                                        : Optional.<T>empty()))
                .toCompletableFuture();
    }

    /**
     * Stores the value under the key in both levels. A {@code null} value is
     * routed to the core's null-marker policy.
     *
     * @param key   the key to store the value under
     * @param value the value to store; {@code null} follows the null policy
     * @return a future producing {@code true} when the write is done
     */
    @Override
    public CompletableFuture<Boolean> put(Object key, Object value) {
        CompletionStage<Void> stage = value == null
                ? delegate.putNullAsync(key)
                : delegate.putAsync(key, value);
        return stage.thenApply(done -> Boolean.TRUE).toCompletableFuture();
    }

    /**
     * Evicts the key from both levels and publishes a cross-instance
     * invalidation event.
     *
     * @param key the key to evict
     * @return a future producing {@code true} when the eviction is done
     */
    @Override
    public CompletableFuture<Boolean> invalidate(Object key) {
        return delegate.evictAsync(key).thenApply(done -> Boolean.TRUE).toCompletableFuture();
    }

    /**
     * Evicts all entries of this named cache from both levels (scoped to
     * this cache's L2 namespace) and publishes a cross-instance
     * invalidation event.
     *
     * @return a future producing {@code true} when the eviction is done
     */
    @Override
    public CompletableFuture<Boolean> invalidateAll() {
        return delegate.evictAllAsync().thenApply(done -> Boolean.TRUE).toCompletableFuture();
    }
}
