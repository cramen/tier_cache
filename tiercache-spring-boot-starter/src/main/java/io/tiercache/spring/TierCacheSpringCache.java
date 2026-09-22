package io.tiercache.spring;

import io.tiercache.AsyncTierCache;
import io.tiercache.LookupResult;
import io.tiercache.TierCache;
import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.cache.support.NullValue;
import org.springframework.cache.support.SimpleValueWrapper;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Spring Cache adapter over a core {@link TierCache}, built on
 * {@link AbstractValueAdaptingCache} so Spring's null-value conventions
 * ({@link NullValue} wrapping/unwrapping at the interceptor level) work as
 * with Spring's own cache managers.
 *
 * <p>Load-bearing mappings: {@link #get(Object, Callable)} delegates to
 * {@code getOrCompute} — singleflight and cluster-wide rebuild
 * coordination apply to synchronous {@code @Cacheable(sync = true)} calls.
 * Both retrieve overloads use the factory-managed async view. Ordinary
 * {@code sync = false} annotations keep Spring's separate read/invoke/write path.
 * Null store values map onto the null-marker policy: stored as a marker under {@code allow},
 * skipped under {@code deny}.
 *
 * <p><strong>Internal:</strong> not part of the supported public API.
 * Instances are created by {@link TierCacheManager}; applications interact
 * with the cache through Spring's
 * {@link org.springframework.cache.Cache} abstraction. It may change in any
 * release without notice.
 *
 * @since 0.1.0
 */
@SuppressWarnings("unchecked")
public class TierCacheSpringCache extends AbstractValueAdaptingCache {

    private final String name;
    private final TierCache<Object, Object> delegate;
    private final AsyncTierCache<Object, Object> async;

    /**
     * Creates a synchronous-only adapter. Both retrieve overloads return failed
     * futures without a managed async view; use TierCacheManager or the constructor
     * accepting both views for asynchronous retrieval.
     *
     * @param name     the cache name exposed to Spring's cache abstraction
     * @param delegate the core two-level cache backing this adapter
     */
    public TierCacheSpringCache(String name, TierCache<?, ?> delegate) {
        this(name, delegate, null);
    }

    /**
     * Creates a fully wired adapter with both views of the same factory cache.
     * The adapter does not own or close either view or their executor.
     */
    public TierCacheSpringCache(String name, TierCache<?, ?> delegate, AsyncTierCache<?, ?> async) {
        super(true); // we convert nulls ourselves (NullValue <-> null-marker)
        this.name = name;
        this.delegate = (TierCache<Object, Object>) delegate;
        this.async = (AsyncTierCache<Object, Object>) async;
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
     * Returns the underlying core cache.
     *
     * @return the core {@link TierCache} backing this adapter
     */
    @Override
    public Object getNativeCache() {
        return delegate;
    }

    /**
     * Performs the honest multilevel lookup: L1, then L2 (warming L1 on a
     * hit). A cached null maps to {@link NullValue#INSTANCE} so Spring's
     * interceptor conventions apply.
     *
     * @param key the key to look up
     * @return the store value (possibly {@link NullValue#INSTANCE} for a
     *         cached null), or {@code null} on a miss
     */
    @Override
    protected Object lookup(Object key) {
        LookupResult<Object> result = delegate.lookup(key);
        if (result instanceof LookupResult.Hit<Object> hit) {
            return hit.value();
        }
        if (result instanceof LookupResult.CachedNull<Object>) {
            return NullValue.INSTANCE;
        }
        return null;
    }

    /**
     * Returns the value for the key, loading and caching it through the core
     * {@code getOrCompute} on a miss — singleflight and cluster-wide rebuild
     * coordination apply, so this is the stamped-protected path
     * {@code @Cacheable(sync = true)} maps onto.
     *
     * @param key         the key whose value is requested
     * @param valueLoader the loader invoked on a cache miss
     * @param <T>         the value type
     * @return the cached or freshly loaded value
     * @throws ValueRetrievalException if the value loader fails; the loader's
     *         original exception is the cause
     */
    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        try {
            Object value = delegate.getOrCompute(key, k -> {
                try {
                    return fromStoreValue(valueLoader.call());
                } catch (Exception e) {
                    throw new LoaderException(e);
                }
            });
            return (T) value;
        } catch (LoaderException e) {
            throw new ValueRetrievalException(key, valueLoader, e.getCause());
        }
    }

    /**
     * Asynchronous cascade lookup. A miss completes with null, a value hit
     * with a wrapper, and a cached null with a non-null wrapper holding null.
     * The factory's bounded async view performs all cache I/O.
     */
    @Override
    public CompletableFuture<ValueWrapper> retrieve(Object key) {
        if (async == null) return missingAsyncView();
        return async.lookupAsync(key).thenApply(result -> {
            if (result instanceof LookupResult.Hit<Object> hit) {
                return (ValueWrapper) new SimpleValueWrapper(hit.value());
            }
            if (result instanceof LookupResult.CachedNull<Object>) {
                return (ValueWrapper) new SimpleValueWrapper(null);
            }
            return null;
        }).toCompletableFuture();
    }

    /** Loads through the same engine claim as other async and synchronous callers. */
    @Override
    public <T> CompletableFuture<T> retrieve(Object key, Supplier<CompletableFuture<T>> valueLoader) {
        if (async == null) return missingAsyncView();
        return async.getOrComputeAsyncStage(key, ignored -> valueLoader.get())
                .thenApply(value -> (T) value).toCompletableFuture();
    }

    private static <T> CompletableFuture<T> missingAsyncView() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "Async retrieval requires a factory-managed AsyncTierCache; use TierCacheManager "
                        + "or the TierCacheSpringCache constructor accepting both cache views"));
    }

    /**
     * Stores the value under the key in both levels. A {@code null} value is
     * routed to the core's null-marker policy (stored as a marker under
     * {@code allow}, skipped under {@code deny}).
     *
     * @param key   the key to store the value under
     * @param value the value to store; {@code null} follows the null policy
     */
    @Override
    public void put(Object key, Object value) {
        Object storeValue = toStoreValue(value); // interceptor passes raw nulls
        if (storeValue == NullValue.INSTANCE) {
            delegate.putNull(key);
        } else {
            delegate.put(key, storeValue);
        }
    }

    /**
     * Atomically stores the value only if the key is absent, backed by the
     * L2 {@code SET NX PX} primitive so the atomicity holds across instances
     * (narrowed in degraded mode, surfaced via the {@code tiercache.degraded}
     * metric).
     *
     * @param key   the key to store the value under
     * @param value the value to store if the key is absent
     * @return {@code null} if the value was stored, or a wrapper around the
     *         existing value if the key was already present
     */
    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        Object storeValue = toStoreValue(value);
        if (storeValue == NullValue.INSTANCE) {
            delegate.putNull(key);
            return null;
        }
        boolean won = delegate.putIfAbsent(key, storeValue);
        return won ? null : toValueWrapper(lookup(key));
    }

    /**
     * Evicts the key from both levels and publishes a cross-instance
     * invalidation event.
     *
     * @param key the key to evict
     */
    @Override
    public void evict(Object key) {
        delegate.evict(key);
    }

    /**
     * Evicts the key if present, in both levels.
     *
     * @param key the key to evict
     * @return {@code true} if an entry (including a cached null) was present
     *         before the eviction
     */
    @Override
    public boolean evictIfPresent(Object key) {
        boolean present = !(delegate.lookup(key) instanceof LookupResult.Miss);
        delegate.evict(key);
        return present;
    }

    /**
     * Evicts all entries of this named cache from both levels (scoped to
     * this cache's L2 namespace) and publishes a cross-instance
     * invalidation event.
     */
    @Override
    public void clear() {
        delegate.evictAll();
    }

    // Tags/batch eviction are programmatic-only for now; the annotation
    // surface does not expose them.

    private static final class LoaderException extends RuntimeException {
        LoaderException(Throwable cause) {
            super(cause);
        }
    }
}
