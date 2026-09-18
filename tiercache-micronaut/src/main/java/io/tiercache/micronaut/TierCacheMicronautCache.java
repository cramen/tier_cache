package io.tiercache.micronaut;

import io.micronaut.cache.AsyncCache;
import io.micronaut.cache.SyncCache;
import io.micronaut.core.type.Argument;
import io.tiercache.LookupResult;
import io.tiercache.TierCache;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Micronaut {@link SyncCache} adapter over a core {@link TierCache}.
 *
 * <p>Load-bearing mappings: {@link #get(Object, Argument, Supplier)}
 * delegates to {@code getOrCompute} — singleflight and cluster-wide rebuild
 * coordination apply under {@code @Cacheable} by construction. Null mapping
 * at the Micronaut boundary (design D3): an engine hit with a real value
 * maps to {@code Optional.of(value)}; an engine miss AND a cached-null
 * marker both map to {@link Optional#empty()}. The tri-state distinction
 * (miss / cached-null / hit) stays available through
 * {@link #getNativeCache()} and {@link TierCache#lookup}. A {@code null}
 * value passed to {@link #put} or {@link #putIfAbsent} is routed to the
 * core's null-marker policy (stored as a marker under {@code allow},
 * skipped under {@code deny}).
 *
 * <p>{@link #async()} returns the true non-blocking view backed by the
 * engine's {@link io.tiercache.AsyncTierCache} — not the default blocking
 * wrapper.
 *
 * <p><strong>Internal:</strong> not part of the supported public API.
 * Instances are created by {@link TierCacheMicronautManager}; applications
 * interact with the cache through Micronaut's
 * {@link io.micronaut.cache.SyncCache} abstraction. It may change in any
 * release without notice.
 *
 * @since 1.1.0
 */
@SuppressWarnings("unchecked")
public class TierCacheMicronautCache implements SyncCache<TierCache<Object, Object>> {

    private final String name;
    private final TierCache<Object, Object> delegate;
    private final AsyncCache<TierCache<Object, Object>> async;

    /**
     * Creates an adapter over the given core cache.
     *
     * @param name     the cache name exposed to Micronaut's cache abstraction
     * @param delegate the core two-level cache backing this adapter
     * @param async    the async view of the same underlying cache
     */
    public TierCacheMicronautCache(String name, TierCache<?, ?> delegate,
            io.tiercache.AsyncTierCache<?, ?> async) {
        this.name = name;
        this.delegate = (TierCache<Object, Object>) delegate;
        this.async = new TierCacheMicronautAsyncCache(name, this.delegate,
                (io.tiercache.AsyncTierCache<Object, Object>) async);
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
     * Returns the underlying core cache; its {@link TierCache#lookup}
     * exposes the tri-state (miss / cached-null / hit) that the Micronaut
     * {@code Optional} API cannot express.
     *
     * @return the core {@link TierCache} backing this adapter
     */
    @Override
    public TierCache<Object, Object> getNativeCache() {
        return delegate;
    }

    /**
     * Performs the honest multilevel lookup: L1, then L2 (warming L1 on a
     * hit). A cached null maps to {@link Optional#empty()}, exactly like a
     * miss — the distinction is available via {@link #getNativeCache()}.
     *
     * @param key          the key to look up
     * @param requiredType the required value type (ignored; values are stored
     *                     and returned as-is)
     * @param <T>          the value type
     * @return the cached value, or {@link Optional#empty()} on a miss or a
     *         cached null
     */
    @Override
    public <T> Optional<T> get(Object key, Argument<T> requiredType) {
        LookupResult<Object> result = delegate.lookup(key);
        if (result instanceof LookupResult.Hit<Object> hit) {
            return Optional.of((T) hit.value());
        }
        return Optional.empty();
    }

    /**
     * Returns the value for the key, loading and caching it through the core
     * {@code getOrCompute} on a miss — singleflight and cluster-wide rebuild
     * coordination apply, so concurrent calls for one absent key share a
     * single supplier execution.
     *
     * @param key          the key whose value is requested
     * @param requiredType the required value type (ignored)
     * @param supplier     the supplier invoked on a cache miss
     * @param <T>          the value type
     * @return the cached or freshly loaded value; {@code null} when the
     *         supplier signalled absence (a cached-null marker likewise
     *         surfaces as {@code null} here, with the supplier suppressed
     *         until the marker expires)
     */
    @Override
    public <T> T get(Object key, Argument<T> requiredType, Supplier<T> supplier) {
        return (T) delegate.getOrCompute(key, k -> supplier.get());
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
        if (value == null) {
            delegate.putNull(key);
        } else {
            delegate.put(key, value);
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
     * @param <T>   the value type
     * @return {@link Optional#empty()} if the value was stored, or the
     *         existing value if the key was already present
     */
    @Override
    public <T> Optional<T> putIfAbsent(Object key, T value) {
        if (value == null) {
            delegate.putNull(key);
            return Optional.empty();
        }
        boolean won = delegate.putIfAbsent(key, value);
        if (won) {
            return Optional.empty();
        }
        LookupResult<Object> result = delegate.lookup(key);
        if (result instanceof LookupResult.Hit<Object> hit) {
            return Optional.of((T) hit.value());
        }
        return Optional.empty();
    }

    /**
     * Evicts the key from both levels and publishes a cross-instance
     * invalidation event.
     *
     * @param key the key to evict
     */
    @Override
    public void invalidate(Object key) {
        delegate.evict(key);
    }

    /**
     * Evicts all entries of this named cache from both levels (scoped to
     * this cache's L2 namespace) and publishes a cross-instance
     * invalidation event.
     */
    @Override
    public void invalidateAll() {
        delegate.evictAll();
    }

    /**
     * Returns the non-blocking async view of this cache, backed by the
     * engine's {@link io.tiercache.AsyncTierCache}.
     *
     * @return the async adapter over the same underlying cache
     */
    @Override
    public AsyncCache<TierCache<Object, Object>> async() {
        return async;
    }
}
