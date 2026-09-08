package io.tiercache.spring;

import io.tiercache.LookupResult;
import io.tiercache.TierCache;
import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.cache.support.NullValue;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Spring Cache adapter over a core {@link TierCache}, built on
 * {@link AbstractValueAdaptingCache} so Spring's null-value conventions
 * ({@link NullValue} wrapping/unwrapping at the interceptor level) work as
 * with Spring's own cache managers.
 *
 * <p>Load-bearing mappings: {@link #get(Object, Callable)} delegates to
 * {@code getOrCompute} — singleflight and cluster-wide rebuild
 * coordination apply under annotations by construction. {@link #retrieve(Object)}
 * implements the honest multilevel cascade. Null store values map onto the
 * null-marker policy: stored as a marker under {@code allow},
 * skipped under {@code deny}.
 */
@SuppressWarnings("unchecked")
public class TierCacheSpringCache extends AbstractValueAdaptingCache {

    private final String name;
    private final TierCache<Object, Object> delegate;

    public TierCacheSpringCache(String name, TierCache<?, ?> delegate) {
        super(true); // we convert nulls ourselves (NullValue <-> null-marker)
        this.name = name;
        this.delegate = (TierCache<Object, Object>) delegate;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getNativeCache() {
        return delegate;
    }

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

    @Override
    public CompletableFuture<ValueWrapper> retrieve(Object key) {
        // Sync core for now; the future completes immediately with the
        // honest cascade result (L1 -> L2 with warm-up -> empty).
        Object storeValue = lookup(key);
        return CompletableFuture.completedFuture(
                storeValue != null ? toValueWrapper(storeValue) : null);
    }

    @Override
    public void put(Object key, Object value) {
        Object storeValue = toStoreValue(value); // interceptor passes raw nulls
        if (storeValue == NullValue.INSTANCE) {
            delegate.putNull(key);
        } else {
            delegate.put(key, storeValue);
        }
    }

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

    @Override
    public void evict(Object key) {
        delegate.evict(key);
    }

    @Override
    public boolean evictIfPresent(Object key) {
        boolean present = !(delegate.lookup(key) instanceof LookupResult.Miss);
        delegate.evict(key);
        return present;
    }

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
