package io.tiercache.internal;

import io.tiercache.Version;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;

import java.time.Duration;

/**
 * {@link RemoteCache} decorator guarded by a {@link CircuitBreaker}: fails
 * fast when open (a shared, preallocated {@link L2UnavailableException} —
 * no allocation on the degraded path), records outcomes otherwise, and wraps
 * infrastructure exceptions so raw client exceptions never cross the SPI
 * boundary.
 */
public final class CircuitBreakerRemoteCache<K, V> implements RemoteCache<K, V> {

    private final RemoteCache<K, V> delegate;
    private final CircuitBreaker breaker;

    public CircuitBreakerRemoteCache(RemoteCache<K, V> delegate, CircuitBreaker breaker) {
        this.delegate = delegate;
        this.breaker = breaker;
    }

    public CircuitBreaker breaker() {
        return breaker;
    }

    public RemoteCache<K, V> delegate() {
        return delegate;
    }

    @Override
    public StoredEntry<V> get(K key) {
        return guard(() -> delegate.get(key));
    }

    @Override
    public void put(K key, StoredEntry<V> entry, Duration ttl) {
        guard(() -> {
            delegate.put(key, entry, ttl);
            return null;
        });
    }

    @Override
    public void put(K key, StoredEntry<V> entry, Duration ttl, Duration staleTtl) {
        guard(() -> {
            delegate.put(key, entry, ttl, staleTtl);
            return null;
        });
    }

    @Override
    public boolean putIfNewer(K key, StoredEntry<V> entry, Duration ttl) {
        return guard(() -> delegate.putIfNewer(key, entry, ttl));
    }

    @Override
    public void evict(K key) {
        guard(() -> {
            delegate.evict(key);
            return null;
        });
    }

    @Override
    public void evict(K key, Version version) {
        guard(() -> {
            delegate.evict(key, version);
            return null;
        });
    }

    @Override
    public void clear() {
        guard(() -> {
            delegate.clear();
            return null;
        });
    }

    @Override
    public boolean setIfAbsent(K key, StoredEntry<V> entry, Duration ttl) {
        return guard(() -> delegate.setIfAbsent(key, entry, ttl));
    }

    @Override
    public void putTagged(K key, StoredEntry<V> entry, Duration ttl, String[] tags) {
        guard(() -> {
            delegate.putTagged(key, entry, ttl, tags);
            return null;
        });
    }

    @Override
    public java.util.List<K> keysByTag(String tag) {
        return guard(() -> delegate.keysByTag(tag));
    }

    private <T> T guard(java.util.concurrent.Callable<T> call) {
        if (!breaker.tryAcquire()) {
            throw L2UnavailableException.OPEN;
        }
        try {
            T result = call.call();
            breaker.onSuccess();
            return result;
        } catch (L2UnavailableException e) {
            throw e;
        } catch (Exception e) {
            breaker.onFailure();
            throw new L2UnavailableException("L2 call failed: " + e.getClass().getSimpleName(), e);
        }
    }
}
