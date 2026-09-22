package io.tiercache.internal;

import io.tiercache.Version;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.spi.TaggedWriteOutcome;

import java.time.Duration;

/**
 * {@link RemoteCache} decorator guarded by a {@link CircuitBreaker}: fails
 * fast when open (a shared, preallocated {@link L2UnavailableException} —
 * no allocation on the degraded path), records outcomes otherwise, and wraps
 * infrastructure exceptions so raw client exceptions never cross the SPI
 * boundary.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @param <K> key type
 * @param <V> value type
 * @since 0.1.0
 */
public final class CircuitBreakerRemoteCache<K, V> implements RemoteCache<K, V> {

    private final RemoteCache<K, V> delegate;
    private final CircuitBreaker breaker;

    /**
     * Creates a guarded L2.
     *
     * @param delegate the L2 to guard
     * @param breaker  the breaker that gates L2 calls
     * @since 0.1.0
     */
    public CircuitBreakerRemoteCache(RemoteCache<K, V> delegate, CircuitBreaker breaker) {
        this.delegate = delegate;
        this.breaker = breaker;
    }

    /**
     * The breaker guarding this L2.
     *
     * @return the circuit breaker
     * @since 0.1.0
     */
    public CircuitBreaker breaker() {
        return breaker;
    }

    /**
     * The guarded L2.
     *
     * @return the delegate remote cache
     * @since 0.1.0
     */
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
    public void clear(io.tiercache.Version version) {
        guard(() -> {
            delegate.clear(version);
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
    public boolean supportsTaggedWriteOutcomes() {
        return delegate.supportsTaggedWriteOutcomes();
    }

    @Override
    public TaggedWriteOutcome putTaggedIfNewer(K key, StoredEntry<V> entry,
            Duration ttl, String[] tags) {
        if (entry.version() != null && !supportsTaggedWriteOutcomes()) {
            return TaggedWriteOutcome.UNSUPPORTED;
        }
        CircuitBreaker.Permit permit = breaker.tryAcquirePermit();
        if (permit == null) {
            throw L2UnavailableException.OPEN;
        }
        try {
            TaggedWriteOutcome result = delegate.putTaggedIfNewer(key, entry, ttl, tags);
            if (result == TaggedWriteOutcome.UNSUPPORTED) {
                permit.cancel();
            } else {
                permit.success();
            }
            return result;
        } catch (L2UnavailableException e) {
            permit.cancel();
            throw e;
        } catch (Exception e) {
            permit.failure();
            throw new L2UnavailableException("L2 call failed: " + e.getClass().getSimpleName(), e);
        } catch (Error e) {
            permit.cancel();
            throw e;
        }
    }

    @Override
    public java.util.List<K> keysByTag(String tag) {
        return guard(() -> delegate.keysByTag(tag));
    }

    private <T> T guard(java.util.concurrent.Callable<T> call) {
        CircuitBreaker.Permit permit = breaker.tryAcquirePermit();
        if (permit == null) {
            throw L2UnavailableException.OPEN;
        }
        try {
            T result = call.call();
            permit.success();
            return result;
        } catch (L2UnavailableException e) {
            permit.cancel();
            throw e;
        } catch (Exception e) {
            permit.failure();
            throw new L2UnavailableException("L2 call failed: " + e.getClass().getSimpleName(), e);
        }
    }
}
