package io.tiercache.internal;

import io.tiercache.CacheSettings;
import io.tiercache.TierCache;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.RemoteCache;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Default {@link TierCache}: cascade read L1 &rarr; L2 &rarr; loader with
 * L1 warm-up (F-01) and per-instance singleflight (F-20).
 *
 * <p>Hot-path discipline (N-03): a steady-state L1 hit performs exactly one
 * {@code LocalCache.get} and allocates nothing.
 */
public final class DefaultTierCache<K, V> implements TierCache<K, V> {

    private final LocalCache<K, V> l1;
    private final RemoteCache<K, V> l2;
    private final CacheSettings settings;
    private final boolean singleflightEnabled;
    private final Map<K, CompletableFuture<V>> inflight = new ConcurrentHashMap<>();

    public DefaultTierCache(LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled) {
        this.l1 = l1;
        this.l2 = l2;
        this.settings = settings;
        this.singleflightEnabled = singleflightEnabled;
    }

    @Override
    public V get(K key) {
        V value = l1.get(key);
        if (value != null) {
            return value;
        }
        value = l2.get(key);
        if (value != null) {
            warmL1(key, value);
        }
        return value;
    }

    @Override
    public V getOrCompute(K key, Function<? super K, ? extends V> loader) {
        V value = l1.get(key);
        if (value != null) {
            return value;
        }
        value = l2.get(key);
        if (value != null) {
            warmL1(key, value);
            return value;
        }
        if (!singleflightEnabled) {
            return loadAndStore(key, loader);
        }
        CompletableFuture<V> future = new CompletableFuture<>();
        CompletableFuture<V> existing = inflight.putIfAbsent(key, future);
        if (existing != null) {
            return existing.join();
        }
        try {
            V loaded = loadAndStore(key, loader);
            future.complete(loaded);
            return loaded;
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
            throw e;
        } finally {
            inflight.remove(key, future);
        }
    }

    @Override
    public void put(K key, V value) {
        // Write order F-02: L2 first, then L1.
        l2.put(key, value, settings.l2Ttl());
        warmL1(key, value);
    }

    @Override
    public void evict(K key) {
        l2.evict(key);
        l1.evict(key);
    }

    private V loadAndStore(K key, Function<? super K, ? extends V> loader) {
        V loaded = loader.apply(key);
        if (loaded == null) {
            // Null-caching policy (F-25) lands in a later change; for now a
            // loader null is a miss and nothing is stored.
            return null;
        }
        l2.put(key, loaded, settings.l2Ttl());
        warmL1(key, loaded);
        return loaded;
    }

    /** Writes into L1 with a jittered TTL that never exceeds the L2 TTL (F-05/F-24). */
    private void warmL1(K key, V value) {
        Duration ttl = TtlJitter.apply(settings.l1ExpireAfterWrite(), settings.jitterAmplitude());
        l1.put(key, value, ttl);
    }
}
