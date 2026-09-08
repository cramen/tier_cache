package io.tiercache.internal;

import io.tiercache.CacheSettings;
import io.tiercache.LookupResult;
import io.tiercache.TierCache;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Default {@link TierCache}: cascade read L1 &rarr; L2 &rarr; loader with
 * L1 warm-up (F-01), per-instance singleflight (F-20), and null-marker
 * handling (F-25).
 *
 * <p>Hot-path discipline (N-03): a steady-state L1 hit performs exactly one
 * {@code LocalCache.get} plus one reference check, and allocates nothing.
 */
public final class DefaultTierCache<K, V> implements TierCache<K, V> {

    private final LocalCache<K, V> l1;
    private final RemoteCache<K, V> l2;
    private final CacheSettings settings;
    private final boolean singleflightEnabled;
    private final Map<K, CompletableFuture<StoredEntry<V>>> inflight = new ConcurrentHashMap<>();

    public DefaultTierCache(LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled) {
        this.l1 = l1;
        this.l2 = l2;
        this.settings = settings;
        this.singleflightEnabled = singleflightEnabled;
    }

    @Override
    public V get(K key) {
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            return entry.isNullMarker() ? null : entry.value();
        }
        entry = l2.get(key);
        if (entry != null) {
            warmL1(key, entry);
            return entry.isNullMarker() ? null : entry.value();
        }
        return null;
    }

    @Override
    public LookupResult<V> lookup(K key) {
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            return toResult(entry);
        }
        entry = l2.get(key);
        if (entry != null) {
            warmL1(key, entry);
            return toResult(entry);
        }
        return LookupResult.miss();
    }

    @Override
    public V getOrCompute(K key, Function<? super K, ? extends V> loader) {
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            return entry.isNullMarker() ? null : entry.value();
        }
        entry = l2.get(key);
        if (entry != null) {
            warmL1(key, entry);
            return entry.isNullMarker() ? null : entry.value();
        }
        if (!singleflightEnabled) {
            return unwrap(loadAndStore(key, loader));
        }
        CompletableFuture<StoredEntry<V>> future = new CompletableFuture<>();
        CompletableFuture<StoredEntry<V>> existing = inflight.putIfAbsent(key, future);
        if (existing != null) {
            return unwrap(existing.join());
        }
        try {
            StoredEntry<V> loaded = loadAndStore(key, loader);
            future.complete(loaded);
            return unwrap(loaded);
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
            throw e;
        } finally {
            inflight.remove(key, future);
        }
    }

    @Override
    public void put(K key, V value) {
        // Write order F-02: L2 first, then L1. Overwrites any marker (F-25).
        StoredEntry<V> entry = StoredEntry.ofValue(value);
        l2.put(key, entry, settings.l2Ttl());
        warmL1(key, entry);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        // F-03: atomic at L2; L1 warm-up only for the winner.
        boolean won = l2.setIfAbsent(key, value, settings.l2Ttl());
        if (won) {
            warmL1(key, StoredEntry.ofValue(value));
        }
        return won;
    }

    @Override
    public void evict(K key) {
        l2.evict(key);
        l1.evict(key);
    }

    /**
     * Loads and stores the result.
     *
     * @return the entry now logically present (a null-marker under
     *         {@code allow}), or {@code null} if nothing was stored
     */
    private StoredEntry<V> loadAndStore(K key, Function<? super K, ? extends V> loader) {
        V loaded = loader.apply(key);
        if (loaded == null) {
            Duration markerTtl = settings.nullPolicy().markerTtl();
            if (markerTtl == null) {
                // deny policy: a miss stays uncached
                return null;
            }
            // F-25: marker in both levels, jittered like any TTL (F-24).
            StoredEntry<V> marker = StoredEntry.nullMarker();
            l2.put(key, marker, markerTtl);
            l1.put(key, marker, TtlJitter.apply(markerTtl, settings.jitterAmplitude()));
            return marker;
        }
        StoredEntry<V> entry = StoredEntry.ofValue(loaded);
        l2.put(key, entry, settings.l2Ttl());
        warmL1(key, entry);
        return entry;
    }

    /** Writes into L1 with a jittered TTL that never exceeds the L2 TTL (F-05/F-24). */
    private void warmL1(K key, StoredEntry<V> entry) {
        Duration ttl = TtlJitter.apply(settings.l1ExpireAfterWrite(), settings.jitterAmplitude());
        l1.put(key, entry, ttl);
    }

    private static <V> LookupResult<V> toResult(StoredEntry<V> entry) {
        return entry.isNullMarker() ? LookupResult.cachedNull() : LookupResult.hit(entry.value());
    }

    private static <V> V unwrap(StoredEntry<V> entry) {
        if (entry == null || entry.isNullMarker()) {
            return null;
        }
        return entry.value();
    }
}
