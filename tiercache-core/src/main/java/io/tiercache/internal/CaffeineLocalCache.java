package io.tiercache.internal;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.tiercache.CacheSettings;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.StoredEntry;

import java.time.Duration;

/**
 * Default {@link LocalCache} backed by (shaded) Caffeine.
 *
 * <p>Per-entry TTLs are carried by a small value holder read by Caffeine's
 * {@link Expiry}. The holder is allocated on write only; the steady-state
 * hit path ({@link #get}) allocates nothing. Null-markers are
 * stored like any other entry.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @param <K> key type
 * @param <V> value type
 * @since 0.1.0
 */
public final class CaffeineLocalCache<K, V> implements LocalCache<K, V> {

    private final Cache<K, Holder<V>> cache;

    /**
     * Creates an L1 cache honoring the given settings (size cap,
     * expire-after-access).
     *
     * @param settings the resolved cache settings
     * @since 0.1.0
     */
    public CaffeineLocalCache(CacheSettings settings) {
        @SuppressWarnings("unchecked")
        Caffeine<K, Holder<V>> builder = (Caffeine<K, Holder<V>>) (Caffeine<?, ?>) Caffeine.newBuilder();
        this.cache = builder
                .maximumSize(settings.l1MaxSize())
                .expireAfter(new Expiry<K, Holder<V>>() {
                    @Override
                    public long expireAfterCreate(K key, Holder<V> value, long currentTime) {
                        return value.ttlNanos;
                    }

                    @Override
                    public long expireAfterUpdate(K key, Holder<V> value, long currentTime,
                            long currentDuration) {
                        return value.ttlNanos;
                    }

                    @Override
                    public long expireAfterRead(K key, Holder<V> value, long currentTime,
                            long currentDuration) {
                        // With the degradation window on, the ENGINE manages
                        // sliding (fresh-access re-put with identity check);
                        // a bare read must never move retention — least of
                        // all extend it for a stale entry. When the window is
                        // off, the legacy expire-after-access rewrite applies.
                        if (settings.degradationStaleTtl().isZero()) {
                            return settings.l1ExpireAfterAccess() != null
                                    ? settings.l1ExpireAfterAccess().toNanos()
                                    : currentDuration;
                        }
                        return currentDuration;
                    }
                })
                .build();
    }

    @Override
    public StoredEntry<V> get(K key) {
        Holder<V> holder = cache.getIfPresent(key);
        return holder != null ? holder.entry : null;
    }

    @Override
    public void put(K key, StoredEntry<V> entry, Duration ttl) {
        cache.put(key, new Holder<>(entry, ttl.toNanos()));
    }

    @Override
    public void evict(K key) {
        cache.invalidate(key);
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }

    @Override
    public boolean setIfAbsent(K key, StoredEntry<V> entry, Duration ttl) {
        return cache.asMap().putIfAbsent(key, new Holder<>(entry, ttl.toNanos())) == null;
    }

    private static final class Holder<V> {
        final StoredEntry<V> entry;
        final long ttlNanos;

        Holder(StoredEntry<V> entry, long ttlNanos) {
            this.entry = entry;
            this.ttlNanos = ttlNanos;
        }
    }
}
