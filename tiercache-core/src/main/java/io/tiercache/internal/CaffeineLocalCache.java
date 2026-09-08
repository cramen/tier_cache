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
 */
public final class CaffeineLocalCache<K, V> implements LocalCache<K, V> {

    private final Cache<K, Holder<V>> cache;

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
                        // expire-after-access, if configured
                        return settings.l1ExpireAfterAccess() != null
                                ? settings.l1ExpireAfterAccess().toNanos()
                                : currentDuration;
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
