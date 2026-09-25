package io.tiercache.internal;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.tiercache.CacheSettings;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.LocalFreshnessResult;
import io.tiercache.spi.StoredEntry;

import java.time.Duration;

/**
 * Default {@link LocalCache} backed by (shaded) Caffeine.
 *
 * <p>Per-entry TTLs are carried by a small value holder read by Caffeine's
 * {@link Expiry}. Stores and atomic freshness refreshes allocate holders;
 * the bare {@link #get} path allocates none. Null-markers are
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
    private final java.util.function.LongSupplier clock;
    private final boolean retained;
    private final long configuredAccessNanos;
    private final long configuredStaleNanos;
    private final java.util.function.BiFunction<K, Holder<V>, Holder<V>> configuredRead;


    /**
     * Creates an L1 cache honoring the given settings (size cap,
     * expire-after-access).
     *
     * @param settings the resolved cache settings
     * @since 0.1.0
     */
    public CaffeineLocalCache(CacheSettings settings) {
        this(settings, System::nanoTime);
    }

    /** Internal clock seam; the engine and L1 must use the same monotonic clock. */
    public CaffeineLocalCache(CacheSettings settings, java.util.function.LongSupplier clock) {
        this.clock = java.util.Objects.requireNonNull(clock);
        this.retained = !settings.degradationStaleTtl().isZero();
        this.configuredAccessNanos = !retained || settings.l1ExpireAfterAccess() == null ? 0 : settings.l1ExpireAfterAccess().toNanos();
        this.configuredStaleNanos = settings.degradationStaleTtl().toNanos();
        this.configuredRead = retained ? (key, holder) -> refresh(holder, configuredAccessNanos, configuredStaleNanos) : null;
        @SuppressWarnings("unchecked")
        Caffeine<K, Holder<V>> builder = (Caffeine<K, Holder<V>>) (Caffeine<?, ?>) Caffeine.newBuilder();
        this.cache = builder
                .ticker(clock::getAsLong)
                .maximumSize(settings.l1MaxSize())
                .expireAfter(new Expiry<K, Holder<V>>() {
                    @Override
                    public long expireAfterCreate(K key, Holder<V> value, long currentTime) {
                        return remaining(value, currentTime);
                    }

                    @Override
                    public long expireAfterUpdate(K key, Holder<V> value, long currentTime,
                            long currentDuration) {
                        return remaining(value, currentTime);
                    }

                    @Override
                    public long expireAfterRead(K key, Holder<V> value, long currentTime,
                            long currentDuration) {
                        // Retained lifetimes slide only through the engine fallback
                        // or the explicit atomic freshness transaction;
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
        cache.put(key, holder(entry, ttl.toNanos()));
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
        return cache.asMap().putIfAbsent(key, holder(entry, ttl.toNanos())) == null;
    }

    @Override
    public boolean supportsAtomicReplace() { return true; }

    @Override
    public boolean replaceIfSame(K key, StoredEntry<V> expected, StoredEntry<V> replacement, Duration ttl) {
        // A no-op computeIfPresent still invokes expireAfterUpdate and can reset
        // another entry's TTL. Read quietly, then compare the exact holder at commit.
        Holder<V> current = cache.policy().getIfPresentQuietly(key);
        if (current == null || current.entry != expected) return false;
        return cache.asMap().replace(key, current, holder(replacement, ttl.toNanos()));
    }

    private Holder<V> holder(StoredEntry<V> entry, long ttlNanos) {
        long expiry = ttlNanos;
        if (retained) {
            StoredEntry.LocalFreshness meta = entry.localFreshness();
            expiry = meta == null ? clock.getAsLong() + ttlNanos : meta.retentionUntilNanos();
        }
        return new Holder<>(entry, expiry);
    }

    private long remaining(Holder<V> holder, long now) {
        return retained ? Math.max(0, holder.expiryNanos - now) : holder.expiryNanos;
    }

    @Override
    public boolean supportsAtomicFreshnessRead() { return retained; }

    @Override
    public LocalFreshnessResult<V> readFreshness(K key, Duration accessTtl, Duration staleTtl) {
        if (!retained) throw new UnsupportedOperationException("Atomic freshness requires retained lifetimes");
        long access = accessTtl.toNanos();
        long window = staleTtl.toNanos();
        if (access <= 0 || window <= 0) throw new IllegalArgumentException("Freshness durations must be positive");
        // The configured path reuses a stateless remapper; no per-read mutable result box.
        var remapper = access == configuredAccessNanos && window == configuredStaleNanos
                ? configuredRead : (java.util.function.BiFunction<K, Holder<V>, Holder<V>>)
                    (ignored, holder) -> refresh(holder, access, window);
        Holder<V> result = cache.asMap().computeIfPresent(key, remapper);
        return result == null ? LocalFreshnessResult.absent() : (ObservedHolder<V>) result;
    }

    private Holder<V> refresh(Holder<V> current, long access, long window) {
        long now = clock.getAsLong();
        // Caffeine may have sampled its clock before waiting for the node lock.
        if (now - current.expiryNanos >= 0) return null;
        StoredEntry<V> entry = current.entry;
        StoredEntry.LocalFreshness meta = entry.localFreshness();
        if (meta == null) return observed(current, LocalFreshnessResult.State.EXPIRED);
        if (now - meta.logicalDeadlineNanos() >= 0) {
            return observed(current, now - meta.staleServeUntilNanos() < 0
                    ? LocalFreshnessResult.State.STALE_ALLOWED : LocalFreshnessResult.State.EXPIRED);
        }
        long logical = now + access;
        long staleUntil = logical + window;
        long retention = now + Math.max(meta.storeRetentionFloorNanos() - now, staleUntil - now);
        var refreshed = entry.withLocalFreshness(new StoredEntry.LocalFreshness(
                logical, staleUntil, meta.storeRetentionFloorNanos(), retention, meta.highestSeen()));
        return new ObservedHolder<>(refreshed, retention, LocalFreshnessResult.State.FRESH);
    }

    private Holder<V> observed(Holder<V> current, LocalFreshnessResult.State state) {
        if (current instanceof ObservedHolder<?> observed && observed.state() == state) return current;
        // Only the private observation carrier changes; the opaque StoredEntry and
        // its absolute physical deadline remain identical on stale/unknown reads.
        return new ObservedHolder<>(current.entry, current.expiryNanos, state);
    }

    private static final class ObservedHolder<V> extends Holder<V> implements LocalFreshnessResult<V> {
        private final State state;
        ObservedHolder(StoredEntry<V> entry, long expiryNanos, State state) {
            super(entry, expiryNanos);
            this.state = state;
        }
        @Override public StoredEntry<V> entry() { return entry; }
        @Override public State state() { return state; }
    }

    private static class Holder<V> {
        final StoredEntry<V> entry;
        final long expiryNanos;

        Holder(StoredEntry<V> entry, long expiryNanos) {
            this.entry = entry;
            this.expiryNanos = expiryNanos;
        }
    }
}
