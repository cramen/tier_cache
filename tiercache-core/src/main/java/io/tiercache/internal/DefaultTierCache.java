package io.tiercache.internal;

import io.tiercache.CacheSettings;
import io.tiercache.LookupResult;
import io.tiercache.TierCache;
import io.tiercache.spi.DistributedLock;
import io.tiercache.spi.DistributedLockProvider;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Default {@link TierCache}: cascade read L1 &rarr; L2 &rarr; loader with
 * L1 warm-up, per-instance singleflight, null-marker handling,
 * and distributed rebuild coordination when a lock provider
 * is available.
 *
 * <p>Coordinated load path: acquire rebuild lock &rarr; <b>mandatory
 * double-check of L1/L2</b> &rarr; load with watchdog lease extension &rarr;
 * store &rarr; release. Losers wait for the value with bounded backoff and
 * attempt a takeover on timeout — they never load uncoordinated unless the
 * overall budget is exhausted (logged safety valve).
 *
 * <p>Hot-path discipline: a steady-state L1 hit performs exactly one
 * {@code LocalCache.get} plus one reference check, and allocates nothing.
 */
public final class DefaultTierCache<K, V> implements TierCache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DefaultTierCache.class);

    /** Rebuild-lock lease. Internal constant — deliberately not user-configurable. */
    static final Duration LOCK_LEASE = Duration.ofSeconds(10);
    /** How long a loser waits for the winner's value before attempting a takeover. */
    static final Duration WAIT_SLICE = Duration.ofSeconds(2);
    /** Overall budget for the coordinated path before the logged fallback. */
    static final Duration OVERALL_BUDGET = Duration.ofSeconds(30);
    /** Polling backoff bounds for the loser wait. */
    static final long POLL_INITIAL_NANOS = Duration.ofMillis(5).toNanos();
    static final long POLL_MAX_NANOS = Duration.ofMillis(50).toNanos();

    private final String cacheName;
    private final LocalCache<K, V> l1;
    private final RemoteCache<K, V> l2;
    private final CacheSettings settings;
    private final boolean singleflightEnabled;
    private final DistributedLockProvider lockProvider; // null = no coordination
    private final ScheduledExecutorService watchdog;
    private final Map<K, CompletableFuture<StoredEntry<V>>> inflight = new ConcurrentHashMap<>();

    /** Legacy constructor: no coordination (used by tests). */
    public DefaultTierCache(LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled) {
        this("test", l1, l2, settings, singleflightEnabled, null, null);
    }

    public DefaultTierCache(String cacheName, LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled,
            DistributedLockProvider lockProvider, ScheduledExecutorService watchdog) {
        this.cacheName = cacheName;
        this.l1 = l1;
        this.l2 = l2;
        this.settings = settings;
        this.singleflightEnabled = singleflightEnabled;
        this.lockProvider = lockProvider;
        this.watchdog = watchdog;
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
            return unwrap(loadPath(key, loader));
        }
        CompletableFuture<StoredEntry<V>> future = new CompletableFuture<>();
        CompletableFuture<StoredEntry<V>> existing = inflight.putIfAbsent(key, future);
        if (existing != null) {
            return unwrap(existing.join());
        }
        try {
            StoredEntry<V> loaded = loadPath(key, loader);
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
        // Write order: L2 first, then L1. Overwrites any marker.
        StoredEntry<V> entry = StoredEntry.ofValue(value);
        l2.put(key, entry, settings.l2Ttl());
        warmL1(key, entry);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        // Atomic at L2; L1 warm-up only for the winner.
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

    @Override
    public void evictAll() {
        l2.clear();
        l1.clear();
    }

    @Override
    public void putNull(K key) {
        Duration markerTtl = settings.nullPolicy().markerTtl();
        if (markerTtl == null) {
            return; // deny policy: nothing to store
        }
        StoredEntry<V> marker = StoredEntry.nullMarker();
        l2.put(key, marker, markerTtl);
        l1.put(key, marker, TtlJitter.apply(markerTtl, settings.jitterAmplitude()));
    }

    // --- Load path selection: coordinated when possible ---

    private StoredEntry<V> loadPath(K key, Function<? super K, ? extends V> loader) {
        if (lockProvider == null || watchdog == null) {
            return loadAndStore(key, loader);
        }
        return coordinatedLoad(key, loader);
    }

    private StoredEntry<V> coordinatedLoad(K key, Function<? super K, ? extends V> loader) {
        String lockName = cacheName + ":" + key;
        long overallDeadline = System.nanoTime() + OVERALL_BUDGET.toNanos();
        long waitDeadline = System.nanoTime() + WAIT_SLICE.toNanos();
        while (true) {
            DistributedLock lock = lockProvider.tryLock(lockName, LOCK_LEASE);
            if (lock != null) {
                try {
                    // Mandatory double-check: the value may have
                    // appeared while we were acquiring the lock.
                    StoredEntry<V> entry = readThrough(key);
                    if (entry != null) {
                        return entry;
                    }
                    return loadWithWatchdog(key, loader, lock);
                } finally {
                    lock.release();
                }
            }
            StoredEntry<V> appeared = awaitValue(key, waitDeadline);
            if (appeared != null) {
                warmL1(key, appeared);
                return appeared;
            }
            if (System.nanoTime() > overallDeadline) {
                log.warn("Rebuild coordination budget exhausted for key '{}' in cache '{}'; "
                        + "loading without coordination (possible stampede after repeated "
                        + "winner failures).", key, cacheName);
                return loadAndStore(key, loader);
            }
            waitDeadline = System.nanoTime() + WAIT_SLICE.toNanos();
        }
    }

    /** Re-read L1 then L2 (warming L1 on an L2 hit). Returns null on full miss. */
    private StoredEntry<V> readThrough(K key) {
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            return entry;
        }
        entry = l2.get(key);
        if (entry != null) {
            warmL1(key, entry);
        }
        return entry;
    }

    private StoredEntry<V> loadWithWatchdog(K key, Function<? super K, ? extends V> loader,
            DistributedLock lock) {
        long periodMillis = LOCK_LEASE.toMillis() / 3;
        ScheduledFuture<?> extension = watchdog.scheduleAtFixedRate(
                () -> lock.extend(LOCK_LEASE),
                periodMillis, periodMillis, TimeUnit.MILLISECONDS);
        try {
            return loadAndStore(key, loader);
        } finally {
            extension.cancel(false);
        }
    }

    /** Bounded wait for the winner's value in L2. Null on timeout. */
    private StoredEntry<V> awaitValue(K key, long deadlineNanos) {
        long backoff = POLL_INITIAL_NANOS;
        while (System.nanoTime() < deadlineNanos) {
            StoredEntry<V> entry = l2.get(key);
            if (entry != null) {
                return entry;
            }
            try {
                TimeUnit.NANOSECONDS.sleep(backoff);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            backoff = Math.min(backoff * 2, POLL_MAX_NANOS);
        }
        return l2.get(key); // final attempt at the deadline
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
            // Null-marker stored in both levels, jittered like any TTL.
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

    /** Writes into L1 with a jittered TTL that never exceeds the L2 TTL. */
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
