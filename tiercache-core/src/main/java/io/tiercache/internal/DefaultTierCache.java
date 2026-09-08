package io.tiercache.internal;

import io.tiercache.CacheSettings;
import io.tiercache.InvalidationMode;
import io.tiercache.InvalidationMessage;
import io.tiercache.LookupResult;
import io.tiercache.TierCache;
import io.tiercache.Version;
import io.tiercache.VersionGenerator;
import io.tiercache.spi.DistributedLock;
import io.tiercache.spi.DistributedLockProvider;
import io.tiercache.spi.InvalidationHandler;
import io.tiercache.spi.InvalidationTarget;
import io.tiercache.spi.CacheMetricsListener;
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
 * L1 warm-up, per-instance singleflight, null-marker handling, distributed
 * rebuild coordination when a lock provider is available, and invalidation
 * publishing when an {@link InvalidationHandler} is configured.
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
public final class DefaultTierCache<K, V> implements TierCache<K, V>, InvalidationTarget {

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
    private final VersionGenerator versionGenerator;   // null = no versioning/publishing
    private final InvalidationHandler invalidation;    // null = single-node
    private final CircuitBreaker breaker;              // null = unguarded L2 (opt-out)
    private final CacheMetricsListener metrics;
    private final Map<K, CompletableFuture<StoredEntry<V>>> inflight = new ConcurrentHashMap<>();

    /** Legacy constructor: no coordination, no invalidation (used by tests). */
    public DefaultTierCache(LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled) {
        this("test", l1, l2, settings, singleflightEnabled, null, null, null, null);
    }

    public DefaultTierCache(String cacheName, LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled,
            DistributedLockProvider lockProvider, ScheduledExecutorService watchdog,
            VersionGenerator versionGenerator, InvalidationHandler invalidation) {
        this(cacheName, l1, l2, settings, singleflightEnabled, lockProvider, watchdog,
                versionGenerator, invalidation, null, CacheMetricsListener.NOOP);
    }

    public DefaultTierCache(String cacheName, LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled,
            DistributedLockProvider lockProvider, ScheduledExecutorService watchdog,
            VersionGenerator versionGenerator, InvalidationHandler invalidation,
            CircuitBreaker breaker, CacheMetricsListener metrics) {
        this.cacheName = cacheName;
        this.l1 = l1;
        this.l2 = l2;
        this.settings = settings;
        this.singleflightEnabled = singleflightEnabled;
        this.lockProvider = lockProvider;
        this.watchdog = watchdog;
        this.versionGenerator = versionGenerator;
        this.invalidation = invalidation;
        this.breaker = breaker;
        this.metrics = metrics;
    }

    @Override
    public V get(K key) {
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L1_HIT);
            return entry.isNullMarker() ? null : entry.value();
        }
        entry = l2Get(key);
        if (entry != null) {
            metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
            warmL1(key, entry);
            return entry.isNullMarker() ? null : entry.value();
        }
        metrics.onRequest(cacheName, CacheMetricsListener.Outcome.MISS);
        return null;
    }

    @Override
    public LookupResult<V> lookup(K key) {
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L1_HIT);
            return toResult(entry);
        }
        entry = l2Get(key);
        if (entry != null) {
            metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
            warmL1(key, entry);
            return toResult(entry);
        }
        metrics.onRequest(cacheName, CacheMetricsListener.Outcome.MISS);
        return LookupResult.miss();
    }

    @Override
    public V getOrCompute(K key, Function<? super K, ? extends V> loader) {
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L1_HIT);
            return entry.isNullMarker() ? null : entry.value();
        }
        entry = l2Get(key);
        if (entry != null) {
            metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
            warmL1(key, entry);
            return entry.isNullMarker() ? null : entry.value();
        }
        if (!singleflightEnabled) {
            StoredEntry<V> result = loadPath(key, loader);
            metrics.onRequest(cacheName, result != null && !result.isNullMarker()
                    ? CacheMetricsListener.Outcome.LOAD : CacheMetricsListener.Outcome.MISS);
            return unwrap(result);
        }
        CompletableFuture<StoredEntry<V>> future = new CompletableFuture<>();
        CompletableFuture<StoredEntry<V>> existing = inflight.putIfAbsent(key, future);
        if (existing != null) {
            metrics.onRequest(cacheName, CacheMetricsListener.Outcome.COALESCED);
            return unwrap(existing.join());
        }
        try {
            StoredEntry<V> loaded = loadPath(key, loader);
            metrics.onRequest(cacheName, loaded != null && !loaded.isNullMarker()
                    ? CacheMetricsListener.Outcome.LOAD : CacheMetricsListener.Outcome.MISS);
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
        // Write order: L2 first, then L1, then publish. Overwrites any marker.
        Version version = nextVersion();
        StoredEntry<V> entry = StoredEntry.ofValue(value, version);
        Boolean stored = l2ConditionalPut(key, entry, settings.l2Ttl(), version != null);
        if (stored == null) {
            // Degraded: L1 only, no publish (the journal has no row either).
            warmL1(key, entry);
            return;
        }
        if (!stored) {
            // Lost version race: converge L1 to the current L2 entry.
            StoredEntry<V> current = l2Get(key);
            if (current != null) {
                warmL1(key, current);
            } else {
                l1.evict(key);
            }
            return;
        }
        warmL1(key, entry);
        publishStore(key, entry, version);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        // Atomic at L2; L1 warm-up and publish only for the winner.
        Version version = nextVersion();
        Boolean won = l2SetIfAbsent(key, StoredEntry.ofValue(value, version), settings.l2Ttl());
        if (won == null) {
            // Degraded: per-instance atomicity on L1 only.
            return l1.setIfAbsent(key, StoredEntry.ofValue(value, version),
                    TtlJitter.apply(settings.l1ExpireAfterWrite(), settings.jitterAmplitude()));
        }
        if (won) {
            StoredEntry<V> stored = StoredEntry.ofValue(value, version);
            warmL1(key, stored);
            publishStore(key, stored, version);
        }
        return won;
    }

    @Override
    public void evict(K key) {
        Version version = nextVersion();
        if (l2Evict(key, version)) {
            publish(key, version, InvalidationMessage.Type.INVALIDATE);
        }
        l1.evict(key);
    }

    @Override
    public void evictAll() {
        Version version = nextVersion();
        if (l2Clear()) {
            publish(null, version, InvalidationMessage.Type.EVICT_ALL);
        }
        l1.clear();
    }

    @Override
    public void putNull(K key) {
        Duration markerTtl = settings.nullPolicy().markerTtl();
        if (markerTtl == null) {
            return; // deny policy: nothing to store
        }
        Version version = nextVersion();
        StoredEntry<V> marker = StoredEntry.nullMarker(version);
        metrics.onNullEntry(cacheName);
        storeVersioned(key, marker, markerTtl, version,
                TtlJitter.apply(markerTtl, settings.jitterAmplitude()));
    }

    @Override
    public void put(K key, V value, String... tags) {
        if (tags == null || tags.length == 0) {
            put(key, value);
            return;
        }
        Version version = nextVersion();
        StoredEntry<V> entry = StoredEntry.ofValue(value, version);
        if (breaker != null && breaker.isOpen()) {
            warmL1(key, entry);
            return;
        }
        try {
            l2.putTagged(key, entry, settings.l2Ttl(), tags);
            warmL1(key, entry);
            publishStore(key, entry, version);
        } catch (L2UnavailableException e) {
            warmL1(key, entry);
        }
    }

    @Override
    public void evictByTag(String tag) {
        for (K key : l2KeysByTag(tag)) {
            evict(key);
        }
    }

    @Override
    public void evictAll(java.util.Collection<K> keys) {
        for (K key : keys) {
            evict(key);
        }
    }

    private java.util.List<K> l2KeysByTag(String tag) {
        if (breaker != null && breaker.isOpen()) {
            return java.util.List.of();
        }
        try {
            return l2.keysByTag(tag);
        } catch (L2UnavailableException e) {
            return java.util.List.of();
        }
    }

    // --- InvalidationTarget (inbound events, applied last-write-wins) ---

    @Override
    public Version versionOfL1Entry(Object key) {
        @SuppressWarnings("unchecked")
        StoredEntry<V> entry = l1.get((K) key);
        return entry != null ? entry.version() : null;
    }

    @Override
    public void evictL1IfNewer(Object key, Version eventVersion) {
        @SuppressWarnings("unchecked")
        K typedKey = (K) key;
        StoredEntry<V> entry = l1.get(typedKey);
        if (entry == null) {
            return;
        }
        // Entries without a version (legacy/unversioned) lose to any event.
        if (entry.version() == null || eventVersion.compareTo(entry.version()) > 0) {
            l1.evict(typedKey);
        }
    }

    @Override
    public void evictAllL1() {
        l1.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void applyUpdateL1(Object key, Object value, Version eventVersion) {
        K typedKey = (K) key;
        StoredEntry<V> current = l1.get(typedKey);
        if (current != null && current.version() != null
                && eventVersion.compareTo(current.version()) <= 0) {
            return; // stale update
        }
        l1.put(typedKey, StoredEntry.ofValue((V) value, eventVersion),
                TtlJitter.apply(settings.l1ExpireAfterWrite(), settings.jitterAmplitude()));
    }

    private Version nextVersion() {
        return versionGenerator != null ? versionGenerator.next() : null;
    }

    private void publish(Object key, Version version, InvalidationMessage.Type type) {
        if (invalidation != null && version != null) {
            invalidation.onLocalWrite(cacheName, key, version, type);
        }
    }

    /** Publish for a stored entry: UPDATE (with payload) in update mode, else INVALIDATE. */
    private void publishStore(K key, StoredEntry<V> entry, Version version) {
        if (invalidation == null || version == null) {
            return;
        }
        if (settings.invalidationMode() == InvalidationMode.UPDATE && !entry.isNullMarker()) {
            invalidation.onLocalUpdate(cacheName, key, entry.value(), version);
        } else {
            invalidation.onLocalWrite(cacheName, key, version, InvalidationMessage.Type.INVALIDATE);
        }
    }

    // --- Load path selection: coordinated when possible ---

    private StoredEntry<V> loadPath(K key, Function<? super K, ? extends V> loader) {
        if (lockProvider == null || watchdog == null || !l2Available()) {
            // No coordination possible (or L2 down): per-instance load.
            return loadAndStore(key, loader);
        }
        return coordinatedLoad(key, loader);
    }

    private StoredEntry<V> coordinatedLoad(K key, Function<? super K, ? extends V> loader) {
        String lockName = cacheName + ":" + key;
        long overallDeadline = System.nanoTime() + OVERALL_BUDGET.toNanos();
        long waitDeadline = System.nanoTime() + WAIT_SLICE.toNanos();
        while (true) {
            DistributedLock lock = tryLockGuarded(lockName);
            if (!l2Available()) {
                // L2 failed between the availability check and lock
                // acquisition: fall back to the per-instance load.
                return loadAndStore(key, loader);
            }
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

    /** Lock acquisition through the breaker: fast-fail when open. */
    private DistributedLock tryLockGuarded(String lockName) {
        if (breaker != null && breaker.isOpen()) {
            return null;
        }
        try {
            return lockProvider.tryLock(lockName, LOCK_LEASE);
        } catch (L2UnavailableException e) {
            return null;
        }
    }

    /** Re-read L1 then L2 (warming L1 on an L2 hit). Returns null on full miss. */
    private StoredEntry<V> readThrough(K key) {
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            return entry;
        }
        entry = l2Get(key);
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
            StoredEntry<V> entry = l2Get(key);
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
        return l2Get(key); // final attempt at the deadline
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
            Version version = nextVersion();
            StoredEntry<V> marker = StoredEntry.nullMarker(version);
            metrics.onNullEntry(cacheName);
            storeVersioned(key, marker, markerTtl, version,
                    TtlJitter.apply(markerTtl, settings.jitterAmplitude()));
            return marker;
        }
        Version version = nextVersion();
        StoredEntry<V> entry = StoredEntry.ofValue(loaded, version);
        storeVersioned(key, entry, settings.l2Ttl(), version, null);
        return entry;
    }

    /** L2 store + L1 warm + publish, version-conditional when versioning is on. */
    private void storeVersioned(K key, StoredEntry<V> entry, Duration l2Ttl, Version version,
            Duration l1TtlOverride) {
        Boolean stored = l2ConditionalPut(key, entry, l2Ttl, version != null);
        if (stored == null) {
            l1.put(key, entry, l1TtlOverride != null ? l1TtlOverride
                    : TtlJitter.apply(settings.l1ExpireAfterWrite(), settings.jitterAmplitude()));
            return; // degraded: L1 only
        }
        if (!stored) {
            StoredEntry<V> current = l2Get(key);
            if (current != null) {
                warmL1(key, current);
            } else {
                l1.evict(key);
            }
            return;
        }
        if (l1TtlOverride != null) {
            l1.put(key, entry, l1TtlOverride);
        } else {
            warmL1(key, entry);
        }
        publishStore(key, entry, version);
    }

    /** Writes into L1 with a jittered TTL that never exceeds the L2 TTL. */
    private void warmL1(K key, StoredEntry<V> entry) {
        Duration ttl = TtlJitter.apply(settings.l1ExpireAfterWrite(), settings.jitterAmplitude());
        l1.put(key, entry, ttl);
    }

    /** True while the L2 circuit breaker is open (L1-only mode). */
    public boolean isDegraded() {
        return breaker != null && breaker.isOpen();
    }

    private boolean l2Available() {
        return breaker == null || !breaker.isOpen();
    }

    /** L2 read through the breaker: null when open or failed (readers fall through to loader). */
    private StoredEntry<V> l2Get(K key) {
        if (breaker != null && breaker.isOpen()) {
            return null;
        }
        long start = System.nanoTime();
        Object span = metrics.onL2OperationStart(cacheName, "get");
        boolean hit = false;
        try {
            StoredEntry<V> result = l2.get(key);
            hit = result != null;
            metrics.onLatency(cacheName, CacheMetricsListener.Level.L2, System.nanoTime() - start);
            return result;
        } catch (L2UnavailableException e) {
            return null; // the decorator already accounted the failure
        } finally {
            metrics.onL2OperationEnd(cacheName, "get", hit, span);
        }
    }

    /** @return TRUE stored, FALSE lost a version race, NULL unavailable/failed. */
    private Boolean l2ConditionalPut(K key, StoredEntry<V> entry, Duration ttl, boolean conditional) {
        if (breaker != null && breaker.isOpen()) {
            return null;
        }
        try {
            return conditional ? l2.putIfNewer(key, entry, ttl) : putPlain(key, entry, ttl);
        } catch (L2UnavailableException e) {
            return null;
        }
    }

    private boolean putPlain(K key, StoredEntry<V> entry, Duration ttl) {
        l2.put(key, entry, ttl);
        return true;
    }

    /** @return TRUE won, FALSE lost, NULL unavailable/failed. */
    private Boolean l2SetIfAbsent(K key, StoredEntry<V> entry, Duration ttl) {
        if (breaker != null && breaker.isOpen()) {
            return null;
        }
        try {
            return l2.setIfAbsent(key, entry, ttl);
        } catch (L2UnavailableException e) {
            return null;
        }
    }

    private boolean l2Evict(K key, Version version) {
        if (breaker != null && breaker.isOpen()) {
            return false;
        }
        try {
            l2.evict(key, version);
            return true;
        } catch (L2UnavailableException e) {
            return false;
        }
    }

    private boolean l2Clear() {
        if (breaker != null && breaker.isOpen()) {
            return false;
        }
        try {
            l2.clear();
            return true;
        } catch (L2UnavailableException e) {
            return false;
        }
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
