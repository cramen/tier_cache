package io.tiercache.internal;

import io.tiercache.CacheSettings;
import io.tiercache.CacheConfigurationException;
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
import io.tiercache.spi.TaggedWriteOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 * <p>Stale-while-revalidate (opt-in via {@code staleTtl}): an L2 entry
 * carrying a write timestamp is classified by age — fresh below the L2 TTL,
 * stale inside the window past it, a miss beyond. Stale entries are served
 * immediately (without warming L1, so an in-flight refresh is never
 * overwritten by the stale copy) and a single asynchronous revalidation per
 * key per instance
 * (claimed on the same in-flight map as singleflight) refreshes them through
 * the coordinated load path. A skipped refresh is not a source miss:
 * foreground demand promotes it to a bounded ordinary load, while stale
 * readers keep their immediate result. Background failures do not replace
 * an already served stale result. XFetch (opt-in via {@code xfetchEnabled})
 * adds a probabilistic early refresh on fresh L2 hits, driven by entry age and a per-cache EMA of
 * loader durations measured internally.
 *
 * <p>Hot-path discipline: a steady-state L1 hit performs exactly one
 * {@code LocalCache.get} plus one reference check, and allocates nothing.
 * With stale serving and XFetch off, the L2-hit path pays one extra boolean
 * check.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @param <K> key type
 * @param <V> value type
 * @since 0.1.0
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
    /** Smoothing factor of the per-cache loader-duration EMA. */
    static final double EMA_ALPHA = 0.125;
    /** Loader-duration EMA value before the first measured load. */
    static final long EMA_UNINITIALIZED = -1L;

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
    private final java.util.function.BooleanSupplier auxiliaryOpen;
    private final Executor revalidationExecutor;       // null = no async revalidation
    private final Duration staleTtl;
    private final boolean staleWindowEnabled;
    private final boolean xfetchEnabled;
    private final boolean ageTrackingEnabled;          // stale window or XFetch on
    /** Extra L1 retention window served stale while the breaker rejects L2 calls. */
    private final Duration degradationStaleTtl;
    private final boolean degradationStaleEnabled;
    private final long l2TtlMillis;
    private final long staleBoundaryMillis;            // l2TtlMillis + staleTtl
    private final double xfetchBetaNanos;
    private final TtlJitter jitter;
    /** EMA of loader durations in nanoseconds; updated on every load. */
    private final AtomicLong loaderDurationEmaNanos = new AtomicLong(EMA_UNINITIALIZED);
    private final Map<K, LoadClaim<K, V>> inflight = new ConcurrentHashMap<>();

    /**
     * Per-key invalidation barrier: the highest version this instance has
     * seen invalidated (including for keys absent from L1). Lives in a
     * bounded engine-side map; eviction of a barrier bumps {@link
     * #l1Generation} so a racing stale commit is refused instead of letting
     * the forgotten barrier reopen the race.
     */
    private static final int L1_META_MAX = 100_000;
    /**
     * Barrier-map expiry. Mutable ONLY as a test seam (accelerated expiry
     * in race tests); production wiring never touches it. Public solely
     * because the race tests live in another package — not for application
     * use.
     */
    public static Duration L1_META_EXPIRY = Duration.ofMinutes(10);
    private static final int L1_STRIPES = 64;

    private final L1BarrierMap<K> l1Metas;
    private final java.util.concurrent.atomic.AtomicLong recoveryGeneration = new java.util.concurrent.atomic.AtomicLong();
    private final Object[] l1Locks;
    /** Bumped when protective L1 state is forgotten (barrier eviction, evictAll). */
    private final AtomicLong l1Generation = new AtomicLong();

    /**
     * Legacy constructor: no coordination, no invalidation (used by tests).
     *
     * @param l1                  the L1 cache
     * @param l2                  the L2 cache
     * @param settings            the resolved cache settings
     * @param singleflightEnabled whether per-instance load coalescing is on
     * @since 0.1.0
     */
    public DefaultTierCache(LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled) {
        this("test", l1, l2, settings, singleflightEnabled, null, null, null, null);
    }

    /**
     * Constructor with rebuild coordination and invalidation, without a
     * circuit breaker or metrics.
     *
     * @param cacheName           the cache name (lock namespacing, logging)
     * @param l1                  the L1 cache
     * @param l2                  the L2 cache
     * @param settings            the resolved cache settings
     * @param singleflightEnabled whether per-instance load coalescing is on
     * @param lockProvider        rebuild-lock provider, or {@code null} for
     *                            no coordination
     * @param watchdog            lease-extension scheduler, or {@code null}
     * @param versionGenerator    write-version source, or {@code null} to
     *                            disable versioning/publishing
     * @param invalidation        the invalidation engine, or {@code null} for
     *                            single-node
     * @since 0.1.0
     */
    public DefaultTierCache(String cacheName, LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled,
            DistributedLockProvider lockProvider, ScheduledExecutorService watchdog,
            VersionGenerator versionGenerator, InvalidationHandler invalidation) {
        this(cacheName, l1, l2, settings, singleflightEnabled, lockProvider, watchdog,
                versionGenerator, invalidation, null, CacheMetricsListener.NOOP);
    }

    /**
     * Constructor with a circuit breaker and metrics, without a revalidation
     * executor.
     *
     * @param cacheName           the cache name (lock namespacing, logging)
     * @param l1                  the L1 cache
     * @param l2                  the L2 cache
     * @param settings            the resolved cache settings
     * @param singleflightEnabled whether per-instance load coalescing is on
     * @param lockProvider        rebuild-lock provider, or {@code null} for
     *                            no coordination
     * @param watchdog            lease-extension scheduler, or {@code null}
     * @param versionGenerator    write-version source, or {@code null} to
     *                            disable versioning/publishing
     * @param invalidation        the invalidation engine, or {@code null} for
     *                            single-node
     * @param breaker             the L2 circuit breaker, or {@code null} for
     *                            unguarded L2
     * @param metrics             the metrics listener
     * @since 0.1.0
     */
    public DefaultTierCache(String cacheName, LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled,
            DistributedLockProvider lockProvider, ScheduledExecutorService watchdog,
            VersionGenerator versionGenerator, InvalidationHandler invalidation,
            CircuitBreaker breaker, CacheMetricsListener metrics) {
        this(cacheName, l1, l2, settings, singleflightEnabled, lockProvider, watchdog,
                versionGenerator, invalidation, breaker, metrics, null);
    }

    /**
     * Full constructor. {@code revalidationExecutor} runs fire-and-forget
     * stale revalidations / XFetch refreshes; when {@code null}, stale
     * entries are still served but never revalidated (legacy wiring).
     *
     * @param cacheName            the cache name (lock namespacing, logging)
     * @param l1                   the L1 cache
     * @param l2                   the L2 cache
     * @param settings             the resolved cache settings
     * @param singleflightEnabled  whether per-instance load coalescing is on
     * @param lockProvider         rebuild-lock provider, or {@code null} for
     *                             no coordination
     * @param watchdog             lease-extension scheduler, or {@code null}
     * @param versionGenerator     write-version source, or {@code null} to
     *                             disable versioning/publishing
     * @param invalidation         the invalidation engine, or {@code null}
     *                             for single-node
     * @param breaker              the L2 circuit breaker, or {@code null} for
     *                             unguarded L2
     * @param metrics              the metrics listener
     * @param revalidationExecutor executor for async revalidation, or
     *                             {@code null}
     * @since 0.1.0
     */
    public DefaultTierCache(String cacheName, LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled,
            DistributedLockProvider lockProvider, ScheduledExecutorService watchdog,
            VersionGenerator versionGenerator, InvalidationHandler invalidation,
            CircuitBreaker breaker, CacheMetricsListener metrics, Executor revalidationExecutor) {
        this(cacheName, l1, l2, settings, singleflightEnabled, lockProvider, watchdog,
                versionGenerator, invalidation, breaker, metrics, revalidationExecutor,
                new TtlJitter());
    }

    /**
     * Full constructor with an explicit TTL jitter source. {@code jitter}
     * is a test seam for deterministic TTL spreads; production wiring uses
     * the overload above, which draws from ThreadLocalRandom.
     *
     * @param cacheName            the cache name (lock namespacing, logging)
     * @param l1                   the L1 cache
     * @param l2                   the L2 cache
     * @param settings             the resolved cache settings
     * @param singleflightEnabled  whether per-instance load coalescing is on
     * @param lockProvider         rebuild-lock provider, or {@code null} for
     *                             no coordination
     * @param watchdog             lease-extension scheduler, or {@code null}
     * @param versionGenerator     write-version source, or {@code null} to
     *                             disable versioning/publishing
     * @param invalidation         the invalidation engine, or {@code null}
     *                             for single-node
     * @param breaker              the L2 circuit breaker, or {@code null} for
     *                             unguarded L2
     * @param metrics              the metrics listener
     * @param revalidationExecutor executor for async revalidation, or
     *                             {@code null}
     * @param jitter               the TTL jitter source
     * @since 0.1.0
     */
    public DefaultTierCache(String cacheName, LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled,
            DistributedLockProvider lockProvider, ScheduledExecutorService watchdog,
            VersionGenerator versionGenerator, InvalidationHandler invalidation,
            CircuitBreaker breaker, CacheMetricsListener metrics, Executor revalidationExecutor,
            TtlJitter jitter) {
        this(cacheName, l1, l2, settings, singleflightEnabled, lockProvider, watchdog,
                versionGenerator, invalidation, breaker, metrics, revalidationExecutor, jitter, () -> true);
    }

    /** Internal factory wiring with an auxiliary admission gate. */
    public DefaultTierCache(String cacheName, LocalCache<K, V> l1, RemoteCache<K, V> l2,
            CacheSettings settings, boolean singleflightEnabled,
            DistributedLockProvider lockProvider, ScheduledExecutorService watchdog,
            VersionGenerator versionGenerator, InvalidationHandler invalidation,
            CircuitBreaker breaker, CacheMetricsListener metrics, Executor revalidationExecutor,
            TtlJitter jitter, java.util.function.BooleanSupplier auxiliaryOpen) {
        this.auxiliaryOpen = auxiliaryOpen;
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
        this.revalidationExecutor = revalidationExecutor;
        this.jitter = jitter;
        this.staleTtl = settings.staleTtl();
        this.staleWindowEnabled = staleTtl.toMillis() > 0;
        this.degradationStaleTtl = settings.degradationStaleTtl();
        this.degradationStaleEnabled = degradationStaleTtl.toNanos() > 0;
        this.xfetchEnabled = settings.xfetchEnabled();
        this.ageTrackingEnabled = staleWindowEnabled || xfetchEnabled;
        this.l2TtlMillis = settings.l2Ttl().toMillis();
        this.staleBoundaryMillis = l2TtlMillis + staleTtl.toMillis();
        this.xfetchBetaNanos = settings.xfetchBeta().toNanos();
        if (ageTrackingEnabled && revalidationExecutor == null) {
            log.warn("Cache '{}' has stale serving or XFetch enabled but no revalidation "
                    + "executor is wired; stale entries are served but never revalidated.",
                    cacheName);
        }
        this.l1Metas = new L1BarrierMap<>(L1_META_MAX, L1_META_EXPIRY,
                l1Generation::incrementAndGet);
        this.l1Locks = new Object[L1_STRIPES];
        for (int i = 0; i < L1_STRIPES; i++) {
            l1Locks[i] = new Object();
        }
    }

    @Override
    public V get(K key) {
        long g0 = l1Generation.get();
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            FreshnessSnapshot<V> snapshot = freshnessOf(key, entry);
            if (snapshot.freshness() == L1Freshness.FRESH) {
                metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L1_HIT);
                return snapshot.entry().isNullMarker() ? null : snapshot.entry().value();
            }
            // Logically expired under the degradation window: one classified
            // L2 read decides — converge on HIT, serve stale on REJECTED.
            L2Result<V> result = l2Read(key);
            if (result.read() == L2Read.HIT) {
                // Admitted read: converge through the normal age/SWR/XFetch
                // classification — a stale SWR frame is never warmed as fresh.
                if (ageTrackingEnabled) {
                    entry = classifyByAge(key, result.entry(), null, g0);
                    if (entry == null) {
                        metrics.onRequest(cacheName, CacheMetricsListener.Outcome.MISS);
                        return null;
                    }
                    metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
                    return entry.isNullMarker() ? null : entry.value();
                }
                warmL1(key, result.entry(), g0);
                metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
                return result.entry().isNullMarker() ? null : result.entry().value();
            }
            if (result.read() == L2Read.REJECTED
                    && snapshot.freshness() == L1Freshness.STALE_ALLOWED) {
                metrics.onRequest(cacheName, CacheMetricsListener.Outcome.STALE_DEGRADED);
                return snapshot.entry().isNullMarker() ? null : snapshot.entry().value();
            }
            metrics.onRequest(cacheName, CacheMetricsListener.Outcome.MISS);
            return null;
        }
        entry = l2Get(key);
        if (entry != null) {
            if (ageTrackingEnabled) {
                entry = classifyByAge(key, entry, null, g0);
                if (entry == null) {
                    metrics.onRequest(cacheName, CacheMetricsListener.Outcome.MISS);
                    return null;
                }
            } else {
                warmL1(key, entry, g0);
            }
            metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
            return entry.isNullMarker() ? null : entry.value();
        }
        metrics.onRequest(cacheName, CacheMetricsListener.Outcome.MISS);
        return null;
    }

    @Override
    public LookupResult<V> lookup(K key) {
        long g0 = l1Generation.get();
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            FreshnessSnapshot<V> snapshot = freshnessOf(key, entry);
            if (snapshot.freshness() == L1Freshness.FRESH) {
                metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L1_HIT);
                return toResult(snapshot.entry());
            }
            L2Result<V> result = l2Read(key);
            if (result.read() == L2Read.HIT) {
                if (ageTrackingEnabled) {
                    entry = classifyByAge(key, result.entry(), null, g0);
                    if (entry == null) {
                        metrics.onRequest(cacheName, CacheMetricsListener.Outcome.MISS);
                        return LookupResult.miss();
                    }
                    metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
                    return toResult(entry);
                }
                warmL1(key, result.entry(), g0);
                metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
                return toResult(result.entry());
            }
            if (result.read() == L2Read.REJECTED
                    && snapshot.freshness() == L1Freshness.STALE_ALLOWED) {
                metrics.onRequest(cacheName, CacheMetricsListener.Outcome.STALE_DEGRADED);
                return toResult(snapshot.entry());
            }
            metrics.onRequest(cacheName, CacheMetricsListener.Outcome.MISS);
            return LookupResult.miss();
        }
        entry = l2Get(key);
        if (entry != null) {
            if (ageTrackingEnabled) {
                entry = classifyByAge(key, entry, null, g0);
                if (entry == null) {
                    metrics.onRequest(cacheName, CacheMetricsListener.Outcome.MISS);
                    return LookupResult.miss();
                }
            } else {
                warmL1(key, entry, g0);
            }
            metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
            return toResult(entry);
        }
        metrics.onRequest(cacheName, CacheMetricsListener.Outcome.MISS);
        return LookupResult.miss();
    }

    @Override
    public V getOrCompute(K key, Function<? super K, ? extends V> loader) {
        long g0 = l1Generation.get();
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            FreshnessSnapshot<V> snapshot = freshnessOf(key, entry);
            if (snapshot.freshness() == L1Freshness.FRESH) {
                metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L1_HIT);
                return snapshot.entry().isNullMarker() ? null : snapshot.entry().value();
            }
            L2Result<V> result = l2Read(key);
            if (result.read() == L2Read.HIT) {
                if (ageTrackingEnabled) {
                    StoredEntry<V> classified = classifyByAge(key, result.entry(), loader, g0);
                    if (classified != null) {
                        metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
                        return classified.isNullMarker() ? null : classified.value();
                    }
                    // Past the SWR horizon: fall through to the loader path —
                    // a hard miss must never surface as a false null here.
                } else {
                    warmL1(key, result.entry(), g0);
                    metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
                    return result.entry().isNullMarker() ? null : result.entry().value();
                }
            }
            if (result.read() == L2Read.REJECTED
                    && snapshot.freshness() == L1Freshness.STALE_ALLOWED) {
                metrics.onRequest(cacheName, CacheMetricsListener.Outcome.STALE_DEGRADED);
                return snapshot.entry().isNullMarker() ? null : snapshot.entry().value();
            }
            // FAILED, MISS, or past the window: the loader fallback below.
        }
        entry = l2Get(key);
        if (entry != null) {
            if (ageTrackingEnabled) {
                entry = classifyByAge(key, entry, loader, g0);
            } else {
                warmL1(key, entry, g0);
            }
            if (entry != null) {
                metrics.onRequest(cacheName, CacheMetricsListener.Outcome.L2_HIT);
                return entry.isNullMarker() ? null : entry.value();
            }
        }
        if (!singleflightEnabled) {
            StoredEntry<V> result = loadPath(key, loader);
            metrics.onRequest(cacheName, result != null && !result.isNullMarker()
                    ? CacheMetricsListener.Outcome.LOAD : CacheMetricsListener.Outcome.MISS);
            return unwrap(result);
        }
        LoadClaim.Demand<K, V> demand = new LoadClaim.Demand<>(loader,
                System.nanoTime() + OVERALL_BUDGET.toNanos());
        LoadClaim<K, V> claim = new LoadClaim<>(demand);
        LoadClaim<K, V> existing = inflight.putIfAbsent(key, claim);
        if (existing == null) {
            return runForegroundClaim(key, claim, demand, true);
        }
        existing.requireResult(demand);
        metrics.onRequest(cacheName, CacheMetricsListener.Outcome.COALESCED);
        LoadClaim.Outcome<V> outcome = existing.result.join();
        if (!outcome.isSkipped()) {
            return unwrap(outcome.resultEntry());
        }
        return recoverSkippedRefresh(key, existing, demand);
    }

    /**
     * One map transition replaces a terminal skip or promotes its replacement.
     * The selected claim cannot skip again: foreground demand is registered
     * before the map transition ends. The old owner's finally uses identity
     * removal and cannot delete this replacement.
     */
    private V recoverSkippedRefresh(K key, LoadClaim<K, V> skipped,
            LoadClaim.Demand<K, V> demand) {
        LoadClaim<K, V> replacement = new LoadClaim<>(demand);
        LoadClaim<K, V> selected = inflight.compute(key, (ignored, current) -> {
            if (current == null || current == skipped || !current.requireResult(demand)) {
                return replacement;
            }
            return current;
        });
        if (selected == replacement) {
            // The original request was already counted as coalesced.
            return runForegroundClaim(key, replacement, demand, false);
        }
        return unwrap(selected.result.join().resultEntry());
    }

    private V runForegroundClaim(K key, LoadClaim<K, V> claim,
            LoadClaim.Demand<K, V> demand, boolean recordOutcome) {
        try {
            StoredEntry<V> loaded = loadPath(key, demand.loader(), demand.deadlineNanos());
            if (recordOutcome) {
                metrics.onRequest(cacheName, loaded != null && !loaded.isNullMarker()
                        ? CacheMetricsListener.Outcome.LOAD : CacheMetricsListener.Outcome.MISS);
            }
            claim.result.complete(LoadClaim.Outcome.result(loaded));
            return unwrap(loaded);
        } catch (RuntimeException | Error e) {
            claim.result.completeExceptionally(e);
            throw e;
        } finally {
            inflight.remove(key, claim);
        }
    }

    @Override
    public void put(K key, V value) {
        // Write order: L2 first, then L1, then publish. Overwrites any marker.
        long g0 = l1Generation.get();
        Version version = nextVersion();
        StoredEntry<V> entry = StoredEntry.ofValue(value, version);
        Boolean stored = l2ConditionalPut(key, entry, settings.l2Ttl(), version != null);
        if (stored == null) {
            // Degraded: L1 only, no publish (the journal has no row either).
            warmL1(key, entry, g0);
            return;
        }
        if (!stored) {
            // Lost version race: converge L1 to the current L2 entry.
            StoredEntry<V> current = l2Get(key);
            if (current != null) {
                warmL1(key, current, g0);
            } else {
                evictLocal(key);
            }
            return;
        }
        warmL1(key, entry, g0);
        publishStore(key, entry, version);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        // Atomic at L2; L1 warm-up and publish only for the winner.
        long g0 = l1Generation.get();
        Version version = nextVersion();
        Boolean won = l2SetIfAbsent(key, StoredEntry.ofValue(value, version), settings.l2Ttl());
        if (won == null) {
            // Degraded: per-instance atomicity on L1 only — the winner gets
            // the full commit (value, barrier, deadlines, extended
            // retention), the loser changes nothing.
            synchronized (l1LockFor(key)) {
                if (l1.get(key) != null) {
                    return false;
                }
                // Success is reported only for a really committed insert: a
                // refused commit (generation/barrier) is a lost race, never
                // a fabricated win.
                return commitL1(key, StoredEntry.ofValue(value, version),
                        jitter.apply(settings.l1ExpireAfterWrite(), settings.jitterAmplitude()),
                        g0);
            }
        }
        if (won) {
            StoredEntry<V> stored = StoredEntry.ofValue(value, version);
            warmL1(key, stored, g0);
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
        if (version != null) {
            applyInvalidateL1(key, version);
        } else {
            evictLocal(key);
        }
    }

    @Override
    public void evictAll() {
        Version version = nextVersion();
        if (l2Clear(version)) {
            publish(null, version, InvalidationMessage.Type.EVICT_ALL);
        }
        clearL1();
    }

    @Override
    public void putNull(K key) {
        Duration markerTtl = settings.nullPolicy().markerTtl();
        if (markerTtl == null) {
            return; // deny policy: nothing to store
        }
        long g0 = l1Generation.get();
        Version version = nextVersion();
        StoredEntry<V> marker = StoredEntry.nullMarker(version);
        metrics.onNullEntry(cacheName);
        storeVersioned(key, marker, markerTtl, version,
                jitter.apply(markerTtl, settings.jitterAmplitude()), g0);
    }

    @Override
    public void put(K key, V value, String... tags) {
        if (tags == null || tags.length == 0) {
            put(key, value);
            return;
        }
        // A missing SPI capability is a configuration error, including while
        // OPEN: it must not silently become a successful local-only write.
        if (versionGenerator != null && !l2.supportsTaggedWriteOutcomes()) {
            throw unsupportedTaggedWrite();
        }
        long g0 = l1Generation.get();
        Version version = nextVersion();
        StoredEntry<V> entry = StoredEntry.ofValue(value, version);
        if (breaker != null && breaker.isOpen()) {
            warmL1(key, entry, g0);
            return;
        }
        TaggedWriteOutcome outcome;
        try {
            outcome = l2.putTaggedIfNewer(key, entry, settings.l2Ttl(), tags);
        } catch (L2UnavailableException e) {
            warmL1(key, entry, g0);
            return;
        }
        if (outcome == TaggedWriteOutcome.UNSUPPORTED) {
            throw unsupportedTaggedWrite();
        }
        if (outcome == TaggedWriteOutcome.LOST) {
            StoredEntry<V> current = l2Get(key);
            if (current != null) {
                warmL1(key, current, g0);
            } else {
                evictLocal(key);
            }
            return;
        }
        warmL1(key, entry, g0);
        publishStore(key, entry, version);
    }

    private CacheConfigurationException unsupportedTaggedWrite() {
        return new CacheConfigurationException("Cache '" + cacheName
                + "' requires versioned tagged-write outcomes. Implement RemoteCache."
                + "supportsTaggedWriteOutcomes() and putTaggedIfNewer() in the custom transport.");
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
        applyInvalidateL1(typedKey, eventVersion);
    }

    @Override
    public void evictAllL1() {
        clearL1();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void applyUpdateL1(Object key, Object value, Version eventVersion) {
        K typedKey = (K) key;
        synchronized (l1LockFor(typedKey)) {
            Version highestSeen = l1Metas.get(typedKey) != null
                    ? l1Metas.get(typedKey).highestSeen() : null;
            // First reject: an UPDATE older than the barrier is stale.
            if (highestSeen != null && eventVersion.compareTo(highestSeen) < 0) {
                return;
            }
            StoredEntry<V> current = l1.get(typedKey);
            if (current != null && current.version() != null
                    && eventVersion.compareTo(current.version()) <= 0) {
                // No value change (idempotent replay), but the barrier still lifts.
                l1Metas.put(typedKey, maxVersion(eventVersion, highestSeen));
                return;
            }
            // One atomic step: lift the barrier AND install the payload (its
            // own version always passes — equality is not staleness).
            Duration ttl = jitter.apply(settings.l1ExpireAfterWrite(), settings.jitterAmplitude());
            long logicalDeadline = System.nanoTime() + ttl.toNanos();
            l1.put(typedKey, StoredEntry.ofValue((V) value, eventVersion),
                    degradationStaleEnabled ? ttl.plus(degradationStaleTtl) : ttl);
            l1Metas.put(typedKey, new L1BarrierMap.L1Meta(
                    maxVersion(eventVersion, highestSeen), logicalDeadline,
                    logicalDeadline + degradationStaleTtl.toNanos()));
        }
    }

    @Override
    public long recoveryGeneration() { return recoveryGeneration.get(); }

    @Override
    public long resetRecovery(long expectedGeneration) {
        if (!recoveryGeneration.compareAndSet(expectedGeneration, expectedGeneration + 1)) return -1;
        clearL1Contents();
        return expectedGeneration + 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public long applyRecovery(InvalidationMessage message, long expectedGeneration) {
        if (message.type() == InvalidationMessage.Type.EVICT_ALL) return resetRecovery(expectedGeneration);
        K key = (K) message.key();
        synchronized (l1LockFor(key)) {
            if (recoveryGeneration.get() != expectedGeneration) return -1;
            if (message.type() == InvalidationMessage.Type.UPDATE) {
                applyUpdateL1(key, message.payload(), message.version());
            } else {
                evictL1IfNewer(key, message.version());
            }
            return expectedGeneration;
        }
    }

    private Version nextVersion() {
        return versionGenerator != null ? versionGenerator.next() : null;
    }

    private Object l1LockFor(K key) {
        int hash = key == null ? 0 : key.hashCode() & 0x7FFF_FFFF;
        return l1Locks[hash % L1_STRIPES];
    }

    private static Version maxVersion(Version a, Version b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) >= 0 ? a : b;
    }

    /**
     * One atomic L1 commit — generation check, barrier check, value write
     * and barrier lift under the per-key stripe lock (no I/O inside).
     * Returns {@code false} when the write was refused: the generation
     * changed (protective state was forgotten mid-flight) or the barrier
     * already knows a strictly newer version. A refusal skips ONLY the L1
     * fill; the caller still returns the value it actually obtained.
     */
    private boolean commitL1(K key, StoredEntry<V> entry, Duration ttl, long generationAtStart) {
        synchronized (l1LockFor(key)) {
            // Read metadata FIRST: its lookup may itself expire a barrier and
            // bump the generation — that bump must be seen by the check
            // below, never after it.
            L1BarrierMap.L1Meta meta = l1Metas.get(key);
            if (generationAtStart != l1Generation.get()) {
                return false;
            }
            Version highestSeen = meta != null ? meta.highestSeen() : null;
            if (entry.version() != null && highestSeen != null
                    && entry.version().compareTo(highestSeen) < 0) {
                return false;
            }
            // Freshness deadlines are stamped with the ACTUAL jittered TTL;
            // physical retention additionally covers the degradation window.
            long logicalDeadline = System.nanoTime() + ttl.toNanos();
            long staleServeUntil = logicalDeadline + degradationStaleTtl.toNanos();
            l1.put(key, entry, degradationStaleEnabled ? ttl.plus(degradationStaleTtl) : ttl);
            l1Metas.put(key, new L1BarrierMap.L1Meta(
                    maxVersion(entry.version(), highestSeen), logicalDeadline, staleServeUntil));
            return true;
        }
    }

    /**
     * Atomic invalidation: lifts the barrier (even for absent keys) and
     * removes the value when the event supersedes it. Local versioned
     * evictions go through the same path, so a racing local write also
     * blocks a stale warm.
     */
    private void applyInvalidateL1(K key, Version eventVersion) {
        synchronized (l1LockFor(key)) {
            Version highestSeen = l1Metas.get(key) != null
                    ? l1Metas.get(key).highestSeen() : null;
            StoredEntry<V> entry = l1.get(key);
            if (entry != null && (entry.version() == null
                    || eventVersion.compareTo(entry.version()) > 0)) {
                l1.evict(key);
            }
            if (eventVersion != null) {
                l1Metas.put(key, maxVersion(eventVersion, highestSeen));
            }
        }
    }

    /**
     * Best-effort lock release: a release failure (Redis down) is a CLEANUP
     * issue, never a business error — the lease self-expires by TTL, and a
     * loaded value or the loader's own failure must never be overridden by
     * it. The token-checked release (Lua compare-and-delete) can never
     * remove another owner's lock.
     */
    private void releaseGuarded(DistributedLock lock, K key) {
        try {
            lock.release();
        } catch (RuntimeException e) {
            log.debug("Rebuild lock release failed for key '{}' in cache '{}'; "
                    + "the lease self-expires.", key, cacheName, e);
        }
    }

    /** Local unversioned evict: drops value and barrier together. */
    private void evictLocal(K key) {
        synchronized (l1LockFor(key)) {
            l1.evict(key);
            l1Metas.invalidate(key);
        }
    }

    /** Full local clear: generation bump first, then per-stripe ordering, then the clears. */
    private void clearL1() {
        recoveryGeneration.incrementAndGet();
        clearL1Contents();
    }

    private void clearL1Contents() {
        l1Generation.incrementAndGet();
        // Ordering point with in-flight per-key commits: a commit that
        // passed its generation check before the bump completes its write
        // first, and the clear below removes it; a commit after the bump is
        // refused by its own check.
        for (Object stripe : l1Locks) {
            synchronized (stripe) {
                // no-op: serialization only
            }
        }
        l1.clear();
        l1Metas.invalidateAll();
    }

    private void publish(Object key, Version version, InvalidationMessage.Type type) {
        if (auxiliaryOpen.getAsBoolean() && invalidation != null && version != null) {
            invalidation.onLocalWrite(cacheName, key, version, type);
        }
    }

    /** Publish for a stored entry: UPDATE (with payload) in update mode, else INVALIDATE. */
    private void publishStore(K key, StoredEntry<V> entry, Version version) {
        if (!auxiliaryOpen.getAsBoolean() || invalidation == null || version == null) {
            return;
        }
        if (settings.invalidationMode() == InvalidationMode.UPDATE && !entry.isNullMarker()) {
            invalidation.onLocalUpdate(cacheName, key, entry.value(), version);
        } else {
            invalidation.onLocalWrite(cacheName, key, version, InvalidationMessage.Type.INVALIDATE);
        }
    }

    // --- Stale serving / XFetch: age classification + async revalidation ---

    /**
     * Classifies an L2 hit by write age and warms L1 for entries that keep
     * being served as fresh. Returns the entry to serve, or {@code null} when
     * it is past its stale window and must be treated as a miss. Stale hits
     * deliberately do NOT warm L1: the in-flight revalidation publishes the
     * fresh value through the normal write path, and warming here could
     * overwrite it with the stale copy. Side effects: stale-hit metric +
     * revalidation trigger on stale hits, XFetch draw on fresh hits. A
     * {@code null} loader (plain {@code get}/{@code lookup}) serves stale but
     * cannot revalidate.
     */
    private StoredEntry<V> classifyByAge(K key, StoredEntry<V> entry,
            Function<? super K, ? extends V> loader, long generationAtStart) {
        if (!entry.hasWriteTimestamp()) {
            warmL1(key, entry, generationAtStart);
            return entry; // legacy frame: behaves exactly as before
        }
        long writeTimestamp = entry.writeTimestampMillis();
        long ageMillis = System.currentTimeMillis() - writeTimestamp;
        if (ageMillis < l2TtlMillis) {
            warmL1(key, entry, generationAtStart);
            if (xfetchEnabled && loader != null) {
                xfetchGate(key, loader, writeTimestamp, ageMillis);
            }
            return entry;
        }
        if (ageMillis < staleBoundaryMillis) {
            metrics.onStaleHit(cacheName);
            if (loader != null) {
                triggerRevalidation(key, loader, writeTimestamp);
            }
            return entry;
        }
        return null; // past the stale window (or window disabled): hard miss
    }

    /**
     * XFetch early-refresh draw on a fresh L2 hit:
     * {@code p = 1 - exp(-ageFraction * delta / beta)} with the current
     * loader-duration EMA as {@code delta}. Uninitialized EMA means
     * probability zero; a lost draw proceeds as a plain hit.
     */
    private void xfetchGate(K key, Function<? super K, ? extends V> loader,
            long writeTimestamp, long ageMillis) {
        long delta = loaderDurationEmaNanos.get();
        if (delta <= 0) {
            return; // no measured load yet (or zero-cost loader): probability 0
        }
        double ageFraction = (double) ageMillis / l2TtlMillis;
        double p = 1.0 - Math.exp(-ageFraction * delta / xfetchBetaNanos);
        if (p > 0.0 && ThreadLocalRandom.current().nextDouble() < p) {
            triggerRevalidation(key, loader, writeTimestamp);
        }
    }

    /**
     * Claims the in-flight slot for {@code key} and submits a fire-and-forget
     * revalidation. A lost claim race (a load or revalidation already in
     * flight) is a no-op. Never blocks the caller beyond the atomic claim and
     * the submit.
     */
    private void triggerRevalidation(K key, Function<? super K, ? extends V> loader,
            long servedWriteTimestamp) {
        if (!auxiliaryOpen.getAsBoolean() || revalidationExecutor == null) {
            return; // legacy wiring: stale keeps serving without revalidation
        }
        LoadClaim<K, V> claim = new LoadClaim<>(null);
        if (inflight.putIfAbsent(key, claim) != null) {
            return; // a load or revalidation for this key is already in flight
        }
        try {
            metrics.onRevalidationTriggered(cacheName);
            revalidationExecutor.execute(new RevalidationTask(key, loader, servedWriteTimestamp, claim));
        } catch (RuntimeException e) {
            // Executor rejected (saturated or shut down): complete the claim
            // first so waiters already joined on it fail fast instead of
            // hanging, then release the slot so a later read retries.
            claim.result.completeExceptionally(e);
            inflight.remove(key, claim);
            metrics.onRevalidationFailed(cacheName);
            log.warn("Revalidation for key '{}' in cache '{}' could not be submitted "
                    + "(executor saturated or shut down); the stale entry keeps serving "
                    + "and a later read will retry.", key, cacheName, e);
        }
    }

    /** A queued task must retire its claim when shutdown discards it. */
    public interface DiscardableTask extends Runnable { void discard(); }

    private final class RevalidationTask implements DiscardableTask {
        private final K key;
        private final Function<? super K, ? extends V> loader;
        private final long timestamp;
        private final LoadClaim<K, V> claim;
        private final java.util.concurrent.atomic.AtomicBoolean claimed = new java.util.concurrent.atomic.AtomicBoolean();
        RevalidationTask(K key, Function<? super K, ? extends V> loader, long timestamp, LoadClaim<K, V> claim) {
            this.key = key; this.loader = loader; this.timestamp = timestamp; this.claim = claim;
        }
        @Override public void run() {
            if (!auxiliaryOpen.getAsBoolean()) { discard(); return; }
            if (claimed.compareAndSet(false, true)) runRevalidation(key, loader, timestamp, claim);
        }
        @Override public void discard() {
            if (claimed.compareAndSet(false, true)) {
                inflight.remove(key, claim);
                claim.result.complete(LoadClaim.Outcome.skippedRefresh());
            }
        }
    }

    private void runRevalidation(K key, Function<? super K, ? extends V> loader,
            long servedWriteTimestamp, LoadClaim<K, V> claim) {
        try {
            LoadClaim.Outcome<V> refreshed = revalidate(key, loader, servedWriteTimestamp);
            if (refreshed.isSkipped()) {
                LoadClaim.Demand<K, V> demand = claim.skipOrForeground();
                if (demand != null) {
                    // Still the same local owner. A skipped acquisition used
                    // no loader budget; this one load path retains its normal
                    // two-execution bound and the original foreground deadline.
                    refreshed = LoadClaim.Outcome.result(
                            loadPath(key, demand.loader(), demand.deadlineNanos()));
                }
            }
            claim.result.complete(refreshed);
            metrics.onRevalidationCompleted(cacheName);
        } catch (RuntimeException | Error e) {
            claim.result.completeExceptionally(e);
            metrics.onRevalidationFailed(cacheName);
            log.warn("Revalidation failed for key '{}' in cache '{}'; the stale entry "
                    + "keeps serving until its window ends.", key, cacheName, e);
            if (e instanceof Error error) {
                throw error;
            }
        } finally {
            inflight.remove(key, claim);
        }
    }

    /**
     * The revalidation load: the same coordinated path as a miss (distributed
     * lock attempt, watchdog lease), with a write-time double-check instead of
     * a presence double-check — only a write <b>newer</b> than the one that was
     * served suppresses the reload. A lost lock race is not a failure: another
     * instance is refreshing, and the stale entry keeps serving.
     */
    private LoadClaim.Outcome<V> revalidate(K key, Function<? super K, ? extends V> loader,
            long servedWriteTimestamp) {
        long g0 = l1Generation.get();
        if (!auxiliaryOpen.getAsBoolean() || lockProvider == null || watchdog == null || !l2Available()) {
            // No coordination possible: the in-flight claim already bounds
            // this to one load per key per instance.
            return LoadClaim.Outcome.result(loadAndStore(key, loader));
        }
        DistributedLock lock;
        try { lock = tryLockGuarded(cacheName + ":" + key); }
        catch (LockProviderClosedException e) { return LoadClaim.Outcome.result(loadAndStore(key, loader)); }
        if (lock == null) {
            return LoadClaim.Outcome.skippedRefresh(); // no source absence was observed
        }
        try (LockScope scope = new LockScope(lock, key)) {
            StoredEntry<V> current = l2Get(key);
            if (current != null && current.hasWriteTimestamp()
                    && current.writeTimestampMillis() > servedWriteTimestamp) {
                // A newer write landed while we claimed the lock: converge, no load.
                warmL1(key, current, g0);
                return LoadClaim.Outcome.result(current);
            }
            return LoadClaim.Outcome.result(loadWithWatchdog(key, loader, scope));
        }
    }

    private void recordLoaderDuration(long nanos) {
        loaderDurationEmaNanos.updateAndGet(previous -> previous < 0
                ? nanos
                : previous + (long) (EMA_ALPHA * (nanos - previous)));
    }

    /**
     * Current EMA of loader durations in nanoseconds, or {@code -1} before
     * the first measured load. Diagnostics/testing; independent of any
     * metrics binding.
     *
     * @return the loader-duration EMA in nanoseconds, or {@code -1} if
     *         uninitialized
     * @since 0.1.0
     */
    public long loaderDurationEmaNanos() {
        return loaderDurationEmaNanos.get();
    }

    // --- Load path selection: coordinated when possible ---

    private StoredEntry<V> loadPath(K key, Function<? super K, ? extends V> loader) {
        return loadPath(key, loader, System.nanoTime() + OVERALL_BUDGET.toNanos());
    }

    private StoredEntry<V> loadPath(K key, Function<? super K, ? extends V> loader,
            long overallDeadline) {
        if (!auxiliaryOpen.getAsBoolean() || lockProvider == null || watchdog == null || !l2Available()) {
            // No coordination possible (or L2 down): per-instance load.
            return loadAndStore(key, loader);
        }
        return coordinatedLoad(key, loader, overallDeadline);
    }

    private StoredEntry<V> coordinatedLoad(K key, Function<? super K, ? extends V> loader,
            long overallDeadline) {
        String lockName = cacheName + ":" + key;
        long g0 = l1Generation.get();
        while (true) {
            if (System.nanoTime() >= overallDeadline) {
                log.warn("Rebuild coordination budget exhausted for key '{}' in cache '{}'; "
                        + "loading without coordination (possible stampede after repeated "
                        + "winner failures).", key, cacheName);
                return loadAndStore(key, loader);
            }
            DistributedLock lock;
            try { lock = tryLockGuarded(lockName); }
            catch (LockProviderClosedException e) { return loadAndStore(key, loader); }
            if (!l2Available()) {
                // L2 failed between the availability check and lock
                // acquisition: fall back to the per-instance load — after
                // releasing the lock we just took, or another node would
                // wait out the remaining lease for nothing.
                if (lock != null) {
                    releaseGuarded(lock, key);
                }
                return loadAndStore(key, loader);
            }
            if (lock != null) {
                try (LockScope scope = new LockScope(lock, key)) {
                    // Mandatory double-check: the value may have
                    // appeared while we were acquiring the lock.
                    StoredEntry<V> entry = readThrough(key, g0);
                    if (entry != null) {
                        return entry;
                    }
                    return loadWithWatchdog(key, loader, scope);
                }
            }
            long waitDeadline = Math.min(overallDeadline,
                    System.nanoTime() + WAIT_SLICE.toNanos());
            StoredEntry<V> appeared = awaitValue(key, waitDeadline);
            if (appeared != null) {
                warmL1(key, appeared, g0);
                return appeared;
            }
        }
    }

    /** Lock acquisition through the breaker: fast-fail when open. */
    private DistributedLock tryLockGuarded(String lockName) {
        if (!auxiliaryOpen.getAsBoolean()) throw new LockProviderClosedException();
        if (breaker != null && breaker.isOpen()) {
            return null;
        }
        try {
            return lockProvider.tryLock(lockName, LOCK_LEASE);
        } catch (L2UnavailableException e) {
            return null;
        }
    }

    /**
     * Re-read L1 then L2 (warming L1 on an L2 hit). A retained entry that
     * is past its logical freshness is NOT a hit — the load path continues
     * to the loader, so a healthy Redis never serves stale data. Returns
     * null on full miss.
     */
    private StoredEntry<V> readThrough(K key, long generationAtStart) {
        StoredEntry<V> entry = l1.get(key);
        if (entry != null) {
            FreshnessSnapshot<V> snapshot = freshnessOf(key, entry);
            if (snapshot.freshness() == L1Freshness.FRESH) {
                return snapshot.entry();
            }
        }
        entry = l2Get(key);
        if (entry != null) {
            warmL1(key, entry, generationAtStart);
        }
        return entry;
    }

    private final class LockScope implements AutoCloseable {
        final DistributedLock lock;
        final K key;
        ScheduledFuture<?> extension;
        boolean retired;
        LockScope(DistributedLock lock, K key) { this.lock = lock; this.key = key; }
        @Override public void close() {
            if (retired) return;
            retired = true;
            if (extension != null) extension.cancel(false);
            releaseGuarded(lock, key);
        }
    }

    private StoredEntry<V> loadWithWatchdog(K key, Function<? super K, ? extends V> loader,
            LockScope scope) {
        if (!auxiliaryOpen.getAsBoolean()) {
            scope.close();
            return loadAndStore(key, loader);
        }
        long periodMillis = LOCK_LEASE.toMillis() / 3;
        try {
            scope.extension = watchdog.scheduleAtFixedRate(
                    () -> { if (auxiliaryOpen.getAsBoolean()) scope.lock.extend(LOCK_LEASE); },
                    periodMillis, periodMillis, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            scope.close();
            if (auxiliaryOpen.getAsBoolean() && !watchdog.isShutdown()) throw e;
        }
        return loadAndStore(key, loader);
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
     * Loads and stores the result. The store version is minted BEFORE the
     * loader runs (claim time): a write or versioned eviction landing
     * during the load carries a newer version and wins the conditional
     * store. A loss to a newer L2 entry converges to that entry; a loss to
     * a tombstone triggers at most one bounded reload (never a false
     * "not found", never a recursive claim wait).
     *
     * @return the entry now logically present (a null-marker under
     *         {@code allow}), or {@code null} if nothing was stored
     */
    private StoredEntry<V> loadAndStore(K key, Function<? super K, ? extends V> loader) {
        return loadAndStore(key, loader, 0);
    }

    private StoredEntry<V> loadAndStore(K key, Function<? super K, ? extends V> loader,
            int attempt) {
        long g0 = l1Generation.get();
        Version version = nextVersion(); // claim time: before the loader runs
        long loadStart = System.nanoTime();
        V loaded;
        try {
            loaded = loader.apply(key);
        } finally {
            recordLoaderDuration(System.nanoTime() - loadStart);
        }
        if (loaded == null) {
            Duration markerTtl = settings.nullPolicy().markerTtl();
            if (markerTtl == null) {
                // deny policy: a miss stays uncached — nothing stored,
                // nothing to fence
                return null;
            }
            // Null-marker stored in both levels, jittered like any TTL.
            StoredEntry<V> marker = StoredEntry.nullMarker(version);
            metrics.onNullEntry(cacheName);
            StoredEntry<V> effective = storeVersioned(key, marker, markerTtl, version,
                    jitter.apply(markerTtl, settings.jitterAmplitude()), g0);
            return effective != null ? effective : maybeReload(key, loader, attempt, marker);
        }
        StoredEntry<V> entry = StoredEntry.ofValue(loaded, version);
        StoredEntry<V> effective = storeVersioned(key, entry, settings.l2Ttl(), version,
                null, g0);
        return effective != null ? effective : maybeReload(key, loader, attempt, entry);
    }

    /**
     * The load lost to an eviction (no newer value is visible). The loaded
     * value is not garbage — {@code cache.evict} does not delete from the
     * source — so attempt ONE bounded reload with a fresh claim version;
     * a second loss returns the reload's own result without re-caching it.
     */
    private StoredEntry<V> maybeReload(K key, Function<? super K, ? extends V> loader,
            int attempt, StoredEntry<V> loadedEntry) {
        if (attempt > 0) {
            return loadedEntry;
        }
        return loadAndStore(key, loader, 1);
    }

    /**
     * L2 store + L1 warm + publish, version-conditional when versioning is
     * on. Returns the EFFECTIVE entry: the stored one on success, the
     * converged current entry on a lost race, or {@code null} when the
     * store lost to a tombstone/absence (the caller then decides on a
     * bounded reload).
     */
    private StoredEntry<V> storeVersioned(K key, StoredEntry<V> entry, Duration l2Ttl,
            Version version, Duration l1TtlOverride, long generationAtStart) {
        Boolean stored = l2ConditionalPut(key, entry, l2Ttl, version != null);
        if (stored == null) {
            commitL1(key, entry, l1TtlOverride != null ? l1TtlOverride
                    : jitter.apply(settings.l1ExpireAfterWrite(), settings.jitterAmplitude()),
                    generationAtStart);
            return entry; // degraded: L1 only
        }
        if (!stored) {
            StoredEntry<V> current = l2Get(key);
            if (current != null) {
                warmL1(key, current, generationAtStart);
                return current;
            }
            evictLocal(key);
            return null; // lost to a tombstone/absence
        }
        if (l1TtlOverride != null) {
            commitL1(key, entry, l1TtlOverride, generationAtStart);
        } else {
            warmL1(key, entry, generationAtStart);
        }
        publishStore(key, entry, version);
        return entry;
    }

    /**
     * Commit-warms with the generation captured at the operation's start:
     * a generation change or a newer barrier skips ONLY the L1 fill. There
     * is deliberately no generation-less overload: every call site must
     * show where its generation was captured, so a late capture is visible
     * to the compiler (and the reviewer).
     */
    private void warmL1(K key, StoredEntry<V> entry, long generationAtStart) {
        commitL1(key, entry,
                jitter.apply(settings.l1ExpireAfterWrite(), settings.jitterAmplitude()),
                generationAtStart);
    }

    /**
     * True while the L2 circuit breaker is open (L1-only mode).
     *
     * @return {@code true} while this cache runs degraded on L1 only
     * @since 0.1.0
     */
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

    /** Classified L2 read outcome for the permit-based degradation rule. */
    private enum L2Read {
        HIT, MISS, REJECTED, FAILED
    }

    private record L2Result<V>(L2Read read, StoredEntry<V> entry) {
    }

    /**
     * One classified L2 read — the breaker permit is acquired exactly once:
     * HIT/MISS on an admitted call, REJECTED when the breaker denies it
     * (OPEN, or HALF_OPEN with no probe permit left), FAILED on a call that
     * errored.
     */
    private L2Result<V> l2Read(K key) {
        if (breaker != null && breaker.isOpen()) {
            return new L2Result<>(L2Read.REJECTED, null);
        }
        long start = System.nanoTime();
        Object span = metrics.onL2OperationStart(cacheName, "get");
        boolean hit = false;
        try {
            StoredEntry<V> result = l2.get(key);
            hit = result != null;
            metrics.onLatency(cacheName, CacheMetricsListener.Level.L2, System.nanoTime() - start);
            return result != null ? new L2Result<>(L2Read.HIT, result)
                    : new L2Result<>(L2Read.MISS, null);
        } catch (L2UnavailableException e) {
            return new L2Result<>(
                    e == L2UnavailableException.OPEN ? L2Read.REJECTED : L2Read.FAILED, null);
        } finally {
            metrics.onL2OperationEnd(cacheName, "get", hit, span);
        }
    }

    /** Freshness of a physically present L1 entry under the degradation window. */
    private enum L1Freshness {
        /** Within the logical deadline (or the window is off). */
        FRESH,
        /** Past the logical deadline but within the stale-serving horizon. */
        STALE_ALLOWED,
        /** Past the horizon (or unknown metadata). Never stale-served. */
        EXPIRED
    }

    /**
     * Classifies a physically present L1 entry by its stamped deadlines,
     * atomically under the per-key stripe lock: a fresh access slides the
     * deadlines (and the physical retention) only when the metadata is
     * still the one just read — a concurrent replace can never inherit
     * another value's TTL. A stale access never moves anything.
     */
    /**
     * The coherent result of a freshness check: the CURRENT L1 entry (re-read
     * under the stripe lock, never a stale caller-side copy) plus its
     * freshness classification. Callers must use {@link #entry()} for every
     * value they serve — the caller-side read may already be superseded.
     */
    private record FreshnessSnapshot<V>(StoredEntry<V> entry, L1Freshness freshness) {
    }

    private FreshnessSnapshot<V> freshnessOf(K key, StoredEntry<V> entry) {
        if (!degradationStaleEnabled) {
            return new FreshnessSnapshot<>(entry, L1Freshness.FRESH);
        }
        synchronized (l1LockFor(key)) {
            // Coherent snapshot under the lock: the entry is re-read here,
            // so a completed concurrent write can never be overwritten by a
            // stale caller-side read. The barrier metadata belongs to the
            // same commit, so value and metadata always describe each other.
            entry = l1.get(key);
            L1BarrierMap.L1Meta meta = l1Metas.get(key);
            if (entry == null) {
                // The value was evicted or removed concurrently: never hand
                // out a null entry (a FRESH classification would NPE the
                // caller). The protective barrier metadata stays untouched;
                // the caller continues to the normal L2/loader path.
                return new FreshnessSnapshot<>(null, L1Freshness.EXPIRED);
            }
            if (meta == null || meta.logicalDeadlineNanos() == 0L) {
                return new FreshnessSnapshot<>(entry, L1Freshness.EXPIRED);
            }
            long now = System.nanoTime();
            if (now <= meta.logicalDeadlineNanos()) {
                java.time.Duration accessTtl = settings.l1ExpireAfterAccess();
                if (accessTtl != null && entry != null && l1Metas.get(key) == meta) {
                    // Fresh access: slide freshness, the stale horizon AND
                    // the physical retention — identity-checked, so a
                    // concurrently replaced value keeps its own deadlines.
                    long logical = now + accessTtl.toNanos();
                    l1Metas.put(key, new L1BarrierMap.L1Meta(meta.highestSeen(), logical,
                            logical + degradationStaleTtl.toNanos()));
                    l1.put(key, entry, accessTtl.plus(degradationStaleTtl));
                }
                return new FreshnessSnapshot<>(entry, L1Freshness.FRESH);
            }
            return new FreshnessSnapshot<>(entry,
                    now <= meta.staleServeUntilNanos() ? L1Freshness.STALE_ALLOWED
                            : L1Freshness.EXPIRED);
        }
    }

    /** @return TRUE stored, FALSE lost a version race, NULL unavailable/failed. */
    private Boolean l2ConditionalPut(K key, StoredEntry<V> entry, Duration ttl, boolean conditional) {
        if (breaker != null && breaker.isOpen()) {
            return null;
        }
        try {
            if (staleWindowEnabled) {
                // Refreshed entries keep their stale window (extended frame,
                // physical expiry ttl + staleTtl).
                return conditional ? l2.putIfNewer(key, entry, ttl, staleTtl)
                        : putPlainWithStaleWindow(key, entry, ttl);
            }
            return conditional ? l2.putIfNewer(key, entry, ttl) : putPlain(key, entry, ttl);
        } catch (L2UnavailableException e) {
            return null;
        }
    }

    private boolean putPlain(K key, StoredEntry<V> entry, Duration ttl) {
        l2.put(key, entry, ttl);
        return true;
    }

    private boolean putPlainWithStaleWindow(K key, StoredEntry<V> entry, Duration ttl) {
        l2.put(key, entry, ttl, staleTtl);
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

    private boolean l2Clear(Version version) {
        if (breaker != null && breaker.isOpen()) {
            return false;
        }
        try {
            l2.clear(version);
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
