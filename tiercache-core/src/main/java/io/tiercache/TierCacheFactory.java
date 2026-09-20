package io.tiercache;

import io.tiercache.internal.CacheConfigValidator;
import io.tiercache.internal.BreakerLockProvider;
import io.tiercache.internal.CircuitBreaker;
import io.tiercache.internal.CircuitBreakerRemoteCache;
import io.tiercache.internal.CaffeineLocalCache;
import io.tiercache.internal.DefaultAsyncTierCache;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.internal.TtlJitter;
import io.tiercache.spi.DistributedLockProvider;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.DegradationListener;
import io.tiercache.spi.InvalidationHandler;
import io.tiercache.spi.InvalidationEventListener;

import java.util.function.Function;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.LockProviderSource;
import io.tiercache.spi.RemoteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiFunction;

/**
 * Builds {@link TierCache} instances from global defaults plus per-cache
 * overrides. Configuration is validated at {@link Builder#build()}
 * time — an invalid configuration aborts initialization before any cache
 * serves traffic (fail-fast startup validation).
 *
 * <p>The factory owns a daemon watchdog scheduler used for rebuild-lock
 * lease extension and a shared daemon executor used for fire-and-forget
 * revalidation and the async view's offloaded operations; close the
 * factory when done.
 *
 * <p>Consistency model: caches built here are eventually consistent;
 * no strong-consistency guarantees are given or implied.
 *
 * @since 0.1.0
 */
public final class TierCacheFactory implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TierCacheFactory.class);

    private final CacheSettings defaults;
    private final Map<String, CacheSettings> caches;
    private final RemoteCache<Object, Object> remoteCache;
    private final Function<String, ? extends RemoteCache<?, ?>> remoteCacheFactory; // null = single shared L2
    private final BiFunction<String, CacheSettings, LocalCache<?, ?>> localCacheFactory;
    private final boolean singleflightEnabled;
    private final boolean coordinationEnabled;
    private final DistributedLockProvider lockProvider;
    private final ScheduledExecutorService watchdog;
    private final VersionGenerator versionGenerator;
    private final InvalidationHandler invalidation; // null = single-node
    private final CircuitBreaker breaker;           // null = unguarded L2 (opt-out)
    private final CacheMetricsListener metricsListener;
    private final TtlJitter jitter;
    private final java.util.concurrent.ExecutorService revalidationExecutor;
    private final java.util.concurrent.ExecutorService asyncExecutor;
    private final Map<String, TierCache<?, ?>> liveCaches = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, AsyncTierCache<?, ?>> liveAsyncCaches = new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * Single ordering point for async-view publication and the factory's
     * CLOSED transition: {@link #asyncCache(String)} performs its closed
     * check, creation and memoization inside it, and {@link #close()}
     * sets {@link #closed} and snapshots the live views under the same
     * lock — so a view is never published after the drain snapshot.
     */
    private final Object factoryLifecycleLock = new Object();
    /** Set under {@link #factoryLifecycleLock} by {@link #close()}. */
    private boolean closed;

    private TierCacheFactory(Builder builder) {
        this.defaults = builder.defaults;
        RemoteCache<Object, Object> rawRemoteCache = builder.remoteCache;
        this.remoteCacheFactory = builder.remoteCacheFactory;
        this.localCacheFactory = builder.localCacheFactory;
        this.singleflightEnabled = builder.singleflightEnabled;
        this.coordinationEnabled = builder.coordinationEnabled;
        this.caches = new LinkedHashMap<>();
        builder.overrides.forEach((name, override) -> caches.put(name, override.resolve(defaults)));

        // Fail-fast validation of every configured cache.
        CacheConfigValidator.validate("<global defaults>", defaults);
        caches.forEach(CacheConfigValidator::validate);

        DistributedLockProvider provider = builder.lockProvider;
        if (provider == null && coordinationEnabled
                && rawRemoteCache instanceof LockProviderSource source) {
            provider = source.lockProvider();
        }

        if (!coordinationEnabled) {
            log.warn("Distributed rebuild coordination disabled by explicit opt-in. "
                    + "Concurrent misses of one key across instances will each run the loader "
                    + "(cluster-wide stampede risk).");
        } else if (provider == null) {
            log.warn("No distributed lock provider available for the configured L2. "
                    + "Falling back to per-instance coalescing only: up to one loader execution "
                    + "per instance per rebuild round.");
        }

        this.watchdog = coordinationEnabled && provider != null
                ? Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("tiercache-watchdog"))
                : null;
        // Fire-and-forget revalidations (stale-while-revalidate / XFetch):
        // small bounded pool of its own so background churn never starves
        // latency-sensitive async API work; overflow revalidations are
        // rejected loudly (AbortPolicy) so the submit site can release the
        // in-flight claim — a silent discard would leak the claim and hang
        // the next reader of the key. A dropped refresh is safe: the stale
        // entry keeps serving until its window ends.
        this.revalidationExecutor = new java.util.concurrent.ThreadPoolExecutor(
                2, 2, 0L, java.util.concurrent.TimeUnit.MILLISECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(1_000),
                new DaemonThreadFactory("tiercache-revalidation"),
                new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
        // Async API executor: bounded by design (see the async-api spec).
        // Submissions past the queue fail their CompletionStage via
        // RejectedExecutionException rather than growing threads unbounded.
        int asyncThreads = builder.asyncExecutorThreads > 0
                ? builder.asyncExecutorThreads
                : Math.max(4, Runtime.getRuntime().availableProcessors());
        this.asyncExecutor = new java.util.concurrent.ThreadPoolExecutor(
                asyncThreads, asyncThreads, 0L, java.util.concurrent.TimeUnit.MILLISECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(10_000),
                new DaemonThreadFactory("tiercache-async"),
                new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());

        this.versionGenerator = new VersionGenerator();
        this.invalidation = builder.invalidationFactory != null
                ? builder.invalidationFactory.apply(versionGenerator)
                : null;
        if (this.invalidation != null) {
            this.invalidation.setEventListener(builder.invalidationEventListener);
        }
        this.metricsListener = builder.metricsListener;
        this.jitter = builder.jitter;

        DegradationListener degradationListener = builder.degradationListener;
        if (builder.circuitBreakerEnabled) {
            this.breaker = new CircuitBreaker(builder.breakerConfig, new CircuitBreaker.Listener() {
                @Override
                public void onOpen() {
                    log.warn("L2 circuit breaker OPEN: cache runs L1-only. Cross-instance "
                            + "atomicity (putIfAbsent, rebuild coordination) is per-instance "
                            + "until recovery.");
                    degradationListener.onDegraded();
                }

                @Override
                public void onClose() {
                    // Recovery: replay missed invalidations BEFORE we report
                    // recovery; L1 is never flushed here.
                    if (invalidation != null) {
                        invalidation.onL2Recovery();
                    }
                    log.info("L2 circuit breaker CLOSED: L2 recovered, missed invalidations replayed.");
                    degradationListener.onRecovered();
                }
            });
            if (rawRemoteCache != null) {
                rawRemoteCache = new CircuitBreakerRemoteCache<>(rawRemoteCache, breaker);
            }
            if (provider != null) {
                provider = new BreakerLockProvider(provider, breaker);
            }
        } else {
            this.breaker = null;
            log.warn("L2 circuit breaker disabled by explicit opt-in. Redis failures will "
                    + "propagate into cache operations (cascade-failure risk).");
        }
        this.remoteCache = rawRemoteCache;
        this.lockProvider = provider;
    }

    /**
     * Starts a new factory configuration.
     *
     * @return a fresh builder with all protections on their safe defaults
     * @since 0.1.0
     */
    public static Builder builder() {
        return new Builder();
    }


    /**
     * Returns the named cache. The same instance is returned for repeated
     * calls with the same name: a cache's L1 is shared, not duplicated.
     *
     * @param <K>  key type
     * @param <V>  value type
     * @param name the cache name; resolved against the per-cache overrides
     *             given to the builder, falling back to the global defaults
     * @return the cache instance for {@code name}; never {@code null}
     * @since 0.1.0
     */
    @SuppressWarnings("unchecked")
    public <K, V> TierCache<K, V> getCache(String name) {
        return (TierCache<K, V>) liveCaches.computeIfAbsent(name, n -> {
            CacheSettings settings = caches.getOrDefault(n, defaults);
            LocalCache<K, V> l1 = (LocalCache<K, V>) localCacheFactory.apply(n, settings);
            DefaultTierCache<K, V> cache = new DefaultTierCache<>(n, l1,
                    (RemoteCache<K, V>) l2For(n), settings, singleflightEnabled,
                    coordinationEnabled ? lockProvider : null, watchdog, versionGenerator, invalidation,
                    breaker, metricsListener, revalidationExecutor, jitter);
            if (invalidation != null) {
                invalidation.registerTarget(n, cache);
            }
            return cache;
        });
    }

    /**
     * Returns the async (non-blocking) view of the named cache. The
     * factory form is the accessor by design: the view's operations run
     * on the factory's bounded daemon executor (sized via
     * {@link Builder#asyncExecutorThreads(int)}), so the owner of the
     * executor hands out the view. Memoized alongside {@link #getCache} —
     * repeated calls with the same name return the same view over the
     * same underlying cache (shared L1, singleflight state, metrics).
     * The executor is bounded with a bounded handoff queue: under
     * saturation, submissions fail their returned {@code CompletionStage}
     * with {@link java.util.concurrent.RejectedExecutionException} rather
     * than growing threads without bound. Closing the factory shuts the
     * executor down; creating a view after the close began is rejected
     * with {@link IllegalStateException}, and async operations submitted
     * afterwards fail their stage.
     *
     * @param <K>  key type
     * @param <V>  value type
     * @param name the cache name
     * @return the async view of the cache for {@code name}; never {@code null}
     * @throws IllegalStateException if the factory is closed
     * @since 0.3.0
     */
    @SuppressWarnings("unchecked")
    public <K, V> AsyncTierCache<K, V> asyncCache(String name) {
        synchronized (factoryLifecycleLock) {
            if (closed) {
                throw new IllegalStateException("TierCacheFactory is closed");
            }
            viewCreationProbe.run();
            return (AsyncTierCache<K, V>) liveAsyncCaches.computeIfAbsent(name,
                    n -> new DefaultAsyncTierCache<>(getCache(n), asyncExecutor));
        }
    }

    /**
     * Test hook: invoked inside the factory lifecycle lock during every
     * {@link #asyncCache(String)} call, letting tests park view creation at
     * the exact publication boundary. No-op in production; not for
     * application use.
     */
    static volatile Runnable viewCreationProbe = () -> {
    };

    /**
     * Resolves the L2 for a cache name: the single shared instance, or a
     * per-name instance from {@link Builder#remoteCacheFactory}. Called
     * inside {@code liveCaches.computeIfAbsent}, so a factory-supplied L2
     * is created once per cache name — the same memoization discipline as
     * {@link #getCache} itself. Per-name instances are wrapped with the
     * circuit breaker here (the shared instance is wrapped at construction).
     */
    @SuppressWarnings("unchecked")
    private RemoteCache<?, ?> l2For(String name) {
        if (remoteCacheFactory == null) {
            return remoteCache;
        }
        RemoteCache<?, ?> l2 = Objects.requireNonNull(remoteCacheFactory.apply(name),
                () -> "remoteCacheFactory returned null for cache '" + name + "'");
        return breaker != null
                ? new CircuitBreakerRemoteCache<>((RemoteCache<Object, Object>) l2, breaker)
                : l2;
    }

    /**
     * True while the L2 circuit breaker is open (L1-only degraded mode).
     *
     * @return {@code true} while the cache runs degraded on L1 only
     * @since 0.1.0
     */
    public boolean isDegraded() {
        return breaker != null && breaker.isOpen();
    }

    /**
     * Current state of the L2 circuit breaker machine.
     *
     * <p>Distinct from {@link #isDegraded()}: degradation is the
     * business-facing "L1-only" flag, while this exposes the breaker machine
     * itself — including {@link BreakerState#HALF_OPEN} recovery probing,
     * during which {@code isDegraded()} is already {@code false}. Returns
     * {@link BreakerState#CLOSED} when the breaker is disabled (L2 calls are
     * never rejected).
     *
     * @return the current breaker state; never {@code null}
     * @since 0.1.0
     */
    public BreakerState breakerState() {
        return breaker == null ? BreakerState.CLOSED : breaker.state();
    }

    /**
    /**
     * Shuts down the factory: outstanding async operations handed out
     * through {@link #asyncCache(String)} are failed with
     * {@link java.util.concurrent.CancellationException}, the revalidation
     * executor and the watchdog scheduler are stopped and the invalidation
     * engine is closed. Caches already obtained remain usable but lose
     * lease extension for in-flight coordination, and async operations
     * submitted afterwards are rejected.
     *
     * @since 0.1.0
     */
    @Override
    public void close() {
        java.util.List<AsyncTierCache<?, ?>> views;
        synchronized (factoryLifecycleLock) {
            closed = true;
            views = new java.util.ArrayList<>(liveAsyncCaches.values());
        }
        // Draining happens outside the lock: completing stages may run
        // user callbacks.
        views.forEach(view ->
                ((io.tiercache.internal.DefaultAsyncTierCache<?, ?>) view).closeOutstanding());
        revalidationExecutor.shutdownNow();
        asyncExecutor.shutdownNow();
        if (watchdog != null) {
            watchdog.shutdownNow();
        }
        if (invalidation != null) {
            invalidation.close();
        }
    }

    private static final class DaemonThreadFactory implements ThreadFactory {
        private final String name;

        DaemonThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        }
    }

    /**
     * Configuration for a {@link TierCacheFactory}. All failure-mode
     * protections (singleflight, distributed rebuild coordination, circuit
     * breaker, TTL jitter) are on by default; disabling any of them is an
     * explicit opt-in and is logged as a risk.
     *
     * @since 0.1.0
     */
    public static final class Builder {

        private CacheSettings defaults = CacheSettings.defaults();
        private final Map<String, CacheOverride> overrides = new LinkedHashMap<>();
        private RemoteCache<Object, Object> remoteCache;
        private Function<String, ? extends RemoteCache<?, ?>> remoteCacheFactory;
        private BiFunction<String, CacheSettings, LocalCache<?, ?>> localCacheFactory =
                (name, settings) -> new CaffeineLocalCache<>(settings);
        private boolean singleflightEnabled = true;
        private boolean coordinationEnabled = true;
        private DistributedLockProvider lockProvider;
        private Function<VersionGenerator, InvalidationHandler> invalidationFactory;
        private InvalidationEventListener invalidationEventListener = InvalidationEventListener.NOOP;
        private boolean circuitBreakerEnabled = true;
        private CircuitBreaker.Config breakerConfig = CircuitBreaker.Config.defaults();
        private DegradationListener degradationListener = DegradationListener.NOOP;
        private CacheMetricsListener metricsListener = CacheMetricsListener.NOOP;
        private TtlJitter jitter = new TtlJitter();
        private int asyncExecutorThreads; // 0 = default max(4, availableProcessors)

        /**
         * Creates a builder with all protections on their safe defaults.
         *
         * @since 0.1.0
         */
        public Builder() {
        }

        /**
         * Sets the global default settings every named cache inherits.
         *
         * @param defaults the global defaults; must not be {@code null}
         * @return this builder
         * @since 0.1.0
         */
        public Builder defaults(CacheSettings defaults) {
            this.defaults = Objects.requireNonNull(defaults, "defaults");
            return this;
        }

        /**
         * Registers per-cache overrides for the cache named {@code name};
         * fields left unset in {@code override} inherit the global defaults.
         *
         * @param name     the cache name; must not be {@code null}
         * @param override the overrides for that cache; must not be
         *                 {@code null}
         * @return this builder
         * @since 0.1.0
         */
        public Builder cache(String name, CacheOverride override) {
            overrides.put(Objects.requireNonNull(name, "name"), Objects.requireNonNull(override, "override"));
            return this;
        }

        /**
         * The L2 implementation shared by all caches. Required unless
         * {@link #remoteCacheFactory} is used; the two are mutually
         * exclusive. If it implements {@link LockProviderSource}, the
         * rebuild-lock provider is derived automatically unless set
         * explicitly.
         *
         * <p><b>Shared namespace:</b> every named cache stores through
         * this one instance, so the same key in two named caches collides
         * in L2 (and one cache's {@code evictAll} may wipe another cache's
         * entries when the transport prefixes keys by instance). Use
         * {@link #remoteCacheFactory} for per-cache key-space isolation.
         *
         * @param remoteCache the shared L2; must not be {@code null}
         * @return this builder
         * @since 0.1.0
         */
        @SuppressWarnings("unchecked")
        public Builder remoteCache(RemoteCache<?, ?> remoteCache) {
            this.remoteCache = (RemoteCache<Object, Object>) Objects.requireNonNull(remoteCache, "remoteCache");
            return this;
        }

        /**
         * Supplies the L2 per cache name, for key-space isolation between
         * named caches: the factory resolves one instance per name
         * (memoized with the same discipline as
         * {@link TierCacheFactory#getCache}), so the same key in two caches
         * never collides in L2 and each cache's {@code evictAll} is scoped
         * to its own instance. Mutually exclusive with
         * {@link #remoteCache}.
         *
         * <p>Note: {@link LockProviderSource} auto-derivation applies only
         * to the single-instance form; with a factory, set
         * {@link #lockProvider} explicitly when distributed rebuild
         * coordination is needed.
         *
         * @param factory maps a cache name to its L2 instance; must not be
         *                {@code null} and must not return {@code null}
         * @return this builder
         * @since 0.2.0
         */
        public Builder remoteCacheFactory(Function<String, ? extends RemoteCache<?, ?>> factory) {
            this.remoteCacheFactory = Objects.requireNonNull(factory, "factory");
            return this;
        }

        /**
         * Replaces the L1 implementation (default: shaded Caffeine); see
         * {@link LocalCache}.
         *
         * @param factory maps a cache name and its resolved settings to an
         *                L1 instance; must not be {@code null}
         * @return this builder
         * @since 0.1.0
         */
        public Builder localCacheFactory(BiFunction<String, CacheSettings, LocalCache<?, ?>> factory) {
            this.localCacheFactory = Objects.requireNonNull(factory, "factory");
            return this;
        }

        /**
         * Explicit lock provider for rebuild coordination. Usually
         * omitted: derived from the L2 transport when it implements
         * {@link LockProviderSource}.
         *
         * @param lockProvider the lock provider to use, or {@code null} to
         *                     rely on derivation / per-instance coalescing
         * @return this builder
         * @since 0.1.0
         */
        public Builder lockProvider(DistributedLockProvider lockProvider) {
            this.lockProvider = lockProvider;
            return this;
        }

        /**
         * Cross-instance invalidation engine, given as a factory receiving
         * this factory's {@link VersionGenerator} (so event origin IDs and
         * write versions share one instance identity). When absent, caches
         * are single-node: nothing is published, nothing is subscribed.
         * Typical usage:
         * {@code .invalidation(versions -> new InvalidationService(transport, journal, versions.instanceId(), listener))}
         *
         * @param invalidationFactory builds the invalidation engine from the
         *                            factory's version generator, or
         *                            {@code null} for single-node caches
         * @return this builder
         * @since 0.1.0
         */
        public Builder invalidation(Function<VersionGenerator, InvalidationHandler> invalidationFactory) {
            this.invalidationFactory = invalidationFactory;
            return this;
        }

        /**
         * Application-facing observer of inbound invalidation events: the
         * invalidation engine invokes it after each incoming event has been
         * applied locally, in arrival order per cache. Default: no
         * observation. See {@link InvalidationEventListener}.
         *
         * @param listener the observer; must not be {@code null}
         * @return this builder
         * @since 0.1.0
         */
        public Builder invalidationEventListener(InvalidationEventListener listener) {
            this.invalidationEventListener = Objects.requireNonNull(listener, "listener");
            return this;
        }

        /**
         * Explicit opt-out of the L2 circuit breaker. Degradation protection
         * is on by default; disabling it lets infrastructure exceptions
         * escape into business code (cascade-failure risk) and is logged.
         *
         * @return this builder
         * @since 0.1.0
         */
        public Builder disableCircuitBreaker() {
            this.circuitBreakerEnabled = false;
            return this;
        }

        /**
         * Breaker thresholds.
         *
         * <p><b>Internal — not part of the supported API.</b> Intended for
         * testing; the defaults are safe.
         *
         * @param config the breaker configuration; must not be {@code null}
         * @return this builder
         * @since 0.1.0
         */
        public Builder circuitBreakerConfig(CircuitBreaker.Config config) {
            this.breakerConfig = Objects.requireNonNull(config, "config");
            return this;
        }

        /**
         * Listener for degradation transitions (metrics bind here).
         *
         * @param listener the degradation listener; must not be {@code null}
         * @return this builder
         * @since 0.1.0
         */
        public Builder degradationListener(DegradationListener listener) {
            this.degradationListener = Objects.requireNonNull(listener, "listener");
            return this;
        }

        /**
         * Metrics events listener (bind a registry via the metrics module).
         *
         * @param listener the metrics listener; must not be {@code null}
         * @return this builder
         * @since 0.1.0
         */
        public Builder metricsListener(CacheMetricsListener listener) {
            this.metricsListener = Objects.requireNonNull(listener, "listener");
            return this;
        }

        /**
         * TTL jitter source.
         *
         * <p><b>Internal — not part of the supported API.</b> The default
         * draws from ThreadLocalRandom; inject a seeded instance for
         * deterministic TTL spreads in tests.
         *
         * @param jitter the jitter source; must not be {@code null}
         * @return this builder
         * @since 0.1.0
         */
        public Builder jitter(TtlJitter jitter) {
            this.jitter = Objects.requireNonNull(jitter, "jitter");
            return this;
        }

        /**
         * Maximum number of threads serving {@link AsyncTierCache} operations
         * (the bounded async executor); defaults to
         * {@code max(4, availableProcessors)}. Async cache work is offloaded
         * to a bounded library-managed pool with a bounded handoff queue:
         * when the pool and queue are saturated, new submissions fail their
         * returned {@code CompletionStage} with
         * {@link java.util.concurrent.RejectedExecutionException} instead of
         * growing threads without bound. Raise this when async loaders are
         * IO-bound and the default starves throughput.
         *
         * @param asyncExecutorThreads the maximum async worker threads; must
         *                             be &gt; 0
         * @return this builder
         * @throws IllegalArgumentException if {@code asyncExecutorThreads} is
         *                                  not positive
         * @since 1.2.0
         */
        public Builder asyncExecutorThreads(int asyncExecutorThreads) {
            if (asyncExecutorThreads <= 0) {
                throw new IllegalArgumentException(
                        "asyncExecutorThreads must be > 0, got " + asyncExecutorThreads);
            }
            this.asyncExecutorThreads = asyncExecutorThreads;
            return this;
        }

        /**
         * Explicit opt-out of singleflight protection. Stampede
         * protection is on by default; disabling it is logged as a risk.
         *
         * @return this builder
         * @since 0.1.0
         */
        public Builder disableSingleflight() {
            this.singleflightEnabled = false;
            return this;
        }

        /**
         * Explicit opt-out of distributed rebuild coordination.
         * Coordination is on by default when a lock provider is available;
         * disabling it is logged as a risk.
         *
         * @return this builder
         * @since 0.1.0
         */
        public Builder disableDistributedCoordination() {
            this.coordinationEnabled = false;
            return this;
        }

        /**
         * Validates the configuration and creates the factory. Validation
         * is fail-fast: an invalid configuration aborts initialization
         * before any cache serves traffic.
         *
         * @return the configured factory
         * @throws CacheConfigurationException if the resolved configuration
         *         violates a startup invariant (e.g. TTL ordering), or if
         *         both {@link #remoteCache} and {@link #remoteCacheFactory}
         *         were set
         * @throws NullPointerException if no L2 was configured at all
         * @since 0.1.0
         */
        public TierCacheFactory build() {
            if (remoteCache != null && remoteCacheFactory != null) {
                throw new CacheConfigurationException(
                        "remoteCache and remoteCacheFactory are mutually exclusive: choose a single "
                                + "shared L2 instance or a per-cache L2 factory, not both.");
            }
            if (remoteCache == null && remoteCacheFactory == null) {
                throw new NullPointerException("remoteCache is required");
            }
            if (!singleflightEnabled) {
                log.warn("Singleflight protection disabled by explicit opt-in. "
                        + "Concurrent misses of one key will each invoke the loader "
                        + "(cache stampede risk).");
            }
            return new TierCacheFactory(this);
        }
    }
}
