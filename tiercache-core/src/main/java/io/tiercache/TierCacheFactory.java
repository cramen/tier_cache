package io.tiercache;

import io.tiercache.internal.CacheConfigValidator;
import io.tiercache.internal.BreakerLockProvider;
import io.tiercache.internal.CircuitBreaker;
import io.tiercache.internal.CircuitBreakerRemoteCache;
import io.tiercache.internal.CaffeineLocalCache;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.DistributedLockProvider;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.DegradationListener;
import io.tiercache.spi.InvalidationHandler;

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
 * lease extension; close the factory when done.
 *
 * <p><b>Incubating:</b> 0.x API, may change before 1.0.
 *
 * <p>Consistency model: caches built here are eventually consistent;
 * no strong-consistency guarantees are given or implied.
 */
public final class TierCacheFactory implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TierCacheFactory.class);

    private final CacheSettings defaults;
    private final Map<String, CacheSettings> caches;
    private final RemoteCache<Object, Object> remoteCache;
    private final BiFunction<String, CacheSettings, LocalCache<?, ?>> localCacheFactory;
    private final boolean singleflightEnabled;
    private final boolean coordinationEnabled;
    private final DistributedLockProvider lockProvider;
    private final ScheduledExecutorService watchdog;
    private final VersionGenerator versionGenerator;
    private final InvalidationHandler invalidation; // null = single-node
    private final CircuitBreaker breaker;           // null = unguarded L2 (opt-out)
    private final CacheMetricsListener metricsListener;

    private TierCacheFactory(Builder builder) {
        this.defaults = builder.defaults;
        RemoteCache<Object, Object> rawRemoteCache = builder.remoteCache;
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
                ? Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory())
                : null;

        this.versionGenerator = new VersionGenerator();
        this.invalidation = builder.invalidationFactory != null
                ? builder.invalidationFactory.apply(versionGenerator)
                : null;
        this.metricsListener = builder.metricsListener;

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
            rawRemoteCache = new CircuitBreakerRemoteCache<>(rawRemoteCache, breaker);
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

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the named cache, creating it on first access. Unconfigured
     * names operate with the global defaults.
     */
    @SuppressWarnings("unchecked")
    public <K, V> TierCache<K, V> getCache(String name) {
        CacheSettings settings = caches.getOrDefault(name, defaults);
        LocalCache<K, V> l1 = (LocalCache<K, V>) localCacheFactory.apply(name, settings);
        DefaultTierCache<K, V> cache = new DefaultTierCache<>(name, l1,
                (RemoteCache<K, V>) remoteCache, settings, singleflightEnabled,
                coordinationEnabled ? lockProvider : null, watchdog, versionGenerator, invalidation,
                breaker, metricsListener);
        if (invalidation != null) {
            invalidation.registerTarget(name, cache);
        }
        return cache;
    }

    /**
     * Shuts down the watchdog scheduler. Caches already obtained remain
     * usable but lose lease extension for in-flight coordination.
     */
    /** True while the L2 circuit breaker is open (L1-only degraded mode). */
    public boolean isDegraded() {
        return breaker != null && breaker.isOpen();
    }

    @Override
    public void close() {
        if (watchdog != null) {
            watchdog.shutdownNow();
        }
        if (invalidation != null) {
            invalidation.close();
        }
    }

    private static final class DaemonThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "tiercache-watchdog");
            t.setDaemon(true);
            return t;
        }
    }

    public static final class Builder {

        private CacheSettings defaults = CacheSettings.defaults();
        private final Map<String, CacheOverride> overrides = new LinkedHashMap<>();
        private RemoteCache<Object, Object> remoteCache;
        private BiFunction<String, CacheSettings, LocalCache<?, ?>> localCacheFactory =
                (name, settings) -> new CaffeineLocalCache<>(settings);
        private boolean singleflightEnabled = true;
        private boolean coordinationEnabled = true;
        private DistributedLockProvider lockProvider;
        private Function<VersionGenerator, InvalidationHandler> invalidationFactory;
        private boolean circuitBreakerEnabled = true;
        private CircuitBreaker.Config breakerConfig = CircuitBreaker.Config.defaults();
        private DegradationListener degradationListener = DegradationListener.NOOP;
        private CacheMetricsListener metricsListener = CacheMetricsListener.NOOP;

        public Builder defaults(CacheSettings defaults) {
            this.defaults = Objects.requireNonNull(defaults, "defaults");
            return this;
        }

        public Builder cache(String name, CacheOverride override) {
            overrides.put(Objects.requireNonNull(name, "name"), Objects.requireNonNull(override, "override"));
            return this;
        }

        /**
         * The L2 implementation shared by all caches. Required. If it
         * implements {@link LockProviderSource}, the rebuild-lock provider
         * is derived automatically unless set explicitly.
         */
        @SuppressWarnings("unchecked")
        public Builder remoteCache(RemoteCache<?, ?> remoteCache) {
            this.remoteCache = (RemoteCache<Object, Object>) Objects.requireNonNull(remoteCache, "remoteCache");
            return this;
        }

        /**
         * Replaces the L1 implementation (default: shaded Caffeine). SPI is
         * incubating; see {@link LocalCache}.
         */
        public Builder localCacheFactory(BiFunction<String, CacheSettings, LocalCache<?, ?>> factory) {
            this.localCacheFactory = Objects.requireNonNull(factory, "factory");
            return this;
        }

        /**
         * Explicit lock provider for rebuild coordination. Usually
         * omitted: derived from the L2 transport when it implements
         * {@link LockProviderSource}.
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
         */
        public Builder invalidation(Function<VersionGenerator, InvalidationHandler> invalidationFactory) {
            this.invalidationFactory = invalidationFactory;
            return this;
        }

        /**
         * Explicit opt-out of the L2 circuit breaker. Degradation protection
         * is on by default; disabling it lets infrastructure exceptions
         * escape into business code (cascade-failure risk) and is logged.
         */
        public Builder disableCircuitBreaker() {
            this.circuitBreakerEnabled = false;
            return this;
        }

        /** Breaker thresholds. Internal/testing; defaults are safe. */
        public Builder circuitBreakerConfig(CircuitBreaker.Config config) {
            this.breakerConfig = Objects.requireNonNull(config, "config");
            return this;
        }

        /** Listener for degradation transitions (metrics bind here). */
        public Builder degradationListener(DegradationListener listener) {
            this.degradationListener = Objects.requireNonNull(listener, "listener");
            return this;
        }

        /** Metrics events listener (bind a registry via the metrics module). */
        public Builder metricsListener(CacheMetricsListener listener) {
            this.metricsListener = Objects.requireNonNull(listener, "listener");
            return this;
        }

        /**
         * Explicit opt-out of singleflight protection. Stampede
         * protection is on by default; disabling it is logged as a risk.
         */
        public Builder disableSingleflight() {
            this.singleflightEnabled = false;
            return this;
        }

        /**
         * Explicit opt-out of distributed rebuild coordination.
         * Coordination is on by default when a lock provider is available;
         * disabling it is logged as a risk.
         */
        public Builder disableDistributedCoordination() {
            this.coordinationEnabled = false;
            return this;
        }

        public TierCacheFactory build() {
            Objects.requireNonNull(remoteCache, "remoteCache is required");
            if (!singleflightEnabled) {
                log.warn("Singleflight protection disabled by explicit opt-in. "
                        + "Concurrent misses of one key will each invoke the loader "
                        + "(cache stampede risk).");
            }
            return new TierCacheFactory(this);
        }
    }
}
