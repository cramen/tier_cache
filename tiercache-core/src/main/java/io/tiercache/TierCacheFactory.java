package io.tiercache;

import io.tiercache.internal.CacheConfigValidator;
import io.tiercache.internal.CaffeineLocalCache;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.DistributedLockProvider;
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
 * overrides (F-04). Configuration is validated at {@link Builder#build()}
 * time — an invalid configuration aborts initialization before any cache
 * serves traffic (fail-fast, F-04/F-05).
 *
 * <p>The factory owns a daemon watchdog scheduler used for rebuild-lock
 * lease extension (F-21); close the factory when done.
 *
 * <p><b>Incubating:</b> 0.x API, may change until CP-0.
 *
 * <p>Consistency model (F-15): caches built here are eventually consistent;
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

    private TierCacheFactory(Builder builder) {
        this.defaults = builder.defaults;
        this.remoteCache = builder.remoteCache;
        this.localCacheFactory = builder.localCacheFactory;
        this.singleflightEnabled = builder.singleflightEnabled;
        this.coordinationEnabled = builder.coordinationEnabled;
        this.caches = new LinkedHashMap<>();
        builder.overrides.forEach((name, override) -> caches.put(name, override.resolve(defaults)));

        // Fail-fast validation of every configured cache (F-04).
        CacheConfigValidator.validate("<global defaults>", defaults);
        caches.forEach(CacheConfigValidator::validate);

        DistributedLockProvider provider = builder.lockProvider;
        if (provider == null && coordinationEnabled
                && remoteCache instanceof LockProviderSource source) {
            provider = source.lockProvider();
        }
        this.lockProvider = provider;

        if (!coordinationEnabled) {
            log.warn("Distributed rebuild coordination (F-21) disabled by explicit opt-in. "
                    + "Concurrent misses of one key across instances will each run the loader "
                    + "(cluster-wide stampede risk).");
        } else if (lockProvider == null) {
            log.warn("No distributed lock provider available for the configured L2 (F-21). "
                    + "Falling back to per-instance coalescing only: up to one loader execution "
                    + "per instance per rebuild round.");
        }

        this.watchdog = coordinationEnabled && lockProvider != null
                ? Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory())
                : null;
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
        return new DefaultTierCache<>(name, l1, (RemoteCache<K, V>) remoteCache, settings,
                singleflightEnabled,
                coordinationEnabled ? lockProvider : null, watchdog);
    }

    /**
     * Shuts down the watchdog scheduler. Caches already obtained remain
     * usable but lose lease extension for in-flight coordination.
     */
    @Override
    public void close() {
        if (watchdog != null) {
            watchdog.shutdownNow();
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
         * (F-21) is derived automatically unless set explicitly.
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
         * Explicit lock provider for rebuild coordination (F-21). Usually
         * omitted: derived from the L2 transport when it implements
         * {@link LockProviderSource}.
         */
        public Builder lockProvider(DistributedLockProvider lockProvider) {
            this.lockProvider = lockProvider;
            return this;
        }

        /**
         * Explicit opt-out of singleflight protection (F-20). Stampede
         * protection is on by default; disabling it is logged as a risk.
         */
        public Builder disableSingleflight() {
            this.singleflightEnabled = false;
            return this;
        }

        /**
         * Explicit opt-out of distributed rebuild coordination (F-21).
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
                log.warn("Singleflight protection (F-20) disabled by explicit opt-in. "
                        + "Concurrent misses of one key will each invoke the loader "
                        + "(cache stampede risk).");
            }
            return new TierCacheFactory(this);
        }
    }
}
