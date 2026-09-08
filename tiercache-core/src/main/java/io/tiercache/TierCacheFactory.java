package io.tiercache;

import io.tiercache.internal.CacheConfigValidator;
import io.tiercache.internal.CaffeineLocalCache;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.RemoteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Builds {@link TierCache} instances from global defaults plus per-cache
 * overrides (F-04). Configuration is validated at {@link Builder#build()}
 * time — an invalid configuration aborts initialization before any cache
 * serves traffic (fail-fast, F-04/F-05).
 *
 * <p><b>Incubating:</b> 0.x API, may change until CP-0.
 *
 * <p>Consistency model (F-15): caches built here are eventually consistent;
 * no strong-consistency guarantees are given or implied.
 */
public final class TierCacheFactory {

    private static final Logger log = LoggerFactory.getLogger(TierCacheFactory.class);

    private final CacheSettings defaults;
    private final Map<String, CacheSettings> caches;
    private final RemoteCache<Object, Object> remoteCache;
    private final BiFunction<String, CacheSettings, LocalCache<?, ?>> localCacheFactory;
    private final boolean singleflightEnabled;

    private TierCacheFactory(Builder builder) {
        this.defaults = builder.defaults;
        this.remoteCache = builder.remoteCache;
        this.localCacheFactory = builder.localCacheFactory;
        this.singleflightEnabled = builder.singleflightEnabled;
        this.caches = new LinkedHashMap<>();
        builder.overrides.forEach((name, override) -> caches.put(name, override.resolve(defaults)));

        // Fail-fast validation of every configured cache (F-04).
        CacheConfigValidator.validate("<global defaults>", defaults);
        caches.forEach(CacheConfigValidator::validate);
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
        return new DefaultTierCache<>(l1, (RemoteCache<K, V>) remoteCache, settings, singleflightEnabled);
    }

    public static final class Builder {

        private CacheSettings defaults = CacheSettings.defaults();
        private final Map<String, CacheOverride> overrides = new LinkedHashMap<>();
        private RemoteCache<Object, Object> remoteCache;
        private BiFunction<String, CacheSettings, LocalCache<?, ?>> localCacheFactory =
                (name, settings) -> new CaffeineLocalCache<>(settings);
        private boolean singleflightEnabled = true;

        public Builder defaults(CacheSettings defaults) {
            this.defaults = Objects.requireNonNull(defaults, "defaults");
            return this;
        }

        public Builder cache(String name, CacheOverride override) {
            overrides.put(Objects.requireNonNull(name, "name"), Objects.requireNonNull(override, "override"));
            return this;
        }

        /**
         * The L2 implementation shared by all caches. Required.
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
         * Explicit opt-out of singleflight protection (F-20). Stampede
         * protection is on by default; disabling it is logged as a risk.
         */
        public Builder disableSingleflight() {
            this.singleflightEnabled = false;
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
