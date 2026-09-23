package io.tiercache.micronaut;

import io.tiercache.invalidation.JournalProtocol;
import io.micronaut.context.annotation.ConfigurationInject;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.bind.annotation.Bindable;
import io.tiercache.CacheOverride;
import io.tiercache.CacheSettings;
import io.tiercache.NullPolicy;

import java.time.Duration;

/**
 * Configuration properties under the {@code tiercache.*} prefix.
 * Maps 1:1 onto core's configuration model; validation is core's
 * (fail-fast at startup: an invalid combination, such as an L1 TTL above
 * the L2 TTL, aborts application startup with an actionable error).
 *
 * <p>The property keys and defaults are identical to the Spring Boot
 * starter's ({@code docs/configuration.md} is the single configuration
 * reference). Per-cache overrides live under {@code tiercache.caches.<name>.*}
 * and are bound as {@link TiercacheCacheProperties} beans (Micronaut's
 * {@code @EachProperty} idiom) rather than a map on this class.
 *
 * <p>This class is part of the supported public API: applications configure
 * the module exclusively through these properties.
 *
 * @since 1.1.0
 */
@ConfigurationProperties("tiercache")
public class TiercacheProperties {

    private final boolean enabled;
    private final String redisUri;
    private final DefaultCacheProps defaults;
    private final InvalidationProps invalidation;
    private final int asyncExecutorThreads;

    /**
     * Creates the properties root, applying the built-in defaults for any
     * level not present in the configuration.
     *
     * @param enabled      the master switch ({@code tiercache.enabled},
     *                     default {@code false})
     * @param redisUri     the Redis/Valkey URI ({@code tiercache.redis-uri})
     * @param defaults     the global defaults level ({@code tiercache.defaults.*}),
     *                     or {@code null} for the built-in defaults
     * @param invalidation the invalidation settings
     *                     ({@code tiercache.invalidation.*}), or {@code null}
     *                     for the built-in defaults
     * @param asyncExecutorThreads the async executor thread cap
     *                     ({@code tiercache.async-executor-threads}), or
     *                     {@code null} for the library default
     * @since 1.1.0
     */
    @ConfigurationInject
    public TiercacheProperties(
            @Bindable(defaultValue = "false") boolean enabled,
            @Nullable String redisUri,
            @Nullable DefaultCacheProps defaults,
            @Nullable InvalidationProps invalidation,
            @Nullable Integer asyncExecutorThreads) {
        this.enabled = enabled;
        this.redisUri = redisUri;
        this.defaults = defaults != null ? defaults : new DefaultCacheProps(
                null, null, null, null, null, null, null, null, null, null, null, null, null);
        this.invalidation = invalidation != null ? invalidation : new InvalidationProps(null, null, null);
        this.asyncExecutorThreads = asyncExecutorThreads != null ? asyncExecutorThreads : 0;
    }

    /**
     * Returns whether the module is active.
     *
     * @return {@code true} when {@code tiercache.enabled=true}; the module
     *         contributes no beans otherwise (default {@code false})
     * @since 1.1.0
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Returns the Redis/Valkey URI for the L2 transport.
     *
     * @return the URI, e.g. {@code redis://localhost:6379}; required when
     *         the module is enabled and no custom {@code RemoteCache} bean
     *         is provided
     * @since 1.1.0
     */
    @Nullable
    public String getRedisUri() {
        return redisUri;
    }

    /**
     * Returns the global defaults applied to every cache without explicit
     * per-cache overrides.
     *
     * @return the defaults level, never {@code null}
     * @since 1.1.0
     */
    public DefaultCacheProps getDefaults() {
        return defaults;
    }

    /**
     * Returns the cross-instance invalidation settings.
     *
     * @return the invalidation settings, never {@code null}
     * @since 1.1.0
     */
    public InvalidationProps getInvalidation() {
        return invalidation;
    }

    /**
     * Returns the configured maximum threads for the bounded async executor
     * ({@code tiercache.async-executor-threads}); 0 means the library default
     * {@code max(4, availableProcessors)}. Under saturation, submissions fail
     * their stage with {@code RejectedExecutionException} rather than growing
     * threads.
     *
     * @return the async executor thread cap, or 0 for the default
     * @since 1.2.0
     */
    public int getAsyncExecutorThreads() {
        return asyncExecutorThreads;
    }

    /**
     * Invalidation settings ({@code tiercache.invalidation.*}): the Pub/Sub
     * profile is on by default when the Redis transport is used.
     *
     * @since 1.1.0
     */
    @ConfigurationProperties("invalidation")
    public static class InvalidationProps {

        private final Boolean enabled;
        private final String profile;
        private final Integer journalCapacity;

        /**
         * Creates the invalidation settings.
         *
         * @param enabled         whether cross-instance invalidation is
         *                        active; {@code null} means the built-in
         *                        default ({@code true})
         * @param profile         the transport profile ({@code pubsub} or
         *                        {@code streams}); {@code null} means
         *                        {@code pubsub}
         * @param journalCapacity the max journal entries kept per cache
         *                        stream; {@code null} means 10,000
         * @since 1.1.0
         */
        @ConfigurationInject
        public InvalidationProps(@Nullable Boolean enabled, @Nullable String profile,
                @Nullable Integer journalCapacity) {
            this.enabled = enabled;
            this.profile = profile;
            this.journalCapacity = journalCapacity;
        }

        /**
         * Returns whether cross-instance invalidation is active.
         *
         * @return {@code true} when {@code tiercache.invalidation.enabled}
         *         is not {@code false} (default {@code true})
         * @since 1.1.0
         */
        public boolean isEnabled() {
            return enabled == null || enabled;
        }

        /**
         * Returns the invalidation transport profile.
         *
         * @return {@code pubsub} (default) or {@code streams}
         * @since 1.1.0
         */
        public String getProfile() {
            return profile != null ? profile : "pubsub";
        }

        /**
         * Returns the maximum number of journal entries kept per cache
         * stream, bounding the replay window after a reconnect.
         *
         * @return the journal capacity (default 10,000)
         * @since 1.1.0
         */
        public int getJournalCapacity() {
            return journalCapacity != null ? journalCapacity : JournalProtocol.DEFAULT_CAPACITY;
        }
    }

    /**
     * The shared shape of cache settings: the fields of the global
     * {@code tiercache.defaults.*} level and of every
     * {@code tiercache.caches.<name>.*} override. Null fields inherit the
     * level below (per-cache overrides inherit the global defaults; the
     * defaults level inherits core's built-in defaults).
     *
     * @since 1.1.0
     */
    @Introspected
    public abstract static class CacheProps {

        private final Long l1MaxSize;
        private final Duration l1ExpireAfterWrite;
        private final Duration l1ExpireAfterAccess;
        private final Duration l2Ttl;
        private final Double jitterAmplitude;
        private final Kind nullPolicy;
        private final Duration nullMarkerTtl;
        private final io.tiercache.InvalidationMode invalidationMode;
        private final Long payloadCapBytes;
        private final Duration staleTtl;
        private final Boolean xfetchEnabled;
        private final Duration xfetchBeta;
        private final Duration degradationStaleTtl;

        /**
         * Null-caching policy bound from the {@code null-policy} property.
         *
         * @since 1.1.0
         */
        public enum Kind {
            /**
             * Do not cache null values; a null loader result is a miss.
             */
            DENY,
            /**
             * Cache null values as a short-lived marker to protect against
             * cache penetration.
             */
            ALLOW
        }

        /**
         * Creates a settings level; every {@code null} field inherits the
         * level below.
         *
         * @param l1MaxSize           the L1 size bound
         * @param l1ExpireAfterWrite  the L1 expire-after-write TTL
         * @param l1ExpireAfterAccess the L1 expire-after-access TTL
         * @param l2Ttl               the L2 entry TTL
         * @param jitterAmplitude     the TTL jitter amplitude as a fraction
         * @param nullPolicy          the null-caching policy
         * @param nullMarkerTtl       the null-marker TTL
         * @param invalidationMode    the invalidation event mode
         * @param payloadCapBytes     the UPDATE-mode payload cap in bytes
         * @param staleTtl            the stale-while-revalidate window
         * @param xfetchEnabled       the XFetch early-refresh switch
         * @param xfetchBeta          the XFetch beta tuning factor
         * @since 1.1.0
         */
        protected CacheProps(@Nullable Long l1MaxSize,
                @Nullable Duration l1ExpireAfterWrite,
                @Nullable Duration l1ExpireAfterAccess,
                @Nullable Duration l2Ttl,
                @Nullable Double jitterAmplitude,
                @Nullable Kind nullPolicy,
                @Nullable Duration nullMarkerTtl,
                @Nullable io.tiercache.InvalidationMode invalidationMode,
                @Nullable Long payloadCapBytes,
                @Nullable Duration staleTtl,
                @Nullable Boolean xfetchEnabled,
                @Nullable Duration xfetchBeta,
                @Nullable Duration degradationStaleTtl) {
            this.l1MaxSize = l1MaxSize;
            this.l1ExpireAfterWrite = l1ExpireAfterWrite;
            this.l1ExpireAfterAccess = l1ExpireAfterAccess;
            this.l2Ttl = l2Ttl;
            this.jitterAmplitude = jitterAmplitude;
            this.nullPolicy = nullPolicy;
            this.nullMarkerTtl = nullMarkerTtl;
            this.invalidationMode = invalidationMode;
            this.payloadCapBytes = payloadCapBytes;
            this.staleTtl = staleTtl;
            this.xfetchEnabled = xfetchEnabled;
            this.xfetchBeta = xfetchBeta;
            this.degradationStaleTtl = degradationStaleTtl;
        }

        /**
         * Returns the maximum number of entries in the L1 (in-process) cache.
         *
         * @return the L1 size bound, or {@code null} to inherit
         * @since 1.1.0
         */
        @Nullable
        public Long getL1MaxSize() {
            return l1MaxSize;
        }

        /**
         * Returns the L1 expire-after-write TTL.
         *
         * @return the L1 write TTL, or {@code null} to inherit
         * @since 1.1.0
         */
        @Nullable
        public Duration getL1ExpireAfterWrite() {
            return l1ExpireAfterWrite;
        }

        /**
         * Returns the L1 expire-after-access TTL.
         *
         * @return the L1 access TTL, or {@code null} to inherit
         * @since 1.1.0
         */
        @Nullable
        public Duration getL1ExpireAfterAccess() {
            return l1ExpireAfterAccess;
        }

        /**
         * Returns the L2 (Redis) entry TTL. Must satisfy the TTL ordering
         * invariant {@code TTL_L1_effective <= TTL_L2}; violations abort
         * startup with an actionable error.
         *
         * @return the L2 TTL, or {@code null} to inherit
         * @since 1.1.0
         */
        @Nullable
        public Duration getL2Ttl() {
            return l2Ttl;
        }

        /**
         * Returns the TTL jitter amplitude applied to L1 expirations to
         * prevent synchronized expiry (cache avalanche).
         *
         * @return the jitter amplitude as a fraction, or {@code null} to
         *         inherit
         * @since 1.1.0
         */
        @Nullable
        public Double getJitterAmplitude() {
            return jitterAmplitude;
        }

        /**
         * Returns the null-caching policy.
         *
         * @return {@code DENY} or {@code ALLOW}, or {@code null} to inherit
         * @since 1.1.0
         */
        @Nullable
        public Kind getNullPolicy() {
            return nullPolicy;
        }

        /**
         * Returns the TTL of the null marker stored under the
         * {@code ALLOW} null policy.
         *
         * @return the null-marker TTL, or {@code null} for the built-in
         *         default (1 minute)
         * @since 1.1.0
         */
        @Nullable
        public Duration getNullMarkerTtl() {
            return nullMarkerTtl;
        }

        /**
         * Returns the invalidation event mode for this cache.
         *
         * @return the invalidation mode, or {@code null} to inherit
         * @since 1.1.0
         */
        @Nullable
        public io.tiercache.InvalidationMode getInvalidationMode() {
            return invalidationMode;
        }

        /**
         * Returns the payload size cap for UPDATE-mode invalidation events.
         *
         * @return the cap in bytes, or {@code null} to inherit
         * @since 1.1.0
         */
        @Nullable
        public Long getPayloadCapBytes() {
            return payloadCapBytes;
        }

        /**
         * Returns the stale-while-revalidate window: how long an expired
         * entry may be served while it is reloaded in the background.
         *
         * @return the stale TTL, or {@code null} to inherit
         * @since 1.1.0
         */
        @Nullable
        public Duration getStaleTtl() {
            return staleTtl;
        }

        /**
         * Returns the degradation stale window (extra L1 retention served
         * stale while the L2 circuit breaker rejects calls).
         *
         * @return the degradation stale TTL, or {@code null} to inherit
         * @since 1.4.0
         */
        @Nullable
        public Duration getDegradationStaleTtl() {
            return degradationStaleTtl;
        }

        /**
         * Returns whether XFetch early refresh of hot keys is enabled.
         *
         * @return the XFetch switch, or {@code null} to inherit
         * @since 1.1.0
         */
        @Nullable
        public Boolean getXfetchEnabled() {
            return xfetchEnabled;
        }

        /**
         * Returns the XFetch beta: the tuning factor of the probabilistic
         * early-refresh lottery; smaller values refresh hot entries
         * earlier/more aggressively.
         *
         * @return the XFetch beta, or {@code null} to inherit
         * @since 1.1.0
         */
        @Nullable
        public Duration getXfetchBeta() {
            return xfetchBeta;
        }

        @Nullable
        NullPolicy toNullPolicy() {
            if (nullPolicy == null) {
                return null;
            }
            return switch (nullPolicy) {
                case DENY -> NullPolicy.deny();
                case ALLOW -> NullPolicy.allow(
                        nullMarkerTtl != null ? nullMarkerTtl : Duration.ofMinutes(1));
            };
        }

        /** Resolves into full settings against the given base. */
        CacheSettings toSettings(CacheSettings base) {
            return toOverride().resolve(base);
        }

        CacheOverride toOverride() {
            CacheOverride override = new CacheOverride();
            if (l1MaxSize != null) {
                override.l1MaxSize(l1MaxSize);
            }
            if (l1ExpireAfterWrite != null) {
                override.l1ExpireAfterWrite(l1ExpireAfterWrite);
            }
            if (l1ExpireAfterAccess != null) {
                override.l1ExpireAfterAccess(l1ExpireAfterAccess);
            }
            if (l2Ttl != null) {
                override.l2Ttl(l2Ttl);
            }
            if (jitterAmplitude != null) {
                override.jitterAmplitude(jitterAmplitude);
            }
            NullPolicy policy = toNullPolicy();
            if (policy != null) {
                override.nullPolicy(policy);
            }
            if (invalidationMode != null) {
                override.invalidationMode(invalidationMode);
            }
            if (payloadCapBytes != null) {
                override.payloadCapBytes(payloadCapBytes);
            }
            if (staleTtl != null) {
                override.staleTtl(staleTtl);
            }
            if (xfetchEnabled != null) {
                override.xfetchEnabled(xfetchEnabled);
            }
            if (xfetchBeta != null) {
                override.xfetchBeta(xfetchBeta);
            }
            if (degradationStaleTtl != null) {
                override.degradationStaleTtl(degradationStaleTtl);
            }
            return override;
        }
    }

    /**
     * The global defaults level ({@code tiercache.defaults.*}), inherited by
     * every cache without explicit per-cache overrides.
     *
     * @since 1.1.0
     */
    @ConfigurationProperties("defaults")
    public static class DefaultCacheProps extends CacheProps {

        /**
         * Creates the defaults level; see {@link CacheProps} for the field
         * semantics.
         *
         * @param l1MaxSize           the L1 size bound
         * @param l1ExpireAfterWrite  the L1 expire-after-write TTL
         * @param l1ExpireAfterAccess the L1 expire-after-access TTL
         * @param l2Ttl               the L2 entry TTL
         * @param jitterAmplitude     the TTL jitter amplitude as a fraction
         * @param nullPolicy          the null-caching policy
         * @param nullMarkerTtl       the null-marker TTL
         * @param invalidationMode    the invalidation event mode
         * @param payloadCapBytes     the UPDATE-mode payload cap in bytes
         * @param staleTtl            the stale-while-revalidate window
         * @param xfetchEnabled       the XFetch early-refresh switch
         * @param xfetchBeta          the XFetch beta tuning factor
         * @since 1.1.0
         */
        @ConfigurationInject
        public DefaultCacheProps(@Nullable Long l1MaxSize,
                @Nullable Duration l1ExpireAfterWrite,
                @Nullable Duration l1ExpireAfterAccess,
                @Nullable Duration l2Ttl,
                @Nullable Double jitterAmplitude,
                @Nullable Kind nullPolicy,
                @Nullable Duration nullMarkerTtl,
                @Nullable io.tiercache.InvalidationMode invalidationMode,
                @Nullable Long payloadCapBytes,
                @Nullable Duration staleTtl,
                @Nullable Boolean xfetchEnabled,
                @Nullable Duration xfetchBeta,
                @Nullable Duration degradationStaleTtl) {
            super(l1MaxSize, l1ExpireAfterWrite, l1ExpireAfterAccess, l2Ttl, jitterAmplitude,
                    nullPolicy, nullMarkerTtl, invalidationMode, payloadCapBytes, staleTtl,
                    xfetchEnabled, xfetchBeta, degradationStaleTtl);
        }
    }
}
