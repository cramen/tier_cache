package io.tiercache.spring;

import io.tiercache.CacheOverride;
import io.tiercache.CacheSettings;
import io.tiercache.NullPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuration properties under the {@code tiercache.*} prefix.
 * Maps 1:1 onto core's configuration model; validation is core's
 * (fail-fast at startup: an invalid combination, such as an L1 TTL above
 * the L2 TTL, aborts application startup with an actionable error).
 *
 * <p>This class is part of the supported public API: applications configure
 * the starter exclusively through these properties (relaxed binding
 * applies, e.g. {@code tiercache.caches.orders.l2-ttl=10m}).
 *
 * @since 0.1.0
 */
@ConfigurationProperties("tiercache")
public class TiercacheProperties {

    /** Master switch: the starter contributes beans only when true. */
    private boolean enabled = false;

    /** Redis/Valkey URI for the L2 transport, e.g. redis://localhost:6379. */
    private String redisUri;

    /** Global defaults applied to every cache without explicit overrides. */
    private CacheProps defaults = new CacheProps();

    /** Per-cache overrides by cache name. */
    private Map<String, CacheProps> caches = new LinkedHashMap<>();

    /** Cross-instance invalidation settings. */
    private InvalidationProps invalidation = new InvalidationProps();

    /**
     * Maximum threads serving {@code AsyncTierCache} operations (the bounded
     * async executor); 0 means the library default
     * {@code max(4, availableProcessors)}.
     */
    private int asyncExecutorThreads = 0;

    /**
     * Returns whether the starter is active.
     *
     * @return {@code true} when {@code tiercache.enabled=true}; the starter
     *         contributes no beans otherwise (default {@code false})
     * @since 0.1.0
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the master switch for the starter.
     *
     * @param enabled {@code true} to activate the auto-configuration
     * @since 0.1.0
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns the Redis/Valkey URI for the L2 transport.
     *
     * @return the URI, e.g. {@code redis://localhost:6379}; required when
     *         the starter is enabled and no custom {@code RemoteCache} bean
     *         is provided
     * @since 0.1.0
     */
    public String getRedisUri() {
        return redisUri;
    }

    /**
     * Sets the Redis/Valkey URI for the L2 transport.
     *
     * @param redisUri the URI, e.g. {@code redis://localhost:6379}
     * @since 0.1.0
     */
    public void setRedisUri(String redisUri) {
        this.redisUri = redisUri;
    }

    /**
     * Returns the global defaults applied to every cache without explicit
     * per-cache overrides.
     *
     * @return the defaults level, never {@code null}
     * @since 0.1.0
     */
    public CacheProps getDefaults() {
        return defaults;
    }

    /**
     * Sets the global defaults level.
     *
     * @param defaults the defaults applied to every cache without explicit
     *        per-cache overrides
     * @since 0.1.0
     */
    public void setDefaults(CacheProps defaults) {
        this.defaults = defaults;
    }

    /**
     * Returns the per-cache overrides keyed by cache name.
     *
     * @return the overrides map ({@code tiercache.caches.<name>.*}), never
     *         {@code null}
     * @since 0.1.0
     */
    public Map<String, CacheProps> getCaches() {
        return caches;
    }

    /**
     * Returns the cross-instance invalidation settings.
     *
     * @return the invalidation settings, never {@code null}
     * @since 0.1.0
     */
    public InvalidationProps getInvalidation() {
        return invalidation;
    }

    /**
     * Sets the cross-instance invalidation settings.
     *
     * @param invalidation the invalidation settings
     * @since 0.1.0
     */
    public void setInvalidation(InvalidationProps invalidation) {
        this.invalidation = invalidation;
    }

    /**
     * Returns the configured maximum threads for the bounded async executor
     * (0 = library default {@code max(4, availableProcessors)}).
     *
     * @return the async executor thread cap, or 0 for the default
     * @since 1.2.0
     */
    public int getAsyncExecutorThreads() {
        return asyncExecutorThreads;
    }

    /**
     * Sets the maximum threads serving {@code AsyncTierCache} operations.
     * Under saturation, submissions fail their stage with
     * {@code RejectedExecutionException} rather than growing threads.
     *
     * @param asyncExecutorThreads the thread cap; must be &gt;= 0
     * @since 1.2.0
     */
    public void setAsyncExecutorThreads(int asyncExecutorThreads) {
        this.asyncExecutorThreads = asyncExecutorThreads;
    }

    /**
     * Invalidation settings: the Pub/Sub profile is on by default when the
     * Redis transport is used.
     *
     * @since 0.1.0
     */
    public static class InvalidationProps {

        private boolean enabled = true;

        /** Invalidation transport profile: pubsub (default) or streams. */
        private String profile = "pubsub";

        /** Max journal entries kept per cache stream. */
        private int journalCapacity = 10_000;

        /**
         * Returns whether cross-instance invalidation is active.
         *
         * @return {@code true} when {@code tiercache.invalidation.enabled}
         *         is not {@code false} (default {@code true})
         * @since 0.1.0
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Sets whether cross-instance invalidation is active.
         *
         * @param enabled {@code false} opts out of invalidation wiring
         * @since 0.1.0
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * Returns the invalidation transport profile.
         *
         * @return {@code pubsub} (default) or {@code streams}
         * @since 0.1.0
         */
        public String getProfile() {
            return profile;
        }

        /**
         * Sets the invalidation transport profile.
         *
         * @param profile {@code pubsub} or {@code streams}
         * @since 0.1.0
         */
        public void setProfile(String profile) {
            this.profile = profile;
        }

        /**
         * Returns the maximum number of journal entries kept per cache
         * stream, bounding the replay window after a reconnect.
         *
         * @return the journal capacity (default 10,000)
         * @since 0.1.0
         */
        public int getJournalCapacity() {
            return journalCapacity;
        }

        /**
         * Sets the maximum number of journal entries kept per cache stream.
         *
         * @param journalCapacity the journal capacity
         * @since 0.1.0
         */
        public void setJournalCapacity(int journalCapacity) {
            this.journalCapacity = journalCapacity;
        }
    }

    /**
     * Sets the per-cache overrides keyed by cache name.
     *
     * @param caches the overrides map ({@code tiercache.caches.<name>.*})
     * @since 0.1.0
     */
    public void setCaches(Map<String, CacheProps> caches) {
        this.caches = caches;
    }

    /**
     * Per-cache settings; null fields inherit from the defaults level.
     *
     * @since 0.1.0
     */
    public static class CacheProps {

        private Long l1MaxSize;
        private Duration l1ExpireAfterWrite;
        private Duration l1ExpireAfterAccess;
        private Duration l2Ttl;
        private Double jitterAmplitude;
        private Kind nullPolicy;
        private Duration nullMarkerTtl;
        private io.tiercache.InvalidationMode invalidationMode;
        private Long payloadCapBytes;
        private Duration staleTtl;
        private Boolean xfetchEnabled;
        private Duration xfetchBeta;

        /**
         * Null-caching policy bound from the {@code null-policy} property.
         *
         * @since 0.1.0
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
         * Returns the maximum number of entries in the L1 (in-process) cache.
         *
         * @return the L1 size bound, or {@code null} to inherit the defaults
         *         level
         * @since 0.1.0
         */
        public Long getL1MaxSize() {
            return l1MaxSize;
        }

        /**
         * Sets the maximum number of entries in the L1 (in-process) cache.
         *
         * @param l1MaxSize the L1 size bound
         * @since 0.1.0
         */
        public void setL1MaxSize(Long l1MaxSize) {
            this.l1MaxSize = l1MaxSize;
        }

        /**
         * Returns the L1 expire-after-write TTL.
         *
         * @return the L1 write TTL, or {@code null} to inherit the defaults
         *         level
         * @since 0.1.0
         */
        public Duration getL1ExpireAfterWrite() {
            return l1ExpireAfterWrite;
        }

        /**
         * Sets the L1 expire-after-write TTL.
         *
         * @param l1ExpireAfterWrite the L1 write TTL
         * @since 0.1.0
         */
        public void setL1ExpireAfterWrite(Duration l1ExpireAfterWrite) {
            this.l1ExpireAfterWrite = l1ExpireAfterWrite;
        }

        /**
         * Returns the L1 expire-after-access TTL.
         *
         * @return the L1 access TTL, or {@code null} to inherit the defaults
         *         level
         * @since 0.1.0
         */
        public Duration getL1ExpireAfterAccess() {
            return l1ExpireAfterAccess;
        }

        /**
         * Sets the L1 expire-after-access TTL.
         *
         * @param l1ExpireAfterAccess the L1 access TTL
         * @since 0.1.0
         */
        public void setL1ExpireAfterAccess(Duration l1ExpireAfterAccess) {
            this.l1ExpireAfterAccess = l1ExpireAfterAccess;
        }

        /**
         * Returns the L2 (Redis) entry TTL. Must satisfy the TTL ordering
         * invariant {@code TTL_L1_effective <= TTL_L2}; violations abort
         * startup with an actionable error.
         *
         * @return the L2 TTL, or {@code null} to inherit the defaults level
         * @since 0.1.0
         */
        public Duration getL2Ttl() {
            return l2Ttl;
        }

        /**
         * Sets the L2 (Redis) entry TTL.
         *
         * @param l2Ttl the L2 TTL
         * @since 0.1.0
         */
        public void setL2Ttl(Duration l2Ttl) {
            this.l2Ttl = l2Ttl;
        }

        /**
         * Returns the TTL jitter amplitude applied to L1 expirations to
         * prevent synchronized expiry (cache avalanche).
         *
         * @return the jitter amplitude as a fraction, or {@code null} to
         *         inherit the defaults level
         * @since 0.1.0
         */
        public Double getJitterAmplitude() {
            return jitterAmplitude;
        }

        /**
         * Sets the TTL jitter amplitude.
         *
         * @param jitterAmplitude the jitter amplitude as a fraction
         * @since 0.1.0
         */
        public void setJitterAmplitude(Double jitterAmplitude) {
            this.jitterAmplitude = jitterAmplitude;
        }

        /**
         * Returns the null-caching policy.
         *
         * @return {@code DENY} or {@code ALLOW}, or {@code null} to inherit
         *         the defaults level
         * @since 0.1.0
         */
        public Kind getNullPolicy() {
            return nullPolicy;
        }

        /**
         * Sets the null-caching policy.
         *
         * @param nullPolicy {@code DENY} or {@code ALLOW}
         * @since 0.1.0
         */
        public void setNullPolicy(Kind nullPolicy) {
            this.nullPolicy = nullPolicy;
        }

        /**
         * Returns the invalidation event mode for this cache.
         *
         * @return the invalidation mode, or {@code null} to inherit the
         *         defaults level
         * @since 0.1.0
         */
        public io.tiercache.InvalidationMode getInvalidationMode() {
            return invalidationMode;
        }

        /**
         * Sets the invalidation event mode for this cache.
         *
         * @param invalidationMode the invalidation mode
         * @since 0.1.0
         */
        public void setInvalidationMode(io.tiercache.InvalidationMode invalidationMode) {
            this.invalidationMode = invalidationMode;
        }

        /**
         * Returns the payload size cap for UPDATE-mode invalidation events.
         *
         * @return the cap in bytes, or {@code null} to inherit the defaults
         *         level
         * @since 0.1.0
         */
        public Long getPayloadCapBytes() {
            return payloadCapBytes;
        }

        /**
         * Sets the payload size cap for UPDATE-mode invalidation events.
         *
         * @param payloadCapBytes the cap in bytes
         * @since 0.1.0
         */
        public void setPayloadCapBytes(Long payloadCapBytes) {
            this.payloadCapBytes = payloadCapBytes;
        }

        /**
         * Returns the TTL of the null marker stored under the
         * {@code ALLOW} null policy.
         *
         * @return the null-marker TTL, or {@code null} for the built-in
         *         default (1 minute)
         * @since 0.1.0
         */
        public Duration getNullMarkerTtl() {
            return nullMarkerTtl;
        }

        /**
         * Sets the TTL of the null marker stored under the {@code ALLOW}
         * null policy.
         *
         * @param nullMarkerTtl the null-marker TTL
         * @since 0.1.0
         */
        public void setNullMarkerTtl(Duration nullMarkerTtl) {
            this.nullMarkerTtl = nullMarkerTtl;
        }

        /**
         * Returns the stale-while-revalidate window: how long an expired
         * entry may be served while it is reloaded in the background.
         *
         * @return the stale TTL, or {@code null} to inherit the defaults
         *         level
         * @since 0.1.0
         */
        public Duration getStaleTtl() {
            return staleTtl;
        }

        /**
         * Sets the stale-while-revalidate window.
         *
         * @param staleTtl the stale TTL
         * @since 0.1.0
         */
        public void setStaleTtl(Duration staleTtl) {
            this.staleTtl = staleTtl;
        }

        /**
         * Returns whether XFetch early refresh of hot keys is enabled.
         *
         * @return the XFetch switch, or {@code null} to inherit the defaults
         *         level
         * @since 0.1.0
         */
        public Boolean getXfetchEnabled() {
            return xfetchEnabled;
        }

        /**
         * Sets whether XFetch early refresh of hot keys is enabled.
         *
         * @param xfetchEnabled the XFetch switch
         * @since 0.1.0
         */
        public void setXfetchEnabled(Boolean xfetchEnabled) {
            this.xfetchEnabled = xfetchEnabled;
        }

        /**
         * Returns the XFetch beta: the tuning factor of the probabilistic
         * early-refresh lottery; smaller values refresh hot entries
         * earlier/more aggressively.
         *
         * @return the XFetch beta, or {@code null} to inherit the defaults
         *         level
         * @since 0.1.0
         */
        public Duration getXfetchBeta() {
            return xfetchBeta;
        }

        /**
         * Sets the XFetch beta. Must be positive when XFetch is enabled.
         *
         * @param xfetchBeta the XFetch beta
         * @since 0.1.0
         */
        public void setXfetchBeta(Duration xfetchBeta) {
            this.xfetchBeta = xfetchBeta;
        }

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
            return override;
        }
    }
}
