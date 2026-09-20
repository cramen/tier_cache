package io.tiercache;

import java.time.Duration;

/**
 * Per-cache configuration overrides. Any field left {@code null} inherits
 * the global default (see {@link TierCacheFactory}).
 *
 * @since 0.1.0
 */
public final class CacheOverride {

    private Long l1MaxSize;
    private Duration l1ExpireAfterWrite;
    private Duration l1ExpireAfterAccess;
    private Duration l2Ttl;
    private Double jitterAmplitude;
    private NullPolicy nullPolicy;
    private InvalidationMode invalidationMode;
    private Long payloadCapBytes;
    private Duration staleTtl;
    private Boolean xfetchEnabled;
    private Duration xfetchBeta;
    private Duration degradationStaleTtl;

    /**
     * Creates an empty override: every field inherits the global defaults
     * until set.
     *
     * @since 0.1.0
     */
    public CacheOverride() {
    }

    /**
     * Overrides the maximum number of entries in L1.
     *
     * @param l1MaxSize maximum L1 entry count; must be positive
     * @return this override
     * @since 0.1.0
     */
    public CacheOverride l1MaxSize(long l1MaxSize) {
        this.l1MaxSize = l1MaxSize;
        return this;
    }

    /**
     * Overrides the L1 TTL since write (before jitter).
     *
     * @param l1ExpireAfterWrite L1 write TTL, or {@code null} to inherit
     * @return this override
     * @since 0.1.0
     */
    public CacheOverride l1ExpireAfterWrite(Duration l1ExpireAfterWrite) {
        this.l1ExpireAfterWrite = l1ExpireAfterWrite;
        return this;
    }

    /**
     * Overrides the L1 TTL since last access.
     *
     * @param l1ExpireAfterAccess L1 access TTL, or {@code null} to disable /
     *                            inherit
     * @return this override
     * @since 0.1.0
     */
    public CacheOverride l1ExpireAfterAccess(Duration l1ExpireAfterAccess) {
        this.l1ExpireAfterAccess = l1ExpireAfterAccess;
        return this;
    }

    /**
     * Overrides the L2 entry TTL.
     *
     * @param l2Ttl L2 TTL, or {@code null} to inherit
     * @return this override
     * @since 0.1.0
     */
    public CacheOverride l2Ttl(Duration l2Ttl) {
        this.l2Ttl = l2Ttl;
        return this;
    }

    /**
     * Overrides the TTL jitter amplitude.
     *
     * @param jitterAmplitude jitter amplitude as a fraction in [0, 1)
     * @return this override
     * @since 0.1.0
     */
    public CacheOverride jitterAmplitude(double jitterAmplitude) {
        this.jitterAmplitude = jitterAmplitude;
        return this;
    }

    /**
     * Overrides the null-caching policy.
     *
     * @param nullPolicy the null policy, or {@code null} to inherit
     * @return this override
     * @since 0.1.0
     */
    public CacheOverride nullPolicy(NullPolicy nullPolicy) {
        this.nullPolicy = nullPolicy;
        return this;
    }

    /**
     * Overrides the invalidation event mode.
     *
     * @param invalidationMode the invalidation mode, or {@code null} to
     *                         inherit
     * @return this override
     * @since 0.1.0
     */
    public CacheOverride invalidationMode(InvalidationMode invalidationMode) {
        this.invalidationMode = invalidationMode;
        return this;
    }

    /**
     * Overrides the maximum payload bytes for UPDATE events.
     *
     * @param payloadCapBytes payload cap in bytes; must be at least 1024
     * @return this override
     * @since 0.1.0
     */
    public CacheOverride payloadCapBytes(long payloadCapBytes) {
        this.payloadCapBytes = payloadCapBytes;
        return this;
    }

    /**
     * Overrides the stale-while-revalidate window.
     *
     * @param staleTtl stale window served from L2 past the entry TTL, or
     *                 {@code null} to inherit; {@code Duration.ZERO} disables
     *                 stale serving
     * @return this override
     * @since 0.1.0
     */
    public CacheOverride staleTtl(Duration staleTtl) {
        this.staleTtl = staleTtl;
        return this;
    }

    /**
     * Enables or disables XFetch probabilistic early refresh.
     *
     * @param xfetchEnabled whether XFetch is enabled
     * @return this override
     * @since 0.1.0
     */
    public CacheOverride xfetchEnabled(boolean xfetchEnabled) {
        this.xfetchEnabled = xfetchEnabled;
        return this;
    }

    /**
     * Sets the degradation stale window: extra L1 retention served stale
     * while the L2 circuit breaker rejects calls.
     *
     * @param degradationStaleTtl the window, or {@code null} to inherit;
     *                            {@code Duration.ZERO} disables stale
     *                            degraded serving
     * @return this override
     * @since 1.4.0
     */
    public CacheOverride degradationStaleTtl(Duration degradationStaleTtl) {
        this.degradationStaleTtl = degradationStaleTtl;
        return this;
    }

    /**
     * Overrides the XFetch tuning factor.
     *
     * @param xfetchBeta XFetch beta; smaller values trigger early refresh
     *                   more aggressively, or {@code null} to inherit
     * @return this override
     * @since 0.1.0
     */
    public CacheOverride xfetchBeta(Duration xfetchBeta) {
        this.xfetchBeta = xfetchBeta;
        return this;
    }

    /**
     * Resolves this override against the given global defaults.
     *
     * @param defaults the global defaults to inherit unset fields from;
     *                 must not be {@code null}
     * @return the fully-resolved settings for one named cache
     * @since 0.1.0
     */
    public CacheSettings resolve(CacheSettings defaults) {
        return new CacheSettings(
                l1MaxSize != null ? l1MaxSize : defaults.l1MaxSize(),
                l1ExpireAfterWrite != null ? l1ExpireAfterWrite : defaults.l1ExpireAfterWrite(),
                l1ExpireAfterAccess != null ? l1ExpireAfterAccess : defaults.l1ExpireAfterAccess(),
                l2Ttl != null ? l2Ttl : defaults.l2Ttl(),
                jitterAmplitude != null ? jitterAmplitude : defaults.jitterAmplitude(),
                nullPolicy != null ? nullPolicy : defaults.nullPolicy(),
                invalidationMode != null ? invalidationMode : defaults.invalidationMode(),
                payloadCapBytes != null ? payloadCapBytes : defaults.payloadCapBytes(),
                staleTtl != null ? staleTtl : defaults.staleTtl(),
                xfetchEnabled != null ? xfetchEnabled : defaults.xfetchEnabled(),
                xfetchBeta != null ? xfetchBeta : defaults.xfetchBeta(),
                degradationStaleTtl != null ? degradationStaleTtl : defaults.degradationStaleTtl());
    }
}
