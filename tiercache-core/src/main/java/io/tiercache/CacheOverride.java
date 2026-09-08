package io.tiercache;

import java.time.Duration;

/**
 * Per-cache configuration overrides. Any field left {@code null} inherits
 * the global default (see {@link TierCacheFactory}).
 *
 * <p><b>Incubating:</b> 0.x API, may change before 1.0.
 */
public final class CacheOverride {

    private Long l1MaxSize;
    private Duration l1ExpireAfterWrite;
    private Duration l1ExpireAfterAccess;
    private Duration l2Ttl;
    private Double jitterAmplitude;
    private NullPolicy nullPolicy;

    public CacheOverride l1MaxSize(long l1MaxSize) {
        this.l1MaxSize = l1MaxSize;
        return this;
    }

    public CacheOverride l1ExpireAfterWrite(Duration l1ExpireAfterWrite) {
        this.l1ExpireAfterWrite = l1ExpireAfterWrite;
        return this;
    }

    public CacheOverride l1ExpireAfterAccess(Duration l1ExpireAfterAccess) {
        this.l1ExpireAfterAccess = l1ExpireAfterAccess;
        return this;
    }

    public CacheOverride l2Ttl(Duration l2Ttl) {
        this.l2Ttl = l2Ttl;
        return this;
    }

    public CacheOverride jitterAmplitude(double jitterAmplitude) {
        this.jitterAmplitude = jitterAmplitude;
        return this;
    }

    public CacheOverride nullPolicy(NullPolicy nullPolicy) {
        this.nullPolicy = nullPolicy;
        return this;
    }

    /**
     * Resolves this override against the given global defaults.
     */
    public CacheSettings resolve(CacheSettings defaults) {
        return new CacheSettings(
                l1MaxSize != null ? l1MaxSize : defaults.l1MaxSize(),
                l1ExpireAfterWrite != null ? l1ExpireAfterWrite : defaults.l1ExpireAfterWrite(),
                l1ExpireAfterAccess != null ? l1ExpireAfterAccess : defaults.l1ExpireAfterAccess(),
                l2Ttl != null ? l2Ttl : defaults.l2Ttl(),
                jitterAmplitude != null ? jitterAmplitude : defaults.jitterAmplitude(),
                nullPolicy != null ? nullPolicy : defaults.nullPolicy());
    }
}
