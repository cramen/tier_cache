package io.tiercache;

import java.time.Duration;
import java.util.Objects;

/**
 * Fully-resolved settings for one named cache: every field is concrete.
 *
 * <p><b>Incubating:</b> 0.x API, may change before 1.0.
 *
 * @param l1MaxSize          maximum number of entries in L1
 * @param l1ExpireAfterWrite L1 TTL since write (before jitter)
 * @param l1ExpireAfterAccess L1 TTL since last access, or {@code null} to disable
 * @param l2Ttl              L2 entry TTL
 * @param jitterAmplitude    TTL jitter amplitude as a fraction in [0, 1)
 *                           (e.g. 0.1 = up to 10% shorter TTLs)
 * @param nullPolicy         null-caching policy; default {@code deny}
 */
public record CacheSettings(
        long l1MaxSize,
        Duration l1ExpireAfterWrite,
        Duration l1ExpireAfterAccess,
        Duration l2Ttl,
        double jitterAmplitude,
        NullPolicy nullPolicy) {

    public CacheSettings {
        Objects.requireNonNull(l1ExpireAfterWrite, "l1ExpireAfterWrite");
        Objects.requireNonNull(l2Ttl, "l2Ttl");
        Objects.requireNonNull(nullPolicy, "nullPolicy");
        if (l1MaxSize <= 0) {
            throw new IllegalArgumentException("l1MaxSize must be positive, got " + l1MaxSize);
        }
        if (l1ExpireAfterWrite.isZero() || l1ExpireAfterWrite.isNegative()) {
            throw new IllegalArgumentException("l1ExpireAfterWrite must be positive, got " + l1ExpireAfterWrite);
        }
        if (l2Ttl.isZero() || l2Ttl.isNegative()) {
            throw new IllegalArgumentException("l2Ttl must be positive, got " + l2Ttl);
        }
        if (l1ExpireAfterAccess != null
                && (l1ExpireAfterAccess.isZero() || l1ExpireAfterAccess.isNegative())) {
            throw new IllegalArgumentException("l1ExpireAfterAccess must be positive, got " + l1ExpireAfterAccess);
        }
        if (jitterAmplitude < 0.0 || jitterAmplitude >= 1.0) {
            throw new IllegalArgumentException("jitterAmplitude must be in [0, 1), got " + jitterAmplitude);
        }
    }

    /**
     * Sensible global defaults: jitter on by default with 10% amplitude;
     * null caching denied unless explicitly allowed.
     */
    public static CacheSettings defaults() {
        return new CacheSettings(
                10_000,
                Duration.ofMinutes(5),
                null,
                Duration.ofHours(1),
                0.10,
                NullPolicy.deny());
    }
}
