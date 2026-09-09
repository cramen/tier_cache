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
 * @param invalidationMode   UPDATE to include payloads in invalidation events
 * @param payloadCapBytes    max payload bytes for UPDATE events; larger
 *                           writes fall back to plain INVALIDATE
 * @param staleTtl           stale-while-revalidate window served from L2 past
 *                           the entry TTL; {@code Duration.ZERO} disables
 *                           stale serving (default)
 * @param xfetchEnabled      probabilistic early refresh (XFetch) of fresh
 *                           L2 entries; default off
 * @param xfetchBeta         XFetch tuning factor; smaller values trigger
 *                           early refresh more aggressively
 */
public record CacheSettings(
        long l1MaxSize,
        Duration l1ExpireAfterWrite,
        Duration l1ExpireAfterAccess,
        Duration l2Ttl,
        double jitterAmplitude,
        NullPolicy nullPolicy,
        InvalidationMode invalidationMode,
        long payloadCapBytes,
        Duration staleTtl,
        boolean xfetchEnabled,
        Duration xfetchBeta) {

    public CacheSettings {
        Objects.requireNonNull(l1ExpireAfterWrite, "l1ExpireAfterWrite");
        Objects.requireNonNull(l2Ttl, "l2Ttl");
        Objects.requireNonNull(nullPolicy, "nullPolicy");
        Objects.requireNonNull(invalidationMode, "invalidationMode");
        if (payloadCapBytes < 1024) {
            throw new IllegalArgumentException("payloadCapBytes must be >= 1024, got " + payloadCapBytes);
        }
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
        // Value validation of staleTtl/xfetchBeta (negative window, non-positive
        // beta, XFetch without L2 TTL) is startup validation's job; see
        // io.tiercache.internal.CacheConfigValidator.
        Objects.requireNonNull(staleTtl, "staleTtl");
        Objects.requireNonNull(xfetchBeta, "xfetchBeta");
    }

    /**
     * Convenience constructor with stale serving and XFetch disabled
     * (the default behavior).
     */
    public CacheSettings(long l1MaxSize, Duration l1ExpireAfterWrite,
            Duration l1ExpireAfterAccess, Duration l2Ttl, double jitterAmplitude,
            NullPolicy nullPolicy, InvalidationMode invalidationMode, long payloadCapBytes) {
        this(l1MaxSize, l1ExpireAfterWrite, l1ExpireAfterAccess, l2Ttl, jitterAmplitude,
                nullPolicy, invalidationMode, payloadCapBytes,
                Duration.ZERO, false, Duration.ofSeconds(1));
    }

    /**
     * Sensible global defaults: jitter on by default with 10% amplitude;
     * null caching denied unless explicitly allowed; stale serving and
     * XFetch off.
     */
    public static CacheSettings defaults() {
        return new CacheSettings(
                10_000,
                Duration.ofMinutes(5),
                null,
                Duration.ofHours(1),
                0.10,
                NullPolicy.deny(),
                InvalidationMode.INVALIDATE,
                64 * 1024,
                Duration.ZERO,
                false,
                Duration.ofSeconds(1));
    }
}
