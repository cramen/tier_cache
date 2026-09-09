package io.tiercache.internal;

import io.tiercache.CacheConfigurationException;
import io.tiercache.CacheSettings;

/**
 * Startup validation of cache configuration invariants (fail-fast).
 *
 * <p>Kept independent of the factory plumbing so that framework adapters
 * (e.g. a Spring Boot starter with relaxed binding) can reuse it verbatim.
 */
public final class CacheConfigValidator {

    private CacheConfigValidator() {
    }

    /**
     * Validates the resolved settings of one named cache. Jitter only ever
     * shortens TTLs, so the effective L1 TTL equals the configured
     * expire-after-write/access values.
     *
     * @throws CacheConfigurationException if TTL ordering or a stale-serving
     *         invariant is violated
     */
    public static void validate(String cacheName, CacheSettings settings) {
        // Stale-serving invariants first: the TTL-ordering checks below
        // dereference l2Ttl and would NPE instead of failing fast if it
        // were ever absent.
        if (settings.staleTtl().isNegative()) {
            throw new CacheConfigurationException(
                    "Cache '" + cacheName + "' violates stale-serving configuration: staleTtl="
                            + settings.staleTtl() + " is negative. The stale window must be zero"
                            + " (disabled) or positive. Set staleTtl >= 0 for cache '" + cacheName + "'.");
        }
        if (settings.xfetchEnabled()) {
            if (settings.xfetchBeta().isZero() || settings.xfetchBeta().isNegative()) {
                throw new CacheConfigurationException(
                        "Cache '" + cacheName + "' violates stale-serving configuration: xfetchBeta="
                                + settings.xfetchBeta() + " must be positive when XFetch is enabled."
                                + " Set xfetchBeta > 0 for cache '" + cacheName + "'.");
            }
            if (settings.l2Ttl() == null) {
                // Unreachable through the public config model today (l2Ttl is
                // mandatory in CacheSettings); kept so framework adapters that
                // relax that invariant still fail fast here.
                throw new CacheConfigurationException(
                        "Cache '" + cacheName + "' violates stale-serving configuration: XFetch"
                                + " measures entry age against the L2 TTL, but no l2Ttl is configured."
                                + " Set l2Ttl for cache '" + cacheName + "'.");
            }
        }
        requireNotExceeding(cacheName, "l1ExpireAfterWrite",
                settings.l1ExpireAfterWrite(), settings);
        if (settings.l1ExpireAfterAccess() != null) {
            requireNotExceeding(cacheName, "l1ExpireAfterAccess",
                    settings.l1ExpireAfterAccess(), settings);
        }
        if (settings.nullPolicy().markerTtl() != null) {
            // The null-marker obeys the same TTL ordering invariant.
            requireNotExceeding(cacheName, "nullPolicy.markerTtl",
                    settings.nullPolicy().markerTtl(), settings);
        }
    }

    private static void requireNotExceeding(String cacheName, String setting,
            java.time.Duration l1Ttl, CacheSettings settings) {
        if (l1Ttl.compareTo(settings.l2Ttl()) > 0) {
            throw new CacheConfigurationException(
                    "Cache '" + cacheName + "' violates TTL ordering: " + setting + "=" + l1Ttl
                            + " exceeds l2Ttl=" + settings.l2Ttl() + ". An L1 entry could outlive its L2"
                            + " counterpart and serve stale data after the L2 entry expired. Set " + setting
                            + " <= l2Ttl for cache '" + cacheName + "'.");
        }
    }
}
