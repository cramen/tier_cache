package io.tiercache.internal;

import io.tiercache.CacheConfigurationException;
import io.tiercache.CacheSettings;

/**
 * Startup validation of cache configuration invariants (F-04/F-05).
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
     * @throws CacheConfigurationException if TTL ordering (F-05) is violated
     */
    public static void validate(String cacheName, CacheSettings settings) {
        requireNotExceeding(cacheName, "l1ExpireAfterWrite",
                settings.l1ExpireAfterWrite(), settings);
        if (settings.l1ExpireAfterAccess() != null) {
            requireNotExceeding(cacheName, "l1ExpireAfterAccess",
                    settings.l1ExpireAfterAccess(), settings);
        }
    }

    private static void requireNotExceeding(String cacheName, String setting,
            java.time.Duration l1Ttl, CacheSettings settings) {
        if (l1Ttl.compareTo(settings.l2Ttl()) > 0) {
            throw new CacheConfigurationException(
                    "Cache '" + cacheName + "' violates TTL ordering (F-05): " + setting + "=" + l1Ttl
                            + " exceeds l2Ttl=" + settings.l2Ttl() + ". An L1 entry could outlive its L2"
                            + " counterpart and serve stale data after the L2 entry expired. Set " + setting
                            + " <= l2Ttl for cache '" + cacheName + "'.");
        }
    }
}
