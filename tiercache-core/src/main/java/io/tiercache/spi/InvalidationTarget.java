package io.tiercache.spi;

import io.tiercache.Version;

/**
 * The L1 side of a cache instance, exposed to the invalidation engine for
 * inbound events. Implementations apply events with last-write-wins
 * semantics.
 *
 * <p><b>Incubating:</b> 0.x API, may change before the public API freeze.
 */
public interface InvalidationTarget {

    /**
     * The version of the L1 entry for {@code key}, or {@code null} if absent
     * or unknown.
     */
    Version versionOfL1Entry(Object key);

    /**
     * Evicts the L1 entry for {@code key} only if the event's version is
     * newer than the entry's (entries with unknown versions are evicted).
     */
    void evictL1IfNewer(Object key, Version eventVersion);

    /**
     * Clears the L1 entirely (EVICT_ALL events, journal overflow).
     */
    void evictAllL1();
}
