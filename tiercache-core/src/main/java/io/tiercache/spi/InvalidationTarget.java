package io.tiercache.spi;

import io.tiercache.Version;

/**
 * The L1 side of a cache instance, exposed to the invalidation engine for
 * inbound events. Implementations apply events with last-write-wins
 * semantics.
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

    /**
     * Applies an UPDATE event: stores the payload in L1 if the event's
     * version is newer than the current entry's (or the entry is absent).
     */
    default void applyUpdateL1(Object key, Object value, Version eventVersion) {
        evictL1IfNewer(key, eventVersion); // default: no payload application
    }
}
