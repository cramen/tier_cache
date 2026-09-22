package io.tiercache.spi;

import io.tiercache.Version;

/**
 * The L1 side of a cache instance, exposed to the invalidation engine for
 * inbound events. Implementations apply events with last-write-wins
 * semantics.
 *
 * <p><b>Internal — not part of the supported API.</b> Implemented by the
 * cache engine.
 *
 * @since 0.1.0
 */
public interface InvalidationTarget {

    /**
     * The version of the L1 entry for {@code key}, or {@code null} if absent
     * or unknown.
     *
     * @param key the key to inspect
     * @return the entry's write version, or {@code null}
     * @since 0.1.0
     */
    Version versionOfL1Entry(Object key);

    /**
     * Evicts the L1 entry for {@code key} only if the event's version is
     * newer than the entry's (entries with unknown versions are evicted).
     *
     * @param key          the key to evict
     * @param eventVersion the version of the inbound event
     * @since 0.1.0
     */
    void evictL1IfNewer(Object key, Version eventVersion);

    /**
     * Clears the L1 entirely (EVICT_ALL events, journal overflow).
     *
     * @since 0.1.0
     */
    void evictAllL1();

    /**
     * Applies an UPDATE event: stores the payload in L1 if the event's
     * version is newer than the current entry's (or the entry is absent).
     *
     * @param key          the key to update
     * @param value        the new value from the event payload
     * @param eventVersion the version of the inbound event
     * @since 0.1.0
     */
    default void applyUpdateL1(Object key, Object value, Version eventVersion) {
        evictL1IfNewer(key, eventVersion); // default: no payload application
    }
    /** Local clear epoch used to reject recovery responses obtained before a clear. */
    default long recoveryGeneration() { return 0; }

    /**
     * Applies one recovered row if its local clear epoch is still valid.
     * Returns the next epoch, or -1 when superseded. Targets with concurrent
     * clears override this together with recoveryGeneration for atomic checks.
     */
    default long applyRecovery(io.tiercache.InvalidationMessage message, long expectedGeneration) {
        if (recoveryGeneration() != expectedGeneration) return -1;
        switch (message.type()) {
            case INVALIDATE -> evictL1IfNewer(message.key(), message.version());
            case UPDATE -> applyUpdateL1(message.key(), message.payload(), message.version());
            case EVICT_ALL -> evictAllL1();
        }
        return recoveryGeneration();
    }

    /** Clears against a reserved epoch; -1 means a concurrent clear superseded the reservation. */
    default long resetRecovery(long expectedGeneration) {
        if (recoveryGeneration() != expectedGeneration) return -1;
        evictAllL1();
        return recoveryGeneration();
    }

}
