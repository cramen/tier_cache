package io.tiercache.micrometer;

import io.tiercache.TierCacheFactory;
import io.tiercache.spi.InvalidationJournal;

import java.util.List;

/**
 * JMX view of cache state. Deliberately no top-N keys: per-key counting
 * taxes the hot path, so key-level inspection is refused by design.
 *
 * @since 0.1.0
 */
public interface TiercacheInspectionMXBean {

    /**
     * Returns the names of all registered caches.
     *
     * @return the cache names; never {@code null}, possibly empty
     * @since 0.1.0
     */
    String[] getCacheNames();

    /**
     * Returns the L1 hit ratio of a cache: L1 hits over all requests
     * (L1 hits, L2 hits, misses, and loads).
     *
     * @param cache the cache name
     * @return the ratio in {@code [0, 1]}; {@code 0} when no requests have
     *     been recorded yet
     * @since 0.1.0
     */
    double getL1HitRatio(String cache);

    /**
     * Returns the L2 hit ratio of a cache: L2 hits over all requests
     * (L1 hits, L2 hits, misses, and loads).
     *
     * @param cache the cache name
     * @return the ratio in {@code [0, 1]}; {@code 0} when no requests have
     *     been recorded yet
     * @since 0.1.0
     */
    double getL2HitRatio(String cache);

    /**
     * Returns the circuit-breaker state of the factory.
     *
     * @return {@code "closed"}, {@code "half_open"}, or {@code "open"}
     *     (open = degraded L1-only mode)
     * @since 0.1.0
     */
    String getBreakerState();

    /**
     * Returns the number of entries currently held in the invalidation
     * journal for a cache.
     *
     * @param cache the cache name
     * @return the journal size, or {@code -1} when no journal is configured
     * @since 0.1.0
     */
    long getJournalSize(String cache);
}
