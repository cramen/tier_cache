package io.tiercache.spi;

import java.time.Duration;

/**
 * SPI for the L1 (in-process) cache level.
 *
 * <p><b>Incubating:</b> this interface is part of the 0.x API and may change
 * incompatibly until the public API freeze (roadmap checkpoint CP-0).
 *
 * <p>Implementations must be thread-safe. TTLs are per entry: each
 * {@link #put} carries the effective TTL computed by the core (base TTL with
 * jitter already applied).
 *
 * <p>Note: a two-level cache is eventually consistent by design.
 * Implementations must not claim or attempt to provide strong consistency.
 */
public interface LocalCache<K, V> {

    /**
     * Returns the value for {@code key}, or {@code null} if absent or expired.
     */
    V get(K key);

    /**
     * Stores {@code value} under {@code key} with the given effective TTL.
     */
    void put(K key, V value, Duration ttl);

    /**
     * Removes {@code key} if present.
     */
    void evict(K key);
}
