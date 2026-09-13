package io.tiercache.spi;

import java.time.Duration;

/**
 * SPI for the L1 (in-process) cache level.
 *
 * <p>Implementations must be thread-safe. TTLs are per entry: each
 * {@link #put} carries the effective TTL computed by the core (base TTL with
 * jitter already applied). Entries are opaque {@link StoredEntry} holders —
 * implementations must store null-markers like any other entry.
 *
 * <p>Note: a two-level cache is eventually consistent by design.
 * Implementations must not claim or attempt to provide strong consistency.
 */
public interface LocalCache<K, V> {

    /**
     * Returns the entry for {@code key}, or {@code null} if absent or expired.
     */
    StoredEntry<V> get(K key);

    /**
     * Stores {@code entry} under {@code key} with the given effective TTL.
     */
    void put(K key, StoredEntry<V> entry, Duration ttl);

    /**
     * Removes {@code key} if present.
     */
    void evict(K key);

    /**
     * Removes all entries. Used for {@code evictAll} / Spring's
     * {@code Cache.clear()}.
     */
    void clear();

    /**
     * Atomically stores {@code entry} under {@code key} if absent (within
     * this instance). Used for degraded-mode putIfAbsent.
     */
    boolean setIfAbsent(K key, StoredEntry<V> entry, Duration ttl);
}
