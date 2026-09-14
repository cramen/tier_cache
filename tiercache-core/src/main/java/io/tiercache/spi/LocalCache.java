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
 *
 * <p><b>Internal — not part of the supported API.</b> Extension point for
 * L1 implementations.
 *
 * @param <K> key type
 * @param <V> value type
 * @since 0.1.0
 */
public interface LocalCache<K, V> {

    /**
     * Returns the entry for {@code key}, or {@code null} if absent or expired.
     *
     * @param key the key to look up
     * @return the stored entry, or {@code null}
     * @since 0.1.0
     */
    StoredEntry<V> get(K key);

    /**
     * Stores {@code entry} under {@code key} with the given effective TTL.
     *
     * @param key   the key to store under
     * @param entry the entry to store
     * @param ttl   the effective entry TTL (jitter already applied)
     * @since 0.1.0
     */
    void put(K key, StoredEntry<V> entry, Duration ttl);

    /**
     * Removes {@code key} if present.
     *
     * @param key the key to remove
     * @since 0.1.0
     */
    void evict(K key);

    /**
     * Removes all entries. Used for {@code evictAll} / Spring's
     * {@code Cache.clear()}.
     *
     * @since 0.1.0
     */
    void clear();

    /**
     * Atomically stores {@code entry} under {@code key} if absent (within
     * this instance). Used for degraded-mode putIfAbsent.
     *
     * @param key   the key to store under
     * @param entry the entry to store
     * @param ttl   the effective entry TTL
     * @return {@code true} if this call created the entry
     * @since 0.1.0
     */
    boolean setIfAbsent(K key, StoredEntry<V> entry, Duration ttl);
}
