package io.tiercache.spi;

import java.time.Duration;

/**
 * SPI for the L2 (distributed) cache level.
 *
 * <p><b>Incubating:</b> this interface is part of the 0.x API and may change
 * incompatibly until the public API freeze (roadmap checkpoint CP-0).
 *
 * <p>Implementations must be thread-safe. TTLs are per entry (F-06): each
 * {@link #put} carries the entry TTL. Entries are opaque {@link StoredEntry}
 * holders — implementations must persist null-markers like any other entry.
 * Implementations backed by a remote store must apply their own
 * connect/read/write timeouts and must throw only unchecked exceptions from
 * this interface.
 *
 * <p>Note: a two-level cache is eventually consistent by design.
 * Implementations must not claim or attempt to provide strong consistency.
 */
public interface RemoteCache<K, V> {

    /**
     * Returns the entry for {@code key}, or {@code null} if absent or expired.
     */
    StoredEntry<V> get(K key);

    /**
     * Stores {@code entry} under {@code key} with the given TTL (F-06).
     */
    void put(K key, StoredEntry<V> entry, Duration ttl);

    /**
     * Removes {@code key} if present.
     */
    void evict(K key);

    /**
     * Atomically stores {@code value} under {@code key} only if the key is
     * absent (or expired), with the given TTL. This is the foundation for
     * distributed rebuild coordination (F-21) and {@code putIfAbsent} (F-03).
     *
     * <p>Implementations backed by a remote store MUST make this operation
     * atomic across all clients of that store. Note: a stored null-marker
     * counts as PRESENT for this operation.
     *
     * @return {@code true} if this call created the entry, {@code false} if
     *         the key already existed (not expired)
     */
    boolean setIfAbsent(K key, V value, Duration ttl);
}
