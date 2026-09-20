package io.tiercache.spi;

import java.time.Duration;

/**
 * SPI for the L2 (distributed) cache level.
 *
 * <p>Implementations must be thread-safe. TTLs are per entry: each
 * {@link #put} carries the entry TTL. Entries are opaque {@link StoredEntry}
 * holders — implementations must persist null-markers like any other entry.
 * Implementations backed by a remote store must apply their own
 * connect/read/write timeouts and must throw only unchecked exceptions from
 * this interface.
 *
 * <p>Note: a two-level cache is eventually consistent by design.
 * Implementations must not claim or attempt to provide strong consistency.
 *
 * <p><b>Internal — not part of the supported API.</b> Extension point for
 * L2 transport implementations.
 *
 * @param <K> key type
 * @param <V> value type
 * @since 0.1.0
 */
public interface RemoteCache<K, V> {

    /**
     * Returns the entry for {@code key}, or {@code null} if absent or expired.
     *
     * @param key the key to look up
     * @return the stored entry, or {@code null}
     * @since 0.1.0
     */
    StoredEntry<V> get(K key);

    /**
     * Stores {@code entry} under {@code key} with the given TTL.
     *
     * @param key   the key to store under
     * @param entry the entry to store
     * @param ttl   the entry TTL
     * @since 0.1.0
     */
    void put(K key, StoredEntry<V> entry, Duration ttl);

    /**
     * Stores {@code entry} under {@code key} with the given logical TTL and
     * a stale window: the physical entry lifetime becomes
     * {@code ttl + staleTtl} and the stored frame carries the write
     * timestamp, so readers can serve the entry stale after its logical TTL
     * expires. A {@code null}, zero, or negative {@code staleTtl} means no
     * stale window — identical to {@link #put(Object, StoredEntry, Duration)}.
     * The default ignores the stale window; stale-capable transports
     * override.
     *
     * @param key      the key to store under
     * @param entry    the entry to store
     * @param ttl      the logical entry TTL
     * @param staleTtl the stale window beyond the logical TTL
     * @since 0.1.0
     */
    default void put(K key, StoredEntry<V> entry, Duration ttl, Duration staleTtl) {
        put(key, entry, ttl);
    }

    /**
     * Stores {@code entry} under {@code key} only if the entry's version is
     * not older than the currently stored one (unversioned current entries
     * lose). Returns {@code true} if the write won. The default
     * implementation writes unconditionally; versioned transports override
     * with an atomic compare. This is the write side of last-write-wins.
     *
     * @param key   the key to store under
     * @param entry the entry to store
     * @param ttl   the entry TTL
     * @return {@code true} if the write won
     * @since 0.1.0
     */
    default boolean putIfNewer(K key, StoredEntry<V> entry, Duration ttl) {
        put(key, entry, ttl);
        return true;
    }

    /**
     * Stale-window variant of {@link #putIfNewer(Object, StoredEntry, Duration)}:
     * the physical entry lifetime becomes {@code ttl + staleTtl} and the
     * stored frame carries the write timestamp, so the refreshed entry keeps
     * its stale window. The default writes through the stale-window
     * {@link #put(Object, StoredEntry, Duration, Duration)}; versioned
     * transports override to keep the compare atomic.
     *
     * @param key      the key to store under
     * @param entry    the entry to store
     * @param ttl      the logical entry TTL
     * @param staleTtl the stale window beyond the logical TTL
     * @return {@code true} if the write won
     * @since 0.1.0
     */
    default boolean putIfNewer(K key, StoredEntry<V> entry, Duration ttl, Duration staleTtl) {
        put(key, entry, ttl, staleTtl);
        return true;
    }

    /**
     * Removes {@code key} if present.
     *
     * @param key the key to remove
     * @since 0.1.0
     */
    void evict(K key);

    /**
     * Removes {@code key} if present, stamping the tombstone with the given
     * write version. Implementations with an invalidation journal use the
     * version for the journal entry; the default ignores it.
     *
     * @param key     the key to remove
     * @param version the write version of the removal
     * @since 0.1.0
     */
    default void evict(K key, io.tiercache.Version version) {
        evict(key);
    }

    /**
     * Removes all entries of this cache's namespace. Used for
     * {@code evictAll} / Spring's {@code Cache.clear()}.
     *
     * @since 0.1.0
     */
    void clear();

    /**
     * Versioned clear: removes all entries of this cache's namespace AND
     * records the eviction with the given version so the recovery journal
     * can replay it to receivers that missed the live notification (see the
     * invalidation journal requirement). The default delegates to
     * {@link #clear()} — implementations without a journal keep their
     * previous behavior, but then {@code evictAll} is not covered by
     * reconnect replay.
     *
     * @param version the version stamped on the journal row, or {@code null}
     *                for an unversioned clear
     * @since 1.2.1
     */
    default void clear(io.tiercache.Version version) {
        clear();
    }

    /**
     * Atomically stores {@code entry} under {@code key} only if the key is
     * absent (or expired), with the given TTL. This is the foundation for
     * distributed rebuild coordination and {@code putIfAbsent}.
     *
     * <p>Implementations backed by a remote store MUST make this operation
     * atomic across all clients of that store. Note: a stored null-marker
     * counts as PRESENT for this operation.
     *
     * @param key   the key to store under
     * @param entry the entry to store
     * @param ttl   the entry TTL
     * @return {@code true} if this call created the entry, {@code false} if
     *         the key already existed (not expired)
     * @since 0.1.0
     */
    boolean setIfAbsent(K key, StoredEntry<V> entry, Duration ttl);

    /**
     * Stores {@code entry} recording tag membership for later
     * tag-based eviction. Default: plain put (tags not tracked).
     *
     * @param key   the key to store under
     * @param entry the entry to store
     * @param ttl   the entry TTL
     * @param tags  the tags to associate with the entry
     * @since 0.1.0
     */
    default void putTagged(K key, StoredEntry<V> entry, Duration ttl, String[] tags) {
        put(key, entry, ttl);
    }

    /**
     * Keys currently tagged with {@code tag} (deserialized). Default: none.
     *
     * @param tag the tag to look up
     * @return the tagged keys; empty if the tag is unknown or tags are not
     *         tracked
     * @since 0.1.0
     */
    default java.util.List<K> keysByTag(String tag) {
        return java.util.List.of();
    }
}
