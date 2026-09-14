package io.tiercache;

/**
 * Invalidation event mode for a cache: plain INVALIDATE events (default) or
 * UPDATE events carrying the new value payload, letting receivers warm L1
 * without an L2 read (for read-heavy caches).
 *
 * @since 0.1.0
 */
public enum InvalidationMode {
    /** Publish plain invalidation events; receivers evict the key from L1. */
    INVALIDATE,
    /** Publish events carrying the new value; receivers warm L1 directly. */
    UPDATE
}
