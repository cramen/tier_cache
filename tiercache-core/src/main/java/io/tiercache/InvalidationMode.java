package io.tiercache;

/**
 * Invalidation event mode for a cache: plain INVALIDATE events (default) or
 * UPDATE events carrying the new value payload, letting receivers warm L1
 * without an L2 read (for read-heavy caches).
 */
public enum InvalidationMode {
    INVALIDATE, UPDATE
}
