package io.tiercache.spi;

/**
 * Metrics events from the cache core. Semantic events only — timing and
 * aggregation live in the binder (the micrometer module).
 *
 * <p>Hot-path contract: an L1 hit produces exactly one {@link #onRequest}
 * call with no allocation and no clock reads; latency is timed only for
 * L2-touching operations. The default {@link #NOOP} costs nothing.
 */
public interface CacheMetricsListener {

    enum Outcome {
        L1_HIT, L2_HIT, MISS, LOAD, COALESCED
    }

    enum Level {
        L1, L2
    }

    enum Direction {
        SENT, RECEIVED, REPLAYED, DROPPED
    }

    CacheMetricsListener NOOP = new CacheMetricsListener() {
    };

    default void onRequest(String cache, Outcome outcome) {
    }

    default void onLatency(String cache, Level level, long nanos) {
    }

    default void onInvalidation(String cache, Direction direction) {
    }

    /** A null-marker was stored. */
    default void onNullEntry(String cache) {
    }

    /** An L2 entry past its TTL but within its stale window was served. */
    default void onStaleHit(String cache) {
    }

    /** An asynchronous revalidation was claimed and submitted for a key. */
    default void onRevalidationTriggered(String cache) {
    }

    /** An asynchronous revalidation finished without an error. */
    default void onRevalidationCompleted(String cache) {
    }

    /** An asynchronous revalidation failed; the stale entry keeps serving. */
    default void onRevalidationFailed(String cache) {
    }

    /**
     * Starts an L2 operation observation (tracing span in the binder).
     * Returns an opaque handle passed back to {@link #onL2OperationEnd}.
     * Never called on L1 hits.
     */
    default Object onL2OperationStart(String cache, String operation) {
        return null;
    }

    default void onL2OperationEnd(String cache, String operation, boolean hit, Object handle) {
    }

    /**
     * Wraps inbound invalidation processing (tracing span in the binder).
     */
    default Object onInvalidationStart(String cache) {
        return null;
    }

    default void onInvalidationEnd(String cache, Object handle) {
    }
}
