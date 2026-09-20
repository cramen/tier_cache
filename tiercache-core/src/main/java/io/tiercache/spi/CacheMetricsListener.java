package io.tiercache.spi;

/**
 * Metrics events from the cache core. Semantic events only — timing and
 * aggregation live in the binder (the micrometer module).
 *
 * <p>Hot-path contract: an L1 hit produces exactly one {@link #onRequest}
 * call with no allocation and no clock reads; latency is timed only for
 * L2-touching operations. The default {@link #NOOP} costs nothing.
 *
 * <p><b>Internal — not part of the supported API.</b> Extension point for
 * the observability module.
 *
 * @since 0.1.0
 */
public interface CacheMetricsListener {

    /**
     * Outcome of a single cache request.
     *
     * @since 0.1.0
     */
    enum Outcome {
        /** Served from L1. */
        /** Served a physically retained L1 entry stale while the breaker rejected L2. */
        STALE_DEGRADED,
        L1_HIT,
        /** Served from L2 (L1 warmed). */
        L2_HIT,
        /** Nothing stored; no loader run or loader signaled absence. */
        MISS,
        /** The loader ran and produced a value. */
        LOAD,
        /** Joined an in-flight load for the same key (singleflight). */
        COALESCED
    }

    /**
     * The cache level an observation applies to.
     *
     * @since 0.1.0
     */
    enum Level {
        /** The in-process level. */
        L1,
        /** The distributed level. */
        L2
    }

    /**
     * Direction of an invalidation event relative to this instance.
     *
     * @since 0.1.0
     */
    enum Direction {
        /** Published by this instance. */
        SENT,
        /** Received from another instance. */
        RECEIVED,
        /** Replayed from the journal after a reconnect. */
        REPLAYED,
        /** Dropped (e.g. bounded buffer overflow). */
        DROPPED
    }

    /**
     * A listener that ignores every event; costs nothing on the hot path.
     *
     * @since 0.1.0
     */
    CacheMetricsListener NOOP = new CacheMetricsListener() {
    };

    /**
     * A cache request completed with the given outcome.
     *
     * @param cache   the cache name
     * @param outcome the request outcome
     * @since 0.1.0
     */
    default void onRequest(String cache, Outcome outcome) {
    }

    /**
     * An L2-touching operation took the given time.
     *
     * @param cache the cache name
     * @param level the level the latency belongs to
     * @param nanos the elapsed time in nanoseconds
     * @since 0.1.0
     */
    default void onLatency(String cache, Level level, long nanos) {
    }

    /**
     * An invalidation event flowed past this instance.
     *
     * @param cache     the cache name
     * @param direction the event direction
     * @since 0.1.0
     */
    default void onInvalidation(String cache, Direction direction) {
    }

    /**
     * A null-marker was stored.
     *
     * @param cache the cache name
     * @since 0.1.0
     */
    default void onNullEntry(String cache) {
    }

    /**
     * An L2 entry past its TTL but within its stale window was served.
     *
     * @param cache the cache name
     * @since 0.1.0
     */
    default void onStaleHit(String cache) {
    }

    /**
     * An asynchronous revalidation was claimed and submitted for a key.
     *
     * @param cache the cache name
     * @since 0.1.0
     */
    default void onRevalidationTriggered(String cache) {
    }

    /**
     * An asynchronous revalidation finished without an error.
     *
     * @param cache the cache name
     * @since 0.1.0
     */
    default void onRevalidationCompleted(String cache) {
    }

    /**
     * An asynchronous revalidation failed; the stale entry keeps serving.
     *
     * @param cache the cache name
     * @since 0.1.0
     */
    default void onRevalidationFailed(String cache) {
    }

    /**
     * Starts an L2 operation observation (tracing span in the binder).
     * Returns an opaque handle passed back to {@link #onL2OperationEnd}.
     * Never called on L1 hits.
     *
     * @param cache     the cache name
     * @param operation the operation name (e.g. {@code "get"})
     * @return an opaque observation handle, or {@code null}
     * @since 0.1.0
     */
    default Object onL2OperationStart(String cache, String operation) {
        return null;
    }

    /**
     * Ends an L2 operation observation started by
     * {@link #onL2OperationStart}.
     *
     * @param cache     the cache name
     * @param operation the operation name
     * @param hit       whether the operation found an entry
     * @param handle    the handle returned by the matching start call
     * @since 0.1.0
     */
    default void onL2OperationEnd(String cache, String operation, boolean hit, Object handle) {
    }

    /**
     * Wraps inbound invalidation processing (tracing span in the binder).
     *
     * @param cache the cache name
     * @return an opaque observation handle, or {@code null}
     * @since 0.1.0
     */
    default Object onInvalidationStart(String cache) {
        return null;
    }

    /**
     * Ends an inbound invalidation observation started by
     * {@link #onInvalidationStart}.
     *
     * @param cache  the cache name
     * @param handle the handle returned by the matching start call
     * @since 0.1.0
     */
    default void onInvalidationEnd(String cache, Object handle) {
    }
}
