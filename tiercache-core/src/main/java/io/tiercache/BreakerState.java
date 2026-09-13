package io.tiercache;

/**
 * State of the L2 circuit breaker machine, as exposed by
 * {@link TierCacheFactory#breakerState()}.
 *
 * <p>Declaration order matches the {@code tiercache.breaker.state} gauge
 * mapping: {@code 0 = CLOSED}, {@code 1 = HALF_OPEN}, {@code 2 = OPEN}.
 */
public enum BreakerState {

    /** L2 calls flow normally. */
    CLOSED,

    /** Recovery probing: a bounded number of trial L2 calls is let through. */
    HALF_OPEN,

    /** L2 calls fail fast; the cache runs L1-only (degraded mode). */
    OPEN
}
