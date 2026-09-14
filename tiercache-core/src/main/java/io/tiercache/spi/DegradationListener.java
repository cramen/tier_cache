package io.tiercache.spi;

/**
 * Signals L2 degradation transitions: the circuit breaker opened
 * ({@link #onDegraded}) or closed after recovery ({@link #onRecovered}).
 * While degraded, cross-instance guarantees (putIfAbsent, rebuild
 * coordination) hold only per-instance. Operators must see these
 * transitions; metrics bind to this callback.
 *
 * <p><b>Internal — not part of the supported API.</b> Extension point for
 * the observability module.
 *
 * @since 0.1.0
 */
public interface DegradationListener {

    /**
     * A listener that ignores every transition.
     *
     * @since 0.1.0
     */
    DegradationListener NOOP = new DegradationListener() {
    };

    /**
     * The L2 circuit breaker opened; the cache runs L1-only.
     *
     * @since 0.1.0
     */
    default void onDegraded() {
    }

    /**
     * The L2 circuit breaker closed after recovery; missed invalidations
     * have been replayed.
     *
     * @since 0.1.0
     */
    default void onRecovered() {
    }
}
