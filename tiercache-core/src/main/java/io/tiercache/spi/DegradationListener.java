package io.tiercache.spi;

/**
 * Signals L2 degradation transitions: the circuit breaker opened
 * ({@link #onDegraded}) or closed after recovery ({@link #onRecovered}).
 * While degraded, cross-instance guarantees (putIfAbsent, rebuild
 * coordination) hold only per-instance. Operators must see these
 * transitions; metrics bind to this callback.
 */
public interface DegradationListener {

    DegradationListener NOOP = new DegradationListener() {
    };

    default void onDegraded() {
    }

    default void onRecovered() {
    }
}
