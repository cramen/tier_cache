package io.tiercache.spi;

/**
 * Signals L2 degradation transitions: the circuit breaker opened
 * ({@link #onDegraded}) or closed after recovery ({@link #onRecovered}).
 * While degraded, cross-instance guarantees (putIfAbsent, rebuild
 * coordination) hold only per-instance. Operators must see these
 * transitions; metrics bind to this callback.
 *
 * <p><b>Incubating:</b> 0.x API, may change before 1.0.
 */
public interface DegradationListener {

    DegradationListener NOOP = new DegradationListener() {
    };

    default void onDegraded() {
    }

    default void onRecovered() {
    }
}
