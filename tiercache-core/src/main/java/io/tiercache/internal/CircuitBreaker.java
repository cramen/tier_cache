package io.tiercache.internal;

import io.tiercache.BreakerState;

import java.time.Duration;
import java.util.Objects;

/**
 * Circuit breaker over L2 operations: closed / open / half-open.
 *
 * <p>Failure accounting is a count-based sliding window (a ring of the last
 * {@code windowSize} outcomes) — allocation-free in steady state. Opens when
 * failures reach {@code failureRatio} of the window (after
 * {@code minimumCalls}); half-opens after {@code halfOpenAfter}, allowing
 * {@code probesToClose} probe calls; closes when all probes succeed, reopens
 * on any probe failure.
 *
 * <p>Thread-safe. Transitions are reported via a callback (wired by the
 * factory to the degradation listener and recovery hooks).
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @since 0.1.0
 */
public final class CircuitBreaker {

    /**
     * Breaker thresholds.
     *
     * @param windowSize    number of recent outcomes kept in the sliding
     *                      window
     * @param failureRatio  fraction of the window that must be failures to
     *                      open, in (0, 1]
     * @param minimumCalls  minimum outcomes before the ratio is evaluated
     * @param halfOpenAfter wait before recovery probing starts
     * @param probesToClose successful probes required to close again
     * @since 0.1.0
     */
    public record Config(int windowSize, double failureRatio, int minimumCalls,
            Duration halfOpenAfter, int probesToClose) {

        /**
         * Validates the thresholds.
         *
         * @param windowSize    number of recent outcomes kept in the sliding
         *                      window
         * @param failureRatio  fraction of the window that must be failures
         *                      to open, in (0, 1]
         * @param minimumCalls  minimum outcomes before the ratio is
         *                      evaluated
         * @param halfOpenAfter wait before recovery probing starts
         * @param probesToClose successful probes required to close again
         * @throws NullPointerException     if {@code halfOpenAfter} is
         *                                  {@code null}
         * @throws IllegalArgumentException if a threshold is out of range
         * @since 0.1.0
         */
        public Config {
            if (windowSize < 1 || minimumCalls < 1 || probesToClose < 1) {
                throw new IllegalArgumentException("windowSize/minimumCalls/probesToClose must be >= 1");
            }
            if (failureRatio <= 0.0 || failureRatio > 1.0) {
                throw new IllegalArgumentException("failureRatio must be in (0, 1], got " + failureRatio);
            }
            Objects.requireNonNull(halfOpenAfter, "halfOpenAfter");
        }

        /**
         * Safe defaults: open at 50% failures over the last 20 calls
         * (min 5), probe after 5 s, close after 3 good probes.
         *
         * @return the default configuration
         * @since 0.1.0
         */
        public static Config defaults() {
            return new Config(20, 0.5, 5, Duration.ofSeconds(5), 3);
        }
    }

    /**
     * Transition listener: called on open and on close (after recovery
     * hooks).
     *
     * @since 0.1.0
     */
    public interface Listener {

        /**
         * Called when the breaker opens.
         *
         * @since 0.1.0
         */
        void onOpen();

        /**
         * Called when the breaker closes after recovery.
         *
         * @since 0.1.0
         */
        void onClose();
    }

    private final Config config;
    private final Listener listener;
    private final boolean[] window;
    private int windowPos;
    private int windowCount;
    private int windowFailures;

    private enum State {
        CLOSED, OPEN, HALF_OPEN
    }

    private State state = State.CLOSED;
    private long openedAtNanos;
    private long epoch;
    private int probesInFlight;
    private int probesSucceeded;

    /**
     * Creates a breaker.
     *
     * @param config   the thresholds
     * @param listener transition callback
     * @since 0.1.0
     */
    public CircuitBreaker(Config config, Listener listener) {
        this.config = config;
        this.listener = listener;
        this.window = new boolean[config.windowSize()]; // true = failure
    }

    /**
     * Whether the breaker is currently open (failing L2 calls fast). An open
     * breaker whose wait has elapsed lazily transitions to half-open here.
     *
     * @return {@code true} while the breaker is open
     * @since 0.1.0
     */
    public synchronized boolean isOpen() {
        if (state == State.OPEN
                && System.nanoTime() - openedAtNanos >= config.halfOpenAfter().toNanos()) {
            state = State.HALF_OPEN;
            probesInFlight = 0;
            probesSucceeded = 0;
        }
        return state == State.OPEN;
    }

    /**
     * Current state of the breaker machine. An open breaker whose wait has
     * elapsed is reported as {@link BreakerState#HALF_OPEN} even before the
     * next probe call (the same lazy transition {@link #isOpen()} performs).
     *
     * @return the current state; never {@code null}
     * @since 0.1.0
     */
    public synchronized BreakerState state() {
        if (state == State.OPEN
                && System.nanoTime() - openedAtNanos >= config.halfOpenAfter().toNanos()) {
            state = State.HALF_OPEN;
            probesInFlight = 0;
            probesSucceeded = 0;
        }
        return switch (state) {
            case CLOSED -> BreakerState.CLOSED;
            case OPEN -> BreakerState.OPEN;
            case HALF_OPEN -> BreakerState.HALF_OPEN;
        };
    }

    /**
     * Asks permission for an L2 call. When open (and not yet probe time) or
     * when the probe budget is exhausted, returns {@code false}.
     *
     * @return {@code true} if the call may proceed
     * @since 0.1.0
     */
    public synchronized boolean tryAcquire() {
        switch (state) {
            case CLOSED:
                return true;
            case OPEN:
                if (System.nanoTime() - openedAtNanos < config.halfOpenAfter().toNanos()) {
                    return false;
                }
                state = State.HALF_OPEN;
                probesInFlight = 0;
                probesSucceeded = 0;
                // fall through
            case HALF_OPEN:
                if (probesInFlight >= config.probesToClose()) {
                    return false;
                }
                probesInFlight++;
                return true;
            default:
                throw new IllegalStateException("unknown state");
        }
    }

    /**
     * Admits one operation whose completion may be neutral (for example, an
     * unsupported capability discovered by a custom SPI). The permit cannot
     * retire a probe from a subsequent breaker episode.
     *
     * @return a once-completable permit, or null when admission is rejected
     * @since 1.5.0
     */
    public synchronized Permit tryAcquirePermit() {
        return tryAcquire() ? new Permit(epoch) : null;
    }

    /**
     * Once-only accounting for an admitted operation; callbacks use the same
     * transition rules as the legacy accounting methods.
     * @since 1.5.0
     */
    public final class Permit {
        private final long acquiredEpoch;
        private boolean completed;

        private Permit(long acquiredEpoch) {
            this.acquiredEpoch = acquiredEpoch;
        }

        /**
         * Records successful remote execution.
         * @since 1.5.0
         */
        public void success() { complete(1); }

        /**
         * Records failed remote execution.
         * @since 1.5.0
         */
        public void failure() { complete(-1); }

        /**
         * Retires admission without recording a remote outcome.
         * @since 1.5.0
         */
        public void cancel() { complete(0); }

        private void complete(int outcome) {
            synchronized (CircuitBreaker.this) {
                if (completed) {
                    return;
                }
                completed = true;
                if (acquiredEpoch != epoch) {
                    return;
                }
                if (outcome == 0) {
                    if (state == State.HALF_OPEN) {
                        probesInFlight--;
                    }
                } else if (outcome > 0) {
                    onSuccess();
                } else {
                    onFailure();
                }
            }
        }
    }

    /**
     * Records a successful L2 call.
     *
     * @since 0.1.0
     */
    public synchronized void onSuccess() {
        if (state == State.HALF_OPEN) {
            probesInFlight--;
            if (++probesSucceeded >= config.probesToClose()) {
                close();
            }
            return;
        }
        record(false);
    }

    /**
     * Records a failed L2 call.
     *
     * @since 0.1.0
     */
    public synchronized void onFailure() {
        if (state == State.HALF_OPEN) {
            open(); // probe failed: reopen, restart the wait
            return;
        }
        record(true);
        if (windowCount >= config.minimumCalls()
                && windowFailures >= Math.ceil(config.failureRatio() * Math.min(windowCount, config.windowSize()))) {
            open();
        }
    }

    private void record(boolean failure) {
        if (windowCount < config.windowSize()) {
            windowCount++;
        } else if (window[windowPos]) {
            windowFailures--; // overwritten failure leaves the window
        }
        if (failure) {
            windowFailures++;
        }
        window[windowPos] = failure;
        windowPos = (windowPos + 1) % config.windowSize();
    }

    private void open() {
        epoch++;
        if (state != State.OPEN) {
            state = State.OPEN;
            openedAtNanos = System.nanoTime();
            listener.onOpen();
        } else {
            openedAtNanos = System.nanoTime(); // re-opened: restart the wait
        }
    }

    private void close() {
        epoch++;
        state = State.CLOSED;
        windowPos = 0;
        windowCount = 0;
        windowFailures = 0;
        listener.onClose();
    }
}
