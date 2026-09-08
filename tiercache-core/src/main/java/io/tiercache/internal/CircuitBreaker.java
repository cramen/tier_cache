package io.tiercache.internal;

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
 */
public final class CircuitBreaker {

    public record Config(int windowSize, double failureRatio, int minimumCalls,
            Duration halfOpenAfter, int probesToClose) {
        public Config {
            if (windowSize < 1 || minimumCalls < 1 || probesToClose < 1) {
                throw new IllegalArgumentException("windowSize/minimumCalls/probesToClose must be >= 1");
            }
            if (failureRatio <= 0.0 || failureRatio > 1.0) {
                throw new IllegalArgumentException("failureRatio must be in (0, 1], got " + failureRatio);
            }
            Objects.requireNonNull(halfOpenAfter, "halfOpenAfter");
        }

        /** Safe defaults: open at 50% failures over the last 20 calls (min 5),
         *  probe after 5 s, close after 3 good probes. */
        public static Config defaults() {
            return new Config(20, 0.5, 5, Duration.ofSeconds(5), 3);
        }
    }

    /** Transition listener: called on open and on close (after recovery hooks). */
    public interface Listener {
        void onOpen();

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
    private int probesInFlight;
    private int probesSucceeded;

    public CircuitBreaker(Config config, Listener listener) {
        this.config = config;
        this.listener = listener;
        this.window = new boolean[config.windowSize()]; // true = failure
    }

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
     * Asks permission for an L2 call. When open (and not yet probe time) or
     * when the probe budget is exhausted, returns {@code false}.
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
        if (state != State.OPEN) {
            state = State.OPEN;
            openedAtNanos = System.nanoTime();
            listener.onOpen();
        } else {
            openedAtNanos = System.nanoTime(); // re-opened: restart the wait
        }
    }

    private void close() {
        state = State.CLOSED;
        windowPos = 0;
        windowCount = 0;
        windowFailures = 0;
        listener.onClose();
    }
}
