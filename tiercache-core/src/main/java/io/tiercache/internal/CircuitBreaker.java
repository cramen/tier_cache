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
    private final java.util.function.LongSupplier clock;
    private final boolean[] window;
    private int windowPos, windowCount, windowFailures;
    private enum State { CLOSED, OPEN, HALF_OPEN }
    private State state = State.CLOSED;
    private long openedAtNanos, epoch, recoveryDelayNanos;
    private int probesInFlight, probesSucceeded, recoveryFailures;
    private boolean recoveryPending;
    private java.util.concurrent.Executor recoveryExecutor;
    private java.util.function.Supplier<java.util.concurrent.CompletionStage<Boolean>> recoveryHook;

    /** Creates a breaker over count-based thresholds. */
    public CircuitBreaker(Config config, Listener listener) {
        this(config, listener, System::nanoTime);
    }

    CircuitBreaker(Config config, Listener listener, java.util.function.LongSupplier clock) {
        this.clock = clock;
        this.config = Objects.requireNonNull(config);
        this.listener = Objects.requireNonNull(listener);
        this.window = new boolean[config.windowSize()];
    }

    /** Configures asynchronous coherence recovery; callers install this before use. */
    public synchronized void configureRecovery(java.util.concurrent.Executor executor,
            java.util.function.Supplier<java.util.concurrent.CompletionStage<Boolean>> hook) {
        recoveryExecutor = Objects.requireNonNull(executor);
        recoveryHook = Objects.requireNonNull(hook);
    }

    /** Retires the coherence hook without manufacturing a successful data probe. */
    public synchronized void detachRecovery() {
        recoveryHook = null;
        recoveryExecutor = null;
        recoveryDelayNanos = 0;
        recoveryFailures = 0;
        epoch++;
        recoveryPending = false;
        probesInFlight = 0;
        probesSucceeded = 0;
        // An actual OPEN episode keeps its original probe wait.
    }

    private void advance() {
        if (state == State.OPEN && clock.getAsLong() - openedAtNanos >=
                Math.max(config.halfOpenAfter().toNanos(), recoveryDelayNanos)) {
            state = State.HALF_OPEN;
            probesInFlight = 0;
            probesSucceeded = 0;
        }
    }

    /** Whether the breaker is currently OPEN, including its configured wait. */
    public synchronized boolean isOpen() { advance(); return state == State.OPEN; }

    /** Current state; pending coherence recovery is HALF_OPEN, never CLOSED. */
    public synchronized BreakerState state() {
        advance();
        return switch (state) {
            case CLOSED -> BreakerState.CLOSED;
            case OPEN -> BreakerState.OPEN;
            case HALF_OPEN -> BreakerState.HALF_OPEN;
        };
    }

    /** Admits one remote attempt without waiting for recovery. */
    public synchronized boolean tryAcquire() {
        advance();
        if (state == State.CLOSED) return true;
        if (state == State.OPEN || recoveryPending || probesInFlight >= config.probesToClose()) return false;
        probesInFlight++;
        return true;
    }

    /** Returns an epoch-bound, once-completable admission or null on rejection. */
    public synchronized Permit tryAcquirePermit() { return tryAcquire() ? new Permit(epoch) : null; }

    /** Once-only accounting, including neutral completion of unsupported calls. */
    public final class Permit {
        private final long acquiredEpoch;
        private boolean completed;
        private Permit(long acquiredEpoch) { this.acquiredEpoch = acquiredEpoch; }
        /** Records a successful remote execution. */
        public void success() { complete(1); }
        /** Records a failed remote execution. */
        public void failure() { complete(-1); }
        /** Returns admission without adding a remote outcome to the window. */
        public void cancel() { complete(0); }
        private void complete(int outcome) {
            Runnable action;
            synchronized (CircuitBreaker.this) {
                if (completed) return;
                completed = true;
                if (acquiredEpoch != epoch) return;
                if (outcome == 0) {
                    if (state == State.HALF_OPEN && probesInFlight > 0) probesInFlight--;
                    return;
                }
                action = outcome > 0 ? successLocked() : failureLocked();
            }
            notifySafely(action);
        }
    }

    /** Compatibility accounting method; transition callbacks run outside the monitor. */
    public void onSuccess() {
        Runnable action;
        synchronized (this) { action = successLocked(); }
        notifySafely(action);
    }

    /** Compatibility accounting method; transition callbacks run outside the monitor. */
    public void onFailure() {
        Runnable action;
        synchronized (this) { action = failureLocked(); }
        notifySafely(action);
    }

    private Runnable successLocked() {
        if (state == State.OPEN || recoveryPending) return null;
        if (state == State.HALF_OPEN) {
            if (probesInFlight > 0) probesInFlight--;
            if (++probesSucceeded < config.probesToClose()) return null;
            if (recoveryHook == null) return closeLocked();
            recoveryPending = true;
            long expected = epoch;
            var executor = recoveryExecutor;
            var hook = recoveryHook;
            return () -> {
                try { executor.execute(() -> startRecovery(expected, hook)); }
                catch (RuntimeException e) { finishRecovery(expected, false); }
            };
        }
        record(false);
        return null;
    }

    private void startRecovery(long expected,
            java.util.function.Supplier<java.util.concurrent.CompletionStage<Boolean>> hook) {
        synchronized (this) {
            if (epoch != expected || !recoveryPending || recoveryHook != hook) return;
        }
        try {
            hook.get().whenComplete((result, error) -> finishRecovery(expected,
                    error == null && Boolean.TRUE.equals(result)));
        } catch (Throwable e) { finishRecovery(expected, false); }
    }

    private void finishRecovery(long expected, boolean success) {
        Runnable action;
        synchronized (this) {
            if (epoch != expected || !recoveryPending || recoveryHook == null) return;
            recoveryPending = false;
            if (success) {
                recoveryFailures = 0;
                recoveryDelayNanos = 0;
                action = closeLocked();
            } else {
                recoveryFailures = Math.min(6, recoveryFailures + 1);
                recoveryDelayNanos = java.util.concurrent.TimeUnit.SECONDS.toNanos(
                        Math.min(30, 1L << (recoveryFailures - 1)));
                action = openLocked();
            }
        }
        notifySafely(action);
    }

    private Runnable failureLocked() {
        if (state == State.HALF_OPEN) return openLocked();
        record(true);
        if (windowCount >= config.minimumCalls()
                && windowFailures >= Math.ceil(config.failureRatio() * Math.min(windowCount, config.windowSize()))) {
            return openLocked();
        }
        return null;
    }

    private void record(boolean failure) {
        if (windowCount < config.windowSize()) windowCount++;
        else if (window[windowPos]) windowFailures--;
        if (failure) windowFailures++;
        window[windowPos] = failure;
        windowPos = (windowPos + 1) % config.windowSize();
    }

    private Runnable openLocked() {
        epoch++;
        recoveryPending = false;
        boolean changed = state != State.OPEN;
        state = State.OPEN;
        openedAtNanos = clock.getAsLong();
        return changed ? listener::onOpen : null;
    }

    private Runnable closeLocked() {
        epoch++;
        state = State.CLOSED;
        recoveryPending = false;
        windowPos = windowCount = windowFailures = 0;
        return listener::onClose;
    }

    private static void notifySafely(Runnable action) {
        if (action == null) return;
        try { action.run(); }
        catch (Throwable e) {
            org.slf4j.LoggerFactory.getLogger(CircuitBreaker.class).warn("Circuit breaker observer failed", e);
        }
    }
}
