package io.tiercache.spi;

/** Completion of an internal recovery attempt, not a delivery/linearizability guarantee. */
public record RecoveryResult(Status status, String baseline, long generation) {
    /** Distinguishes verified replay/reset from incomplete recovery or shutdown. */
    public enum Status { CAUGHT_UP, RESET_SAFE, NO_JOURNAL, FAILED, CLOSED }
    /** True only for a completed replay or a configured safe clear. */
    public boolean succeeded() {
        return status == Status.CAUGHT_UP || status == Status.RESET_SAFE || status == Status.NO_JOURNAL;
    }
    /** Failed attempts do not authorize row skipping or closing the breaker. */
    public static RecoveryResult failed() { return new RecoveryResult(Status.FAILED, null, -1); }
    /** Shutdown is cancellation, never recovered coherence. */
    public static RecoveryResult closed() { return new RecoveryResult(Status.CLOSED, null, -1); }
}
