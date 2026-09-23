package io.tiercache.invalidation;

/** Internal protocol limits shared by receivers, Redis journals and framework wiring. */
public final class JournalProtocol {
    public static final int CURSOR_CADENCE = 64;
    public static final int MIN_CAPACITY = CURSOR_CADENCE + 1;
    public static final int DEFAULT_CAPACITY = 10_000;

    private JournalProtocol() { }

    /** Validates the configured row target, not a guarantee of sufficient recovery retention. */
    public static int requireCapacity(int capacity) {
        if (capacity < MIN_CAPACITY) {
            throw new IllegalArgumentException("tiercache.invalidation.journal-capacity=" + capacity
                    + " must be at least " + MIN_CAPACITY + ": the " + CURSOR_CADENCE
                    + "-event cursor cadence also requires the previous confirmed cursor row");
        }
        return capacity;
    }
}
