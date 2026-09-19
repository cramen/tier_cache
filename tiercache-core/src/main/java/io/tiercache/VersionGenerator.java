package io.tiercache;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates monotonically increasing, time-ordered {@link Version}s for one
 * instance. The instance ID is a random UUID assigned at startup: a
 * restarted instance is a new writer with an empty L1, so no cross-restart
 * continuity is needed.
 *
 * <p>The sequence is wall-clock time at microsecond resolution with an
 * implicit per-instance counter: {@code max(epochMicros, previous + 1)}.
 * This orders writes by real time across instances (within normal NTP-grade
 * clock sync) while keeping per-instance sequences strictly monotonic even
 * when one instance outpaces the clock's resolution (the sequence runs ahead
 * of the clock for the burst's duration, then real time catches up).
 * Cross-instance writes within the same microsecond are ordered by the
 * instance-ID tiebreak in {@link Version#compareTo} — the accepted
 * last-write-wins trade-off: a strict "later always wins" guarantee would
 * require per-write coordination, which is out of scope by design.
 *
 * <p>The clock derives from {@link java.time.Instant#now()}; on platforms
 * returning millisecond-granular instants the scheme degrades to
 * millisecond-resolution ordering (no worse than the 1.2.0 scheme).
 *
 * <p><strong>Clock-skew caveat:</strong> ordering follows wall-clock time, so
 * an instance whose clock lags behind its peers can have a later write order
 * before an earlier one — standard behavior for wall-clock last-write-wins
 * (as in Redis itself). Keep instance clocks NTP-synced.
 *
 * @since 0.1.0
 */
public final class VersionGenerator {

    private final UUID instanceId = UUID.randomUUID();
    private final AtomicLong sequence = new AtomicLong();

    /**
     * Creates a generator with a fresh random instance ID.
     *
     * @since 0.1.0
     */
    public VersionGenerator() {
    }

    /**
     * Returns the next version: a time-ordered sequence stamped with this
     * instance's ID. Sequences are strictly monotonic per instance and order
     * by real time across instances at microsecond resolution (see the class
     * Javadoc for the scheme, the tiebreak trade-off, and the clock-skew
     * caveat).
     *
     * @return the next version; never {@code null}
     * @since 0.1.0
     */
    public Version next() {
        java.time.Instant now = java.time.Instant.now();
        long candidate = now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
        return new Version(sequence.updateAndGet(prev -> Math.max(candidate, prev + 1)), instanceId);
    }

    /**
     * The instance ID shared by all versions from this generator.
     *
     * @return the instance ID; never {@code null}
     * @since 0.1.0
     */
    public UUID instanceId() {
        return instanceId;
    }
}
