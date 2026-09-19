package io.tiercache;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates monotonically increasing, time-ordered {@link Version}s for one
 * instance. The instance ID is a random UUID assigned at startup: a
 * restarted instance is a new writer with an empty L1, so no cross-restart
 * continuity is needed.
 *
 * <p>The sequence is a hybrid of wall-clock time and a per-instance counter:
 * {@code max(epochMillis * 1000 + counterWithinMillis, previous + 1)}. This
 * orders writes by real time across instances (within normal NTP-grade clock
 * sync) while keeping per-instance sequences strictly monotonic even when one
 * instance produces more than 1000 writes in a millisecond (the sequence runs
 * ahead of the clock for the burst's duration, then real time catches up).
 * Same-millisecond writes from different instances are ordered by the
 * instance-ID tiebreak in {@link Version#compareTo}.
 *
 * <p><strong>Clock-skew caveat:</strong> ordering follows wall-clock time, so
 * an instance whose clock lags behind its peers can have a later write order
 * before an earlier one — standard behavior for wall-clock last-write-wins
 * (as in Redis itself). Keep instance clocks NTP-synced.
 *
 * @since 0.1.0
 */
public final class VersionGenerator {

    private static final long SLOTS_PER_MILLIS = 1000L;

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
     * by real time across instances (see the class Javadoc for the hybrid
     * scheme and the clock-skew caveat).
     *
     * @return the next version; never {@code null}
     * @since 0.1.0
     */
    public Version next() {
        long candidate = System.currentTimeMillis() * SLOTS_PER_MILLIS;
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
