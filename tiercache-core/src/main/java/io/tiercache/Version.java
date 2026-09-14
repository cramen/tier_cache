package io.tiercache;

import java.util.Objects;
import java.util.UUID;

/**
 * Version of a cache write: a per-instance monotonically increasing sequence
 * number plus the origin instance ID. Compared lexicographically (sequence
 * first, instance ID as tiebreak) — a total order per key across writers,
 * which is all last-write-wins invalidation needs.
 *
 * @param sequence   per-instance monotonically increasing sequence number;
 *                   must be &gt;= 0
 * @param instanceId ID of the instance that produced the write
 * @since 0.1.0
 */
public record Version(long sequence, UUID instanceId) implements Comparable<Version> {

    /**
     * Validates the version fields.
     *
     * @param sequence   per-instance monotonically increasing sequence
     *                   number; must be &gt;= 0
     * @param instanceId ID of the instance that produced the write
     * @throws NullPointerException     if {@code instanceId} is {@code null}
     * @throws IllegalArgumentException if {@code sequence} is negative
     * @since 0.1.0
     */
    public Version {
        Objects.requireNonNull(instanceId, "instanceId");
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence must be >= 0, got " + sequence);
        }
    }

    @Override
    public int compareTo(Version other) {
        int bySeq = Long.compare(sequence, other.sequence);
        // Tiebreak by instance ID string (lexicographic): matches the
        // wire-format comparison done inside Redis scripts, so Java-side and
        // Lua-side last-write-wins agree exactly.
        return bySeq != 0 ? bySeq : instanceId.toString().compareTo(other.instanceId.toString());
    }

    /**
     * The version as a compact wire string ({@code seq:uuid}); ordering by
     * (numeric seq, lexicographic uuid) matches {@link Version#compareTo}.
     *
     * @return the wire representation
     * @since 0.1.0
     */
    public String toWire() {
        return sequence + ":" + instanceId;
    }

    /**
     * Parses a wire string produced by {@link #toWire()}.
     *
     * @param wire the wire representation ({@code seq:uuid})
     * @return the parsed version
     * @throws NumberFormatException           if the sequence part is not a
     *                                         number
     * @throws IllegalArgumentException        if the instance ID part is not
     *                                         a UUID
     * @since 0.1.0
     */
    public static Version fromWire(String wire) {
        int colon = wire.indexOf(':');
        return new Version(Long.parseLong(wire.substring(0, colon)),
                UUID.fromString(wire.substring(colon + 1)));
    }

    @Override
    public String toString() {
        return toWire();
    }
}
