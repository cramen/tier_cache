package io.tiercache;

import java.util.Objects;
import java.util.UUID;

/**
 * Version of a cache write: a per-instance monotonically increasing sequence
 * number plus the origin instance ID. Compared lexicographically (sequence
 * first, instance ID as tiebreak) — a total order per key across writers,
 * which is all last-write-wins invalidation needs.
 */
public record Version(long sequence, UUID instanceId) implements Comparable<Version> {

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
     */
    public String toWire() {
        return sequence + ":" + instanceId;
    }

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
