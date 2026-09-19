package io.tiercache;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Version ordering and wire format. */
class VersionTest {

    @Test
    void ordersBySequenceThenInstanceId() {
        UUID low = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID high = UUID.fromString("00000000-0000-0000-0000-000000000002");
        assertTrue(new Version(1, low).compareTo(new Version(2, high)) < 0);
        assertTrue(new Version(5, low).compareTo(new Version(5, high)) < 0);
        assertEquals(0, new Version(5, low).compareTo(new Version(5, low)));
    }

    @Test
    void wireRoundTrip() {
        Version v = new Version(123, UUID.randomUUID());
        assertEquals(v, Version.fromWire(v.toWire()));
    }

    @Test
    void rejectsNegativeSequenceAndNullInstance() {
        assertThrows(IllegalArgumentException.class, () -> new Version(-1, UUID.randomUUID()));
        assertThrows(NullPointerException.class, () -> new Version(1, null));
    }

    @Test
    void generatorIsMonotonicPerInstance() {
        VersionGenerator generator = new VersionGenerator();
        Version a = generator.next();
        Version b = generator.next();
        assertTrue(a.compareTo(b) < 0);
        assertEquals(a.instanceId(), b.instanceId());
        assertNotEquals(generator.instanceId(), new VersionGenerator().instanceId());
    }

    @Test
    void generatorIsStrictlyMonotonicUnderBurst() {
        VersionGenerator generator = new VersionGenerator();
        Version previous = generator.next();
        for (int i = 0; i < 100_000; i++) {
            Version current = generator.next();
            assertTrue(previous.compareTo(current) < 0,
                    "strictly increasing at burst index " + i);
            previous = current;
        }
    }

    @Test
    void freshGeneratorBeatsLongRunningGenerator() throws InterruptedException {
        VersionGenerator longRunning = new VersionGenerator();
        Version lastFromLongRunning = null;
        for (int i = 0; i < 100; i++) {
            lastFromLongRunning = longRunning.next();
        }
        // Strictly later in real time: let the clock move past the burst.
        Thread.sleep(5);
        Version fromFresh = new VersionGenerator().next();
        assertTrue(fromFresh.compareTo(lastFromLongRunning) > 0,
                "a fresh instance's later write must win cross-instance");
    }

    @Test
    void subMillisecondCrossInstanceWriteWins() {
        VersionGenerator a = new VersionGenerator();
        // Burst inside one millisecond: A's sequence may run ahead of the
        // clock by up to the burst size (the documented burst behavior).
        Version lastFromA = null;
        for (int i = 0; i < 100; i++) {
            lastFromA = a.next();
        }
        // B writes later in real time — within the same wall-clock
        // millisecond — and must win. Spin until the wall clock is
        // verifiably past A's last sequence (self-calibrating, no fixed
        // sleep assumptions), so B's clock-derived sequence beats the burst.
        long target = lastFromA.sequence() + 10;
        while (System.currentTimeMillis() * 1_000L <= target) {
            Thread.onSpinWait();
        }
        Version fromB = new VersionGenerator().next();
        assertTrue(fromB.compareTo(lastFromA) > 0,
                "microsecond resolution must order B after A's same-millisecond burst: A="
                        + lastFromA + " B=" + fromB);
    }
}
