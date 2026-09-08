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
}
