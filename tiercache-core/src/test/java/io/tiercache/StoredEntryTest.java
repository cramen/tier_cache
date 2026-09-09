package io.tiercache;

import io.tiercache.spi.StoredEntry;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link StoredEntry} write-timestamp carriers: optional field, absent for
 * legacy frames, convenience accessors.
 */
class StoredEntryTest {

    @Test
    void entriesWithoutWriteTimestampReportAbsent() {
        StoredEntry<String> plain = StoredEntry.ofValue("v");
        assertFalse(plain.hasWriteTimestamp());
        assertThrows(IllegalStateException.class, plain::writeTimestampMillis);

        Version version = new Version(1, UUID.randomUUID());
        assertFalse(StoredEntry.ofValue("v", version).hasWriteTimestamp());
        assertFalse(StoredEntry.nullMarker().hasWriteTimestamp());
        assertFalse(StoredEntry.nullMarker(version).hasWriteTimestamp());
    }

    @Test
    void valueEntryCarriesWriteTimestamp() {
        Version version = new Version(7, UUID.randomUUID());
        StoredEntry<String> entry = StoredEntry.ofValue("v", version, 1_234_567L);
        assertTrue(entry.hasWriteTimestamp());
        assertEquals(1_234_567L, entry.writeTimestampMillis());
        assertEquals("v", entry.value());
        assertEquals(version, entry.version());
    }

    @Test
    void nullMarkerCarriesWriteTimestamp() {
        Version version = new Version(3, UUID.randomUUID());
        StoredEntry<String> entry = StoredEntry.nullMarker(version, 99L);
        assertTrue(entry.isNullMarker());
        assertTrue(entry.hasWriteTimestamp());
        assertEquals(99L, entry.writeTimestampMillis());
        assertEquals(version, entry.version());
        assertThrows(IllegalStateException.class, entry::value);
    }

    @Test
    void unversionedNullMarkerCarriesWriteTimestamp() {
        StoredEntry<String> entry = StoredEntry.nullMarker(null, 42L);
        assertTrue(entry.isNullMarker());
        assertNull(entry.version());
        assertEquals(42L, entry.writeTimestampMillis());
    }

    @Test
    void timestampedFactoryStillRejectsNullValue() {
        assertThrows(NullPointerException.class,
                () -> StoredEntry.ofValue(null, null, 1L));
    }
}
