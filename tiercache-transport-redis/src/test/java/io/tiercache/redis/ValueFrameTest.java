package io.tiercache.redis;

import io.tiercache.Version;
import io.tiercache.spi.StoredEntry;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Frame codec round trips: legacy (v1/v2) frames decode exactly as before,
 * extended (v3) frames carry the write timestamp, unknown tags and garbage
 * fail loudly.
 */
class ValueFrameTest {

    private final JdkCacheSerializer<String> serializer = new JdkCacheSerializer<>();

    @Test
    void legacyValueRoundTrip() {
        byte[] frame = ValueFrame.encode(StoredEntry.ofValue("data"), serializer, false);
        assertEquals(ValueFrame.TAG_VALUE, frame[0]);
        StoredEntry<String> decoded = ValueFrame.decode(frame, serializer);
        assertEquals("data", decoded.value());
        assertNull(decoded.version());
        assertFalse(decoded.hasWriteTimestamp());
    }

    @Test
    void legacyNullMarkerRoundTrip() {
        byte[] frame = ValueFrame.encode(StoredEntry.nullMarker(), serializer, false);
        assertArrayEquals(new byte[]{ValueFrame.TAG_NULL_MARKER}, frame);
        StoredEntry<String> decoded = ValueFrame.decode(frame, serializer);
        assertTrue(decoded.isNullMarker());
        assertNull(decoded.version());
        assertFalse(decoded.hasWriteTimestamp());
    }

    @Test
    void versionedValueRoundTrip() {
        Version version = new Version(42, UUID.randomUUID());
        byte[] frame = ValueFrame.encode(StoredEntry.ofValue("data", version), serializer, false);
        assertEquals(ValueFrame.TAG_VALUE_V2, frame[0]);
        StoredEntry<String> decoded = ValueFrame.decode(frame, serializer);
        assertEquals("data", decoded.value());
        assertEquals(version, decoded.version());
        assertFalse(decoded.hasWriteTimestamp());
    }

    @Test
    void versionedNullMarkerRoundTrip() {
        Version version = new Version(43, UUID.randomUUID());
        byte[] frame = ValueFrame.encode(StoredEntry.nullMarker(version), serializer, false);
        assertEquals(ValueFrame.TAG_NULL_MARKER_V2, frame[0]);
        StoredEntry<String> decoded = ValueFrame.decode(frame, serializer);
        assertTrue(decoded.isNullMarker());
        assertEquals(version, decoded.version());
        assertFalse(decoded.hasWriteTimestamp());
    }

    @Test
    void extendedValueRoundTripCarriesWriteTimestamp() {
        Version version = new Version(44, UUID.randomUUID());
        long before = System.currentTimeMillis();
        byte[] frame = ValueFrame.encode(StoredEntry.ofValue("data", version), serializer, true);
        long after = System.currentTimeMillis();
        assertEquals(ValueFrame.TAG_VALUE_V3, frame[0]);
        StoredEntry<String> decoded = ValueFrame.decode(frame, serializer);
        assertEquals("data", decoded.value());
        assertEquals(version, decoded.version());
        assertTrue(decoded.hasWriteTimestamp());
        assertTrue(decoded.writeTimestampMillis() >= before
                && decoded.writeTimestampMillis() <= after);
    }

    @Test
    void extendedUnversionedValueRoundTrip() {
        byte[] frame = ValueFrame.encode(StoredEntry.ofValue("data"), serializer, true);
        assertEquals(ValueFrame.TAG_VALUE_V3, frame[0]);
        StoredEntry<String> decoded = ValueFrame.decode(frame, serializer);
        assertEquals("data", decoded.value());
        assertNull(decoded.version());
        assertTrue(decoded.hasWriteTimestamp());
    }

    @Test
    void extendedNullMarkerRoundTrip() {
        Version version = new Version(45, UUID.randomUUID());
        byte[] frame = ValueFrame.encode(StoredEntry.nullMarker(version), serializer, true);
        assertEquals(ValueFrame.TAG_NULL_MARKER_V3, frame[0]);
        StoredEntry<String> decoded = ValueFrame.decode(frame, serializer);
        assertTrue(decoded.isNullMarker());
        assertEquals(version, decoded.version());
        assertTrue(decoded.hasWriteTimestamp());
    }

    @Test
    void tombstoneReadsAsAbsent() {
        byte[] version = new Version(46, UUID.randomUUID()).toWire().getBytes(StandardCharsets.UTF_8);
        byte[] frame = new byte[5 + version.length];
        frame[0] = ValueFrame.TAG_TOMBSTONE;
        ByteBuffer.wrap(frame, 1, 4).putInt(version.length);
        System.arraycopy(version, 0, frame, 5, version.length);
        assertNull(ValueFrame.decode(frame, serializer));
    }

    @Test
    void unknownTagFailsLoudly() {
        byte[] frame = new byte[]{0x7F, 0, 0, 0, 0};
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ValueFrame.decode(frame, serializer));
        assertTrue(e.getMessage().contains("unknown frame tag"));
    }

    @Test
    void garbageFrameFails() {
        // Valid v2 tag but the version field is not "seq:uuid".
        byte[] frame = new byte[]{ValueFrame.TAG_VALUE_V2, 0, 0, 0, 3, 'a', 'b', 'c'};
        assertThrows(RuntimeException.class, () -> ValueFrame.decode(frame, serializer));
    }
}
