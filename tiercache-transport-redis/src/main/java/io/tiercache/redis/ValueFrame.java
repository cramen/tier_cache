package io.tiercache.redis;

import io.tiercache.Version;
import io.tiercache.spi.StoredEntry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Codec for the value frames stored under each key:
 *
 * <ul>
 *   <li>v1 (legacy): {@code [tag][payload]} — unversioned;</li>
 *   <li>v2: {@code [tag][4B version length][version "seq:uuid"][payload]};</li>
 *   <li>v3 (stale window): v2 plus an 8-byte big-endian write timestamp
 *       (millis) between version and payload; a zero version length marks an
 *       unversioned extended frame;</li>
 *   <li>tombstone: v2 layout without payload, reads as absent.</li>
 * </ul>
 *
 * <p>Unknown tags fail loudly, so a build that predates a newer frame
 * version never silently misreads it.
 */
final class ValueFrame {

    static final byte TAG_NULL_MARKER = 0x00;
    static final byte TAG_VALUE = 0x01;
    static final byte TAG_NULL_MARKER_V2 = 0x02;
    static final byte TAG_VALUE_V2 = 0x03;
    static final byte TAG_TOMBSTONE = 0x04;
    static final byte TAG_NULL_MARKER_V3 = 0x05;
    static final byte TAG_VALUE_V3 = 0x06;

    private static final int VERSION_LEN_BYTES = 4;
    private static final int WRITE_TS_BYTES = 8;

    private ValueFrame() {
    }

    static <V> byte[] encode(StoredEntry<V> entry, CacheSerializer<V> valueSerializer,
            boolean withWriteTimestamp) {
        byte[] versionBytes = entry.version() == null
                ? new byte[0]
                : entry.version().toWire().getBytes(StandardCharsets.UTF_8);
        byte[] payload = entry.isNullMarker() ? new byte[0] : valueSerializer.toBytes(entry.value());
        if (entry.version() == null && !withWriteTimestamp) {
            if (entry.isNullMarker()) {
                return new byte[]{TAG_NULL_MARKER};
            }
            byte[] out = new byte[payload.length + 1];
            out[0] = TAG_VALUE;
            System.arraycopy(payload, 0, out, 1, payload.length);
            return out;
        }
        int writeTsBytes = withWriteTimestamp ? WRITE_TS_BYTES : 0;
        byte[] out = new byte[1 + VERSION_LEN_BYTES + versionBytes.length + writeTsBytes + payload.length];
        if (withWriteTimestamp) {
            out[0] = entry.isNullMarker() ? TAG_NULL_MARKER_V3 : TAG_VALUE_V3;
        } else {
            out[0] = entry.isNullMarker() ? TAG_NULL_MARKER_V2 : TAG_VALUE_V2;
        }
        ByteBuffer.wrap(out, 1, VERSION_LEN_BYTES).putInt(versionBytes.length);
        int offset = 1 + VERSION_LEN_BYTES;
        System.arraycopy(versionBytes, 0, out, offset, versionBytes.length);
        offset += versionBytes.length;
        if (withWriteTimestamp) {
            ByteBuffer.wrap(out, offset, WRITE_TS_BYTES).putLong(System.currentTimeMillis());
            offset += WRITE_TS_BYTES;
        }
        System.arraycopy(payload, 0, out, offset, payload.length);
        return out;
    }

    /**
     * Decodes a stored frame; tombstones read as absent ({@code null}).
     *
     * @throws IllegalStateException on an unknown frame tag
     */
    static <V> StoredEntry<V> decode(byte[] bytes, CacheSerializer<V> valueSerializer) {
        byte tag = bytes[0];
        if (tag == TAG_NULL_MARKER) {
            return StoredEntry.nullMarker();
        }
        if (tag == TAG_VALUE) {
            return StoredEntry.ofValue(valueSerializer.fromBytes(
                    Arrays.copyOfRange(bytes, 1, bytes.length)));
        }
        if (tag == TAG_TOMBSTONE) {
            return null; // tombstones read as absent
        }
        if (tag != TAG_NULL_MARKER_V2 && tag != TAG_VALUE_V2
                && tag != TAG_NULL_MARKER_V3 && tag != TAG_VALUE_V3) {
            throw new IllegalStateException("unknown frame tag: " + tag);
        }
        int versionLen = ByteBuffer.wrap(bytes, 1, VERSION_LEN_BYTES).getInt();
        Version version = versionLen == 0 ? null
                : Version.fromWire(new String(bytes, 1 + VERSION_LEN_BYTES, versionLen, StandardCharsets.UTF_8));
        int offset = 1 + VERSION_LEN_BYTES + versionLen;
        if (tag == TAG_NULL_MARKER_V3 || tag == TAG_VALUE_V3) {
            long writeTimestampMillis = ByteBuffer.wrap(bytes, offset, WRITE_TS_BYTES).getLong();
            offset += WRITE_TS_BYTES;
            if (tag == TAG_NULL_MARKER_V3) {
                return StoredEntry.nullMarker(version, writeTimestampMillis);
            }
            byte[] payload = Arrays.copyOfRange(bytes, offset, bytes.length);
            return StoredEntry.ofValue(valueSerializer.fromBytes(payload), version, writeTimestampMillis);
        }
        if (tag == TAG_NULL_MARKER_V2) {
            return StoredEntry.nullMarker(version);
        }
        byte[] payload = Arrays.copyOfRange(bytes, offset, bytes.length);
        return StoredEntry.ofValue(valueSerializer.fromBytes(payload), version);
    }
}
