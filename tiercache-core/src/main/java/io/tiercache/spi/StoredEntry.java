package io.tiercache.spi;

import io.tiercache.Version;

/**
 * What a cache level stores for a key: either a real value or an explicit
 * null-marker, plus (optionally) the {@link Version} of the write that
 * produced it — the basis for last-write-wins invalidation. Null itself is
 * never stored — absence of an entry is signaled by {@code null} returns
 * from the SPI getters.
 *
 * <p>Implementations treat this as an opaque holder; markers flow through
 * storage like any other entry. Entries written before versioning existed
 * (or by custom SPI implementations) have a {@code null} version and are
 * treated as oldest in version comparisons.
 *
 * <p>Entries read from an L2 frame written with a stale window also carry
 * the write timestamp (millis since epoch) of the write that produced them,
 * so readers can classify freshness without extra round trips. Entries from
 * legacy frames (or from stores that do not track write time) have no write
 * timestamp.
 */
public final class StoredEntry<V> {

    private static final long NO_WRITE_TIMESTAMP = -1L;

    private static final StoredEntry<?> NULL_MARKER = new StoredEntry<>(null, true, null, NO_WRITE_TIMESTAMP);

    private final V value;
    private final boolean nullMarker;
    private final Version version;
    private final long writeTimestampMillis;

    private StoredEntry(V value, boolean nullMarker, Version version, long writeTimestampMillis) {
        this.value = value;
        this.nullMarker = nullMarker;
        this.version = version;
        this.writeTimestampMillis = writeTimestampMillis;
    }

    public static <V> StoredEntry<V> ofValue(V value) {
        return ofValue(value, null);
    }

    public static <V> StoredEntry<V> ofValue(V value, Version version) {
        if (value == null) {
            throw new NullPointerException("value must not be null; use nullMarker()");
        }
        return new StoredEntry<>(value, false, version, NO_WRITE_TIMESTAMP);
    }

    public static <V> StoredEntry<V> ofValue(V value, Version version, long writeTimestampMillis) {
        if (value == null) {
            throw new NullPointerException("value must not be null; use nullMarker()");
        }
        return new StoredEntry<>(value, false, version, writeTimestampMillis);
    }

    public static <V> StoredEntry<V> nullMarker() {
        return nullMarker(null);
    }

    @SuppressWarnings("unchecked")
    public static <V> StoredEntry<V> nullMarker(Version version) {
        if (version == null) {
            return (StoredEntry<V>) NULL_MARKER;
        }
        return new StoredEntry<>(null, true, version, NO_WRITE_TIMESTAMP);
    }

    public static <V> StoredEntry<V> nullMarker(Version version, long writeTimestampMillis) {
        return new StoredEntry<>(null, true, version, writeTimestampMillis);
    }

    public boolean isNullMarker() {
        return nullMarker;
    }

    /**
     * The write version, or {@code null} for unversioned entries.
     */
    public Version version() {
        return version;
    }

    /**
     * Whether this entry carries the write timestamp of the write that
     * produced it (extended L2 frames; absent for legacy frames).
     */
    public boolean hasWriteTimestamp() {
        return writeTimestampMillis >= 0;
    }

    /**
     * The write timestamp in milliseconds since the epoch.
     *
     * @throws IllegalStateException if this entry has no write timestamp
     */
    public long writeTimestampMillis() {
        if (!hasWriteTimestamp()) {
            throw new IllegalStateException("entry has no write timestamp (legacy frame)");
        }
        return writeTimestampMillis;
    }

    /**
     * The stored value.
     *
     * @throws IllegalStateException if this entry is a null-marker
     */
    public V value() {
        if (nullMarker) {
            throw new IllegalStateException("null-marker has no value");
        }
        return value;
    }
}
