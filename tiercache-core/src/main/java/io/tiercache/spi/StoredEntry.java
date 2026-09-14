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
 *
 * <p><b>Internal — not part of the supported API.</b> Exchanged between the
 * cache levels and the engine.
 *
 * @param <V> value type
 * @since 0.1.0
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

    /**
     * Creates an unversioned value entry.
     *
     * @param <V>   value type
     * @param value the value to store; must not be {@code null}
     * @return the value entry
     * @throws NullPointerException if {@code value} is {@code null}
     * @since 0.1.0
     */
    public static <V> StoredEntry<V> ofValue(V value) {
        return ofValue(value, null);
    }

    /**
     * Creates a versioned value entry without a write timestamp.
     *
     * @param <V>     value type
     * @param value   the value to store; must not be {@code null}
     * @param version the write version, or {@code null} for unversioned
     * @return the value entry
     * @throws NullPointerException if {@code value} is {@code null}
     * @since 0.1.0
     */
    public static <V> StoredEntry<V> ofValue(V value, Version version) {
        if (value == null) {
            throw new NullPointerException("value must not be null; use nullMarker()");
        }
        return new StoredEntry<>(value, false, version, NO_WRITE_TIMESTAMP);
    }

    /**
     * Creates a versioned value entry with a write timestamp (extended L2
     * frames with a stale window).
     *
     * @param <V>                  value type
     * @param value                the value to store; must not be
     *                             {@code null}
     * @param version              the write version, or {@code null} for
     *                             unversioned
     * @param writeTimestampMillis when the write happened, in milliseconds
     *                             since the epoch
     * @return the value entry
     * @throws NullPointerException if {@code value} is {@code null}
     * @since 0.1.0
     */
    public static <V> StoredEntry<V> ofValue(V value, Version version, long writeTimestampMillis) {
        if (value == null) {
            throw new NullPointerException("value must not be null; use nullMarker()");
        }
        return new StoredEntry<>(value, false, version, writeTimestampMillis);
    }

    /**
     * Returns an unversioned null-marker entry.
     *
     * @param <V> value type
     * @return the null-marker
     * @since 0.1.0
     */
    public static <V> StoredEntry<V> nullMarker() {
        return nullMarker(null);
    }

    /**
     * Returns a versioned null-marker entry without a write timestamp.
     *
     * @param <V>     value type
     * @param version the write version, or {@code null} for unversioned
     * @return the null-marker
     * @since 0.1.0
     */
    @SuppressWarnings("unchecked")
    public static <V> StoredEntry<V> nullMarker(Version version) {
        if (version == null) {
            return (StoredEntry<V>) NULL_MARKER;
        }
        return new StoredEntry<>(null, true, version, NO_WRITE_TIMESTAMP);
    }

    /**
     * Returns a versioned null-marker entry with a write timestamp.
     *
     * @param <V>                  value type
     * @param version              the write version, or {@code null} for
     *                             unversioned
     * @param writeTimestampMillis when the write happened, in milliseconds
     *                             since the epoch
     * @return the null-marker
     * @since 0.1.0
     */
    public static <V> StoredEntry<V> nullMarker(Version version, long writeTimestampMillis) {
        return new StoredEntry<>(null, true, version, writeTimestampMillis);
    }

    /**
     * Whether this entry is a null-marker.
     *
     * @return {@code true} if this entry marks a known-absent key
     * @since 0.1.0
     */
    public boolean isNullMarker() {
        return nullMarker;
    }

    /**
     * The write version, or {@code null} for unversioned entries.
     *
     * @return the write version, or {@code null}
     * @since 0.1.0
     */
    public Version version() {
        return version;
    }

    /**
     * Whether this entry carries the write timestamp of the write that
     * produced it (extended L2 frames; absent for legacy frames).
     *
     * @return {@code true} if a write timestamp is present
     * @since 0.1.0
     */
    public boolean hasWriteTimestamp() {
        return writeTimestampMillis >= 0;
    }

    /**
     * The write timestamp in milliseconds since the epoch.
     *
     * @return the write timestamp
     * @throws IllegalStateException if this entry has no write timestamp
     * @since 0.1.0
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
     * @return the value; never {@code null}
     * @throws IllegalStateException if this entry is a null-marker
     * @since 0.1.0
     */
    public V value() {
        if (nullMarker) {
            throw new IllegalStateException("null-marker has no value");
        }
        return value;
    }
}
