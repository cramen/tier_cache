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
 * <p><b>Incubating:</b> 0.x API, may change before 1.0.
 */
public final class StoredEntry<V> {

    private static final StoredEntry<?> NULL_MARKER = new StoredEntry<>(null, true, null);

    private final V value;
    private final boolean nullMarker;
    private final Version version;

    private StoredEntry(V value, boolean nullMarker, Version version) {
        this.value = value;
        this.nullMarker = nullMarker;
        this.version = version;
    }

    public static <V> StoredEntry<V> ofValue(V value) {
        return ofValue(value, null);
    }

    public static <V> StoredEntry<V> ofValue(V value, Version version) {
        if (value == null) {
            throw new NullPointerException("value must not be null; use nullMarker()");
        }
        return new StoredEntry<>(value, false, version);
    }

    public static <V> StoredEntry<V> nullMarker() {
        return nullMarker(null);
    }

    @SuppressWarnings("unchecked")
    public static <V> StoredEntry<V> nullMarker(Version version) {
        if (version == null) {
            return (StoredEntry<V>) NULL_MARKER;
        }
        return new StoredEntry<>(null, true, version);
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
