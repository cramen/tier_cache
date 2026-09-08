package io.tiercache.spi;

/**
 * What a cache level stores for a key: either a real value or an explicit
 * null-marker. Null itself is never stored — absence of an entry is
 * signaled by {@code null} returns from the SPI getters.
 *
 * <p>Implementations treat this as an opaque holder; the marker flows
 * through storage like any other entry.
 *
 * <p><b>Incubating:</b> 0.x API, may change before 1.0.
 */
public final class StoredEntry<V> {

    private static final StoredEntry<?> NULL_MARKER = new StoredEntry<>(null, true);

    private final V value;
    private final boolean nullMarker;

    private StoredEntry(V value, boolean nullMarker) {
        this.value = value;
        this.nullMarker = nullMarker;
    }

    public static <V> StoredEntry<V> ofValue(V value) {
        if (value == null) {
            throw new NullPointerException("value must not be null; use nullMarker()");
        }
        return new StoredEntry<>(value, false);
    }

    @SuppressWarnings("unchecked")
    public static <V> StoredEntry<V> nullMarker() {
        return (StoredEntry<V>) NULL_MARKER;
    }

    public boolean isNullMarker() {
        return nullMarker;
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
