package io.tiercache;

/**
 * Tri-state result of {@link TierCache#lookup}: distinguishes a hit, a
 * cached-null marker (F-25), and a miss.
 *
 * <p><b>Incubating:</b> 0.x API, may change until CP-0.
 */
public sealed interface LookupResult<V> {

    static <V> LookupResult<V> hit(V value) {
        return new Hit<>(value);
    }

    static <V> LookupResult<V> cachedNull() {
        return CachedNull.instance();
    }

    static <V> LookupResult<V> miss() {
        return Miss.instance();
    }

    /**
     * A present value.
     */
    record Hit<V>(V value) implements LookupResult<V> {
    }

    /**
     * A null-marker is present: the key is known to have no value (F-25).
     */
    record CachedNull<V>() implements LookupResult<V> {
        private static final CachedNull<?> INSTANCE = new CachedNull<>();

        @SuppressWarnings("unchecked")
        static <V> CachedNull<V> instance() {
            return (CachedNull<V>) INSTANCE;
        }
    }

    /**
     * Nothing stored for the key.
     */
    record Miss<V>() implements LookupResult<V> {
        private static final Miss<?> INSTANCE = new Miss<>();

        @SuppressWarnings("unchecked")
        static <V> Miss<V> instance() {
            return (Miss<V>) INSTANCE;
        }
    }
}
