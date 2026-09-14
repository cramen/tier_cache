package io.tiercache;

/**
 * Tri-state result of {@link TierCache#lookup}: distinguishes a hit, a
 * cached-null marker, and a miss.
 *
 * @param <V> value type
 * @since 0.1.0
 */
public sealed interface LookupResult<V> {

    /**
     * Creates a hit result carrying {@code value}.
     *
     * @param <V>   value type
     * @param value the cached value
     * @return the hit result
     * @since 0.1.0
     */
    static <V> LookupResult<V> hit(V value) {
        return new Hit<>(value);
    }

    /**
     * Returns the cached-null result: a null-marker is present, the key is
     * known to have no value.
     *
     * @param <V> value type
     * @return the cached-null result
     * @since 0.1.0
     */
    static <V> LookupResult<V> cachedNull() {
        return CachedNull.instance();
    }

    /**
     * Returns the miss result: nothing is stored for the key.
     *
     * @param <V> value type
     * @return the miss result
     * @since 0.1.0
     */
    static <V> LookupResult<V> miss() {
        return Miss.instance();
    }

    /**
     * A present value.
     *
     * @param value the cached value
     * @param <V>   value type
     * @since 0.1.0
     */
    record Hit<V>(V value) implements LookupResult<V> {
    }

    /**
     * A null-marker is present: the key is known to have no value.
     *
     * @param <V> value type
     * @since 0.1.0
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
     *
     * @param <V> value type
     * @since 0.1.0
     */
    record Miss<V>() implements LookupResult<V> {
        private static final Miss<?> INSTANCE = new Miss<>();

        @SuppressWarnings("unchecked")
        static <V> Miss<V> instance() {
            return (Miss<V>) INSTANCE;
        }
    }
}
