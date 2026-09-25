package io.tiercache.spi;

import java.util.Objects;

/**
 * Immutable coherent observation of a provider-owned freshness read. The entry
 * and classification belong to one observation point. Implementations must not
 * mutate the returned view after publication. Internal provider SPI.
 *
 * @param <V> cached value type
 */
public interface LocalFreshnessResult<V> {
    /** Whether the observation may serve locally, needs degraded admission, or must cascade. */
    enum State { FRESH, STALE_ALLOWED, EXPIRED }

    StoredEntry<V> entry();
    State state();

    /** Construct an immutable observation for a custom provider. */
    static <V> LocalFreshnessResult<V> of(StoredEntry<V> entry, State state) {
        return new Snapshot<>(entry, state);
    }

    /** Absence, never a cached-null marker. */
    @SuppressWarnings("unchecked")
    static <V> LocalFreshnessResult<V> absent() { return (LocalFreshnessResult<V>) Snapshot.ABSENT; }

    /** General-purpose provider result; built-in providers may return an immutable internal carrier. */
    record Snapshot<V>(StoredEntry<V> entry, State state) implements LocalFreshnessResult<V> {
        private static final Snapshot<?> ABSENT = new Snapshot<>(null, State.EXPIRED);
        public Snapshot {
            Objects.requireNonNull(state, "state");
            if (entry == null && state != State.EXPIRED) {
                throw new IllegalArgumentException("An absent holder cannot be fresh or stale-allowed");
            }
        }
    }
}
