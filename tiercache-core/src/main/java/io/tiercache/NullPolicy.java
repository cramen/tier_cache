package io.tiercache;

import java.time.Duration;
import java.util.Objects;

/**
 * Per-cache null-caching policy.
 *
 * <p>{@code deny} (the default): a loader null result is an uncached miss —
 * every lookup of a nonexistent key reaches the loader.
 *
 * <p>{@code allow(markerTtl)}: a loader null result stores an explicit
 * null-marker in both levels with the given (jittered) TTL, so repeated
 * lookups of nonexistent keys are absorbed by the cache (penetration
 * defense). The marker is distinguishable from a miss via
 * {@link TierCache#lookup}.
 *
 * @since 0.1.0
 */
public sealed interface NullPolicy {

    /**
     * Returns the deny policy: null results are never cached.
     *
     * @return the deny policy
     * @since 0.1.0
     */
    static NullPolicy deny() {
        return Deny.INSTANCE;
    }

    /**
     * Returns the allow policy: null results are cached as explicit
     * null-markers with the given TTL.
     *
     * @param markerTtl how long a null-marker suppresses the loader; must
     *                  be positive
     * @return the allow policy for the given marker TTL
     * @since 0.1.0
     */
    static NullPolicy allow(Duration markerTtl) {
        return new Allow(markerTtl);
    }

    /**
     * Marker TTL when the policy allows caching nulls; {@code null} under deny.
     *
     * @return the marker TTL, or {@code null} if nulls are not cached
     * @since 0.1.0
     */
    Duration markerTtl();

    /**
     * The deny policy: a loader null result stays an uncached miss.
     *
     * @since 0.1.0
     */
    final class Deny implements NullPolicy {
        private static final Deny INSTANCE = new Deny();

        private Deny() {
        }

        @Override
        public Duration markerTtl() {
            return null;
        }

        @Override
        public String toString() {
            return "deny";
        }
    }

    /**
     * The allow policy: a loader null result stores an explicit null-marker
     * in both levels with the given (jittered) TTL.
     *
     * @param markerTtl how long a null-marker suppresses the loader; must be
     *                  positive
     * @since 0.1.0
     */
    record Allow(Duration markerTtl) implements NullPolicy {

        /**
         * Validates the marker TTL.
         *
         * @param markerTtl how long a null-marker suppresses the loader;
         *                  must be positive
         * @throws NullPointerException     if {@code markerTtl} is
         *                                  {@code null}
         * @throws IllegalArgumentException if {@code markerTtl} is zero or
         *                                  negative
         * @since 0.1.0
         */
        public Allow {
            Objects.requireNonNull(markerTtl, "markerTtl");
            if (markerTtl.isZero() || markerTtl.isNegative()) {
                throw new IllegalArgumentException("markerTtl must be positive, got " + markerTtl);
            }
        }

        @Override
        public String toString() {
            return "allow(" + markerTtl + ")";
        }
    }
}
