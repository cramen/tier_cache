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
 */
public sealed interface NullPolicy {

    static NullPolicy deny() {
        return Deny.INSTANCE;
    }

    static NullPolicy allow(Duration markerTtl) {
        return new Allow(markerTtl);
    }

    /**
     * Marker TTL when the policy allows caching nulls; {@code null} under deny.
     */
    Duration markerTtl();

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

    record Allow(Duration markerTtl) implements NullPolicy {
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
