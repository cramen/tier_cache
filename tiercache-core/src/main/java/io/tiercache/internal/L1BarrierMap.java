package io.tiercache.internal;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.tiercache.Version;

import java.time.Duration;

/**
 * Bounded per-key invalidation-barrier map for the engine: the highest
 * version this instance has seen invalidated per key (including keys
 * absent from L1). Lives in its own small class so the engine class
 * itself never references the relocated Caffeine package — shadow
 * relocation rewrites referencing classes, which breaks JaCoCo
 * class-matching against the unshaded build output.
 *
 * <p>Every eviction (size cap or expiry) and every bulk invalidation
 * reports through the evict callback so the engine can bump its L1
 * generation instead of silently forgetting protective state.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @param <K> key type
 * @since 1.4.0
 */
final class L1BarrierMap<K> {

    private final com.github.benmanes.caffeine.cache.Cache<K, Version> barriers;

    /**
     * @param maxSize      maximum entries before eldest eviction
     * @param expiry       per-entry expire-after-write
     * @param onProtection invoked whenever protective state is forgotten
     *                     (entry eviction or bulk invalidation)
     */
    L1BarrierMap(long maxSize, Duration expiry, Runnable onProtection) {
        this.barriers = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expiry)
                .<K, Version>removalListener((key, value, cause) -> onProtection.run())
                .build();
    }

    /** The barrier version for {@code key}, or {@code null} if none. */
    Version get(K key) {
        return barriers.getIfPresent(key);
    }

    /** Records {@code version} as the highest seen for {@code key}. */
    void put(K key, Version version) {
        barriers.put(key, version);
    }

    /** Drops the barrier for {@code key}. */
    void invalidate(K key) {
        barriers.invalidate(key);
    }

    /** Drops all barriers. */
    void invalidateAll() {
        barriers.invalidateAll();
    }
}
