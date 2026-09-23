package io.tiercache.internal;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.tiercache.Version;

import java.time.Duration;

/**
 * Bounded protective versions, including for keys absent from L1.
 * Authoritative freshness lives on the actual L1 entry, independently of
 * this map's eviction and expiry. Forgetting a fence bumps the generation
 * to reject obsolete in-flight commits.
 *
 * <p>Every eviction (size cap or expiry) and every bulk invalidation
 * reports through the protection callback so the engine can bump its L1
 * generation instead of silently forgetting protective state.
 *
 * <p>Lives in its own small class so the engine class itself never
 * references the relocated Caffeine package — shadow relocation rewrites
 * referencing classes, which breaks JaCoCo class-matching against the
 * unshaded build output.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @param <K> key type
 * @since 1.4.0
 */
final class L1BarrierMap<K> {

    /** Highest protective version seen for a key; freshness belongs to its L1 entry. */
    record L1Meta(Version highestSeen) { }

    private final com.github.benmanes.caffeine.cache.Cache<K, L1Meta> barriers;

    /**
     * @param maxSize      maximum entries before eldest eviction
     * @param expiry       per-entry expire-after-write
     * @param onProtection invoked whenever protective state is forgotten
     *                     (entry eviction or bulk invalidation)
     */
    L1BarrierMap(long maxSize, Duration expiry, Runnable onProtection) {
        this(maxSize, expiry, onProtection, System::nanoTime);
    }

    L1BarrierMap(long maxSize, Duration expiry, Runnable onProtection,
            java.util.function.LongSupplier clock) {
        this.barriers = Caffeine.newBuilder()
                .ticker(clock::getAsLong)
                .maximumSize(maxSize)
                .expireAfterWrite(expiry)
                // Removal callbacks must be synchronous with the evicting
                // call: the generation bump they drive is part of the
                // commit's correctness, not a best-effort notification.
                .executor(command -> command.run())
                .<K, L1Meta>removalListener((key, value, cause) -> onProtection.run())
                .build();
    }

    /** The metadata for {@code key}, or {@code null} if none. */
    L1Meta get(K key) {
        return barriers.getIfPresent(key);
    }

    /** Records the highest barrier; unversioned values need no separate fence. */
    void put(K key, Version version) {
        if (version == null) return;
        barriers.asMap().merge(key, new L1Meta(version),
                (existing, incoming) -> existing.highestSeen().compareTo(incoming.highestSeen()) >= 0
                        ? existing : incoming);
    }

    /** Drops the metadata for {@code key}. */
    void invalidate(K key) {
        barriers.invalidate(key);
    }

    /** Drops all metadata. */
    void invalidateAll() {
        barriers.invalidateAll();
    }
}
