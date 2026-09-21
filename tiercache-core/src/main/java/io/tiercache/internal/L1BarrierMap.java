package io.tiercache.internal;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.tiercache.Version;

import java.time.Duration;

/**
 * Bounded per-key L1 metadata for the engine, one entry per key: the
 * invalidation barrier (highest version seen invalidated, recorded even
 * for absent keys) plus the freshness deadlines of the value stored with
 * it — all written and removed in the same atomic commit as the value, so
 * metadata always describes the value it sits with.
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

    /**
     * Per-key metadata.
     *
     * @param highestSeen          highest version invalidated/superseded
     *                             locally (the barrier), or {@code null}
     * @param logicalDeadlineNanos freshness deadline of the stored value
     *                             (store time + actual jittered TTL)
     * @param staleServeUntilNanos stale-serving horizon
     *                             ({@code logicalDeadline + staleWindow})
     */
    record L1Meta(Version highestSeen, long logicalDeadlineNanos, long staleServeUntilNanos) {
    }

    private final com.github.benmanes.caffeine.cache.Cache<K, L1Meta> barriers;

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

    /** Records the barrier version only (freshness deadlines unchanged). */
    void put(K key, Version version) {
        barriers.asMap().merge(key, new L1Meta(version, 0L, 0L),
                (existing, barrierOnly) -> new L1Meta(
                        existing.highestSeen() != null
                                && (barrierOnly.highestSeen() == null
                                || existing.highestSeen().compareTo(barrierOnly.highestSeen()) >= 0)
                                ? existing.highestSeen() : barrierOnly.highestSeen(),
                        existing.logicalDeadlineNanos(), existing.staleServeUntilNanos()));
    }

    /** Records full metadata (barrier + deadlines) for {@code key}. */
    void put(K key, L1Meta meta) {
        barriers.put(key, meta);
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
