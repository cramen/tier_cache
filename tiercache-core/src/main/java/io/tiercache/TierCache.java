package io.tiercache;

/**
 * A two-level cache: cascading reads L1 (local) &rarr; L2 (remote) &rarr; loader.
 *
 * <p><b>Incubating:</b> this interface is part of the 0.x API and may change
 * incompatibly until the public API freeze (roadmap checkpoint CP-0).
 *
 * <p>Read semantics (F-01): a lookup checks L1, then L2, then (for
 * {@link #getOrCompute}) the loader. An L2 hit always warms L1, so the next
 * lookup of the same key is served locally. Concurrent
 * {@link #getOrCompute} calls for the same key share a single loader
 * execution within the instance (singleflight, F-20); this protection is on
 * by default.
 *
 * <p>Consistency model (F-15): this cache is eventually consistent. L1
 * entries never outlive their L2 counterpart. No strong-consistency
 * guarantees are given or implied.
 *
 * @param <K> key type
 * @param <V> value type (null values are never stored in this version)
 */
public interface TierCache<K, V> {

    /**
     * Returns the value for {@code key}, cascading L1 &rarr; L2 and warming
     * L1 on an L2 hit. Returns {@code null} on a miss (absent from both
     * levels); no loader is invoked.
     */
    V get(K key);

    /**
     * Returns the value for {@code key}, cascading L1 &rarr; L2 &rarr; loader.
     * An L2 hit warms L1; a loader result populates both levels. If the
     * loader produces no value, the miss propagates and nothing is stored.
     *
     * <p>Concurrent calls for the same key coalesce onto one loader
     * execution (F-20).
     *
     * @param loader computes the value on a full miss; may return
     *               {@code null} to signal absence
     */
    V getOrCompute(K key, java.util.function.Function<? super K, ? extends V> loader);

    /**
     * Stores {@code value} under {@code key} in L2 and then L1 (F-02 write
     * order). The entry lives no longer than the cache's L2 TTL; the L1 copy
     * expires no later than the L2 copy (F-05).
     */
    void put(K key, V value);

    /**
     * Removes {@code key} from both L1 and L2.
     */
    void evict(K key);
}
