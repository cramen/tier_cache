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
 * <p>Null semantics (F-25): under the default {@code deny} policy a loader
 * null result stays an uncached miss. Under {@code allow(ttl)} an explicit
 * null-marker is stored and suppresses the loader until it expires. Use
 * {@link #lookup} to distinguish miss / cached-null / hit.
 *
 * <p>Consistency model (F-15): this cache is eventually consistent. L1
 * entries never outlive their L2 counterpart. No strong-consistency
 * guarantees are given or implied.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface TierCache<K, V> {

    /**
     * Returns the value for {@code key}, cascading L1 &rarr; L2 and warming
     * L1 on an L2 hit. Returns {@code null} on a miss AND on a cached-null
     * marker (use {@link #lookup} to distinguish); no loader is invoked.
     */
    V get(K key);

    /**
     * Tri-state lookup: reports hit (with value), cached-null (marker
     * present, F-25), or miss. Cascades L1 &rarr; L2 like {@link #get}
     * (including L1 warm-up) and never invokes a loader.
     */
    LookupResult<V> lookup(K key);

    /**
     * Returns the value for {@code key}, cascading L1 &rarr; L2 &rarr; loader.
     * An L2 hit warms L1; a loader result populates both levels. If the
     * loader produces no value, then under the {@code deny} policy the miss
     * propagates and nothing is stored; under {@code allow(ttl)} a
     * null-marker is stored in both levels and suppresses the loader until
     * it expires (F-25).
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
     * order), replacing any null-marker (F-25). The entry lives no longer
     * than the cache's L2 TTL; the L1 copy expires no later than the L2 copy
     * (F-05).
     */
    void put(K key, V value);

    /**
     * Atomically stores {@code value} under {@code key} only if the key is
     * absent (or expired) in L2 (F-03). On winning, L1 is warmed as well; a
     * losing call modifies neither level. A stored null-marker counts as
     * present.
     *
     * <p>Atomicity is cross-instance only while L2 is healthy; the degraded
     * mode (F-30/F-31) lands with the circuit breaker change.
     *
     * @return {@code true} if this call stored the value
     */
    boolean putIfAbsent(K key, V value);

    /**
     * Removes {@code key} from both L1 and L2.
     */
    void evict(K key);
}
