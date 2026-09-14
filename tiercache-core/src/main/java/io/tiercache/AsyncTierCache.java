package io.tiercache;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * A non-blocking view of a {@link TierCache}: every operation returns a
 * {@link CompletionStage} and performs its cache I/O (L2 round trips,
 * loader executions) on the factory's shared daemon executor — never on
 * the calling thread and never on the common pool.
 *
 * <p>Semantics are inherited unchanged from the synchronous engine: the
 * same L1 &rarr; L2 &rarr; loader cascade with L1 warm-up on L2 hits, the
 * same per-instance singleflight coalescing, null-marker policy,
 * degradation handling, and metrics. This view adds no second coalescing
 * layer — concurrent async calls for one absent key join the engine's
 * inflight entry exactly like concurrent synchronous calls.
 *
 * <p>Obtain the view from {@link TierCacheFactory#asyncCache(String)};
 * it is memoized alongside the cache. Closing the factory shuts down the
 * shared executor, after which async operations reject submissions with
 * {@link java.util.concurrent.RejectedExecutionException} instead of
 * running cache work on the calling thread.
 *
 * @param <K> key type
 * @param <V> value type
 * @since 0.3.0
 */
public interface AsyncTierCache<K, V> {

    /**
     * A loader that computes the value asynchronously. Used by
     * {@link #getOrComputeAsyncStage(Object, AsyncLoader)} — a separate
     * name rather than an overload of {@code getOrComputeAsync}, because
     * implicitly typed lambdas would make the two overloads ambiguous.
     *
     * @param <K> key type
     * @param <V> value type
     * @since 0.3.0
     */
    @FunctionalInterface
    interface AsyncLoader<K, V> {

        /**
         * Computes the value for {@code key} asynchronously.
         *
         * @param key the key to load
         * @return a stage producing the value; may produce {@code null} to
         *         signal absence
         * @since 0.3.0
         */
        CompletionStage<? extends V> load(K key);
    }

    /**
     * Async form of {@link TierCache#get}.
     *
     * @param key the key to look up
     * @return a stage producing the cached value, or {@code null} if absent
     *         or cached-null
     * @since 0.3.0
     */
    CompletionStage<V> getAsync(K key);

    /**
     * Async form of {@link TierCache#lookup} (tri-state).
     *
     * @param key the key to look up
     * @return a stage producing the tri-state lookup outcome
     * @since 0.3.0
     */
    CompletionStage<LookupResult<V>> lookupAsync(K key);

    /**
     * Async form of {@link TierCache#getOrCompute}: the synchronous loader
     * runs on the shared executor, off the calling thread. Concurrent
     * calls for the same absent key coalesce onto one loader execution
     * through the engine's singleflight.
     *
     * @param key    the key to look up or compute
     * @param loader computes the value on a full miss; may return
     *               {@code null} to signal absence
     * @return a stage producing the cached or computed value
     * @since 0.3.0
     */
    CompletionStage<V> getOrComputeAsync(K key, Function<? super K, ? extends V> loader);

    /**
     * Asynchronous-loader form: the loader returns its value as a
     * {@link CompletionStage}. Contract: an executor thread runs the
     * engine's synchronous load path and joins the loader's stage there —
     * the documented trade-off that keeps coalescing in the engine's
     * singleflight (one loader execution per key per instance) without a
     * second coalescing layer. If the loader's stage completes
     * exceptionally, the failure is unwrapped and propagated exactly like
     * a synchronous loader throwing it: every coalesced caller's stage
     * completes exceptionally and nothing is stored.
     *
     * @param key    the key to look up or compute
     * @param loader computes the value asynchronously on a full miss
     * @return a stage producing the cached or computed value
     * @since 0.3.0
     */
    CompletionStage<V> getOrComputeAsyncStage(K key, AsyncLoader<? super K, ? extends V> loader);

    /**
     * Async form of {@link TierCache#put}.
     *
     * @param key   the key to store under
     * @param value the value to store
     * @return a stage completing when the write is done
     * @since 0.3.0
     */
    CompletionStage<Void> putAsync(K key, V value);

    /**
     * Async form of {@link TierCache#put(Object, Object, String...)}.
     *
     * @param key   the key to store under
     * @param value the value to store
     * @param tags  tags to associate with the entry
     * @return a stage completing when the write is done
     * @since 0.3.0
     */
    CompletionStage<Void> putAsync(K key, V value, String... tags);

    /**
     * Async form of {@link TierCache#putIfAbsent}.
     *
     * @param key   the key to store under
     * @param value the value to store
     * @return a stage producing {@code true} if this call stored the value
     * @since 0.3.0
     */
    CompletionStage<Boolean> putIfAbsentAsync(K key, V value);

    /**
     * Async form of {@link TierCache#putNull}.
     *
     * @param key the key to mark as known-absent
     * @return a stage completing when the write is done
     * @since 0.3.0
     */
    CompletionStage<Void> putNullAsync(K key);

    /**
     * Async form of {@link TierCache#evict}.
     *
     * @param key the key to remove
     * @return a stage completing when the eviction is done
     * @since 0.3.0
     */
    CompletionStage<Void> evictAsync(K key);

    /**
     * Async form of {@link TierCache#evictAll()}.
     *
     * @return a stage completing when the eviction is done
     * @since 0.3.0
     */
    CompletionStage<Void> evictAllAsync();

    /**
     * Async form of {@link TierCache#evictAll(Collection)}.
     *
     * @param keys the keys to remove
     * @return a stage completing when the eviction is done
     * @since 0.3.0
     */
    CompletionStage<Void> evictAllAsync(Collection<K> keys);

    /**
     * Async form of {@link TierCache#evictByTag}.
     *
     * @param tag the tag whose entries are removed
     * @return a stage completing when the eviction is done
     * @since 0.3.0
     */
    CompletionStage<Void> evictByTagAsync(String tag);
}
