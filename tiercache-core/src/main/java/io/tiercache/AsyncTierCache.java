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
 */
public interface AsyncTierCache<K, V> {

    /**
     * A loader that computes the value asynchronously. Used by
     * {@link #getOrComputeAsyncStage(Object, AsyncLoader)} — a separate
     * name rather than an overload of {@code getOrComputeAsync}, because
     * implicitly typed lambdas would make the two overloads ambiguous.
     */
    @FunctionalInterface
    interface AsyncLoader<K, V> {
        CompletionStage<? extends V> load(K key);
    }

    /** Async form of {@link TierCache#get}. */
    CompletionStage<V> getAsync(K key);

    /** Async form of {@link TierCache#lookup} (tri-state). */
    CompletionStage<LookupResult<V>> lookupAsync(K key);

    /**
     * Async form of {@link TierCache#getOrCompute}: the synchronous loader
     * runs on the shared executor, off the calling thread. Concurrent
     * calls for the same absent key coalesce onto one loader execution
     * through the engine's singleflight.
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
     */
    CompletionStage<V> getOrComputeAsyncStage(K key, AsyncLoader<? super K, ? extends V> loader);

    /** Async form of {@link TierCache#put}. */
    CompletionStage<Void> putAsync(K key, V value);

    /** Async form of {@link TierCache#put(Object, Object, String...)}. */
    CompletionStage<Void> putAsync(K key, V value, String... tags);

    /** Async form of {@link TierCache#putIfAbsent}. */
    CompletionStage<Boolean> putIfAbsentAsync(K key, V value);

    /** Async form of {@link TierCache#putNull}. */
    CompletionStage<Void> putNullAsync(K key);

    /** Async form of {@link TierCache#evict}. */
    CompletionStage<Void> evictAsync(K key);

    /** Async form of {@link TierCache#evictAll()}. */
    CompletionStage<Void> evictAllAsync();

    /** Async form of {@link TierCache#evictAll(Collection)}. */
    CompletionStage<Void> evictAllAsync(Collection<K> keys);

    /** Async form of {@link TierCache#evictByTag}. */
    CompletionStage<Void> evictByTagAsync(String tag);
}
