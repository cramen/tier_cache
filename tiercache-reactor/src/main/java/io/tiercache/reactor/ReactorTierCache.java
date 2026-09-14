package io.tiercache.reactor;

import io.tiercache.AsyncTierCache;
import io.tiercache.LookupResult;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;

/**
 * Reactor facade over an {@link AsyncTierCache}: every operation returns a
 * cold {@link Mono} whose cache I/O (L2 round trips, loader executions)
 * runs on the executor behind the async view — never on the calling
 * thread (design D1).
 *
 * <p>Semantics are inherited unchanged from the engine: the same
 * L1 &rarr; L2 &rarr; loader cascade with L1 warm-up on L2 hits, the same
 * per-instance singleflight coalescing, null-marker policy, degradation
 * handling, and metrics. Reactor types cannot emit {@code null}, so a
 * miss (or a cached-null) surfaces as an empty {@code Mono}; use
 * {@link #lookup} where miss and cached-null must be distinguished.
 * Errors pass through unwrapped (the async view already unwraps
 * {@link java.util.concurrent.CompletionException}).
 *
 * <p>Obtain the facade from {@link ReactorCacheFactory#reactorCache(String)},
 * or wrap any {@link AsyncTierCache} directly.
 *
 * @param <K> key type
 * @param <V> value type
 * @since 0.4.0
 */
public final class ReactorTierCache<K, V> {

    private final AsyncTierCache<K, V> delegate;

    /**
     * Wraps the given async view in a Reactor facade. The delegate is used
     * directly (not copied or memoized); its lifecycle stays with the owning
     * factory.
     *
     * @param delegate the async view to wrap; must not be {@code null}
     * @throws NullPointerException if {@code delegate} is {@code null}
     * @since 0.4.0
     */
    public ReactorTierCache(AsyncTierCache<K, V> delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /**
     * Returns the value for {@code key}: an empty {@code Mono} on a miss
     * or cached-null, the value otherwise.
     *
     * @param key the cache key; must not be {@code null}
     * @return a cold {@code Mono} emitting the cached value, or empty on a miss
     *     or a cached-null marker
     * @since 0.4.0
     */
    public Mono<V> get(K key) {
        return Mono.fromCompletionStage(() -> delegate.getAsync(key));
    }

    /**
     * Tri-state lookup: hit (with value), cached-null, or miss.
     *
     * @param key the cache key; must not be {@code null}
     * @return a cold {@code Mono} emitting the tri-state
     *     {@link LookupResult} for {@code key}
     * @since 0.4.0
     */
    public Mono<LookupResult<V>> lookup(K key) {
        return Mono.fromCompletionStage(() -> delegate.lookupAsync(key));
    }

    /**
     * Returns the value for {@code key}, cascading L1 &rarr; L2 &rarr;
     * {@code loader}; empty when the loader returns {@code null}. The
     * synchronous loader runs on the executor behind the async view.
     * Concurrent calls for the same absent key coalesce onto one loader
     * execution through the engine's singleflight — this facade adds no
     * second coalescing layer.
     *
     * @param key the cache key; must not be {@code null}
     * @param loader the synchronous loader computing the value on a miss;
     *     must not be {@code null}
     * @return a cold {@code Mono} emitting the resolved value, or empty when
     *     the loader returns {@code null}
     * @throws NullPointerException if {@code loader} is {@code null}
     * @since 0.4.0
     */
    public Mono<V> getOrCompute(K key, Function<? super K, ? extends V> loader) {
        Objects.requireNonNull(loader, "loader");
        return Mono.fromCompletionStage(() -> delegate.getOrComputeAsync(key, loader));
    }

    /**
     * Mono-loader form of {@link #getOrCompute(Object, Function)} (design
     * D2): the loader's {@code Mono} is adapted with {@code toFuture()}
     * into the async view's async-loader form, so coalescing stays in the
     * engine's singleflight — only the singleflight leader's loader runs,
     * hence exactly one loader {@code Mono} is subscribed per key per
     * instance. A separate name rather than an overload of
     * {@code getOrCompute}, because implicitly typed lambdas would make
     * the two forms ambiguous.
     *
     * <p>An executor thread runs the engine's synchronous load path and
     * joins the loader {@code Mono} there — the documented adapter
     * contract (same as the async view). A loader error is unwrapped and
     * reaches every coalesced subscriber; nothing is stored. Cancelling
     * one subscriber abandons only that subscriber's own outer
     * subscription: the engine's inflight entry is shared, so the shared
     * load completes for the remaining subscribers.
     *
     * @param key the cache key; must not be {@code null}
     * @param loader the reactive loader computing the value on a miss; must
     *     not be {@code null} and must not return a {@code null} {@code Mono}
     * @return a cold {@code Mono} emitting the resolved value, or empty when
     *     the loader's {@code Mono} is empty
     * @throws NullPointerException if {@code loader} is {@code null}
     * @since 0.4.0
     */
    public Mono<V> getOrComputeMono(K key, Function<? super K, Mono<? extends V>> loader) {
        Objects.requireNonNull(loader, "loader");
        return Mono.fromCompletionStage(
                () -> delegate.getOrComputeAsyncStage(key, k -> loader.apply(k).toFuture()));
    }

    /**
     * Stores {@code value} under {@code key} in L2 and then L1.
     *
     * @param key the cache key; must not be {@code null}
     * @param value the value to store; must not be {@code null} (use
     *     {@link #putNull} for an explicit null-marker)
     * @return a cold {@code Mono} completing when the value is stored in
     *     both levels
     * @since 0.4.0
     */
    public Mono<Void> put(K key, V value) {
        return Mono.fromCompletionStage(() -> delegate.putAsync(key, value));
    }

    /**
     * Stores {@code value} under {@code key}, tagging it for later
     * {@link #evictByTag}.
     *
     * @param key the cache key; must not be {@code null}
     * @param value the value to store; must not be {@code null}
     * @param tags tags associated with the entry; may be empty
     * @return a cold {@code Mono} completing when the value is stored in
     *     both levels
     * @since 0.4.0
     */
    public Mono<Void> put(K key, V value, String... tags) {
        return Mono.fromCompletionStage(() -> delegate.putAsync(key, value, tags));
    }

    /**
     * Stores {@code value} only if {@code key} is absent; emits {@code true}
     * if this call stored it.
     *
     * @param key the cache key; must not be {@code null}
     * @param value the value to store; must not be {@code null}
     * @return a cold {@code Mono} emitting {@code true} if this call stored
     *     the value, {@code false} if the key was already present
     * @since 0.4.0
     */
    public Mono<Boolean> putIfAbsent(K key, V value) {
        return Mono.fromCompletionStage(() -> delegate.putIfAbsentAsync(key, value));
    }

    /**
     * Stores an explicit null-marker for {@code key} (a no-op under the
     * {@code deny} null-caching policy).
     *
     * @param key the cache key; must not be {@code null}
     * @return a cold {@code Mono} completing when the marker is stored
     * @since 0.4.0
     */
    public Mono<Void> putNull(K key) {
        return Mono.fromCompletionStage(() -> delegate.putNullAsync(key));
    }

    /**
     * Removes {@code key} from both L1 and L2.
     *
     * @param key the cache key; must not be {@code null}
     * @return a cold {@code Mono} completing when the entry is removed from
     *     both levels
     * @since 0.4.0
     */
    public Mono<Void> evict(K key) {
        return Mono.fromCompletionStage(() -> delegate.evictAsync(key));
    }

    /**
     * Removes all entries of this cache from both levels.
     *
     * @return a cold {@code Mono} completing when the cache is cleared in
     *     both levels
     * @since 0.4.0
     */
    public Mono<Void> evictAll() {
        return Mono.fromCompletionStage(delegate::evictAllAsync);
    }

    /**
     * Removes the given {@code keys} from both levels, on all instances.
     *
     * @param keys the keys to remove; must not be {@code null}
     * @return a cold {@code Mono} completing when the keys are removed
     * @since 0.4.0
     */
    public Mono<Void> evictAll(Collection<K> keys) {
        return Mono.fromCompletionStage(() -> delegate.evictAllAsync(keys));
    }

    /**
     * Removes all entries tagged with {@code tag} from both levels, on all
     * instances.
     *
     * @param tag the tag whose entries are removed; must not be {@code null}
     * @return a cold {@code Mono} completing when the tagged entries are
     *     removed
     * @since 0.4.0
     */
    public Mono<Void> evictByTag(String tag) {
        return Mono.fromCompletionStage(() -> delegate.evictByTagAsync(tag));
    }
}
