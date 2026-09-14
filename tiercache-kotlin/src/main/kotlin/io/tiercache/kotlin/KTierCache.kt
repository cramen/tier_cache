package io.tiercache.kotlin

import io.tiercache.AsyncTierCache
import io.tiercache.LookupResult
import io.tiercache.TierCache
import io.tiercache.internal.DefaultAsyncTierCache
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Suspend facade over a blocking [TierCache] (design D3): every operation
 * awaits the [java.util.concurrent.CompletionStage] of the cache's
 * [AsyncTierCache] view, so the caller's coroutine suspends without parking
 * any worker of [dispatcher] on cache I/O. Cache work runs on the executor
 * behind the async view — the factory's shared daemon executor for facades
 * from [KTierCacheFactory.getCache].
 *
 * Null-safety is expressed in Kotlin types: [get] returns `null` on a miss
 * AND on a cached-null marker; use [lookup] to distinguish hit / cached-null
 * / miss. All semantics (cascade, L1 warm-up, null-markers, tags) are exactly
 * those of the underlying [TierCache].
 *
 * @param K the key type
 * @param V the value type
 * @since 0.1.0
 */
class KTierCache<K, V> internal constructor(
    /**
     * The underlying blocking cache.
     *
     * @since 0.1.0
     */
    val delegate: TierCache<K, V>,
    private val async: AsyncTierCache<K, V>,
    /**
     * Retained for source compatibility. No longer used for cache calls:
     * operations suspend on the async view's stages instead of being
     * offloaded to this dispatcher.
     *
     * @since 0.1.0
     */
    val dispatcher: CoroutineDispatcher,
) {

    /**
     * Creates a facade over [delegate] with its own standalone daemon
     * executor behind the async view. Prefer [KTierCacheFactory.getCache],
     * which shares the factory's executor across caches.
     *
     * @param delegate the underlying blocking cache
     * @param dispatcher retained for source compatibility; not used for
     *   cache calls (see the property documentation)
     * @since 0.1.0
     */
    constructor(
        delegate: TierCache<K, V>,
        dispatcher: CoroutineDispatcher = Dispatchers.IO,
    ) : this(delegate, DefaultAsyncTierCache(delegate, STANDALONE_EXECUTOR), dispatcher)

    /**
     * Returns the value for [key], or `null` on a miss or cached-null.
     *
     * @param key the cache key
     * @return the cached value, or `null` on a miss or a cached-null marker
     * @since 0.1.0
     */
    suspend fun get(key: K): V? = async.getAsync(key).await()

    /**
     * Tri-state lookup: hit (with value), cached-null, or miss.
     *
     * @param key the cache key
     * @return the lookup outcome distinguishing hit, cached-null, and miss
     * @since 0.1.0
     */
    suspend fun lookup(key: K): LookupResult<V> = async.lookupAsync(key).await()

    /**
     * Returns the value for [key], cascading L1 → L2 → [loader]. The
     * suspending loader is adapted to the async view's async-loader form
     * (design D3): it runs as a child coroutine of the calling scope, and
     * cancelling the awaiting coroutine cancels the loader with it.
     * Concurrent calls for the same key coalesce onto a single loader
     * execution through core's singleflight — no second coalescing layer
     * exists here. Loader failures reach every coalesced caller unwrapped.
     *
     * @param key the cache key
     * @param loader the suspending value loader invoked on a miss; a `null`
     *   result is cached according to the cache's null policy
     * @return the cached or freshly loaded value, or `null` if the loader
     *   produced no value
     * @since 0.1.0
     */
    suspend fun getOrCompute(key: K, loader: suspend (K) -> V?): V? = coroutineScope {
        async.getOrComputeAsyncStage(key) { k -> future { loader(k) } }.await()
    }

    /**
     * Stores [value] under [key] in L2 and then L1.
     *
     * @param key the cache key
     * @param value the value to store
     * @since 0.1.0
     */
    suspend fun put(key: K, value: V) {
        async.putAsync(key, value).await()
    }

    /**
     * Stores [value] under [key], tagging it for later [evictByTag].
     *
     * @param key the cache key
     * @param value the value to store
     * @param tags tags to associate with the entry
     * @since 0.1.0
     */
    suspend fun put(key: K, value: V, vararg tags: String) {
        async.putAsync(key, value, *tags).await()
    }

    /**
     * Stores [value] only if [key] is absent; returns `true` if this call stored it.
     *
     * @param key the cache key
     * @param value the value to store
     * @return `true` if this call stored the value, `false` if the key was
     *   already present
     * @since 0.1.0
     */
    suspend fun putIfAbsent(key: K, value: V): Boolean = async.putIfAbsentAsync(key, value).await()

    /**
     * Stores an explicit null-marker for [key] (a no-op under the `deny` policy).
     *
     * @param key the cache key
     * @since 0.1.0
     */
    suspend fun putNull(key: K) {
        async.putNullAsync(key).await()
    }

    /**
     * Removes [key] from both L1 and L2.
     *
     * @param key the cache key
     * @since 0.1.0
     */
    suspend fun evict(key: K) {
        async.evictAsync(key).await()
    }

    /**
     * Removes all entries of this cache from both levels.
     *
     * @since 0.1.0
     */
    suspend fun evictAll() {
        async.evictAllAsync().await()
    }

    /**
     * Removes the given [keys] from both levels, on all instances.
     *
     * @param keys the cache keys to remove
     * @since 0.1.0
     */
    suspend fun evictAll(keys: Collection<K>) {
        async.evictAllAsync(keys).await()
    }

    /**
     * Removes all entries tagged with [tag] from both levels, on all instances.
     *
     * @param tag the tag whose entries are removed
     * @since 0.1.0
     */
    suspend fun evictByTag(tag: String) {
        async.evictByTagAsync(tag).await()
    }

    private companion object {
        /**
         * Offload executor for facades built directly from a bare [TierCache]
         * (no factory to borrow the shared executor from). Daemon threads;
         * facades from [KTierCacheFactory.getCache] use the factory's
         * executor instead.
         */
        val STANDALONE_EXECUTOR: ExecutorService = Executors.newCachedThreadPool { runnable ->
            Thread(runnable, "tiercache-kotlin-standalone").apply { isDaemon = true }
        }
    }
}

/**
 * Wraps this blocking cache in a [KTierCache] suspend facade.
 *
 * @param dispatcher retained for source compatibility; not used for cache
 *   calls (see [KTierCache.dispatcher])
 * @return a suspend facade over this cache
 * @since 0.1.0
 */
fun <K, V> TierCache<K, V>.asKotlin(dispatcher: CoroutineDispatcher = Dispatchers.IO): KTierCache<K, V> =
    KTierCache(this, dispatcher)
