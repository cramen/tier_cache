package io.tiercache.kotlin

import io.tiercache.LookupResult
import io.tiercache.TierCache
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext

/**
 * Suspend facade over a blocking [TierCache] (design D1): every operation
 * delegates to the synchronous call inside `withContext(dispatcher)`, so the
 * caller's dispatcher suspends while only a worker of [dispatcher] (default
 * [Dispatchers.IO]) blocks on cache I/O.
 *
 * Null-safety is expressed in Kotlin types: [get] returns `null` on a miss
 * AND on a cached-null marker; use [lookup] to distinguish hit / cached-null
 * / miss. All semantics (cascade, L1 warm-up, null-markers, tags) are exactly
 * those of the underlying [TierCache].
 *
 * **Incubating:** 0.x API, may change before 1.0.
 */
class KTierCache<K, V>(
    /** The underlying blocking cache. */
    val delegate: TierCache<K, V>,
    /** Dispatcher the blocking calls are offloaded to. */
    val dispatcher: CoroutineDispatcher = Dispatchers.IO,
) {

    /** Returns the value for [key], or `null` on a miss or cached-null. */
    suspend fun get(key: K): V? = withContext(dispatcher) { delegate.get(key) }

    /** Tri-state lookup: hit (with value), cached-null, or miss. */
    suspend fun lookup(key: K): LookupResult<V> = withContext(dispatcher) { delegate.lookup(key) }

    /**
     * Returns the value for [key], cascading L1 → L2 → [loader]. The
     * suspending loader is adapted with [runBlocking] on the [dispatcher]
     * worker (design D2); concurrent calls for the same key coalesce onto a
     * single loader execution through core's singleflight — no second
     * coalescing layer exists here.
     */
    suspend fun getOrCompute(key: K, loader: suspend (K) -> V?): V? =
        withContext(dispatcher) {
            delegate.getOrCompute(key) { k -> runBlocking { loader(k) } }
        }

    /** Stores [value] under [key] in L2 and then L1. */
    suspend fun put(key: K, value: V) {
        withContext(dispatcher) { delegate.put(key, value) }
    }

    /** Stores [value] under [key], tagging it for later [evictByTag]. */
    suspend fun put(key: K, value: V, vararg tags: String) {
        withContext(dispatcher) { delegate.put(key, value, *tags) }
    }

    /** Stores [value] only if [key] is absent; returns `true` if this call stored it. */
    suspend fun putIfAbsent(key: K, value: V): Boolean =
        withContext(dispatcher) { delegate.putIfAbsent(key, value) }

    /** Stores an explicit null-marker for [key] (a no-op under the `deny` policy). */
    suspend fun putNull(key: K) {
        withContext(dispatcher) { delegate.putNull(key) }
    }

    /** Removes [key] from both L1 and L2. */
    suspend fun evict(key: K) {
        withContext(dispatcher) { delegate.evict(key) }
    }

    /** Removes all entries of this cache from both levels. */
    suspend fun evictAll() {
        withContext(dispatcher) { delegate.evictAll() }
    }

    /** Removes the given [keys] from both levels, on all instances. */
    suspend fun evictAll(keys: Collection<K>) {
        withContext(dispatcher) { delegate.evictAll(keys) }
    }

    /** Removes all entries tagged with [tag] from both levels, on all instances. */
    suspend fun evictByTag(tag: String) {
        withContext(dispatcher) { delegate.evictByTag(tag) }
    }
}

/** Wraps this blocking cache in a [KTierCache] suspend facade. */
fun <K, V> TierCache<K, V>.asKotlin(dispatcher: CoroutineDispatcher = Dispatchers.IO): KTierCache<K, V> =
    KTierCache(this, dispatcher)
