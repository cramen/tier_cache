package io.tiercache.kotlin

import io.tiercache.CacheOverride
import io.tiercache.CacheSettings
import io.tiercache.InvalidationMessage
import io.tiercache.TierCacheFactory
import io.tiercache.VersionGenerator
import io.tiercache.spi.CacheMetricsListener
import io.tiercache.spi.DegradationListener
import io.tiercache.spi.DistributedLockProvider
import io.tiercache.spi.InvalidationEventListener
import io.tiercache.spi.InvalidationHandler
import io.tiercache.spi.LocalCache
import io.tiercache.spi.RemoteCache
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.BiFunction
import java.util.function.Function

/**
 * Kotlin entry point over [TierCacheFactory]: hands out [KTierCache] suspend
 * facades and exposes inbound invalidation events as cold [Flow]s (design D3).
 *
 * The factory must be built through [builder] (or the `tierCache { }` DSL):
 * the builder installs a fan-out listener on the core invalidation-event hook
 * at build time, so exactly one transport subscription is shared by any
 * number of flow collectors.
 *
 * **Incubating:** 0.x API, may change before 1.0.
 */
class KTierCacheFactory internal constructor(
    /** The underlying blocking factory. */
    val delegate: TierCacheFactory,
    private val dispatcher: CoroutineDispatcher,
    private val fanout: InvalidationEventFanout,
) : AutoCloseable {

    /**
     * Returns the suspend facade for the named cache. The underlying
     * [TierCacheFactory.getCache] is memoized by name, so all facades for one
     * name share the same cache.
     */
    fun <K, V> getCache(name: String): KTierCache<K, V> =
        KTierCache(delegate.getCache(name), dispatcher)

    /** True while the L2 circuit breaker is open (L1-only degraded mode). */
    fun isDegraded(): Boolean = delegate.isDegraded()

    /**
     * Cold flow of inbound invalidation events applied to [cacheName], in
     * arrival order. Each collector registers its own listener on
     * subscription and unregisters on cancellation or completion.
     *
     * Producers never block: events are offered with `trySend`; once the
     * collector-side buffers (the flow's internal channel plus
     * [bufferCapacity]) are full, further events are dropped for that
     * collector and each drop is reported through [onEventDropped].
     */
    fun invalidationEvents(
        cacheName: String,
        bufferCapacity: Int = DEFAULT_INVALIDATION_BUFFER_CAPACITY,
        onEventDropped: (InvalidationMessage) -> Unit = {},
    ): Flow<InvalidationMessage> = callbackFlow {
        val unregister = fanout.register(cacheName) { event ->
            if (trySend(event).isFailure) {
                // The listener contract forbids throwing on the receive path.
                runCatching { onEventDropped(event) }
            }
        }
        awaitClose { unregister() }
    }.buffer(capacity = bufferCapacity, onBufferOverflow = BufferOverflow.SUSPEND)

    internal fun collectorCount(cacheName: String): Int = fanout.registrationCount(cacheName)

    override fun close() = delegate.close()

    /**
     * Builder mirroring [TierCacheFactory.Builder]; pass-throughs delegate
     * verbatim, only [invalidationEventListener] is composed with the
     * internal fan-out.
     */
    class Builder internal constructor(private val dispatcher: CoroutineDispatcher) {

        private val delegate = TierCacheFactory.builder()
        private val fanout = InvalidationEventFanout()
        private var userEventListener = InvalidationEventListener.NOOP

        fun defaults(defaults: CacheSettings) = apply { delegate.defaults(defaults) }

        fun cache(name: String, override: CacheOverride) = apply { delegate.cache(name, override) }

        /** The L2 implementation shared by all caches. Required. */
        fun remoteCache(remoteCache: RemoteCache<*, *>) = apply { delegate.remoteCache(remoteCache) }

        fun localCacheFactory(factory: BiFunction<String, CacheSettings, LocalCache<*, *>>) =
            apply { delegate.localCacheFactory(factory) }

        fun lockProvider(lockProvider: DistributedLockProvider) = apply { delegate.lockProvider(lockProvider) }

        fun invalidation(invalidationFactory: Function<VersionGenerator, InvalidationHandler>) =
            apply { delegate.invalidation(invalidationFactory) }

        fun metricsListener(listener: CacheMetricsListener) = apply { delegate.metricsListener(listener) }

        fun degradationListener(listener: DegradationListener) = apply { delegate.degradationListener(listener) }

        /**
         * Application-facing observer of inbound invalidation events;
         * invoked after the internal flow fan-out.
         */
        fun invalidationEventListener(listener: InvalidationEventListener) = apply {
            userEventListener = listener
        }

        fun disableSingleflight() = apply { delegate.disableSingleflight() }

        fun disableDistributedCoordination() = apply { delegate.disableDistributedCoordination() }

        fun disableCircuitBreaker() = apply { delegate.disableCircuitBreaker() }

        fun build(): KTierCacheFactory {
            delegate.invalidationEventListener { cache, event ->
                fanout.onEvent(cache, event)
                userEventListener.onEvent(cache, event)
            }
            return KTierCacheFactory(delegate.build(), dispatcher, fanout)
        }
    }

    companion object {
        /** Default collector-side buffer capacity for [invalidationEvents]. */
        const val DEFAULT_INVALIDATION_BUFFER_CAPACITY: Int = 64

        @JvmStatic
        fun builder(dispatcher: CoroutineDispatcher = Dispatchers.IO): Builder = Builder(dispatcher)
    }
}

/**
 * In-process fan-out over the single core invalidation-event hook: each flow
 * collector holds its own registration; dispatch is a plain `trySend`, so
 * event producers are never blocked by slow collectors.
 */
internal class InvalidationEventFanout {

    private class Registration(val cacheName: String, val handler: (InvalidationMessage) -> Unit)

    private val registrations = CopyOnWriteArrayList<Registration>()

    /** Registers [handler] for [cacheName]; returns the unregister action. */
    fun register(cacheName: String, handler: (InvalidationMessage) -> Unit): () -> Unit {
        val registration = Registration(cacheName, handler)
        registrations += registration
        return { registrations -= registration }
    }

    fun onEvent(cacheName: String, event: InvalidationMessage) {
        for (registration in registrations) {
            if (registration.cacheName == cacheName) {
                registration.handler(event)
            }
        }
    }

    fun registrationCount(cacheName: String): Int = registrations.count { it.cacheName == cacheName }
}
