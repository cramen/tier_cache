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
 * @since 0.1.0
 */
class KTierCacheFactory internal constructor(
    /**
     * The underlying blocking factory.
     *
     * @since 0.1.0
     */
    val delegate: TierCacheFactory,
    private val dispatcher: CoroutineDispatcher,
    private val fanout: InvalidationEventFanout,
) : AutoCloseable {

    /**
     * Returns the suspend facade for the named cache. The underlying
     * [TierCacheFactory.getCache] and [TierCacheFactory.asyncCache] are
     * memoized by name, so all facades for one name share the same cache and
     * the same async view (whose operations run on the factory's shared
     * daemon executor).
     *
     * @param K the key type
     * @param V the value type
     * @param name the cache name
     * @return the suspend facade for [name]
     * @since 0.1.0
     */
    fun <K, V> getCache(name: String): KTierCache<K, V> =
        KTierCache(delegate.getCache(name), delegate.asyncCache(name), dispatcher)

    /**
     * True while the L2 circuit breaker is open (L1-only degraded mode).
     *
     * @return `true` while the factory is in L1-only degraded mode
     * @since 0.1.0
     */
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
     *
     * @param cacheName the cache whose inbound invalidation events are observed
     * @param bufferCapacity collector-side buffer capacity on top of the
     *   flow's internal channel; defaults to [DEFAULT_INVALIDATION_BUFFER_CAPACITY]
     * @param onEventDropped callback invoked with each event dropped because
     *   this collector's buffers were full
     * @return a cold flow of inbound invalidation events in arrival order
     * @since 0.1.0
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

    /**
     * Closes the underlying factory, releasing its caches, executors, and
     * transport subscriptions.
     *
     * @since 0.1.0
     */
    override fun close() = delegate.close()

    /**
     * Builder mirroring [TierCacheFactory.Builder]; pass-throughs delegate
     * verbatim, only [invalidationEventListener] is composed with the
     * internal fan-out.
     *
     * @since 0.1.0
     */
    class Builder internal constructor(private val dispatcher: CoroutineDispatcher) {

        private val delegate = TierCacheFactory.builder()
        private val fanout = InvalidationEventFanout()
        private var userEventListener = InvalidationEventListener.NOOP

        /**
         * Sets the global defaults applied to every cache without a per-cache
         * override.
         *
         * @param defaults the global cache defaults
         * @return this builder
         * @since 0.1.0
         */
        fun defaults(defaults: CacheSettings) = apply { delegate.defaults(defaults) }

        /**
         * Registers a named cache with per-cache overrides on top of the
         * global defaults.
         *
         * @param name the cache name
         * @param override the per-cache overrides
         * @return this builder
         * @since 0.1.0
         */
        fun cache(name: String, override: CacheOverride) = apply { delegate.cache(name, override) }

        /**
         * The L2 implementation shared by all caches. Required.
         *
         * @param remoteCache the shared L2 implementation
         * @return this builder
         * @since 0.1.0
         */
        fun remoteCache(remoteCache: RemoteCache<*, *>) = apply { delegate.remoteCache(remoteCache) }

        /**
         * Sets the factory creating the L1 for each named cache.
         *
         * @param factory L1 factory receiving the cache name and resolved settings
         * @return this builder
         * @since 0.1.0
         */
        fun localCacheFactory(factory: BiFunction<String, CacheSettings, LocalCache<*, *>>) =
            apply { delegate.localCacheFactory(factory) }

        /**
         * Sets the distributed lock provider used for cluster-wide rebuild
         * coordination.
         *
         * @param lockProvider the distributed lock provider
         * @return this builder
         * @since 0.1.0
         */
        fun lockProvider(lockProvider: DistributedLockProvider) = apply { delegate.lockProvider(lockProvider) }

        /**
         * Sets the invalidation handler factory, given the version generator
         * assigned to this instance.
         *
         * @param invalidationFactory factory creating the invalidation handler
         * @return this builder
         * @since 0.1.0
         */
        fun invalidation(invalidationFactory: Function<VersionGenerator, InvalidationHandler>) =
            apply { delegate.invalidation(invalidationFactory) }

        /**
         * Registers a metrics listener receiving cache-level request and
         * latency signals.
         *
         * @param listener the metrics listener
         * @return this builder
         * @since 0.1.0
         */
        fun metricsListener(listener: CacheMetricsListener) = apply { delegate.metricsListener(listener) }

        /**
         * Registers a listener notified on circuit-breaker state transitions.
         *
         * @param listener the degradation listener
         * @return this builder
         * @since 0.1.0
         */
        fun degradationListener(listener: DegradationListener) = apply { delegate.degradationListener(listener) }

        /**
         * Application-facing observer of inbound invalidation events;
         * invoked after the internal flow fan-out.
         *
         * @param listener the invalidation event listener
         * @return this builder
         * @since 0.1.0
         */
        fun invalidationEventListener(listener: InvalidationEventListener) = apply {
            userEventListener = listener
        }

        /**
         * Disables singleflight (opt-out of a correct-by-default protection;
         * logged as a risk).
         *
         * @return this builder
         * @since 0.1.0
         */
        fun disableSingleflight() = apply { delegate.disableSingleflight() }

        /**
         * Disables cluster-wide rebuild coordination (opt-out of a
         * correct-by-default protection; logged as a risk).
         *
         * @return this builder
         * @since 0.1.0
         */
        fun disableDistributedCoordination() = apply { delegate.disableDistributedCoordination() }

        /**
         * Disables the L2 circuit breaker (opt-out of a correct-by-default
         * protection; logged as a risk).
         *
         * @return this builder
         * @since 0.1.0
         */
        fun disableCircuitBreaker() = apply { delegate.disableCircuitBreaker() }

        /**
         * Builds the factory, validating the resolved configuration with
         * fail-fast semantics.
         *
         * @return the configured factory
         * @throws io.tiercache.CacheConfigurationException if the resolved
         *   configuration is invalid
         * @throws NullPointerException if no [remoteCache] was configured
         * @since 0.1.0
         */
        fun build(): KTierCacheFactory {
            delegate.invalidationEventListener { cache, event ->
                fanout.onEvent(cache, event)
                userEventListener.onEvent(cache, event)
            }
            return KTierCacheFactory(delegate.build(), dispatcher, fanout)
        }
    }

    companion object {
        /**
         * Default collector-side buffer capacity for [invalidationEvents].
         *
         * @since 0.1.0
         */
        const val DEFAULT_INVALIDATION_BUFFER_CAPACITY: Int = 64

        /**
         * Creates a builder for a [KTierCacheFactory].
         *
         * @param dispatcher retained for source compatibility; cache calls
         *   suspend on the async view's stages (see [KTierCache.dispatcher])
         * @return a new builder
         * @since 0.1.0
         */
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
