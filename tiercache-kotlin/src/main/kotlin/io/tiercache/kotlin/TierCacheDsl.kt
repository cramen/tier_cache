package io.tiercache.kotlin

import io.tiercache.CacheOverride
import io.tiercache.CacheSettings
import io.tiercache.InvalidationMode
import io.tiercache.NullPolicy
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
import java.time.Duration
import java.util.function.BiFunction
import java.util.function.Function

/**
 * DSL marker for the `tierCache { }` configuration DSL, restricting implicit
 * receiver access inside nested blocks.
 *
 * @since 0.1.0
 */
@DslMarker
annotation class TierCacheDslMarker

/**
 * Builds a [KTierCacheFactory] from an idiomatic Kotlin configuration block
 * (design D4). The DSL is a thin veneer over `TierCacheFactory.Builder`:
 * [TierCacheFactoryDsl.defaults] and [TierCacheFactoryDsl.cache] blocks
 * resolve into the same [CacheSettings]/[CacheOverride] objects and go
 * through the same builder methods (and the same fail-fast startup
 * validation) as the programmatic path — there is no parallel config model.
 *
 * ```
 * val factory = tierCache {
 *     remoteCache(redisCache)
 *     defaults {
 *         l1MaxSize = 5_000
 *         l2Ttl = Duration.ofMinutes(30)
 *     }
 *     cache("users") {
 *         l2Ttl = Duration.ofMinutes(5)
 *         nullPolicy = NullPolicy.allow(Duration.ofMinutes(1))
 *     }
 * }
 * ```
 *
 * @param dispatcher retained for source compatibility; cache calls suspend on
 *   the async view's stages (see [KTierCache.dispatcher])
 * @param block the configuration block
 * @return the configured factory
 * @throws io.tiercache.CacheConfigurationException if the resolved
 *   configuration is invalid
 * @since 0.1.0
 */
fun tierCache(
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
    block: TierCacheFactoryDsl.() -> Unit,
): KTierCacheFactory = TierCacheFactoryDsl(dispatcher).apply(block).build()

/**
 * Root block of the `tierCache { }` DSL: global defaults, named per-cache
 * overrides, and pass-throughs for the builder's infrastructure setters.
 *
 * @since 0.1.0
 */
@TierCacheDslMarker
class TierCacheFactoryDsl internal constructor(dispatcher: CoroutineDispatcher) {

    private val builder = KTierCacheFactory.builder(dispatcher)
    private var defaultsDsl: CacheSettingsDsl? = null
    private val cacheDsls = LinkedHashMap<String, CacheOverrideDsl>()

    /**
     * Global defaults; unset properties keep `CacheSettings.defaults()` values.
     *
     * @param block the defaults configuration block
     * @since 0.1.0
     */
    fun defaults(block: CacheSettingsDsl.() -> Unit) {
        defaultsDsl = CacheSettingsDsl().apply(block)
    }

    /**
     * Per-cache overrides; unset properties inherit the global defaults.
     *
     * @param name the cache name
     * @param block the per-cache configuration block
     * @since 0.1.0
     */
    fun cache(name: String, block: CacheOverrideDsl.() -> Unit) {
        cacheDsls[name] = CacheOverrideDsl().apply(block)
    }

    /**
     * The L2 implementation shared by all caches. Required.
     *
     * @param remoteCache the shared L2 implementation
     * @since 0.1.0
     */
    fun remoteCache(remoteCache: RemoteCache<*, *>) {
        builder.remoteCache(remoteCache)
    }

    /**
     * Sets the factory creating the L1 for each named cache.
     *
     * @param factory L1 factory receiving the cache name and resolved settings
     * @since 0.1.0
     */
    fun localCacheFactory(factory: (String, CacheSettings) -> LocalCache<*, *>) {
        builder.localCacheFactory(BiFunction { name, settings -> factory(name, settings) })
    }

    /**
     * Sets the distributed lock provider used for cluster-wide rebuild
     * coordination.
     *
     * @param lockProvider the distributed lock provider
     * @since 0.1.0
     */
    fun lockProvider(lockProvider: DistributedLockProvider) {
        builder.lockProvider(lockProvider)
    }

    /**
     * Sets the invalidation handler factory, given the version generator
     * assigned to this instance.
     *
     * @param invalidationFactory factory creating the invalidation handler
     * @since 0.1.0
     */
    fun invalidation(invalidationFactory: (VersionGenerator) -> InvalidationHandler) {
        builder.invalidation(Function { versions -> invalidationFactory(versions) })
    }

    /**
     * Registers a metrics listener receiving cache-level request and latency
     * signals.
     *
     * @param listener the metrics listener
     * @since 0.1.0
     */
    fun metricsListener(listener: CacheMetricsListener) {
        builder.metricsListener(listener)
    }

    /**
     * Registers a listener notified on circuit-breaker state transitions.
     *
     * @param listener the degradation listener
     * @since 0.1.0
     */
    fun degradationListener(listener: DegradationListener) {
        builder.degradationListener(listener)
    }

    /**
     * Registers an application-facing observer of inbound invalidation
     * events, invoked after the internal flow fan-out.
     *
     * @param listener the invalidation event listener
     * @since 0.1.0
     */
    fun invalidationEventListener(listener: InvalidationEventListener) {
        builder.invalidationEventListener(listener)
    }

    /**
     * Disables singleflight (opt-out of a correct-by-default protection;
     * logged as a risk).
     *
     * @since 0.1.0
     */
    fun disableSingleflight() {
        builder.disableSingleflight()
    }

    /**
     * Disables cluster-wide rebuild coordination (opt-out of a
     * correct-by-default protection; logged as a risk).
     *
     * @since 0.1.0
     */
    fun disableDistributedCoordination() {
        builder.disableDistributedCoordination()
    }

    /**
     * Disables the L2 circuit breaker (opt-out of a correct-by-default
     * protection; logged as a risk).
     *
     * @since 0.1.0
     */
    fun disableCircuitBreaker() {
        builder.disableCircuitBreaker()
    }

    /**
     * Maximum threads serving async cache operations (the bounded async
     * executor); defaults to `max(4, availableProcessors)`. Under
     * saturation, submissions fail their stage with
     * `RejectedExecutionException` rather than growing threads.
     *
     * @since 1.2.0
     */
    fun asyncExecutorThreads(asyncExecutorThreads: Int) {
        builder.asyncExecutorThreads(asyncExecutorThreads)
    }

    internal fun resolvedDefaults(): CacheSettings? = defaultsDsl?.resolve()

    internal fun resolvedOverrides(): Map<String, CacheOverride> =
        cacheDsls.mapValues { (_, dsl) -> dsl.resolve() }

    internal fun build(): KTierCacheFactory {
        resolvedDefaults()?.let(builder::defaults)
        resolvedOverrides().forEach(builder::cache)
        return builder.build()
    }
}

/**
 * `defaults { }` block: var-properties mirroring every [CacheSettings] field,
 * initialized from `CacheSettings.defaults()`.
 *
 * @since 0.1.0
 */
@TierCacheDslMarker
class CacheSettingsDsl internal constructor() {

    private val base = CacheSettings.defaults()

    /** Maximum number of entries held in L1. See `CacheSettings.l1MaxSize`. */
    var l1MaxSize: Long = base.l1MaxSize()

    /** L1 time-to-live after write. See `CacheSettings.l1ExpireAfterWrite`. */
    var l1ExpireAfterWrite: Duration = base.l1ExpireAfterWrite()

    /** L1 time-to-live after access, or `null` to disable. See `CacheSettings.l1ExpireAfterAccess`. */
    var l1ExpireAfterAccess: Duration? = base.l1ExpireAfterAccess()

    /** L2 time-to-live. See `CacheSettings.l2Ttl`. */
    var l2Ttl: Duration = base.l2Ttl()

    /** TTL jitter amplitude in `[0, 1)`. See `CacheSettings.jitterAmplitude`. */
    var jitterAmplitude: Double = base.jitterAmplitude()

    /** Null-caching policy. See `CacheSettings.nullPolicy`. */
    var nullPolicy: NullPolicy = base.nullPolicy()

    /** Invalidation mode for this cache. See `CacheSettings.invalidationMode`. */
    var invalidationMode: InvalidationMode = base.invalidationMode()

    /** Maximum serialized payload size in bytes. See `CacheSettings.payloadCapBytes`. */
    var payloadCapBytes: Long = base.payloadCapBytes()

    /** How long stale entries remain servable. See `CacheSettings.staleTtl`. */
    var staleTtl: Duration = base.staleTtl()

    /** Whether XFetch early refresh is enabled. See `CacheSettings.xfetchEnabled`. */
    var xfetchEnabled: Boolean = base.xfetchEnabled()

    /** XFetch beta threshold. See `CacheSettings.xfetchBeta`. */
    var xfetchBeta: Duration = base.xfetchBeta()

    /** Degradation stale window. See `CacheSettings.degradationStaleTtl`. */
    var degradationStaleTtl: Duration = base.degradationStaleTtl()

    internal fun resolve(): CacheSettings = CacheSettings(
        l1MaxSize, l1ExpireAfterWrite, l1ExpireAfterAccess, l2Ttl, jitterAmplitude,
        nullPolicy, invalidationMode, payloadCapBytes, staleTtl, xfetchEnabled, xfetchBeta,
        degradationStaleTtl)
}

/**
 * `cache("name") { }` block: var-properties mirroring every [CacheOverride]
 * field; properties left `null` inherit the global defaults.
 *
 * @since 0.1.0
 */
@TierCacheDslMarker
class CacheOverrideDsl internal constructor() {

    /** Maximum number of entries held in L1, or `null` to inherit the defaults. */
    var l1MaxSize: Long? = null

    /** L1 time-to-live after write, or `null` to inherit the defaults. */
    var l1ExpireAfterWrite: Duration? = null

    /** L1 time-to-live after access, or `null` to inherit the defaults. */
    var l1ExpireAfterAccess: Duration? = null

    /** L2 time-to-live, or `null` to inherit the defaults. */
    var l2Ttl: Duration? = null

    /** TTL jitter amplitude in `[0, 1)`, or `null` to inherit the defaults. */
    var jitterAmplitude: Double? = null

    /** Null-caching policy, or `null` to inherit the defaults. */
    var nullPolicy: NullPolicy? = null

    /** Invalidation mode, or `null` to inherit the defaults. */
    var invalidationMode: InvalidationMode? = null

    /** Maximum serialized payload size in bytes, or `null` to inherit the defaults. */
    var payloadCapBytes: Long? = null

    /** How long stale entries remain servable, or `null` to inherit the defaults. */
    var staleTtl: Duration? = null

    /** Whether XFetch early refresh is enabled, or `null` to inherit the defaults. */
    var xfetchEnabled: Boolean? = null

    /** XFetch beta threshold, or `null` to inherit the defaults. */
    var xfetchBeta: Duration? = null

    /** Degradation stale window override. See `CacheSettings.degradationStaleTtl`. */
    var degradationStaleTtl: Duration? = null

    internal fun resolve(): CacheOverride {
        val override = CacheOverride()
        l1MaxSize?.let { override.l1MaxSize(it) }
        l1ExpireAfterWrite?.let { override.l1ExpireAfterWrite(it) }
        l1ExpireAfterAccess?.let { override.l1ExpireAfterAccess(it) }
        l2Ttl?.let { override.l2Ttl(it) }
        jitterAmplitude?.let { override.jitterAmplitude(it) }
        nullPolicy?.let { override.nullPolicy(it) }
        invalidationMode?.let { override.invalidationMode(it) }
        payloadCapBytes?.let { override.payloadCapBytes(it) }
        staleTtl?.let { override.staleTtl(it) }
        degradationStaleTtl?.let { override.degradationStaleTtl(it) }
        xfetchEnabled?.let { override.xfetchEnabled(it) }
        xfetchBeta?.let { override.xfetchBeta(it) }
        return override
    }
}
