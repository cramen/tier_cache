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
 */
fun tierCache(
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
    block: TierCacheFactoryDsl.() -> Unit,
): KTierCacheFactory = TierCacheFactoryDsl(dispatcher).apply(block).build()

/**
 * Root block of the `tierCache { }` DSL: global defaults, named per-cache
 * overrides, and pass-throughs for the builder's infrastructure setters.
 */
@TierCacheDslMarker
class TierCacheFactoryDsl internal constructor(dispatcher: CoroutineDispatcher) {

    private val builder = KTierCacheFactory.builder(dispatcher)
    private var defaultsDsl: CacheSettingsDsl? = null
    private val cacheDsls = LinkedHashMap<String, CacheOverrideDsl>()

    /** Global defaults; unset properties keep `CacheSettings.defaults()` values. */
    fun defaults(block: CacheSettingsDsl.() -> Unit) {
        defaultsDsl = CacheSettingsDsl().apply(block)
    }

    /** Per-cache overrides; unset properties inherit the global defaults. */
    fun cache(name: String, block: CacheOverrideDsl.() -> Unit) {
        cacheDsls[name] = CacheOverrideDsl().apply(block)
    }

    /** The L2 implementation shared by all caches. Required. */
    fun remoteCache(remoteCache: RemoteCache<*, *>) {
        builder.remoteCache(remoteCache)
    }

    fun localCacheFactory(factory: (String, CacheSettings) -> LocalCache<*, *>) {
        builder.localCacheFactory(BiFunction { name, settings -> factory(name, settings) })
    }

    fun lockProvider(lockProvider: DistributedLockProvider) {
        builder.lockProvider(lockProvider)
    }

    fun invalidation(invalidationFactory: (VersionGenerator) -> InvalidationHandler) {
        builder.invalidation(Function { versions -> invalidationFactory(versions) })
    }

    fun metricsListener(listener: CacheMetricsListener) {
        builder.metricsListener(listener)
    }

    fun degradationListener(listener: DegradationListener) {
        builder.degradationListener(listener)
    }

    fun invalidationEventListener(listener: InvalidationEventListener) {
        builder.invalidationEventListener(listener)
    }

    fun disableSingleflight() {
        builder.disableSingleflight()
    }

    fun disableDistributedCoordination() {
        builder.disableDistributedCoordination()
    }

    fun disableCircuitBreaker() {
        builder.disableCircuitBreaker()
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
 */
@TierCacheDslMarker
class CacheSettingsDsl internal constructor() {

    private val base = CacheSettings.defaults()

    var l1MaxSize: Long = base.l1MaxSize()
    var l1ExpireAfterWrite: Duration = base.l1ExpireAfterWrite()
    var l1ExpireAfterAccess: Duration? = base.l1ExpireAfterAccess()
    var l2Ttl: Duration = base.l2Ttl()
    var jitterAmplitude: Double = base.jitterAmplitude()
    var nullPolicy: NullPolicy = base.nullPolicy()
    var invalidationMode: InvalidationMode = base.invalidationMode()
    var payloadCapBytes: Long = base.payloadCapBytes()
    var staleTtl: Duration = base.staleTtl()
    var xfetchEnabled: Boolean = base.xfetchEnabled()
    var xfetchBeta: Duration = base.xfetchBeta()

    internal fun resolve(): CacheSettings = CacheSettings(
        l1MaxSize, l1ExpireAfterWrite, l1ExpireAfterAccess, l2Ttl, jitterAmplitude,
        nullPolicy, invalidationMode, payloadCapBytes, staleTtl, xfetchEnabled, xfetchBeta)
}

/**
 * `cache("name") { }` block: var-properties mirroring every [CacheOverride]
 * field; properties left `null` inherit the global defaults.
 */
@TierCacheDslMarker
class CacheOverrideDsl internal constructor() {

    var l1MaxSize: Long? = null
    var l1ExpireAfterWrite: Duration? = null
    var l1ExpireAfterAccess: Duration? = null
    var l2Ttl: Duration? = null
    var jitterAmplitude: Double? = null
    var nullPolicy: NullPolicy? = null
    var invalidationMode: InvalidationMode? = null
    var payloadCapBytes: Long? = null
    var staleTtl: Duration? = null
    var xfetchEnabled: Boolean? = null
    var xfetchBeta: Duration? = null

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
        xfetchEnabled?.let { override.xfetchEnabled(it) }
        xfetchBeta?.let { override.xfetchBeta(it) }
        return override
    }
}
