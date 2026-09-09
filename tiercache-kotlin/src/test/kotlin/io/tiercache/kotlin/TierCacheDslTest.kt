package io.tiercache.kotlin

import io.tiercache.CacheConfigurationException
import io.tiercache.CacheOverride
import io.tiercache.CacheSettings
import io.tiercache.InvalidationMode
import io.tiercache.NullPolicy
import io.tiercache.TierCacheFactory
import io.tiercache.testkit.InMemoryRemoteCache
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * Configuration DSL (design D4): the DSL resolves into the same
 * [CacheSettings]/[CacheOverride] objects as the programmatic builders and
 * goes through the same fail-fast startup validation.
 */
class TierCacheDslTest {

    @Test
    fun `DSL parity with programmatic configuration`() {
        val dsl = TierCacheFactoryDsl(Dispatchers.IO)
        dsl.defaults {
            l1MaxSize = 500
            l1ExpireAfterWrite = Duration.ofMinutes(2)
            l1ExpireAfterAccess = Duration.ofSeconds(30)
            l2Ttl = Duration.ofMinutes(10)
            jitterAmplitude = 0.2
            nullPolicy = NullPolicy.allow(Duration.ofMinutes(1))
            invalidationMode = InvalidationMode.UPDATE
            payloadCapBytes = 4 * 1024
            staleTtl = Duration.ofSeconds(45)
            xfetchEnabled = true
            xfetchBeta = Duration.ofMillis(500)
        }
        dsl.cache("special") {
            l1MaxSize = 50
            l2Ttl = Duration.ofMinutes(3)
            xfetchEnabled = false
        }

        val programmaticDefaults = CacheSettings(
            500, Duration.ofMinutes(2), Duration.ofSeconds(30), Duration.ofMinutes(10), 0.2,
            NullPolicy.allow(Duration.ofMinutes(1)), InvalidationMode.UPDATE, 4 * 1024,
            Duration.ofSeconds(45), true, Duration.ofMillis(500))
        val programmaticOverride = CacheOverride()
            .l1MaxSize(50)
            .l2Ttl(Duration.ofMinutes(3))
            .xfetchEnabled(false)

        val dslDefaults = dsl.resolvedDefaults()
        val dslOverride = dsl.resolvedOverrides().getValue("special")
        assertThat(dslDefaults).isEqualTo(programmaticDefaults)
        assertThat(dslOverride.resolve(programmaticDefaults))
            .isEqualTo(programmaticOverride.resolve(programmaticDefaults))
    }

    @Test
    fun `DSL defaults inherit core defaults for unset properties`() {
        val dsl = TierCacheFactoryDsl(Dispatchers.IO)
        dsl.defaults { l1MaxSize = 123 }
        assertThat(dsl.resolvedDefaults())
            .isEqualTo(CacheSettings(123, Duration.ofMinutes(5), null, Duration.ofHours(1), 0.10,
                NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                Duration.ZERO, false, Duration.ofSeconds(1)))
    }

    @Test
    fun `invalid DSL configuration fails with the same validation error as the programmatic path`() {
        val programmaticError = runCatching {
            TierCacheFactory.builder()
                .remoteCache(InMemoryRemoteCache<String, String>())
                .defaults(CacheSettings(10_000, Duration.ofHours(2), null, Duration.ofMinutes(5), 0.10,
                    NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                .build()
        }.exceptionOrNull()!!

        val dslError = runCatching {
            tierCache {
                remoteCache(InMemoryRemoteCache<String, String>())
                defaults {
                    l1ExpireAfterWrite = Duration.ofHours(2)
                    l2Ttl = Duration.ofMinutes(5)
                }
            }
        }.exceptionOrNull()!!

        assertThat(dslError).isInstanceOf(CacheConfigurationException::class.java)
        assertThat(dslError.message).isEqualTo(programmaticError.message)
    }

    @Test
    fun `DSL built factory serves caches end to end`() = runTest {
        tierCache {
            remoteCache(InMemoryRemoteCache<String, String>())
            defaults { l1MaxSize = 100 }
            cache("a") { l2Ttl = Duration.ofMinutes(30) }
        }.use { factory ->
            val cache = factory.getCache<String, String>("a")
            cache.put("k", "v")
            assertThat(cache.get("k")).isEqualTo("v")
        }
    }
}
