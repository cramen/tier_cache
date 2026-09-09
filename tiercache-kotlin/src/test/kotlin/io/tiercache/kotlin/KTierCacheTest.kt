package io.tiercache.kotlin

import io.tiercache.CacheOverride
import io.tiercache.LookupResult
import io.tiercache.NullPolicy
import io.tiercache.spi.RemoteCache
import io.tiercache.spi.StoredEntry
import io.tiercache.testkit.CountingRemoteCache
import io.tiercache.testkit.InMemoryRemoteCache
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * Facade semantics: every suspend operation must preserve the behavior of
 * the underlying blocking `TierCache` over an in-memory L2.
 */
class KTierCacheTest {

    private fun newFactory(
        remote: RemoteCache<String, String> = CountingRemoteCache(),
        configure: KTierCacheFactory.Builder.() -> Unit = {},
    ): KTierCacheFactory = KTierCacheFactory.builder()
        .remoteCache(remote)
        .apply(configure)
        .build()

    @Test
    fun `get returns value for present key and null for miss`() = runTest {
        newFactory().use { factory ->
            val cache = factory.getCache<String, String>("c")
            cache.put("k", "v")
            assertThat(cache.get("k")).isEqualTo("v")
            assertThat(cache.get("absent")).isNull()
        }
    }

    @Test
    fun `l2 hit warms l1 so the next lookup is served locally`() = runTest {
        val remote = CountingRemoteCache<String, String>()
        remote.put("k", StoredEntry.ofValue("v"), Duration.ofHours(1))
        newFactory(remote).use { factory ->
            val cache = factory.getCache<String, String>("c")
            assertThat(cache.get("k")).isEqualTo("v")
            assertThat(cache.get("k")).isEqualTo("v")
            assertThat(remote.gets.get()).isEqualTo(1)
        }
    }

    @Test
    fun `lookup distinguishes hit cached-null and miss`() = runTest {
        newFactory().use { factory ->
            val cache = factory.getCache<String, String>("c")
            assertThat(cache.lookup("absent")).isEqualTo(LookupResult.miss<String>())
            cache.put("k", "v")
            assertThat(cache.lookup("k")).isEqualTo(LookupResult.hit("v"))
        }
    }

    @Test
    fun `null marker under allow policy suppresses the loader`() = runTest {
        val allowNulls: KTierCacheFactory.Builder.() -> Unit = {
            cache("nulls", CacheOverride().nullPolicy(NullPolicy.allow(Duration.ofMinutes(1))))
        }
        newFactory(configure = allowNulls).use { factory ->
            val cache = factory.getCache<String, String>("nulls")
            val loads = AtomicInteger()
            assertThat(cache.getOrCompute("k") { loads.incrementAndGet(); null }).isNull()
            assertThat(cache.lookup("k")).isEqualTo(LookupResult.cachedNull<String>())
            assertThat(cache.getOrCompute("k") { loads.incrementAndGet(); "x" }).isNull()
            assertThat(loads.get()).isEqualTo(1)
            assertThat(cache.get("k")).isNull()
        }
    }

    @Test
    fun `putNull is a no-op under the deny policy`() = runTest {
        newFactory().use { factory ->
            val cache = factory.getCache<String, String>("c")
            cache.putNull("k")
            assertThat(cache.lookup("k")).isEqualTo(LookupResult.miss<String>())
        }
    }

    @Test
    fun `putIfAbsent stores only the winning value`() = runTest {
        newFactory().use { factory ->
            val cache = factory.getCache<String, String>("c")
            assertThat(cache.putIfAbsent("k", "v1")).isTrue()
            assertThat(cache.putIfAbsent("k", "v2")).isFalse()
            assertThat(cache.get("k")).isEqualTo("v1")
        }
    }

    @Test
    fun `evict removes the key from both levels`() = runTest {
        val remote = CountingRemoteCache<String, String>()
        newFactory(remote).use { factory ->
            val cache = factory.getCache<String, String>("c")
            cache.put("k", "v")
            cache.evict("k")
            assertThat(cache.lookup("k")).isEqualTo(LookupResult.miss<String>())
            assertThat(remote.get("k")).isNull()
        }
    }

    @Test
    fun `evictAll removes every entry`() = runTest {
        newFactory().use { factory ->
            val cache = factory.getCache<String, String>("c")
            cache.put("k1", "v1")
            cache.put("k2", "v2")
            cache.evictAll()
            assertThat(cache.get("k1")).isNull()
            assertThat(cache.get("k2")).isNull()
        }
    }

    @Test
    fun `evictAll keys removes only the listed keys`() = runTest {
        newFactory().use { factory ->
            val cache = factory.getCache<String, String>("c")
            cache.put("k1", "v1")
            cache.put("k2", "v2")
            cache.evictAll(listOf("k1"))
            assertThat(cache.get("k1")).isNull()
            assertThat(cache.get("k2")).isEqualTo("v2")
        }
    }

    @Test
    fun `tagged put and evictByTag`() = runTest {
        // Tag indexing lives in InMemoryRemoteCache; CountingRemoteCache keeps
        // the SPI defaults (no tag index), so use the in-memory cache directly.
        newFactory(InMemoryRemoteCache()).use { factory ->
            val cache = factory.getCache<String, String>("c")
            cache.put("k1", "v1", "tag")
            cache.put("k2", "v2")
            cache.evictByTag("tag")
            assertThat(cache.get("k1")).isNull()
            assertThat(cache.get("k2")).isEqualTo("v2")
        }
    }

    @Test
    fun `asKotlin extension wraps a blocking cache`() = runTest {
        newFactory().use { factory ->
            val cache = factory.delegate.getCache<String, String>("c").asKotlin()
            cache.put("k", "v")
            assertThat(cache.get("k")).isEqualTo("v")
        }
    }

    @Test
    fun `operations run on the configured dispatcher`() = runTest {
        val executor = Executors.newSingleThreadExecutor { r -> Thread(r, "cache-io") }
        val dispatcher = executor.asCoroutineDispatcher()
        try {
            newFactory().use { factory ->
                val cache = KTierCache(factory.delegate.getCache<String, String>("c"), dispatcher)
                val loaderThread = cache.getOrCompute("k") { Thread.currentThread().name }
                assertThat(loaderThread).startsWith("cache-io")
            }
        } finally {
            dispatcher.close()
        }
    }
}
