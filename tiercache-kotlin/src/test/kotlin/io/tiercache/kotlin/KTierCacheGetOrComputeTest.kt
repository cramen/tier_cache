package io.tiercache.kotlin

import io.tiercache.LookupResult
import io.tiercache.testkit.InMemoryRemoteCache
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors as JExecutors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Suspending loader (design D2): coalescing stays in core's singleflight,
 * loader failures reach every coalesced caller, and the caller dispatcher
 * never performs the blocking call.
 */
class KTierCacheGetOrComputeTest {

    private fun newFactory(): KTierCacheFactory = KTierCacheFactory.builder()
        .remoteCache(InMemoryRemoteCache<String, String>())
        .build()

    @Test
    fun `concurrent misses coalesce onto one loader execution`() = runTest {
        newFactory().use { factory ->
            val cache = factory.getCache<String, String>("c")
            val loads = AtomicInteger()
            val gate = CountDownLatch(1)
            val callers = (1..32).map {
                async(Dispatchers.Default) {
                    cache.getOrCompute("k") {
                        loads.incrementAndGet()
                        gate.await(10, TimeUnit.SECONDS)
                        "v"
                    }
                }
            }
            Thread.sleep(100) // give followers real time to park on the coalesced future
            gate.countDown()
            val results = callers.awaitAll()
            assertThat(results).allMatch { it == "v" }
            assertThat(loads.get()).isEqualTo(1)
            assertThat(cache.get("k")).isEqualTo("v")
        }
    }

    @Test
    fun `loader failure reaches all callers and stores nothing`() = runTest {
        newFactory().use { factory ->
            val cache = factory.getCache<String, String>("c")
            val gate = CountDownLatch(1)
            val callers = (1..16).map {
                async(Dispatchers.Default) {
                    runCatching {
                        cache.getOrCompute("k") {
                            gate.await(10, TimeUnit.SECONDS)
                            throw IllegalStateException("boom")
                        }
                    }
                }
            }
            Thread.sleep(100)
            gate.countDown()
            val results = callers.awaitAll()
            assertThat(results).allMatch { it.isFailure }
            assertThat(results.map { unwrap(it.exceptionOrNull()!!) })
                .allMatch { it is IllegalStateException && it.message == "boom" }
            assertThat(cache.lookup("k")).isEqualTo(LookupResult.miss<String>())
        }
    }

    @Test
    fun `blocking call never runs on the caller dispatcher`() = runTest {
        val executor = JExecutors.newSingleThreadExecutor { r -> Thread(r, "caller-dispatcher") }
        val callerDispatcher = executor.asCoroutineDispatcher()
        try {
            newFactory().use { factory ->
                val cache = factory.getCache<String, String>("c")
                val loaderThread = CompletableFuture<String>()
                val gate = CountDownLatch(1)
                withContext(callerDispatcher) {
                    val operation = async {
                        cache.getOrCompute("k") {
                            loaderThread.complete(Thread.currentThread().name)
                            gate.await(10, TimeUnit.SECONDS)
                            "v"
                        }
                    }
                    // While the cache operation above is in flight, another
                    // coroutine on the same single-threaded dispatcher proceeds.
                    val progressed = CompletableDeferred<Boolean>()
                    launch { progressed.complete(true) }
                    assertThat(progressed.await()).isTrue()
                    gate.countDown()
                    assertThat(operation.await()).isEqualTo("v")
                }
                assertThat(loaderThread.get()).isNotEqualTo("caller-dispatcher")
            }
        } finally {
            callerDispatcher.close()
        }
    }

    private fun unwrap(t: Throwable): Throwable =
        if (t is CompletionException && t.cause != null) unwrap(t.cause!!) else t
}
