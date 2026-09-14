package io.tiercache.kotlin

import io.tiercache.LookupResult
import io.tiercache.testkit.InMemoryRemoteCache
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.withContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors as JExecutors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Suspending loader (design D3): coalescing stays in core's singleflight,
 * loader failures reach every coalesced caller unwrapped, and awaiting a
 * cache call suspends the coroutine without parking an IO worker.
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
    fun `loader failure reaches all callers unwrapped and stores nothing`() = runTest {
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
            assertThat(results.map { it.exceptionOrNull()!! })
                .allMatch { it is IllegalStateException && it.message == "boom" }
            assertThat(cache.lookup("k")).isEqualTo(LookupResult.miss<String>())
        }
    }

    @Test
    fun `suspension needs no IO dispatcher worker`() = runTest {
        val executor = JExecutors.newSingleThreadExecutor { r -> Thread(r, "caller-dispatcher") }
        val callerDispatcher = executor.asCoroutineDispatcher()
        try {
            newFactory().use { factory ->
                val cache = factory.getCache<String, String>("c")
                val loaderThread = CompletableFuture<String>()
                val release = CompletableDeferred<Unit>()
                withContext(callerDispatcher) {
                    val operation = async {
                        cache.getOrCompute("k") {
                            loaderThread.complete(Thread.currentThread().name)
                            release.await()
                            "v"
                        }
                    }
                    // While the cache operation above is in flight, another
                    // coroutine on the same single-threaded dispatcher
                    // proceeds: the awaiting coroutine suspends instead of
                    // blocking, and no Dispatchers.IO worker is involved.
                    val progressed = CompletableDeferred<Boolean>()
                    launch { progressed.complete(true) }
                    assertThat(progressed.await()).isTrue()
                    release.complete(Unit)
                    assertThat(operation.await()).isEqualTo("v")
                }
                // The suspending loader runs as a child of the calling scope —
                // on the caller's own dispatcher, not an IO worker.
                assertThat(loaderThread.get()).startsWith("caller-dispatcher")
            }
        } finally {
            callerDispatcher.close()
        }
    }

    @Test
    // Generous timeout: runTest's default cancels the whole test on loaded CI
    // runners (observed flake: JobCancellationException at the test line).
    fun `cancelling the awaiting coroutine cancels the loader coroutine`() = runTest(timeout = 120.seconds) {
        newFactory().use { factory ->
            val cache = factory.getCache<String, String>("c")
            val loaderStarted = CompletableDeferred<Unit>()
            val loaderFinished = CompletableDeferred<Unit>()
            val job = launch(Dispatchers.Default) {
                runCatching {
                    cache.getOrCompute("k") {
                        loaderStarted.complete(Unit)
                        try {
                            awaitCancellation()
                        } finally {
                            loaderFinished.complete(Unit)
                        }
                    }
                }
            }
            loaderStarted.await()
            // cancelAndJoin also waits for the loader child coroutine: it is
            // cancelled with the caller, not leaked.
            job.cancelAndJoin()
            assertThat(loaderFinished.isCompleted).isTrue()
            assertThat(cache.lookup("k")).isEqualTo(LookupResult.miss<String>())
            assertThat(cache.getOrCompute("k") { "v" }).isEqualTo("v")
        }
    }
}
