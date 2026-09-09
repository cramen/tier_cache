package io.tiercache.kotlin

import io.tiercache.InvalidationMessage
import io.tiercache.Version
import io.tiercache.VersionGenerator
import io.tiercache.spi.InvalidationEventListener
import io.tiercache.spi.InvalidationHandler
import io.tiercache.spi.InvalidationTarget
import io.tiercache.testkit.InMemoryInvalidationTransport
import io.tiercache.testkit.InMemoryRemoteCache
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong

/**
 * Invalidation Flow (design D3): collectors receive inbound events in order,
 * cancellation unregisters the listener, and slow collectors never block
 * producers — overflow is dropped for that collector and counted.
 */
class KTierCacheInvalidationFlowTest {

    @Test
    fun `collector receives events in order`() = runTest {
        Fixture().use { f ->
            f.factory.getCache<String, String>("events")
            val received = ConcurrentLinkedQueue<InvalidationMessage>()
            val job = launch(Dispatchers.Default) {
                f.factory.invalidationEvents("events").take(3).collect { received.add(it) }
            }
            awaitTrue { f.factory.collectorCount("events") == 1 }
            f.publish("events", "k1")
            f.publish("events", "k2")
            f.publish("events", "k3")
            awaitTrue { received.size == 3 }
            job.join()
            assertThat(received.map { it.key() }).containsExactly("k1", "k2", "k3")
        }
    }

    @Test
    fun `cancellation unregisters the listener and stops delivery`() = runTest {
        Fixture().use { f ->
            f.factory.getCache<String, String>("events")
            val received = ConcurrentLinkedQueue<InvalidationMessage>()
            val job = launch(Dispatchers.Default) {
                f.factory.invalidationEvents("events").collect { received.add(it) }
            }
            awaitTrue { f.factory.collectorCount("events") == 1 }
            f.publish("events", "k1")
            awaitTrue { received.size == 1 }
            job.cancelAndJoin()
            awaitTrue { f.factory.collectorCount("events") == 0 }
            f.publish("events", "k2")
            assertThat(received).hasSize(1)
        }
    }

    @Test
    fun `slow collector does not block producers and drops are counted`() = runTest {
        Fixture().use { f ->
            f.factory.getCache<String, String>("events")
            val received = ConcurrentLinkedQueue<InvalidationMessage>()
            val drops = AtomicLong()
            val gate = CompletableDeferred<Unit>()
            val job = launch(Dispatchers.Default) {
                f.factory.invalidationEvents("events", bufferCapacity = 8) { drops.incrementAndGet() }
                    .collect {
                        gate.await()
                        received.add(it)
                    }
            }
            awaitTrue { f.factory.collectorCount("events") == 1 }
            val total = 500
            // Synchronous return proves the producer is never blocked by the parked collector.
            repeat(total) { i -> f.publish("events", "k$i") }
            gate.complete(Unit)
            awaitTrue { received.size.toLong() + drops.get() == total.toLong() }
            job.cancelAndJoin()
            assertThat(drops.get()).isGreaterThan(0)
            assertThat(received.size.toLong() + drops.get()).isEqualTo(total.toLong())
        }
    }

    @Test
    fun `own writes do not produce flow events`() = runTest {
        Fixture().use { f ->
            val cache = f.factory.getCache<String, String>("events")
            val received = ConcurrentLinkedQueue<InvalidationMessage>()
            val job = launch(Dispatchers.Default) {
                f.factory.invalidationEvents("events").collect { received.add(it) }
            }
            awaitTrue { f.factory.collectorCount("events") == 1 }
            cache.put("mine", "v") // own-origin event: filtered by the engine, never notified
            f.publish("events", "theirs")
            awaitTrue { received.size == 1 }
            job.cancelAndJoin()
            assertThat(received.single().key()).isEqualTo("theirs")
        }
    }

    /**
     * Two transports on one hub: the factory's engine subscribes through one,
     * a raw publisher drives events through the other (different origin).
     */
    private class Fixture : AutoCloseable {
        private val hub = InMemoryInvalidationTransport.Hub()
        private val factoryTransport = InMemoryInvalidationTransport(hub)
        private val publisherTransport = InMemoryInvalidationTransport(hub)
        private val publisherVersions = VersionGenerator()

        val factory: KTierCacheFactory = KTierCacheFactory.builder()
            .remoteCache(InMemoryRemoteCache<String, String>())
            .invalidation { versions -> TestInvalidationHandler(factoryTransport, versions.instanceId()) }
            .build()

        fun publish(cache: String, key: String) {
            publisherTransport.publish(
                InvalidationMessage(cache, key, publisherVersions.next(),
                    publisherVersions.instanceId(), InvalidationMessage.Type.INVALIDATE))
        }

        override fun close() {
            factory.close()
            publisherTransport.close()
        }
    }

    /**
     * Minimal in-test invalidation engine over [InMemoryInvalidationTransport],
     * mirroring the real one's receive path: ignore own origin, apply to the
     * registered target, then notify the event listener.
     */
    private class TestInvalidationHandler(
        private val transport: InMemoryInvalidationTransport,
        private val originInstanceId: UUID,
    ) : InvalidationHandler {

        @Volatile
        private var eventListener = InvalidationEventListener.NOOP
        private val targets = ConcurrentHashMap<String, InvalidationTarget>()
        private val subscriptions = CopyOnWriteArrayList<AutoCloseable>()

        override fun onLocalWrite(cache: String, key: Any?, version: Version, type: InvalidationMessage.Type) {
            transport.publish(InvalidationMessage(cache, key, version, originInstanceId, type))
        }

        override fun registerTarget(cache: String, target: InvalidationTarget) {
            targets[cache] = target
            subscriptions += transport.subscribe(cache) { message ->
                if (message.originInstanceId() == originInstanceId) {
                    return@subscribe
                }
                val registered = targets[message.cache()] ?: return@subscribe
                when (message.type()) {
                    InvalidationMessage.Type.INVALIDATE ->
                        registered.evictL1IfNewer(message.key(), message.version())

                    InvalidationMessage.Type.UPDATE ->
                        registered.applyUpdateL1(message.key(), message.payload(), message.version())

                    InvalidationMessage.Type.EVICT_ALL -> registered.evictAllL1()
                }
                eventListener.onEvent(message.cache(), message)
            }
        }

        override fun setEventListener(listener: InvalidationEventListener) {
            eventListener = listener
        }

        override fun close() {
            subscriptions.forEach { it.close() }
            transport.close()
        }
    }

    private fun awaitTrue(timeoutMillis: Long = 10_000, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (!condition()) {
            if (System.currentTimeMillis() > deadline) {
                throw AssertionError("condition not met within ${timeoutMillis}ms")
            }
            Thread.sleep(5)
        }
    }
}
