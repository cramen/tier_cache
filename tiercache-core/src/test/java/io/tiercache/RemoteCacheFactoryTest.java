package io.tiercache;

import io.tiercache.spi.DistributedLock;
import io.tiercache.testkit.InMemoryLockProvider;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: per-cache L2 namespacing — the factory form of the L2 provider
 * (one L2 instance per cache name) and cache-namespaced rebuild locks.
 */
class RemoteCacheFactoryTest {

    @Test
    void factoryIsInvokedOncePerCacheName() {
        AtomicInteger calls = new AtomicInteger();
        try (TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCacheFactory(name -> {
                    calls.incrementAndGet();
                    return new InMemoryRemoteCache<>();
                })
                .build()) {
            factory.getCache("a");
            factory.getCache("a");
            factory.getCache("b");
            assertEquals(2, calls.get(), "one L2 per cache name, memoized like the caches themselves");
        }
    }

    @Test
    void perNameL2IsolatesTheSameKeyAcrossCaches() {
        Map<String, InMemoryRemoteCache<Object, Object>> l2s = new ConcurrentHashMap<>();
        try (TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCacheFactory(name -> l2s.computeIfAbsent(name, n -> new InMemoryRemoteCache<>()))
                .build()) {
            TierCache<String, String> a = factory.getCache("a");
            TierCache<String, String> b = factory.getCache("b");

            a.put("k", "va");
            b.put("k", "vb");

            assertEquals("va", a.get("k"));
            assertEquals("vb", b.get("k"));
            assertEquals(2, l2s.size(), "each cache must store through its own L2 instance");
            assertEquals("va", l2s.get("a").get("k").value());
            assertEquals("vb", l2s.get("b").get("k").value());

            b.evict("k");
            assertNotNull(l2s.get("a").get("k"), "evicting in one cache must not touch the other's L2");
        }
    }

    @Test
    void remoteCacheAndFactoryAreMutuallyExclusive() {
        TierCacheFactory.Builder builder = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .remoteCacheFactory(InMemoryRemoteCache.perName());
        CacheConfigurationException e = assertThrows(CacheConfigurationException.class, builder::build);
        assertTrue(e.getMessage().contains("mutually exclusive"), e.getMessage());
    }

    @Test
    void factoryFormSatisfiesTheL2Requirement() {
        assertThrows(NullPointerException.class, () -> TierCacheFactory.builder().build(),
                "neither remoteCache nor remoteCacheFactory still fails fast");
        try (TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCacheFactory(InMemoryRemoteCache.perName())
                .build()) {
            assertNotNull(factory.getCache("c"));
        }
    }

    @Test
    void nullFromFactoryFailsFast() {
        try (TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCacheFactory(name -> null)
                .build()) {
            assertThrows(NullPointerException.class, () -> factory.getCache("c"));
        }
    }

    @Test
    void factoryFormWorksWithBreakerDisabled() {
        try (TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCacheFactory(InMemoryRemoteCache.perName())
                .disableCircuitBreaker()
                .build()) {
            TierCache<String, String> cache = factory.getCache("c");
            cache.put("k", "v");
            assertEquals("v", cache.get("k"));
        }
    }

    @Test
    void rebuildLocksAreNamespacedPerCache() {
        InMemoryLockProvider locks = new InMemoryLockProvider();
        try (TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCacheFactory(InMemoryRemoteCache.perName())
                .lockProvider(locks)
                .build()) {
            TierCache<String, String> b = factory.getCache("b");

            DistributedLock heldByA = locks.tryLock("a:k", Duration.ofMinutes(1));
            assertNotNull(heldByA);
            try {
                // Cache b coordinates its rebuild under "b:k": a's held lock
                // on the same key must not push b into the loser-wait path
                // (which polls L2 for a value that never comes, for seconds).
                long started = System.nanoTime();
                assertEquals("vb", b.getOrCompute("k", key -> "vb"));
                long elapsedMillis = Duration.ofNanos(System.nanoTime() - started).toMillis();
                assertTrue(elapsedMillis < 2_000,
                        "b must not wait on a's rebuild lock; took " + elapsedMillis + " ms");

                DistributedLock heldByB = locks.tryLock("b:k", Duration.ofMinutes(1));
                assertNotNull(heldByB, "a's and b's rebuild locks for one key are independent");
                heldByB.release();
            } finally {
                heldByA.release();
            }
        }
    }
}
