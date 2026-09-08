package io.tiercache;

import io.tiercache.internal.DefaultTierCache;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.CountingRemoteCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: core-read-path — cascading read with L1 warm-up and
 * per-instance singleflight.
 */
class DefaultTierCacheTest {

    private CountingLocalCache<String, String> l1;
    private CountingRemoteCache<String, String> l2;
    private TierCache<String, String> cache;

    @BeforeEach
    void setUp() {
        l1 = new CountingLocalCache<>();
        l2 = new CountingRemoteCache<>();
        cache = new DefaultTierCache<>(l1, l2, CacheSettings.defaults(), true);
    }

    // --- Scenario: L1 hit short-circuits ---

    @Test
    void l1HitShortCircuits() {
        cache.put("k", "v");
        int l2GetsBefore = l2.gets.get();

        assertEquals("v", cache.get("k"));
        assertEquals(l2GetsBefore, l2.gets.get(), "L1 hit must not touch L2");
    }

    // --- Scenario: L2 hit warms L1 ---

    @Test
    void l2HitWarmsL1() {
        l2.put("k", io.tiercache.spi.StoredEntry.ofValue("v"), Duration.ofMinutes(1));

        assertEquals("v", cache.get("k"));
        assertEquals(1, l1.puts.get(), "L2 hit must warm L1");

        int l2GetsBefore = l2.gets.get();
        assertEquals("v", cache.get("k"));
        assertEquals(l2GetsBefore, l2.gets.get(), "subsequent lookup must be served from L1");
    }

    // --- Scenario: loader miss populates both levels ---

    @Test
    void loaderMissPopulatesBothLevels() {
        String value = cache.getOrCompute("k", key -> "loaded");

        assertEquals("loaded", value);
        assertEquals(1, l2.puts.get(), "loader result must be stored in L2");
        assertEquals(1, l1.puts.get(), "loader result must be stored in L1");
    }

    // --- Scenario: loader absence propagates as a miss ---

    @Test
    void loaderAbsencePropagatesAsMiss() {
        assertNull(cache.getOrCompute("k", key -> null));
        assertEquals(0, l1.puts.get(), "nothing may be stored on a miss");
        assertEquals(0, l2.puts.get(), "nothing may be stored on a miss");
    }

    // --- Write/evict behavior ---

    @Test
    void putWritesBothLevels() {
        cache.put("k", "v");
        assertEquals(1, l2.puts.get());
        assertEquals(1, l1.puts.get());
    }

    @Test
    void evictRemovesFromBothLevels() {
        cache.put("k", "v");
        cache.evict("k");
        assertEquals(1, l2.evicts.get());
        assertEquals(1, l1.evicts.get());
        assertNull(cache.get("k"));
    }

    // --- Requirement: per-instance request coalescing ---

    @Test
    void concurrentReadersShareOneLoaderExecution() throws Exception {
        int threads = 32;
        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch loaderRelease = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<String>> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            results.add(pool.submit(() -> {
                start.await();
                return cache.getOrCompute("hot", key -> {
                    loaderCalls.incrementAndGet();
                    loaderEntered.countDown();
                    awaitQuietly(loaderRelease);
                    return "v";
                });
            }));
        }
        start.countDown();
        assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
        // Give the herd a chance to pile onto the same key.
        Thread.sleep(200);
        loaderRelease.countDown();

        for (Future<String> f : results) {
            assertEquals("v", f.get(5, TimeUnit.SECONDS));
        }
        pool.shutdown();
        assertEquals(1, loaderCalls.get(), "singleflight: exactly one loader execution");
    }

    @Test
    void differentKeysLoadIndependently() throws Exception {
        CountDownLatch bothEntered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<String> a = pool.submit(() -> cache.getOrCompute("a", key -> {
            bothEntered.countDown();
            awaitQuietly(release);
            return "va";
        }));
        Future<String> b = pool.submit(() -> cache.getOrCompute("b", key -> {
            bothEntered.countDown();
            awaitQuietly(release);
            return "vb";
        }));

        // Both loaders must be in flight simultaneously: neither caller waits
        // for the other's key.
        assertTrue(bothEntered.await(5, TimeUnit.SECONDS),
                "different keys must load independently");
        release.countDown();
        assertEquals("va", a.get(5, TimeUnit.SECONDS));
        assertEquals("vb", b.get(5, TimeUnit.SECONDS));
        pool.shutdown();
    }

    @Test
    void loaderExceptionPropagatesAndClearsInflight() {
        AtomicInteger calls = new AtomicInteger();
        for (int i = 0; i < 2; i++) {
            try {
                cache.getOrCompute("k", key -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("boom");
                });
            } catch (RuntimeException expected) {
                // fall through
            }
        }
        // A failed load must not poison the in-flight map: each attempt retries.
        assertEquals(2, calls.get());
    }

    @Test
    void singleflightCanBeExplicitlyDisabled() {
        TierCache<String, String> unprotected =
                new DefaultTierCache<>(new CountingLocalCache<>(), new CountingRemoteCache<>(),
                        CacheSettings.defaults(), false);
        AtomicInteger calls = new AtomicInteger();
        unprotected.getOrCompute("k", key -> {
            calls.incrementAndGet();
            return "v";
        });
        assertEquals(1, calls.get());
        assertEquals("v", unprotected.get("k"));
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
