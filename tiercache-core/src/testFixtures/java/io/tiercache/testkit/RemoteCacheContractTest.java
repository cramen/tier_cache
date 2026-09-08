package io.tiercache.testkit;

import io.tiercache.spi.RemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract test for {@link RemoteCache} implementations. Subclass and
 * implement {@link #newCache()} to run the suite against an implementation.
 */
public abstract class RemoteCacheContractTest {

    protected abstract RemoteCache<String, String> newCache();

    @Test
    void getReturnsNullWhenAbsent() {
        assertNull(newCache().get("missing"));
    }

    @Test
    void putThenGetReturnsValue() {
        RemoteCache<String, String> cache = newCache();
        cache.put("k", "v", Duration.ofMinutes(1));
        assertEquals("v", cache.get("k"));
    }

    @Test
    void evictRemovesValue() {
        RemoteCache<String, String> cache = newCache();
        cache.put("k", "v", Duration.ofMinutes(1));
        cache.evict("k");
        assertNull(cache.get("k"));
    }

    @Test
    void entryExpiresAfterTtl() throws InterruptedException {
        RemoteCache<String, String> cache = newCache();
        cache.put("k", "v", Duration.ofMillis(50));
        Thread.sleep(150);
        assertNull(cache.get("k"));
    }

    @Test
    void entriesOfOneCacheMayHaveDifferentTtls() throws InterruptedException {
        // F-06: per-entry TTL in L2.
        RemoteCache<String, String> cache = newCache();
        cache.put("short", "v1", Duration.ofMillis(50));
        cache.put("long", "v2", Duration.ofMinutes(1));
        Thread.sleep(150);
        assertNull(cache.get("short"));
        assertEquals("v2", cache.get("long"));
    }

    @Test
    void setIfAbsentCreatesEntryWhenAbsent() {
        RemoteCache<String, String> cache = newCache();
        assertTrue(cache.setIfAbsent("k", "v", Duration.ofMinutes(1)));
        assertEquals("v", cache.get("k"));
    }

    @Test
    void setIfAbsentLosesAndDoesNotOverwriteWhenPresent() {
        RemoteCache<String, String> cache = newCache();
        cache.put("k", "original", Duration.ofMinutes(1));
        assertFalse(cache.setIfAbsent("k", "intruder", Duration.ofMinutes(1)));
        assertEquals("original", cache.get("k"));
    }

    @Test
    void setIfAbsentSucceedsAfterExpiry() throws InterruptedException {
        RemoteCache<String, String> cache = newCache();
        cache.put("k", "old", Duration.ofMillis(50));
        Thread.sleep(150);
        assertTrue(cache.setIfAbsent("k", "new", Duration.ofMinutes(1)));
        assertEquals("new", cache.get("k"));
    }

    @Test
    void concurrentSetIfAbsentHasSingleWinner() throws Exception {
        RemoteCache<String, String> cache = newCache();
        int threads = 16;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        var start = new java.util.concurrent.CountDownLatch(1);
        var wins = new java.util.concurrent.atomic.AtomicInteger();
        var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
        for (int i = 0; i < threads; i++) {
            String candidate = "v" + i;
            futures.add(pool.submit(() -> {
                start.await();
                if (cache.setIfAbsent("k", candidate, Duration.ofMinutes(1))) {
                    wins.incrementAndGet();
                }
                return null;
            }));
        }
        start.countDown();
        for (var f : futures) {
            f.get(5, java.util.concurrent.TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertEquals(1, wins.get(), "exactly one concurrent setIfAbsent may win");
    }
}
