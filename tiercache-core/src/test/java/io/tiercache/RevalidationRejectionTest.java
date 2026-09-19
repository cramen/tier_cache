package io.tiercache;

import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression: a revalidation rejected by the saturated background pool must
 * release its in-flight claim — before the fix, the discarded task left the
 * claim in the shared singleflight map and the next miss-read of that key
 * joined a never-completing future and hung (reviewer-reproduced in 1.2.0).
 */
class RevalidationRejectionTest {

    @Test
    void rejectedRevalidationNeverHangsAReader() throws Exception {
        CacheSettings stale = new CacheSettings(10_000, Duration.ofMillis(100), null,
                Duration.ofMillis(200), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE,
                64 * 1024, Duration.ofSeconds(30), false, Duration.ofSeconds(1));
        TierCacheFactory factory = TierCacheFactory.builder()
                .defaults(stale)
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        int keys = 1003; // 2 running + 1000 queued + exactly 1 rejected
        CountDownLatch loadersGated = new CountDownLatch(1);
        try {
            TierCache<String, String> cache = factory.getCache("c");
            for (int i = 0; i < keys; i++) {
                assertEquals("v1", cache.getOrCompute("k" + i, key -> "v1"));
            }

            // Let every entry go stale (past l2Ttl, inside the stale window).
            Thread.sleep(400);

            // Each stale read triggers a revalidation whose loader is gated:
            // 2 run (blocked), 1000 queue, the last submit is rejected.
            for (int i = 0; i < keys; i++) {
                String served = cache.getOrCompute("k" + i, key -> {
                    try {
                        loadersGated.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "v2";
                });
                assertEquals("v1", served, "stale value served while revalidating");
            }

            // The miss read after an evict is where the leaked claim used to
            // hang the reader: it must complete (fail fast or retry) now.
            cache.evict("k" + (keys - 1));
            var reader = Executors.newSingleThreadExecutor();
            try {
                String reloaded;
                try {
                    reloaded = reader.submit(() ->
                            cache.getOrCompute("k" + (keys - 1), key -> "v3"))
                            .get(30, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    fail("reader hung on the dropped revalidation's claim", e);
                    return;
                }
                assertEquals("v3", reloaded);
            } finally {
                reader.shutdownNow();
            }
        } finally {
            loadersGated.countDown();
            factory.close();
        }
    }
}
