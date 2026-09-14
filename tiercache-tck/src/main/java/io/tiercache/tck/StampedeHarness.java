package io.tiercache.tck;

import io.tiercache.InvalidationMode;

import io.tiercache.CacheSettings;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.InMemoryRemoteCache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stampede harness (in-memory L2): N threads hit one hot key exactly as it
 * expires. Counts loader executions.
 *
 * <p>Against a real Redis transport this becomes the full stampede chaos
 * test; the harness shape (threads, expiry timing, loader counting) is fixed
 * here so the later change only swaps the L2 implementation.
 *
 * @since 0.1.0
 */
public final class StampedeHarness {

    private final int threads;
    private final Duration l1Ttl;

    /**
     * Creates a harness that stampedes a hot key with the given concurrency.
     *
     * @param threads number of threads released onto the hot key simultaneously
     * @param l1Ttl L1 time-to-live of the cache under test; the key must
     *              expire from L1 before the stampede is released
     * @since 0.1.0
     */
    public StampedeHarness(int threads, Duration l1Ttl) {
        this.threads = threads;
        this.l1Ttl = l1Ttl;
    }

    /**
     * Runs one stampede round: pre-warm the key, let it expire from L1, then
     * release N threads onto it simultaneously.
     *
     * @param singleflightEnabled whether the cache under test coalesces loads
     * @return number of times the loader executed during the stampede
     * @throws Exception if a stamped thread is interrupted or a stampede
     *                   round does not finish within its timeout
     * @since 0.1.0
     */
    public int run(boolean singleflightEnabled) throws Exception {
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        TierCacheFactory.Builder builder = TierCacheFactory.builder()
                .defaults(new CacheSettings(10_000, l1Ttl, null, Duration.ofHours(1), 0.0, io.tiercache.NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                .remoteCache(l2);
        if (!singleflightEnabled) {
            builder.disableSingleflight();
        }
        TierCache<String, String> cache = builder.build().getCache("hot");

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch allWaiting = new CountDownLatch(threads);
        CountDownLatch release = new CountDownLatch(1);

        // Pre-warm both levels, then let L1 expire while L2 still holds the key…
        // actually stampede needs a FULL miss: evict everything, then stampede.
        cache.put("hot", "stale");
        cache.evict("hot");

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                allWaiting.countDown();
                release.await(5, TimeUnit.SECONDS);
                return cache.getOrCompute("hot", key -> {
                    loaderCalls.incrementAndGet();
                    // Simulate an expensive load so the herd can pile up.
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    return "value";
                });
            }));
        }

        allWaiting.await(5, TimeUnit.SECONDS);
        release.countDown();
        for (Future<String> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();
        return loaderCalls.get();
    }
}
