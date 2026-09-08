package io.tiercache;

import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.DistributedLock;
import io.tiercache.spi.DistributedLockProvider;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import io.tiercache.testkit.InMemoryLockProvider;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: rebuild-coordination — coordinated load path in core.
 */
class CoordinationTest {

    private static final ScheduledExecutorService SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "test-watchdog");
                t.setDaemon(true);
                return t;
            });

    private record CacheAndL2(TierCache<String, String> cache, InMemoryRemoteCache<String, String> l2) {
    }

    private static CacheAndL2 coordinatedInstance(InMemoryRemoteCache<String, String> sharedL2,
            InMemoryLockProvider locks, String name, CacheSettings settings) {
        DefaultTierCache<String, String> cache = new DefaultTierCache<>(name,
                new CountingLocalCache<>(), sharedL2, settings, true, locks, SCHEDULER, null, null);
        return new CacheAndL2(cache, sharedL2);
    }

    // --- 3.1 wiring ---

    @Test
    void factoryAutoDerivesLockProviderFromTransport() {
        // A RemoteCache that is also a LockProviderSource.
        class SourcingL2 implements RemoteCache<String, String>, io.tiercache.spi.LockProviderSource {
            private final InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();

            @Override
            public DistributedLockProvider lockProvider() {
                return new InMemoryLockProvider();
            }

            @Override
            public StoredEntry<String> get(String key) {
                return delegate.get(key);
            }

            @Override
            public void put(String key, StoredEntry<String> entry, Duration ttl) {
                delegate.put(key, entry, ttl);
            }

            @Override
            public void evict(String key) {
                delegate.evict(key);
            }

            @Override
            public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) {
                return delegate.setIfAbsent(key, entry, ttl);
            }

            @Override
            public void clear() {
                delegate.clear();
            }
        }
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new SourcingL2())
                .build();
        // Works without explicit lockProvider wiring.
        TierCache<String, String> cache = factory.getCache("c");
        assertEquals("v", cache.getOrCompute("k", key -> "v"));
        factory.close();
    }

    @Test
    void factoryFallsBackToSingleflightWithoutProvider() {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        assertEquals("v", cache.getOrCompute("k", key -> "v"));
        factory.close();
    }

    @Test
    void disabledCoordinationStillCaches() {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .lockProvider(new InMemoryLockProvider())
                .disableDistributedCoordination()
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        assertEquals("v", cache.getOrCompute("k", key -> "v"));
        factory.close();
    }

    // --- 3.2 factory lifecycle ---

    @Test
    void factoryCloseTerminatesWatchdog() throws Exception {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .lockProvider(new InMemoryLockProvider())
                .build();
        TierCache<String, String> cache = factory.getCache("c");

        // The watchdog thread is created lazily: trigger a coordinated load.
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<String> load = pool.submit(() -> cache.getOrCompute("k", key -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "v";
        }));
        Thread.sleep(100); // let the load start and the watchdog be scheduled
        assertTrue(watchdogThreads() > 0, "watchdog thread must exist during a coordinated load");
        factory.close();
        load.get(5, TimeUnit.SECONDS);
        pool.shutdown();
        for (int i = 0; i < 50 && watchdogThreads() > 0; i++) {
            Thread.sleep(50);
        }
        assertEquals(0, watchdogThreads(), "watchdog thread must terminate on close");
    }

    private static long watchdogThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().equals("tiercache-watchdog") && t.isAlive())
                .count();
    }

    // --- 3.3 double-check ---

    @Test
    void valueAppearingDuringLockAcquisitionSkipsLoader() {
        InMemoryLockProvider locks = new InMemoryLockProvider();
        // L2 that misses once (initial cascade read) but holds the value by
        // the time the double-check runs.
        AtomicInteger l2Reads = new AtomicInteger();
        RemoteCache<String, String> l2 = new RemoteCache<>() {
            @Override
            public StoredEntry<String> get(String key) {
                if (l2Reads.incrementAndGet() == 1) {
                    return null;
                }
                return StoredEntry.ofValue("meanwhile");
            }

            @Override
            public void put(String key, StoredEntry<String> entry, Duration ttl) {
            }

            @Override
            public void evict(String key) {
            }

            @Override
            public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) {
                return true;
            }

            @Override
            public void clear() {
            }
        };
        TierCache<String, String> cache = new DefaultTierCache<>("dc",
                new CountingLocalCache<>(), l2, CacheSettings.defaults(), true, locks, SCHEDULER, null, null);

        String value = cache.getOrCompute("k", key -> {
            throw new AssertionError("loader must not run: double-check must see the value");
        });
        assertEquals("meanwhile", value);
    }

    // --- 3.4 loser wait + takeover ---

    @Test
    void loserReadsWinnersResult() throws Exception {
        InMemoryLockProvider locks = new InMemoryLockProvider();
        InMemoryRemoteCache<String, String> sharedL2 = new InMemoryRemoteCache<>();
        TierCache<String, String> winner = coordinatedInstance(sharedL2, locks, "c", CacheSettings.defaults()).cache();
        TierCache<String, String> loser = coordinatedInstance(sharedL2, locks, "c", CacheSettings.defaults()).cache();

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch winnerLoading = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<String> winnerResult = pool.submit(() ->
                winner.getOrCompute("hot", key -> {
                    loaderCalls.incrementAndGet();
                    winnerLoading.countDown();
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    return "loaded";
                }));
        assertTrue(winnerLoading.await(5, TimeUnit.SECONDS));
        Future<String> loserResult = pool.submit(() ->
                loser.getOrCompute("hot", key -> {
                    loaderCalls.incrementAndGet();
                    return "loser-loaded";
                }));

        assertEquals("loaded", winnerResult.get(5, TimeUnit.SECONDS));
        assertEquals("loaded", loserResult.get(5, TimeUnit.SECONDS),
                "loser must read the winner's result");
        assertEquals(1, loaderCalls.get());
        pool.shutdown();
    }

    @Test
    void loserTakesOverAfterLockExpiry() {
        InMemoryLockProvider locks = new InMemoryLockProvider();
        InMemoryRemoteCache<String, String> sharedL2 = new InMemoryRemoteCache<>();
        TierCache<String, String> instance = coordinatedInstance(sharedL2, locks, "c", CacheSettings.defaults()).cache();

        // Simulate a dead winner: lock held with a short lease, never released.
        DistributedLock deadWinnersLock = locks.tryLock("c:hot", Duration.ofMillis(300));
        assert deadWinnersLock != null;

        String value = instance.getOrCompute("hot", key -> "taken-over");
        assertEquals("taken-over", value);
    }

    // --- 3.5 null-marker interaction ---

    @Test
    void coordinatedLoaderNullStoresMarkerOnceClusterWide() {
        InMemoryLockProvider locks = new InMemoryLockProvider();
        InMemoryRemoteCache<String, String> sharedL2 = new InMemoryRemoteCache<>();
        CacheSettings allowNulls = new CacheSettings(10_000, Duration.ofMinutes(5), null,
                Duration.ofHours(1), 0.0, NullPolicy.allow(Duration.ofMinutes(1)));
        TierCache<String, String> a = coordinatedInstance(sharedL2, locks, "c", allowNulls).cache();
        TierCache<String, String> b = coordinatedInstance(sharedL2, locks, "c", allowNulls).cache();

        AtomicInteger loaderCalls = new AtomicInteger();
        assertNull(a.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            return null;
        }));
        // Second instance sees the marker: known-absent without the loader.
        assertNull(b.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            return null;
        }));
        assertEquals(1, loaderCalls.get());
        assertInstanceOf(LookupResult.CachedNull.class, b.lookup("k"));
    }
}
