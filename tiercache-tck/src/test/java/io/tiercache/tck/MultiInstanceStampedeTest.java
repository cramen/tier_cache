package io.tiercache.tck;

import io.tiercache.InvalidationMode;

import io.lettuce.core.RedisClient;
import io.tiercache.CacheSettings;
import io.tiercache.NullPolicy;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.redis.LettuceLockProvider;
import io.tiercache.redis.LettuceRemoteCache;
import io.tiercache.spi.DistributedLock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-instance cache stampede: M cache instances (separate factories,
 * L1s, connections) share one real Redis. With distributed rebuild
 * coordination the loader runs exactly once cluster-wide; with coordination
 * explicitly disabled it runs at most once per instance (harness
 * sensitivity).
 */
class MultiInstanceStampedeTest {

    private static final int INSTANCES = 3;
    private static final int THREADS_PER_INSTANCE = 16;

    private static GenericContainer<?> server;
    private static RedisClient client;
    private static String redisUri;

    @BeforeAll
    static void startServer() {
        server = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine"))
                .withExposedPorts(6379);
        server.start();
        redisUri = "redis://" + server.getHost() + ":" + server.getMappedPort(6379);
        client = RedisClient.create(redisUri);
    }

    @AfterAll
    static void stopServer() {
        client.shutdown();
        server.stop();
    }

    @Test
    void stampedeAcrossInstancesLoadsExactlyOnce() throws Exception {
        int loaderCalls = runStampede(true, "stampede-coordinated");
        assertEquals(1, loaderCalls,
                "coordination must collapse the stampede to a single cluster-wide load");
    }

    @Test
    void stampedeWithoutCoordinationRevertsToPerInstance() throws Exception {
        int loaderCalls = runStampede(false, "stampede-uncoordinated");
        assertTrue(loaderCalls >= 1 && loaderCalls <= INSTANCES,
                "without coordination: at most one load per instance, got " + loaderCalls);
    }

    @Test
    void winnerDeathIsSurvived() throws Exception {
        // A "dead winner" holds the rebuild lock with a short lease and never
        // releases it; a live instance must take over after the lease expiry.
        LettuceLockProvider locks = new LettuceLockProvider(client.connect());
        DistributedLock deadWinnersLock = locks.tryLock(
                "winner-death:hot", Duration.ofMillis(500));
        assertTrue(deadWinnersLock != null);

        LettuceRemoteCache<String, String> l2 = LettuceRemoteCache
                .<String, String>builder(redisUri).cacheName("winner-death").build();
        TierCacheFactory factory = TierCacheFactory.builder()
                .defaults(new CacheSettings(10_000, Duration.ofMinutes(1), null,
                        Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                .remoteCache(l2)
                .build();
        TierCache<String, String> cache = factory.getCache("winner-death");

        long startNanos = System.nanoTime();
        String value = cache.getOrCompute("hot", key -> "recovered");
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
        l2.close();
        factory.close();

        assertEquals("recovered", value);
        assertTrue(elapsedMillis < 30_000,
                "takeover must complete within the coordination budget, took " + elapsedMillis + " ms");
    }

    private int runStampede(boolean coordination, String cacheName) throws Exception {
        AtomicInteger loaderCalls = new AtomicInteger();
        List<TierCacheFactory> factories = new ArrayList<>();
        List<TierCache<String, String>> caches = new ArrayList<>();
        List<LettuceRemoteCache<String, String>> transports = new ArrayList<>();
        for (int i = 0; i < INSTANCES; i++) {
            LettuceRemoteCache<String, String> l2 = LettuceRemoteCache
                    .<String, String>builder(redisUri)
                            .client(client)
                            .cacheName(cacheName)
                            .build();
            transports.add(l2);
            TierCacheFactory.Builder builder = TierCacheFactory.builder()
                    .defaults(new CacheSettings(10_000, Duration.ofMinutes(1), null,
                            Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                    .remoteCache(l2);
            if (!coordination) {
                builder.disableDistributedCoordination();
            }
            TierCacheFactory factory = builder.build();
            factories.add(factory);
            caches.add(factory.getCache(cacheName));
        }

        int totalThreads = INSTANCES * THREADS_PER_INSTANCE;
        ExecutorService pool = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch allWaiting = new CountDownLatch(totalThreads);
        CountDownLatch release = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>();
        for (TierCache<String, String> cache : caches) {
            for (int i = 0; i < THREADS_PER_INSTANCE; i++) {
                futures.add(pool.submit(() -> {
                    allWaiting.countDown();
                    release.await(10, TimeUnit.SECONDS);
                    return cache.getOrCompute("hot", key -> {
                        loaderCalls.incrementAndGet();
                        try {
                            Thread.sleep(100); // expensive load; lets the herd pile up
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(e);
                        }
                        return "value";
                    });
                }));
            }
        }

        allWaiting.await(10, TimeUnit.SECONDS);
        release.countDown();
        for (Future<String> f : futures) {
            assertEquals("value", f.get(15, TimeUnit.SECONDS));
        }
        pool.shutdown();
        transports.forEach(LettuceRemoteCache::close);
        factories.forEach(TierCacheFactory::close);
        return loaderCalls.get();
    }
}
