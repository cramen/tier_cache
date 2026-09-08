package io.tiercache.tck;

import io.lettuce.core.RedisClient;
import io.tiercache.CacheSettings;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.redis.LettuceRemoteCache;
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
 * T-01 multi-instance: M cache instances (separate factories, separate L1s,
 * separate connections) share one real Redis. N threads per instance hit
 * one missing hot key simultaneously.
 *
 * <p>With per-instance singleflight the loader runs at most M times
 * (cluster-wide coordination is F-21, a later change).
 */
class MultiInstanceStampedeTest {

    private static final int INSTANCES = 3;
    private static final int THREADS_PER_INSTANCE = 16;

    private static GenericContainer<?> server;
    private static RedisClient client;

    @BeforeAll
    static void startServer() {
        server = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine"))
                .withExposedPorts(6379);
        server.start();
        client = RedisClient.create(
                "redis://" + server.getHost() + ":" + server.getMappedPort(6379));
    }

    @AfterAll
    static void stopServer() {
        client.shutdown();
        server.stop();
    }

    @Test
    @SuppressWarnings("unchecked")
    void stampedeAcrossInstancesIsBoundedPerInstance() throws Exception {
        AtomicInteger loaderCalls = new AtomicInteger();

        List<TierCache<String, String>> caches = new ArrayList<>();
        List<LettuceRemoteCache<String, String>> transports = new ArrayList<>();
        for (int i = 0; i < INSTANCES; i++) {
            LettuceRemoteCache<String, String> l2 = LettuceRemoteCache
                    .<String, String>builder("redis://unused")
                            .client(client)
                            .cacheName("stampede-multi")
                            .build();
            transports.add(l2);
            caches.add(TierCacheFactory.builder()
                    .defaults(new CacheSettings(10_000, Duration.ofMinutes(1), null,
                            Duration.ofHours(1), 0.0))
                    .remoteCache(l2)
                    .build()
                    .getCache("stampede-multi"));
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

        int calls = loaderCalls.get();
        assertTrue(calls >= 1 && calls <= INSTANCES,
                "expected at most " + INSTANCES + " loader calls (one per instance), got " + calls);
    }
}
