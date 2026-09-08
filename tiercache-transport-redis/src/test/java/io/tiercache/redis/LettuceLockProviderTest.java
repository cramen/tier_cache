package io.tiercache.redis;

import io.lettuce.core.RedisClient;
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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: rebuild-coordination — lock semantics over real Redis.
 */
class LettuceLockProviderTest {

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
    void mutualExclusionAcrossProviders() {
        LettuceLockProvider a = new LettuceLockProvider(client.connect());
        LettuceLockProvider b = new LettuceLockProvider(client.connect());
        DistributedLock lock = a.tryLock("mx", Duration.ofMinutes(1));
        assertNotNull(lock);
        assertNull(b.tryLock("mx", Duration.ofMinutes(1)), "second provider must not acquire");
        lock.release();
        assertNotNull(b.tryLock("mx", Duration.ofMinutes(1)), "acquire after release");
    }

    @Test
    void tokenSafeRelease() throws InterruptedException {
        LettuceLockProvider provider = new LettuceLockProvider(client.connect());
        DistributedLock first = provider.tryLock("tsr", Duration.ofMillis(80));
        assertNotNull(first);
        Thread.sleep(200); // let it expire
        DistributedLock second = provider.tryLock("tsr", Duration.ofMinutes(1));
        assertNotNull(second, "expired lock is re-acquirable");
        first.release(); // stale holder must not release the new lock
        assertNull(provider.tryLock("tsr", Duration.ofMinutes(1)),
                "stale release must not free the new holder's lock");
        second.release();
    }

    @Test
    void extendKeepsLockPastInitialLease() throws InterruptedException {
        LettuceLockProvider provider = new LettuceLockProvider(client.connect());
        DistributedLock lock = provider.tryLock("ext", Duration.ofMillis(300));
        assertNotNull(lock);
        for (int i = 0; i < 5; i++) {
            Thread.sleep(200);
            assertTrue(lock.extend(Duration.ofMillis(300)), "watchdog extension round " + i);
        }
        // Total elapsed ~1s on a 300ms initial lease: still held.
        assertNull(provider.tryLock("ext", Duration.ofMinutes(1)), "extended lock stays held");
        lock.release();
    }

    @Test
    void expiredLockIsReacquiredAfterHolderStopsExtending() throws InterruptedException {
        LettuceLockProvider provider = new LettuceLockProvider(client.connect());
        DistributedLock lock = provider.tryLock("death", Duration.ofMillis(150));
        assertNotNull(lock);
        // Holder "dies": no release, no extension.
        Thread.sleep(300);
        assertNotNull(provider.tryLock("death", Duration.ofMinutes(1)),
                "lock must expire and be re-acquirable");
    }

    @Test
    void extendFailsAfterLoss() throws InterruptedException {
        LettuceLockProvider provider = new LettuceLockProvider(client.connect());
        DistributedLock lock = provider.tryLock("lost", Duration.ofMillis(100));
        assertNotNull(lock);
        Thread.sleep(200);
        assertFalse(lock.extend(Duration.ofMinutes(1)), "expired lock cannot be extended");
    }

    @Test
    void concurrentAcquireHasSingleWinner() throws Exception {
        LettuceLockProvider provider = new LettuceLockProvider(client.connect());
        int threads = 16;
        var pool = Executors.newFixedThreadPool(threads);
        var start = new CountDownLatch(1);
        AtomicInteger wins = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                if (provider.tryLock("race", Duration.ofMinutes(1)) != null) {
                    wins.incrementAndGet();
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertEquals(1, wins.get());
    }
}
