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
        DistributedLock second = awaitLock(provider, "tsr");
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
        // Extend promptly, well inside the lease, until ~1s has elapsed.
        long start = System.nanoTime();
        int round = 0;
        while (System.nanoTime() - start < Duration.ofSeconds(1).toNanos()) {
            TimeUnit.MILLISECONDS.sleep(100);
            assertTrue(lock.extend(Duration.ofMillis(300)), "watchdog extension round " + round++);
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
        assertNotNull(awaitLock(provider, "death"),
                "lock must expire and be re-acquirable");
    }

    @Test
    void extendFailsAfterLoss() throws InterruptedException {
        LettuceLockProvider provider = new LettuceLockProvider(client.connect());
        DistributedLock lock = provider.tryLock("lost", Duration.ofMillis(100));
        assertNotNull(lock);
        // Observe server-side expiry by re-acquiring, then let it go.
        DistributedLock reacquired = awaitLock(provider, "lost");
        assertNotNull(reacquired, "lock expired and re-acquired");
        reacquired.release();
        assertFalse(lock.extend(Duration.ofMinutes(1)), "expired lock cannot be extended");
    }

    /** Polls until the named lock is re-acquirable (i.e. the previous lease expired). */
    private static DistributedLock awaitLock(LettuceLockProvider provider, String name)
            throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        DistributedLock lock;
        while ((lock = provider.tryLock(name, Duration.ofMinutes(1))) == null) {
            if (System.nanoTime() > deadline) {
                return null;
            }
            TimeUnit.MILLISECONDS.sleep(20);
        }
        return lock;
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
