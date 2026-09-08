package io.tiercache;

import io.tiercache.spi.DistributedLock;
import io.tiercache.testkit.InMemoryLockProvider;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: rebuild-coordination — lock provider semantics (in-memory impl).
 */
class InMemoryLockProviderTest {

    private final InMemoryLockProvider provider = new InMemoryLockProvider();

    @Test
    void mutualExclusion() {
        DistributedLock first = provider.tryLock("l", Duration.ofMinutes(1));
        assertNotNull(first);
        assertNull(provider.tryLock("l", Duration.ofMinutes(1)), "second acquire must fail");
        first.release();
        assertNotNull(provider.tryLock("l", Duration.ofMinutes(1)), "acquire after release");
    }

    @Test
    void tokenSafeRelease() {
        DistributedLock first = provider.tryLock("l", Duration.ofMillis(80));
        assertNotNull(first);
        // Let it expire, another holder takes over...
        await(120);
        DistributedLock second = provider.tryLock("l", Duration.ofMinutes(1));
        assertNotNull(second);
        // ...and the stale first holder must not release it.
        first.release();
        assertNull(provider.tryLock("l", Duration.ofMinutes(1)),
                "stale release must not free the new holder's lock");
        second.release();
    }

    @Test
    void extendKeepsAliveOnlyForHolder() {
        DistributedLock lock = provider.tryLock("l", Duration.ofMillis(100));
        assertNotNull(lock);
        await(60);
        assertTrue(lock.extend(Duration.ofMinutes(1)));
        DistributedLock stale = provider.tryLock("l2", Duration.ofMillis(60));
        assertNotNull(stale);
        await(80);
        assertFalse(stale.extend(Duration.ofMinutes(1)), "expired lock cannot be extended");
    }

    @Test
    void concurrentTryLockHasSingleWinner() throws Exception {
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger wins = new AtomicInteger();
        var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                if (provider.tryLock("hot", Duration.ofMinutes(1)) != null) {
                    wins.incrementAndGet();
                }
                return null;
            }));
        }
        start.countDown();
        for (var f : futures) {
            f.get(5, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertEquals(1, wins.get());
    }

    private static void await(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
