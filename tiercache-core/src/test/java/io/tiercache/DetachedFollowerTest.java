package io.tiercache;

import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class DetachedFollowerTest {
    @Test
    void hotKeyProgressesWhileSharedOwnerIsGated() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        try (TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>()).asyncExecutorThreads(2).build()) {
            TierCache<String, String> sync = factory.getCache("c");
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            sync.put("B", "hot");
            var owner = async.getOrComputeAsync("A", k -> {
                loads.incrementAndGet(); entered.countDown(); await(release); return "slow";
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var follower = async.getOrComputeAsync("A", k -> { fail("second owner"); return null; });
            try {
                assertEquals("hot", async.getOrComputeAsync("B", k -> {
                    fail("hot key loader"); return null;
                }).toCompletableFuture().get(2, TimeUnit.SECONDS));
                assertFalse(owner.toCompletableFuture().isDone());
                assertFalse(follower.toCompletableFuture().isDone());
            } finally { release.countDown(); }
            assertEquals("slow", owner.toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals("slow", follower.toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals(1, loads.get());
        } finally { release.countDown(); }
    }
    static void await(CountDownLatch latch) {
        try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("gate timeout"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
}
