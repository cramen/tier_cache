package io.tiercache.internal;

import io.tiercache.*;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.tiercache.internal.AsyncResourceOwnershipTest.*;

class TerminalClaimRetirementTest {
    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void blockedNotificationCannotKeepOldRoundJoinable(boolean failure, boolean expiry) throws Exception {
        CountDownLatch loadGate = new CountDownLatch(1), loadEntered = new CountDownLatch(1);
        CountDownLatch callbackGate = new CountDownLatch(1), callbackEntered = new CountDownLatch(1);
        CountDownLatch nextGate = new CountDownLatch(1), nextEntered = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        var defaults = CacheSettings.defaults();
        var settings = new CacheSettings(defaults.l1MaxSize(), Duration.ofMillis(50), null,
                Duration.ofMillis(100), 0, NullPolicy.deny(), defaults.invalidationMode(),
                defaults.payloadCapBytes(), Duration.ZERO, false, defaults.xfetchBeta());
        try (var factory = TierCacheFactory.builder().defaults(settings).remoteCache(new InMemoryRemoteCache<>())
                .asyncExecutorThreads(3).build()) {
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            TierCache<String, String> sync = factory.getCache("c");
            var owner = async.getOrComputeAsync("k", k -> {
                loads.incrementAndGet(); loadEntered.countDown(); gate(loadGate);
                if (failure) throw new IllegalArgumentException("old failure");
                return "old";
            });
            assertTrue(loadEntered.await(5, TimeUnit.SECONDS));
            var follower = async.getOrComputeAsync("k", k -> fail("duplicate old owner"));
            waitFor(() -> pool(factory).getQueue().isEmpty() && pool(factory).getActiveCount() == 1);
            var dependent = follower.whenComplete((v, e) -> {
                callbackEntered.countDown(); gate(callbackGate);
            });
            loadGate.countDown();
            assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
            if (expiry) Thread.sleep(150); else sync.evict("k");
            var next = async.getOrComputeAsync("k", k -> {
                loads.incrementAndGet(); nextEntered.countDown(); gate(nextGate); return "new";
            });
            try {
                assertTrue(nextEntered.await(5, TimeUnit.SECONDS), "retired result must not remain joinable");
                assertEquals(3, occupied(budget(factory)), "blocked callback and task keep their credits");
                callbackGate.countDown();
                waitFor(() -> occupied(budget(factory)) == 1);
                var nextFollower = async.getOrComputeAsync("k", k -> fail("old cleanup removed new owner"));
                waitFor(() -> pool(factory).getQueue().isEmpty() && pool(factory).getActiveCount() == 1);
                nextGate.countDown();
                assertEquals("new", next.toCompletableFuture().get(5, TimeUnit.SECONDS));
                assertEquals("new", nextFollower.toCompletableFuture().get(5, TimeUnit.SECONDS));
                assertEquals(2, loads.get());
                waitFor(() -> occupied(budget(factory)) == 0);
            } finally { nextGate.countDown(); }
            if (failure) {
                assertThrows(ExecutionException.class, () -> owner.toCompletableFuture().get());
                assertThrows(ExecutionException.class, () -> dependent.toCompletableFuture().get());
            } else {
                assertEquals("old", owner.toCompletableFuture().get());
                assertEquals("old", dependent.toCompletableFuture().get());
            }
        } finally { loadGate.countDown(); callbackGate.countDown(); nextGate.countDown(); }
    }
    static void gate(CountDownLatch gate) {
        try { assertTrue(gate.await(10, TimeUnit.SECONDS)); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
    }
}
