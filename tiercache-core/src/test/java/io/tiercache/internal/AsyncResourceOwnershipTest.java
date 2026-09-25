package io.tiercache.internal;

import io.tiercache.*;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class AsyncResourceOwnershipTest {
    @Test
    void cancelledDetachedFollowersConsumeSharedBudgetUntilSourceTerminates() throws Exception {
        var source = new CompletableFuture<String>();
        var entered = new CountDownLatch(1);
        var syncWorker = Executors.newSingleThreadExecutor();
        try (var factory = TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>())
                .asyncExecutorThreads(2).build()) {
            AsyncTierCache<String, String> a = factory.asyncCache("a"), b = factory.asyncCache("b");
            var budget = budget(factory);
            var owner = a.getOrComputeAsyncStage("slow", k -> { entered.countDown(); return source; });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var syncEntered = new CountDownLatch(1);
            TierCache<String, String> syncB = factory.getCache("b");
            var syncOwner = syncWorker.submit(() -> syncB.getOrCompute("slowB", k -> {
                syncEntered.countDown(); return source.join();
            }));
            assertTrue(syncEntered.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < 10_001; i++) {
                var follower = (i % 2 == 0 ? a : b).getOrComputeAsync(i % 2 == 0 ? "slow" : "slowB",
                        k -> fail("duplicate loader"));
                follower.toCompletableFuture().cancel(false);
            }
            waitFor(() -> pool(factory).getQueue().isEmpty() && pool(factory).getActiveCount() == 1);
            assertEquals(10_002, occupied(budget));
            assertEquals(10_001, attachments(factory.getCache("a")) + attachments(syncB));
            var rejected = b.getOrComputeAsync("other", k -> fail("rejected loader"));
            assertInstanceOf(RejectedExecutionException.class,
                    assertThrows(ExecutionException.class, () -> rejected.toCompletableFuture().get()).getCause());
            assertEquals(10_002, occupied(budget));
            source.complete("done");
            assertEquals("done", owner.toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals("done", syncOwner.get(5, TimeUnit.SECONDS));
            waitFor(() -> occupied(budget) == 0);
            assertEquals("new", b.getOrComputeAsync("other", k -> "new").toCompletableFuture().get());
        } finally { source.complete("cleanup"); syncWorker.shutdownNow(); }
    }

    @Test
    void closeRetainsRunningAndDetachedWorkButDisposesQueuedTasks() throws Exception {
        var source = new CompletableFuture<String>();
        var entered = new CountDownLatch(1);
        var factory = TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>())
                .asyncExecutorThreads(2).build();
        try {
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            var budget = budget(factory);
            var owner = async.getOrComputeAsyncStage("a", k -> { entered.countDown(); return source; });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var follower = async.getOrComputeAsync("a", k -> fail("second load"));
            waitFor(() -> pool(factory).getQueue().isEmpty() && pool(factory).getActiveCount() == 1);
            var source2 = new CompletableFuture<String>();
            var entered2 = new CountDownLatch(1);
            var other = async.getOrComputeAsyncStage("b", k -> { entered2.countDown(); return source2; });
            try {
                assertTrue(entered2.await(5, TimeUnit.SECONDS));
                List<CompletableFuture<String>> queued = new ArrayList<>();
                for (int i = 0; i < 30; i++) queued.add(async.getAsync("q").toCompletableFuture());
                assertEquals(33, occupied(budget));
                factory.close();
                assertEquals(3, occupied(budget));
                for (var stage : queued) assertTrue(stage.isCancelled());
                assertTrue(owner.toCompletableFuture().isCancelled());
                assertTrue(follower.toCompletableFuture().isCancelled());
                assertTrue(other.toCompletableFuture().isCancelled());
                assertFalse(source.isCancelled());
                assertFalse(source2.isCancelled());
                assertTrue(async.getAsync("x").toCompletableFuture().isCancelled());
            } finally { source2.complete("b"); }
            source.complete("a");
            waitFor(() -> occupied(budget) == 0);
        } finally { source.complete("cleanup"); factory.close(); }
    }

    @Test
    void rejectionAndIdempotentQueueDisposalRestoreCredit() throws Exception {
        try (var factory = TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>()).build()) {
            TierCache<String, String> cache = factory.getCache("c");
            var admission = new AsyncAdmission(1);
            var rejected = new DefaultAsyncTierCache<>(cache, task -> {
                throw new RejectedExecutionException("test");
            }, admission, () -> true);
            assertTrue(rejected.getAsync("key").toCompletableFuture().isCompletedExceptionally());
            assertEquals(0, occupied(admission));
            var queue = new ArrayList<Runnable>();
            var view = new DefaultAsyncTierCache<>(cache, queue::add, admission, () -> true);
            var future = view.getAsync("key").toCompletableFuture();
            future.cancel(false);
            assertEquals(1, occupied(admission));
            var task = (DefaultTierCache.DiscardableTask) queue.remove(0);
            task.discard(); task.discard(); task.run();
            assertEquals(0, occupied(admission));
            assertTrue(future.isCancelled());
            // Even a saturated budget must not supersede closed-view cancellation.
            Field used = AsyncAdmission.class.getDeclaredField("occupied"); used.setAccessible(true);
            ((AtomicLong) used.get(admission)).set(10_001);
            view.closeOutstanding();
            assertTrue(view.getAsync("x").toCompletableFuture().isCancelled());
        }
    }

    @Test
    void capacityArithmeticDoesNotOverflow() throws Exception {
        var budget = new AsyncAdmission(Integer.MAX_VALUE);
        Field cap = AsyncAdmission.class.getDeclaredField("capacity"); cap.setAccessible(true);
        assertEquals((long) Integer.MAX_VALUE + 10_000, cap.getLong(budget));
        assertThrows(IllegalArgumentException.class, () -> new AsyncAdmission(0));
    }

    @Test
    void cancellationRacingNotificationDoesNotReleaseTwice() throws Exception {
        var canceller = Executors.newSingleThreadExecutor();
        try (var factory = TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>())
                .asyncExecutorThreads(2).build()) {
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            for (int round = 0; round < 100; round++) {
                var source = new CompletableFuture<String>();
                var entered = new CountDownLatch(1);
                String key = "key-" + round;
                var owner = async.getOrComputeAsyncStage(key, k -> { entered.countDown(); return source; });
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                var follower = async.getOrComputeAsync(key, k -> fail("duplicate"));
                waitFor(() -> attachments(factory.getCache("c")) == 1);
                var cancel = canceller.submit(() -> follower.toCompletableFuture().cancel(false));
                source.complete("value");
                cancel.get(5, TimeUnit.SECONDS);
                assertEquals("value", owner.toCompletableFuture().get(5, TimeUnit.SECONDS));
                waitFor(() -> occupied(budget(factory)) == 0);
            }
        } finally { canceller.shutdownNow(); }
    }

    static int attachments(TierCache<?, ?> cache) throws Exception {
        Field field = DefaultTierCache.class.getDeclaredField("inflight"); field.setAccessible(true);
        int result = 0;
        for (Object value : ((Map<?, ?>) field.get(cache)).values()) {
            result += ((LoadClaim<?, ?>) value).result.getNumberOfDependents();
        }
        return result;
    }

    static AsyncAdmission budget(TierCacheFactory factory) throws Exception {
        Field field = TierCacheFactory.class.getDeclaredField("asyncAdmission"); field.setAccessible(true);
        return (AsyncAdmission) field.get(factory);
    }
    static long occupied(AsyncAdmission budget) throws Exception {
        Field field = AsyncAdmission.class.getDeclaredField("occupied"); field.setAccessible(true);
        return ((AtomicLong) field.get(budget)).get();
    }
    static ThreadPoolExecutor pool(TierCacheFactory factory) throws Exception {
        Field field = TierCacheFactory.class.getDeclaredField("asyncExecutor"); field.setAccessible(true);
        return (ThreadPoolExecutor) field.get(factory);
    }
    interface Check { boolean get() throws Exception; }
    static void waitFor(Check check) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!check.get()) {
            if (System.nanoTime() > end) fail("condition timed out");
            Thread.sleep(1);
        }
    }
}
