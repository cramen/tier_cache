package io.tiercache;

import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.CountingRemoteCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: async-api — non-blocking CompletionStage view over the
 * synchronous engine: caller thread never does cache I/O, bounded shared
 * executor, singleflight coalescing, failure propagation, and semantics
 * parity with the sync path.
 */
class AsyncTierCacheTest {

    private static TierCacheFactory factory() {
        return TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                // Roomy enough for every caller task to start at once: the
                // coalescing tests need all callers inside one flight before
                // the loader completes or fails (default pool is only
                // max(4, cores) — smaller than the caller counts on CI).
                .asyncExecutorThreads(40)
                .build();
    }

    // --- Accessor ---

    @Test
    void asyncViewIsMemoizedAndSharesTheCache() throws Exception {
        try (TierCacheFactory factory = factory()) {
            AsyncTierCache<String, String> a = factory.asyncCache("c");
            AsyncTierCache<String, String> b = factory.asyncCache("c");
            assertSame(a, b, "the async view is memoized alongside the cache");

            a.putAsync("k", "v").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals("v", factory.getCache("c").get("k"),
                    "view and sync cache share the same engine");
        }
    }

    // --- Scenario: caller thread never blocks ---

    @Test
    void callerThreadPerformsNoCacheIoOrLoaderWork() throws Exception {
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<String> loaderThread = new AtomicReference<>();
        AtomicReference<CompletionStage<String>> stageRef = new AtomicReference<>();

        try (TierCacheFactory factory = factory()) {
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            Thread caller = new Thread(() -> stageRef.set(async.getOrComputeAsync("k", key -> {
                loaderThread.set(Thread.currentThread().getName());
                loaderEntered.countDown();
                awaitQuietly(release);
                return "v";
            })), "probe-caller");
            caller.start();

            assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
            caller.join(5_000);
            CompletionStage<String> stage = stageRef.get();
            assertNotNull(stage, "the async call must return while the loader is still parked");
            assertFalse(stage.toCompletableFuture().isDone(),
                    "the caller must not wait for the loader");
            assertNotEquals("probe-caller", loaderThread.get(),
                    "loader work must not run on the calling thread");
            assertTrue(loaderThread.get().startsWith("tiercache-async"),
                    "work must run on the factory's bounded async executor, got " + loaderThread.get());

            release.countDown();
            assertEquals("v", stage.toCompletableFuture().get(5, TimeUnit.SECONDS));
        }
    }

    // --- Scenario: no thread explosion under load ---

    @Test
    void workerThreadsStayBoundedUnderManyConcurrentCalls() throws Exception {
        int calls = 500;
        Set<String> workerThreads = ConcurrentHashMap.newKeySet();
        try (TierCacheFactory factory = factory()) {
            AsyncTierCache<Integer, String> async = factory.asyncCache("c");
            List<CompletableFuture<String>> stages = new ArrayList<>();
            for (int i = 0; i < calls; i++) {
                int key = i;
                stages.add(async.getOrComputeAsync(key, k -> {
                    workerThreads.add(Thread.currentThread().getName());
                    return "v" + k;
                }).toCompletableFuture());
            }
            for (int i = 0; i < calls; i++) {
                assertEquals("v" + i, stages.get(i).get(10, TimeUnit.SECONDS));
            }
            assertFalse(workerThreads.isEmpty());
            assertTrue(workerThreads.stream().allMatch(n -> n.startsWith("tiercache-async")),
                    "no caller or common-pool threads may do cache work: " + workerThreads);
            assertTrue(workerThreads.size() < calls / 2,
                    "threads are reused, not created per call: " + workerThreads.size()
                            + " threads for " + calls + " calls");
        }
    }

    // --- Scenario: executor terminates with factory close ---

    @Test
    void executorStopsWithFactoryClose() throws Exception {
        Set<Thread> baseline = revalidationThreads();
        TierCacheFactory factory = factory();
        AsyncTierCache<String, String> async = factory.asyncCache("c");

        assertEquals("v", async.getOrComputeAsync("k", key -> "v")
                .toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertFalse(newWorkersSince(baseline).isEmpty(), "the shared executor must have run the work");

        factory.close();
        awaitTrue(newWorkersSince(baseline)::isEmpty,
                "executor workers must stop with factory close");

        // Rejection surfaces as a failed stage, never a synchronous throw
        // on the calling thread and never a task run on it.
        CompletableFuture<String> rejected = async.getAsync("k").toCompletableFuture();
        ExecutionException execution = assertThrows(ExecutionException.class, rejected::get,
                "submissions after close are rejected, never run on the calling thread");
        assertTrue(execution.getCause() instanceof RejectedExecutionException,
                "rejection cause must be RejectedExecutionException, got " + execution.getCause());
    }

    // --- Scenario: concurrent async misses coalesce ---

    @Test
    void concurrentAsyncMissesCoalesceOntoOneSyncLoaderExecution() throws Exception {
        int callers = 32;
        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        try (TierCacheFactory factory = factory()) {
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            List<CompletableFuture<String>> stages = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                stages.add(async.getOrComputeAsync("hot", key -> {
                    loaderCalls.incrementAndGet();
                    loaderEntered.countDown();
                    awaitQuietly(release);
                    return "v";
                }).toCompletableFuture());
            }
            assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
            // Give the herd a chance to pile onto the same key.
            Thread.sleep(200);
            release.countDown();

            for (CompletableFuture<String> stage : stages) {
                assertEquals("v", stage.get(5, TimeUnit.SECONDS));
            }
            assertEquals(1, loaderCalls.get(), "singleflight: exactly one loader execution");
        }
    }

    @Test
    void concurrentAsyncMissesCoalesceOntoOneAsyncLoaderExecution() throws Exception {
        int callers = 32;
        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService loaderDriver = Executors.newSingleThreadExecutor();
        try (TierCacheFactory factory = factory()) {
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            List<CompletableFuture<String>> stages = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                stages.add(async.getOrComputeAsyncStage("hot", key -> {
                    loaderCalls.incrementAndGet();
                    loaderEntered.countDown();
                    CompletableFuture<String> result = new CompletableFuture<>();
                    loaderDriver.execute(() -> {
                        awaitQuietly(release);
                        result.complete("v");
                    });
                    return result;
                }).toCompletableFuture());
            }
            assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
            Thread.sleep(200);
            release.countDown();

            for (CompletableFuture<String> stage : stages) {
                assertEquals("v", stage.get(5, TimeUnit.SECONDS));
            }
            assertEquals(1, loaderCalls.get(), "singleflight: exactly one loader execution");
        } finally {
            loaderDriver.shutdownNow();
        }
    }

    // --- Scenario: loader failure reaches every caller ---

    @Test
    void syncLoaderFailureReachesEveryCallerAndStoresNothing() throws Exception {
        int callers = 8;
        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        try (TierCacheFactory factory = factory()) {
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            List<CompletableFuture<String>> stages = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                stages.add(async.getOrComputeAsync("hot", key -> {
                    loaderCalls.incrementAndGet();
                    loaderEntered.countDown();
                    awaitQuietly(release);
                    throw new IllegalStateException("boom");
                }).toCompletableFuture());
            }
            assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
            Thread.sleep(200);
            release.countDown();

            for (CompletableFuture<String> stage : stages) {
                ExecutionException e = assertThrows(ExecutionException.class,
                        () -> stage.get(5, TimeUnit.SECONDS));
                assertCauseChainContains(e, IllegalStateException.class, "boom");
            }
            assertEquals(1, loaderCalls.get(), "one loader execution even on failure");
            assertNull(async.getAsync("hot").toCompletableFuture().get(5, TimeUnit.SECONDS),
                    "nothing may be stored on loader failure");
        }
    }

    @Test
    void asyncLoaderFailureReachesEveryCallerAndStoresNothing() throws Exception {
        int callers = 8;
        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        try (TierCacheFactory factory = factory()) {
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            List<CompletableFuture<String>> stages = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                stages.add(async.getOrComputeAsyncStage("hot", key -> {
                    loaderCalls.incrementAndGet();
                    loaderEntered.countDown();
                    awaitQuietly(release);
                    return CompletableFuture.<String>failedFuture(new IllegalStateException("boom"));
                }).toCompletableFuture());
            }
            assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
            Thread.sleep(200);
            release.countDown();

            for (CompletableFuture<String> stage : stages) {
                ExecutionException e = assertThrows(ExecutionException.class,
                        () -> stage.get(5, TimeUnit.SECONDS));
                assertCauseChainContains(e, IllegalStateException.class, "boom");
            }
            assertEquals(1, loaderCalls.get(), "one loader execution even on failure");
            assertNull(async.getAsync("hot").toCompletableFuture().get(5, TimeUnit.SECONDS),
                    "nothing may be stored on loader failure");
        }
    }

    @Test
    void asyncLoaderFailureIsUnwrappedToItsCause() {
        try (TierCacheFactory factory = factory()) {
            AsyncTierCache<String, String> async = factory.asyncCache("c");

            ExecutionException runtime = assertThrows(ExecutionException.class,
                    () -> async.getOrComputeAsyncStage("k1",
                            key -> CompletableFuture.<String>failedFuture(new IllegalStateException("boom")))
                            .toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, runtime.getCause(),
                    "a failed stage propagates its cause, not a CompletionException wrapper");

            ExecutionException error = assertThrows(ExecutionException.class,
                    () -> async.getOrComputeAsyncStage("k2",
                            key -> CompletableFuture.<String>failedFuture(new AssertionError("bang")))
                            .toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertInstanceOf(AssertionError.class, error.getCause());

            ExecutionException checked = assertThrows(ExecutionException.class,
                    () -> async.getOrComputeAsyncStage("k3",
                            key -> CompletableFuture.<String>failedFuture(new IOException("io")))
                            .toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertCauseChainContains(checked, IOException.class, "io");
        }
    }

    // --- Scenario: semantics are inherited ---

    @Test
    void l2HitWarmsL1() throws Exception {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        try (TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(l2)
                .localCacheFactory((name, settings) -> l1)
                .build()) {
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            l2.put("k", StoredEntry.ofValue("v"), Duration.ofMinutes(1));

            assertEquals("v", async.getAsync("k").toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals(1, l1.puts.get(), "L2 hit must warm L1");

            int l2GetsBefore = l2.gets.get();
            assertEquals("v", async.getAsync("k").toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals(l2GetsBefore, l2.gets.get(), "subsequent lookup must be served from L1");
        }
    }

    @Test
    void nullMarkerUnderAllowSuppressesTheLoader() throws Exception {
        AtomicInteger loaderCalls = new AtomicInteger();
        try (TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .cache("nulls", new CacheOverride().nullPolicy(NullPolicy.allow(Duration.ofSeconds(30))))
                .build()) {
            AsyncTierCache<String, String> async = factory.asyncCache("nulls");

            assertNull(async.getOrComputeAsync("k", key -> {
                loaderCalls.incrementAndGet();
                return null;
            }).toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertInstanceOf(LookupResult.CachedNull.class,
                    async.lookupAsync("k").toCompletableFuture().get(5, TimeUnit.SECONDS),
                    "the null-marker must be distinguishable from a miss");

            assertNull(async.getOrComputeAsync("k", key -> {
                loaderCalls.incrementAndGet();
                return null;
            }).toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals(1, loaderCalls.get(), "the marker must suppress the loader");

            async.putNullAsync("k2").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertInstanceOf(LookupResult.CachedNull.class,
                    async.lookupAsync("k2").toCompletableFuture().get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void lookupReportsTheTriState() throws Exception {
        try (TierCacheFactory factory = factory()) {
            AsyncTierCache<String, String> async = factory.asyncCache("c");

            assertInstanceOf(LookupResult.Miss.class,
                    async.lookupAsync("k").toCompletableFuture().get(5, TimeUnit.SECONDS));

            async.putAsync("k", "v").toCompletableFuture().get(5, TimeUnit.SECONDS);
            LookupResult<String> result = async.lookupAsync("k").toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);
            assertEquals("v", assertInstanceOf(LookupResult.Hit.class, result).value());

            async.evictAsync("k").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertInstanceOf(LookupResult.Miss.class,
                    async.lookupAsync("k").toCompletableFuture().get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void writeAndEvictOperationsMatchTheSyncPath() throws Exception {
        try (TierCacheFactory factory = factory()) {
            AsyncTierCache<String, String> async = factory.asyncCache("c");

            assertTrue(async.putIfAbsentAsync("k", "v").toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertFalse(async.putIfAbsentAsync("k", "w").toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals("v", async.getAsync("k").toCompletableFuture().get(5, TimeUnit.SECONDS));

            async.putAsync("t1", "v1", "tag").toCompletableFuture().get(5, TimeUnit.SECONDS);
            async.putAsync("t2", "v2", "tag").toCompletableFuture().get(5, TimeUnit.SECONDS);
            async.evictByTagAsync("tag").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertNull(async.getAsync("t1").toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertNull(async.getAsync("t2").toCompletableFuture().get(5, TimeUnit.SECONDS));

            async.putAsync("a", "va").toCompletableFuture().get(5, TimeUnit.SECONDS);
            async.putAsync("b", "vb").toCompletableFuture().get(5, TimeUnit.SECONDS);
            async.evictAllAsync(List.of("a")).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertNull(async.getAsync("a").toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals("vb", async.getAsync("b").toCompletableFuture().get(5, TimeUnit.SECONDS));

            async.evictAllAsync().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertNull(async.getAsync("b").toCompletableFuture().get(5, TimeUnit.SECONDS));
        }
    }

    // --- helpers ---

    private static Set<Thread> revalidationThreads() {
        Set<Thread> threads = ConcurrentHashMap.newKeySet();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.isAlive() && t.getName().startsWith("tiercache-async")) {
                threads.add(t);
            }
        }
        return threads;
    }

    private static Set<Thread> newWorkersSince(Set<Thread> baseline) {
        Set<Thread> threads = revalidationThreads();
        threads.removeAll(baseline);
        return threads;
    }

    private interface BoolProbe {
        boolean getAsBoolean();
    }

    private static void awaitTrue(BoolProbe probe, String description) throws InterruptedException {
        // Generous deadline: thread-termination timing varies wildly on
        // shared CI runners (observed flakes at 5 s there, always <1 s locally).
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (probe.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("timed out waiting for: " + description);
    }

    private static void assertCauseChainContains(Throwable error, Class<? extends Throwable> type,
                                                 String message) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (type.isInstance(t) && message.equals(t.getMessage())) {
                return;
            }
        }
        throw new AssertionError("expected " + type.getSimpleName() + "(\"" + message
                + "\") in the cause chain of " + error);
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
