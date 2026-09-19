package io.tiercache;

import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Bounded async executor (async-api spec): the pool serving
 * {@link AsyncTierCache} operations has a fixed maximum thread count, a
 * saturated queue surfaces as a failed stage (never unbounded threads), and
 * background revalidation runs on its own executor.
 */
class AsyncExecutorBoundsTest {

    private static Set<String> asyncThreadNames() {
        Set<String> names = ConcurrentHashMap.newKeySet();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().startsWith("tiercache-async")) {
                names.add(t.getName());
            }
        }
        return names;
    }

    @Test
    void threadCountStaysBoundedUnderStorm() throws Exception {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .asyncExecutorThreads(3)
                .build();
        try {
            AsyncTierCache<String, String> cache = factory.asyncCache("c");
            AtomicInteger inFlight = new AtomicInteger();
            AtomicInteger peak = new AtomicInteger();
            Set<String> loaderThreads = ConcurrentHashMap.newKeySet();
            List<CompletionStage<String>> stages = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                String key = "k" + i;
                stages.add(cache.getOrComputeAsync(key, k -> {
                    loaderThreads.add(Thread.currentThread().getName());
                    int now = inFlight.incrementAndGet();
                    peak.accumulateAndGet(now, Math::max);
                    try {
                        Thread.sleep(5);
                        return "v";
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    } finally {
                        inFlight.decrementAndGet();
                    }
                }));
            }
            for (CompletionStage<String> stage : stages) {
                assertEquals("v", stage.toCompletableFuture().get(30, TimeUnit.SECONDS));
            }
            assertTrue(peak.get() <= 3,
                    "concurrent loader executions must not exceed the pool size, got " + peak.get());
            assertTrue(loaderThreads.size() <= 3,
                    "distinct async worker threads must not exceed the pool size, got "
                            + loaderThreads.size() + " (" + loaderThreads + ")");
        } finally {
            factory.close();
        }
    }

    @Test
    void saturationFailsStageInsteadOfGrowingThreads() throws Exception {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .asyncExecutorThreads(1)
                .build();
        try {
            AsyncTierCache<String, String> cache = factory.asyncCache("c");
            CountDownLatch loaderBlocked = new CountDownLatch(1);
            AtomicReference<Throwable> rejection = new AtomicReference<>();
            List<CompletionStage<String>> stages = new ArrayList<>();
            // 1 running + 10_000 queued + overflow rejected; the method must
            // return a failed stage, never throw on the caller's thread.
            for (int i = 0; i < 10_002; i++) {
                CompletionStage<String> stage = cache.getOrComputeAsync("hot", k -> {
                    try {
                        loaderBlocked.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "v";
                });
                stage.toCompletableFuture().exceptionally(e -> {
                    rejection.compareAndSet(null, e);
                    return null;
                });
                stages.add(stage);
            }
            assertTrue(asyncThreadNames().size() <= 1,
                    "no threads beyond the configured maximum, got " + asyncThreadNames());
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (rejection.get() == null && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            loaderBlocked.countDown();
            Throwable e = rejection.get();
            assertNotNull(e, "overflow must surface as a failed stage");
            Throwable cause = e instanceof java.util.concurrent.CompletionException ? e.getCause() : e;
            assertTrue(cause instanceof RejectedExecutionException,
                    "rejection cause must be RejectedExecutionException, got " + cause);
            for (CompletionStage<String> stage : stages) {
                stage.toCompletableFuture().completeExceptionally(new RejectedExecutionException("test cleanup"));
            }
        } finally {
            factory.close();
        }
    }

    @Test
    void asyncWorkRunsOnDedicatedAsyncExecutor() throws Exception {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        try {
            AsyncTierCache<String, String> cache = factory.asyncCache("c");
            AtomicReference<String> loaderThread = new AtomicReference<>();
            String value = cache.getOrComputeAsync("k", k -> {
                loaderThread.set(Thread.currentThread().getName());
                return "v";
            }).toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertEquals("v", value);
            assertTrue(loaderThread.get().startsWith("tiercache-async"),
                    "async API work must run on the async executor, got " + loaderThread.get());
        } finally {
            factory.close();
        }
    }

    @Test
    void revalidationRunsOnSeparateExecutor() throws Exception {
        CacheSettings stale = new CacheSettings(10_000, Duration.ofMillis(100), null,
                Duration.ofMillis(200), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE,
                64 * 1024, Duration.ofSeconds(30), false, Duration.ofSeconds(1));
        TierCacheFactory factory = TierCacheFactory.builder()
                .defaults(stale)
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        try {
            TierCache<String, String> cache = factory.getCache("c");
            AtomicReference<String> revalidationThread = new AtomicReference<>();
            CountDownLatch revalidated = new CountDownLatch(1);
            assertEquals("v1", cache.getOrCompute("k", key -> "v1"));

            // Let the entry go stale (past l2Ttl, inside the stale window).
            Thread.sleep(400);
            String served = cache.getOrCompute("k", key -> {
                revalidationThread.set(Thread.currentThread().getName());
                revalidated.countDown();
                return "v2";
            });
            assertEquals("v1", served, "stale value is served while revalidating");
            assertTrue(revalidated.await(30, TimeUnit.SECONDS), "revalidation completes");
            assertNotNull(revalidationThread.get(), "stale read must trigger a revalidation");
            assertTrue(revalidationThread.get().startsWith("tiercache-revalidation"),
                    "revalidation must run on its own executor, got " + revalidationThread.get());

            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (!"v2".equals(cache.get("k")) && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals("v2", cache.get("k"));
        } finally {
            factory.close();
        }
    }
}
