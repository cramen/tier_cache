package io.tiercache.internal;

import io.tiercache.*;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.tiercache.internal.AsyncResourceOwnershipTest.*;
import static io.tiercache.internal.TerminalClaimRetirementTest.gate;

class MixedAsyncClaimTest {
    @ParameterizedTest
    @CsvSource({"value,false", "value,true", "null,false", "null,true",
            "absent,false", "absent,true", "error,false", "error,true",
            "cancel,false", "cancel,true"})
    void syncAndAsyncShareTerminalOutcomeAndMetrics(String outcome, boolean syncOwner) throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), joined = new CountDownLatch(2);
        AtomicInteger loads = new AtomicInteger(), coalesced = new AtomicInteger(), loadMetric = new AtomicInteger();
        var source = new CompletableFuture<String>();
        var defaults = CacheSettings.defaults();
        var settings = new CacheSettings(defaults.l1MaxSize(), defaults.l1ExpireAfterWrite(), null,
                defaults.l2Ttl(), 0, outcome.equals("null") ? NullPolicy.allow(Duration.ofSeconds(10)) : NullPolicy.deny(),
                defaults.invalidationMode(), defaults.payloadCapBytes(), Duration.ZERO, false, defaults.xfetchBeta());
        var workers = Executors.newFixedThreadPool(2);
        try (var factory = TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>())
                .defaults(settings).asyncExecutorThreads(2).metricsListener(new CacheMetricsListener() {
                    @Override public void onRequest(String cache, Outcome result) {
                        if (result == Outcome.COALESCED) { coalesced.incrementAndGet(); joined.countDown(); }
                        if (result == Outcome.LOAD || result == Outcome.MISS) loadMetric.incrementAndGet();
                    }
                }).build()) {
            TierCache<String, String> sync = factory.getCache("c");
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            java.util.function.Function<String, String> loader = k -> {
                loads.incrementAndGet(); entered.countDown(); gate(release);
                return source.join();
            };
            Future<String> owner = syncOwner ? workers.submit(() -> sync.getOrCompute("k", loader))
                    : async.getOrComputeAsyncStage("k", k -> {
                        loads.incrementAndGet(); entered.countDown(); return source;
                    }).toCompletableFuture();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var follower = async.getOrComputeAsync("k", k -> fail("async duplicate"));
            Future<String> syncFollower = workers.submit(() -> sync.getOrCompute("k", k -> fail("sync duplicate")));
            assertTrue(joined.await(5, TimeUnit.SECONDS));
            waitFor(() -> pool(factory).getQueue().isEmpty() && pool(factory).getActiveCount() == (syncOwner ? 0 : 1));
            release.countDown();
            switch (outcome) {
                case "error" -> source.completeExceptionally(new IllegalArgumentException("source"));
                case "cancel" -> source.cancel(false);
                case "value" -> source.complete("value");
                default -> source.complete(null);
            }
            for (Future<String> result : java.util.List.of(owner, follower.toCompletableFuture(), syncFollower)) {
                if (outcome.equals("error") || outcome.equals("cancel")) {
                    Throwable error = assertThrows(Exception.class, () -> result.get(5, TimeUnit.SECONDS));
                    while (error instanceof ExecutionException || error instanceof CompletionException) {
                        error = error.getCause();
                    }
                    if (outcome.equals("error")) assertInstanceOf(IllegalArgumentException.class, error);
                    else assertInstanceOf(CancellationException.class, error);
                } else assertEquals(outcome.equals("value") ? "value" : null, result.get(5, TimeUnit.SECONDS));
            }
            assertEquals(1, loads.get());
            assertEquals(2, coalesced.get());
            assertEquals(outcome.equals("error") || outcome.equals("cancel") ? 0 : 1, loadMetric.get());
            if (outcome.equals("null")) assertNull(sync.getOrCompute("k", k -> fail("marker was not retained")));
            waitFor(() -> occupied(budget(factory)) == 0);
        } finally { release.countDown(); source.complete("cleanup"); workers.shutdownNow(); }
    }

    @Test
    void closingFollowerOfSyncOwnerDoesNotCancelOwner() throws Exception {
        var source = new CompletableFuture<String>();
        var entered = new CountDownLatch(1);
        var thread = Executors.newSingleThreadExecutor();
        var factory = TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>()).asyncExecutorThreads(2).build();
        try {
            TierCache<String, String> sync = factory.getCache("c");
            AsyncTierCache<String, String> async = factory.asyncCache("c");
            Future<String> owner = thread.submit(() -> sync.getOrCompute("k", k -> {
                entered.countDown(); return source.join();
            }));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var follower = async.getOrComputeAsync("k", k -> fail("second owner"));
            waitFor(() -> pool(factory).getQueue().isEmpty() && pool(factory).getActiveCount() == 0);
            factory.close();
            assertTrue(follower.toCompletableFuture().isCancelled());
            assertFalse(source.isDone());
            assertEquals(1, occupied(budget(factory)));
            source.complete("owner result");
            assertEquals("owner result", owner.get(5, TimeUnit.SECONDS));
            waitFor(() -> occupied(budget(factory)) == 0);
        } finally { source.complete("cleanup"); factory.close(); thread.shutdownNow(); }
    }

    @Test
    void singleflightOptOutKeepsIndependentBoundedOwners() throws Exception {
        CountDownLatch entered = new CountDownLatch(2), release = new CountDownLatch(1);
        try (var factory = TierCacheFactory.builder().remoteCache(new InMemoryRemoteCache<>())
                .asyncExecutorThreads(2).disableSingleflight().build()) {
            AsyncTierCache<String, String> cache = factory.asyncCache("c");
            java.util.function.Function<String, String> loader = k -> { entered.countDown(); gate(release); return "v"; };
            var first = cache.getOrComputeAsync("k", loader);
            var second = cache.getOrComputeAsync("k", loader);
            try { assertTrue(entered.await(5, TimeUnit.SECONDS)); }
            finally { release.countDown(); }
            assertEquals("v", first.toCompletableFuture().get());
            assertEquals("v", second.toCompletableFuture().get());
        } finally { release.countDown(); }
    }
}
