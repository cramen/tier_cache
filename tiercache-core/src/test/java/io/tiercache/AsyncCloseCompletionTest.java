package io.tiercache;

import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression (reviewer-reported): closing the factory must fail queued
 * async operations with a visible cancellation instead of abandoning their
 * stages — before the fix, `shutdownNow` silently dropped queued tasks and
 * their futures never completed.
 */
class AsyncCloseCompletionTest {

    @Test
    void closeFailsQueuedOperationsInsteadOfAbandoningThem() throws Exception {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .asyncExecutorThreads(1)
                .build();
        AsyncTierCache<String, String> cache = factory.asyncCache("c");

        // A stage that completed before close must be unaffected.
        CompletableFuture<String> completedBefore =
                cache.getOrComputeAsync("x", k -> "vx").toCompletableFuture();
        assertEquals("vx", completedBefore.get(5, TimeUnit.SECONDS));

        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch loaderEntered = new CountDownLatch(1);
        // Occupy the single worker with a gated load; the next call queues.
        cache.getOrComputeAsync("a", k -> {
            loaderEntered.countDown();
            try {
                gate.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "va";
        });
        assertTrue(loaderEntered.await(5, TimeUnit.SECONDS),
                "the gated loader must be inside before queueing");
        CompletableFuture<String> queued = cache.getAsync("b").toCompletableFuture();

        factory.close();

        // CompletableFuture#get rethrows a bare CancellationException.
        assertThrows(CancellationException.class,
                () -> queued.get(30, TimeUnit.SECONDS),
                "the queued operation must be failed by close, never abandoned");
        assertEquals("vx", completedBefore.get(1, TimeUnit.SECONDS),
                "stages completed before close keep their results");
        gate.countDown();
    }
}
