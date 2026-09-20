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

    /**
     * Close wins the lifecycle lock first: a submission afterwards is
     * rejected as a stage failed with CancellationException — a visible
     * failure, never an abandoned stage.
     */
    @Test
    void submissionAfterCloseFailsAsCancelledStage() {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        AsyncTierCache<String, String> cache = factory.asyncCache("c");
        factory.close();

        CompletableFuture<String> stage = cache.getAsync("k").toCompletableFuture();
        assertThrows(CancellationException.class,
                () -> stage.get(5, TimeUnit.SECONDS),
                "a submission after close must fail visibly, not hang");
    }

    /**
     * Close wins the factory lifecycle lock first: creating a view after
     * the CLOSED transition is rejected, so no view escapes the drain.
     */
    @Test
    void viewCreationAfterCloseIsRejected() {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        factory.close();

        assertThrows(IllegalStateException.class, () -> factory.asyncCache("late"),
                "a view must never be published after the close snapshot");
    }

    /**
     * Lock-order race, submit side: whichever side takes the lifecycle
     * lock first, every handed-out stage completes — success, drain
     * cancellation or post-close rejection — and none is abandoned.
     */
    @Test
    void closeRacingSubmissionNeverAbandonsAStage() throws Exception {
        for (int round = 0; round < 30; round++) {
            TierCacheFactory factory = TierCacheFactory.builder()
                    .remoteCache(new InMemoryRemoteCache<>())
                    .asyncExecutorThreads(1)
                    .build();
            AsyncTierCache<String, String> cache = factory.asyncCache("c");

            CountDownLatch gate = new CountDownLatch(1);
            CountDownLatch loaderEntered = new CountDownLatch(1);
            cache.getOrComputeAsync("gate", k -> {
                loaderEntered.countDown();
                try {
                    gate.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "g";
            });
            assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));

            java.util.List<CompletableFuture<String>> stages = new java.util.ArrayList<>();
            Thread closer = new Thread(factory::close);
            closer.start();
            for (int s = 0; s < 8; s++) {
                stages.add(cache.getAsync("k" + s).toCompletableFuture());
            }
            closer.join(5_000);
            gate.countDown();

            for (CompletableFuture<String> stage : stages) {
                try {
                    stage.get(30, TimeUnit.SECONDS);
                } catch (CancellationException | ExecutionException expected) {
                    // Drained by close or rejected after it — both are
                    // visible completions. A timeout would mean the stage
                    // was abandoned and fails the test.
                }
            }
            factory.close();
        }
    }

    /**
     * Lock-order race, view side: a view created concurrently with close
     * is either rejected with IllegalStateException or published before
     * the snapshot and therefore drained with the other views.
     */
    @Test
    void viewCreationRacingCloseIsDrainedOrRejected() throws Exception {
        for (int round = 0; round < 30; round++) {
            TierCacheFactory factory = TierCacheFactory.builder()
                    .remoteCache(new InMemoryRemoteCache<>())
                    .build();

            Thread closer = new Thread(factory::close);
            closer.start();
            AsyncTierCache<String, String> view = null;
            boolean rejected = false;
            try {
                view = factory.asyncCache("race");
            } catch (IllegalStateException e) {
                rejected = true;
            }
            closer.join(5_000);

            if (!rejected) {
                CompletableFuture<String> stage = view.getAsync("k").toCompletableFuture();
                assertThrows(CancellationException.class,
                        () -> stage.get(5, TimeUnit.SECONDS),
                        "a view published before the snapshot must be drained by close");
            }
            factory.close();
        }
    }
}
