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
     * Submit crossing, deterministic and discriminating: a gated executor
     * parks the submission at the executor hand-off — inside the view's
     * lifecycle lock, after registration. Close must then BLOCK on the
     * monitor, and once the hand-off completes, the close's drain must fail
     * the queued stage with CancellationException. Against a
     * registration-after-execute defect the stage is abandoned instead, so
     * this test catches broken synchronization (verified on such a variant).
     */
    @Test
    void closeLandingMidSubmissionDrainsTheRegisteredStage() throws Exception {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        TierCache<String, String> cache = factory.getCache("c");

        CountDownLatch executeEntered = new CountDownLatch(1);
        CountDownLatch releaseExecute = new CountDownLatch(1);
        java.util.concurrent.Executor gated = command -> {
            executeEntered.countDown();
            awaitQuietly(releaseExecute);
            // The command is never run: the stage stays queued.
        };
        io.tiercache.internal.DefaultAsyncTierCache<String, String> view =
                new io.tiercache.internal.DefaultAsyncTierCache<>(cache, gated);

        java.util.concurrent.atomic.AtomicReference<CompletableFuture<String>> stage =
                new java.util.concurrent.atomic.AtomicReference<>();
        Thread submitter = new Thread(() -> stage.set(view.getAsync("k").toCompletableFuture()));
        submitter.start();
        assertTrue(executeEntered.await(5, TimeUnit.SECONDS),
                "the submission must reach the executor hand-off");

        Thread closer = new Thread(view::closeOutstanding);
        closer.start();
        awaitThreadState(closer, Thread.State.BLOCKED,
                "close must wait for the submission's lifecycle lock");
        releaseExecute.countDown();
        submitter.join(5_000);
        closer.join(5_000);

        assertThrows(CancellationException.class,
                () -> stage.get().get(5, TimeUnit.SECONDS),
                "a submission registered before the close snapshot must be drained");
        factory.close();
    }

    /**
     * View-creation crossing, deterministic: a probe parks view creation
     * inside the factory lifecycle lock, close is observed BLOCKED on the
     * factory monitor, and the view published after the snapshot point must
     * still be included in that close's drain (a subsequent op fails as a
     * cancelled stage). Negative control (a snapshot taken outside the
     * lock) lets the view escape — verified on such a variant.
     */
    @Test
    void viewCreatedDuringCloseIsDrainedByThatClose() throws Exception {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        CountDownLatch creationEntered = new CountDownLatch(1);
        CountDownLatch releaseCreation = new CountDownLatch(1);
        TierCacheFactory.viewCreationProbe = () -> {
            creationEntered.countDown();
            awaitQuietly(releaseCreation);
        };
        try {
            java.util.concurrent.atomic.AtomicReference<AsyncTierCache<String, String>> view =
                    new java.util.concurrent.atomic.AtomicReference<>();
            Thread creator = new Thread(() -> view.set(factory.asyncCache("late")));
            creator.start();
            assertTrue(creationEntered.await(5, TimeUnit.SECONDS),
                    "view creation must reach the publication boundary");

            Thread closer = new Thread(factory::close);
            closer.start();
            awaitThreadState(closer, Thread.State.BLOCKED,
                    "close must wait for the factory lifecycle lock");
            releaseCreation.countDown();
            creator.join(5_000);
            closer.join(5_000);

            CompletableFuture<String> stage = view.get().getAsync("k").toCompletableFuture();
            assertThrows(CancellationException.class,
                    () -> stage.get(5, TimeUnit.SECONDS),
                    "a view published before the close snapshot must be drained by that close");
        } finally {
            TierCacheFactory.viewCreationProbe = () -> {
            };
            factory.close();
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitThreadState(Thread thread, Thread.State state, String description)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != state) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(description + " (never reached " + state + ", got "
                        + thread.getState() + ")");
            }
            if (!thread.isAlive()) {
                throw new AssertionError(description + " (thread finished first: " + thread.getState()
                        + " — the crossing did not serialize)");
            }
            Thread.sleep(5);
        }
    }
}
