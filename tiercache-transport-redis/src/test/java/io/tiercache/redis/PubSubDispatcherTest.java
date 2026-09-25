package io.tiercache.redis;

import io.tiercache.spi.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class PubSubDispatcherTest {
    @Test
    void slowLaneDoesNotBlockOtherLaneAndRunningPayloadKeepsItsReservation() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), other = new CountDownLatch(1);
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 3, 12))) {
            dispatcher.register("a", bytes -> { entered.countDown(); await(release); });
            dispatcher.register("b", bytes -> other.countDown());
            dispatcher.accept("a", new byte[4]); await(entered);
            dispatcher.accept("a", new byte[4]);
            assertEquals(2, dispatcher.retainedMessages());
            assertEquals(8, dispatcher.retainedBytes());
            dispatcher.accept("b", new byte[4]); await(other);
            assertEquals(1, release.getCount());
            waitFor(() -> dispatcher.retainedMessages() == 2);
            release.countDown(); waitFor(() -> dispatcher.retainedMessages() == 0);
            assertEquals(0, dispatcher.retainedBytes());
        } finally { release.countDown(); }
    }

    @Test
    void fifoAndNoConcurrentHandlersWithinOneCache() throws Exception {
        List<Integer> actual = new CopyOnWriteArrayList<>();
        AtomicInteger active = new AtomicInteger(), peak = new AtomicInteger();
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(4, 200, 200))) {
            dispatcher.register("a", bytes -> {
                peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                actual.add(Byte.toUnsignedInt(bytes[0])); active.decrementAndGet();
            });
            for (int i = 0; i < 200; i++) dispatcher.accept("a", new byte[]{(byte) i});
            waitFor(() -> actual.size() == 200);
            assertEquals(java.util.stream.IntStream.range(0, 200).boxed().toList(), actual);
            assertEquals(1, peak.get());
        }
    }

    @Test
    void overflowPurgesOnlyQueuedPayloadsAndNeverStartsRepairBeforeHandlerReturns() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger repaired = new AtomicInteger();
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 2, 8))) {
            dispatcher.recovery(recovery(cache -> { repaired.incrementAndGet(); return success(); }));
            dispatcher.register("a", bytes -> { entered.countDown(); await(release); });
            dispatcher.accept("a", new byte[4]); await(entered);
            dispatcher.accept("a", new byte[4]);
            dispatcher.accept("a", new byte[4]);
            assertTrue(dispatcher.pending("a"));
            assertEquals(1, dispatcher.retainedMessages());
            assertEquals(4, dispatcher.retainedBytes());
            assertEquals(0, repaired.get());
            release.countDown(); waitFor(() -> !dispatcher.pending("a"));
            assertEquals(0, dispatcher.retainedMessages()); assertEquals(0, dispatcher.retainedBytes());
            assertEquals(1, repaired.get());
        } finally { release.countDown(); }
    }

    @Test
    void bytesAndOversizeFramesEnforceAggregateBound() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 10, 4))) {
            dispatcher.register("a", bytes -> { entered.countDown(); await(release); });
            dispatcher.register("b", bytes -> fail("rejected frame"));
            dispatcher.accept("a", new byte[4]); await(entered);
            dispatcher.accept("b", new byte[1]);
            assertTrue(dispatcher.pending("b")); assertEquals(4, dispatcher.retainedBytes());
            dispatcher.accept("b", new byte[5]);
            assertEquals(1, dispatcher.retainedMessages());
        } finally { release.countDown(); }
    }

    @Test
    void lossDuringRepairRequiresAnotherAttemptWithoutNewTraffic() throws Exception {
        var first = new CompletableFuture<RecoveryResult>();
        AtomicInteger attempts = new AtomicInteger();
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 2, 2))) {
            dispatcher.register("a", bytes -> fail("oversize"));
            dispatcher.recovery(recovery(cache -> attempts.incrementAndGet() == 1 ? first : success()));
            dispatcher.accept("a", new byte[3]);
            waitFor(() -> attempts.get() == 1);
            dispatcher.accept("a", new byte[1]);
            first.complete(safe());
            waitFor(() -> !dispatcher.pending("a"));
            assertEquals(2, attempts.get());
            waitFor(() -> dispatcher.controlGroups() == 0);
        }
    }

    @Test
    void thrownHandlerReleasesReservationAndRecoversOutsideGate() throws Exception {
        AtomicInteger repaired = new AtomicInteger();
        AtomicBoolean handlerOutside = new AtomicBoolean(), handlerWorker = new AtomicBoolean();
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 2, 2))) {
            var field = PubSubDispatcher.class.getDeclaredField("gate"); field.setAccessible(true);
            Object gate = field.get(dispatcher);
            dispatcher.register("a", bytes -> {
                handlerOutside.set(!Thread.holdsLock(gate));
                handlerWorker.set(Thread.currentThread().getName().startsWith("tiercache-invalidation-"));
                throw new IllegalArgumentException("decoder or handler failure");
            });
            dispatcher.recovery(recovery(cache -> {
                assertFalse(Thread.holdsLock(gate)); repaired.incrementAndGet(); return success();
            }));
            dispatcher.accept("a", new byte[1]);
            waitFor(() -> repaired.get() == 1 && !dispatcher.pending("a"));
            assertTrue(handlerOutside.get()); assertTrue(handlerWorker.get());
            assertEquals(0, dispatcher.retainedMessages()); assertEquals(0, dispatcher.retainedBytes());
        }
    }

    @Test
    void fenceWaitsForRunningHandlerAndRetiresQueuedUpdates() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger applied = new AtomicInteger();
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 3, 3))) {
            dispatcher.register("a", bytes -> { applied.incrementAndGet(); entered.countDown(); await(release); });
            dispatcher.accept("a", new byte[1]); await(entered);
            dispatcher.accept("a", new byte[1]);
            var fence = dispatcher.fence("a").toCompletableFuture();
            assertFalse(fence.isDone()); assertEquals(1, dispatcher.retainedMessages());
            release.countDown();
            fence.get(5, TimeUnit.SECONDS).release();
            waitFor(() -> !dispatcher.pending("a"));
            assertEquals(1, applied.get()); assertEquals(0, dispatcher.retainedMessages());
        } finally { release.countDown(); }
    }

    @Test
    void replacementWaitsForOldRunningHandlerAndOldCloseCannotRemoveNewLane() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), replacement = new CountDownLatch(1);
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 3, 3))) {
            var old = dispatcher.register("a", bytes -> { entered.countDown(); await(release); });
            dispatcher.accept("a", new byte[1]); await(entered);
            dispatcher.accept("a", new byte[1]);
            dispatcher.register("a", bytes -> { assertEquals(0, release.getCount()); replacement.countDown(); });
            old.close(); dispatcher.accept("a", new byte[1]);
            release.countDown(); await(replacement);
            waitFor(() -> dispatcher.retainedMessages() == 0);
        } finally { release.countDown(); }
    }

    @Test
    void hungRepairChurnRetainsAtMostTheControlBudget() throws Exception {
        List<CompletableFuture<RecoveryResult>> stages = new CopyOnWriteArrayList<>();
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 2, 2))) {
            dispatcher.recovery(recovery(cache -> {
                var stage = new CompletableFuture<RecoveryResult>(); stages.add(stage); return stage;
            }));
            for (int i = 0; i < 2; i++) {
                dispatcher.register("a", bytes -> fail("oversize"));
                dispatcher.accept("a", new byte[3]);
                int size = i + 1; waitFor(() -> stages.size() == size);
            }
            dispatcher.register("a", bytes -> fail("oversize")); dispatcher.accept("a", new byte[3]);
            assertEquals(2, dispatcher.controlGroups());
            assertTrue(dispatcher.pending("a"));
            stages.get(0).complete(safe());
            waitFor(() -> stages.size() == 3);
            assertEquals(2, dispatcher.controlGroups());
            dispatcher.close();
            stages.forEach(stage -> stage.complete(safe()));
            waitFor(() -> dispatcher.controlGroups() == 0);
            assertEquals(0, dispatcher.retainedMessages());
        }
    }

    @Test
    void quantumYieldsToAnotherReadyCache() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var second = new CountDownLatch(1);
        AtomicInteger firstCount = new AtomicInteger(), atSecond = new AtomicInteger();
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(1, 200, 200))) {
            dispatcher.register("a", bytes -> {
                if (firstCount.incrementAndGet() == 1) { entered.countDown(); await(release); }
            });
            dispatcher.register("b", bytes -> { atSecond.set(firstCount.get()); second.countDown(); });
            dispatcher.accept("a", new byte[1]); await(entered);
            for (int i = 0; i < 100; i++) dispatcher.accept("a", new byte[1]);
            dispatcher.accept("b", new byte[1]); release.countDown(); await(second);
            assertEquals(32, atSecond.get());
        } finally { release.countDown(); }
    }

    @Test
    void closePurgesQueuedWorkButRunningWorkRetiresOnlyAfterItsHandler() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 3, 12));
        try {
            dispatcher.register("a", bytes -> {
                entered.countDown();
                boolean finished = false;
                while (!finished) {
                    try { finished = release.await(5, TimeUnit.SECONDS); }
                    catch (InterruptedException ignored) { /* Simulated noninterruptible application code. */ }
                }
            });
            dispatcher.accept("a", new byte[4]); await(entered);
            dispatcher.accept("a", new byte[4]);
            dispatcher.close(); dispatcher.close();
            assertEquals(1, dispatcher.retainedMessages()); assertEquals(4, dispatcher.retainedBytes());
            dispatcher.accept("a", new byte[4]);
            assertEquals(1, dispatcher.retainedMessages());
            release.countDown(); waitFor(() -> dispatcher.retainedMessages() == 0);
            assertEquals(0, dispatcher.retainedBytes());
        } finally { release.countDown(); dispatcher.close(); }
    }

    @Test
    void lateFenceReleaseCannotReleaseItsSuccessor() throws Exception {
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 3, 3))) {
            dispatcher.register("a", bytes -> { });
            var first = dispatcher.fence("a").toCompletableFuture().get(5, TimeUnit.SECONDS);
            first.release();
            var second = dispatcher.fence("a").toCompletableFuture().get(5, TimeUnit.SECONDS);
            first.release();
            assertTrue(dispatcher.pending("a"));
            second.release();
            assertFalse(dispatcher.pending("a"));
        }
    }

    @Test
    void retiredRepairCallbackDoesNotRetainOldHandlerAndGaugesCloseOnce() throws Exception {
        var stages = new CopyOnWriteArrayList<CompletableFuture<RecoveryResult>>();
        AtomicInteger gauges = new AtomicInteger(), duplicateCloses = new AtomicInteger();
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 2, 2))) {
            dispatcher.metrics(new CacheMetricsListener() {
                public AutoCloseable registerDispatchPending(String cache, java.util.function.BooleanSupplier state) {
                    gauges.incrementAndGet();
                    var closed = new AtomicBoolean();
                    return () -> {
                        if (closed.compareAndSet(false, true)) gauges.decrementAndGet();
                        else duplicateCloses.incrementAndGet();
                    };
                }
            });
            dispatcher.recovery(recovery(cache -> {
                var stage = new CompletableFuture<RecoveryResult>(); stages.add(stage); return stage;
            }));
            var weak = retiredHandler(dispatcher, stages);
            dispatcher.register("a", bytes -> { });
            assertEquals(1, gauges.get());
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (weak.get() != null && System.nanoTime() < end) { System.gc(); Thread.sleep(10); }
            assertNull(weak.get(), "external repair stage must not retain the retired handler's target");
            assertEquals(1, dispatcher.controlGroups());
            stages.get(0).complete(safe()); waitFor(() -> dispatcher.controlGroups() == 0);
            dispatcher.close(); assertEquals(0, gauges.get()); assertEquals(0, duplicateCloses.get());
        }
    }

    private static java.lang.ref.WeakReference<Object> retiredHandler(PubSubDispatcher dispatcher,
            List<CompletableFuture<RecoveryResult>> stages) throws Exception {
        Object target = new Object();
        var weak = new java.lang.ref.WeakReference<>(target);
        var handle = dispatcher.register("a", bytes -> { target.hashCode(); });
        dispatcher.accept("a", new byte[3]); waitFor(() -> stages.size() == 1);
        handle.close(); handle.close();
        return weak;
    }

    @Test
    void throwingMetricsCannotBreakBudgetReleaseOrRepair() throws Exception {
        var repairs = new AtomicInteger();
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 2, 2))) {
            dispatcher.metrics(new CacheMetricsListener() {
                public void onDispatchRejected(DispatchReason reason, long count) { throw new IllegalStateException("observer"); }
                public void onDispatchRepair(String cache, RecoveryResult.Status status) { throw new IllegalStateException("observer"); }
                public AutoCloseable registerDispatch(java.util.function.LongSupplier count, java.util.function.LongSupplier bytes) {
                    return () -> { throw new IllegalStateException("observer close"); };
                }
            });
            dispatcher.register("a", bytes -> { throw new IllegalStateException("application"); });
            dispatcher.recovery(recovery(cache -> { repairs.incrementAndGet(); return success(); }));
            dispatcher.accept("a", new byte[1]);
            waitFor(() -> repairs.get() == 1 && !dispatcher.pending("a"));
            assertEquals(0, dispatcher.retainedMessages()); assertEquals(0, dispatcher.retainedBytes());
            waitFor(() -> dispatcher.controlGroups() == 0);
        }
    }

    @Test
    void customStageThrowingAfterAttachmentCannotCreateUnboundedRetries() throws Exception {
        var calls = new AtomicInteger();
        var external = new CompletableFuture<RecoveryResult>() {
            @Override public CompletableFuture<RecoveryResult> whenComplete(java.util.function.BiConsumer<? super RecoveryResult, ? super Throwable> action) {
                super.whenComplete(action);
                throw new IllegalStateException("retained before throwing");
            }
        };
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 1, 1))) {
            dispatcher.recovery(recovery(cache -> { calls.incrementAndGet(); return external; }));
            dispatcher.register("a", bytes -> { }); dispatcher.accept("a", new byte[2]);
            waitFor(() -> calls.get() == 1);
            dispatcher.register("a", bytes -> { }); dispatcher.accept("a", new byte[2]);
            assertEquals(1, dispatcher.controlGroups());
            assertEquals(1, calls.get());
            dispatcher.close(); external.complete(safe());
            waitFor(() -> dispatcher.controlGroups() == 0);
        }
    }

    @Test
    void blockedFenceNotificationKeepsItsControlCreditAfterRetirement() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var attempts = new AtomicInteger();
        var gateHandler = new CountDownLatch(1); var handlerEntered = new CountDownLatch(1);
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 1, 1))) {
            var old = dispatcher.register("a", bytes -> { handlerEntered.countDown(); await(gateHandler); });
            dispatcher.recovery(recovery(cache -> { attempts.incrementAndGet(); return success(); }));
            dispatcher.accept("a", new byte[1]); await(handlerEntered);
            var fence = dispatcher.fence("a");
            fence.whenComplete((value, error) -> { entered.countDown(); await(release); });
            gateHandler.countDown(); await(entered);
            old.close();
            dispatcher.register("b", bytes -> { }); dispatcher.accept("b", new byte[2]);
            var independent = new CountDownLatch(1);
            dispatcher.register("c", bytes -> independent.countDown()); dispatcher.accept("c", new byte[1]);
            await(independent);
            assertEquals(1, dispatcher.controlGroups()); assertEquals(0, attempts.get());
            release.countDown();
            waitFor(() -> !dispatcher.pending("b"));
            assertEquals(1, attempts.get());
        } finally { release.countDown(); gateHandler.countDown(); }
    }

    @Test
    void retryDeadlineHandlesNegativeNanoTimeAndCounterWrap() {
        assertTrue(PubSubDispatcher.due(Long.MIN_VALUE + 100, 0), "an unset retry is immediately eligible");
        long now = Long.MAX_VALUE - 50;
        long deadline = now + 100;
        assertFalse(PubSubDispatcher.due(now, deadline));
        assertTrue(PubSubDispatcher.due(now + 100, deadline));
        assertTrue(PubSubDispatcher.due(now + 150, deadline));
    }

    @Test
    void fenceAfterUnsubscribeStillWaitsForTheRetiredRunningHandler() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var dispatcher = new PubSubDispatcher(new PubSubDispatchOptions(2, 2, 2))) {
            var subscription = dispatcher.register("a", bytes -> { entered.countDown(); await(release); });
            dispatcher.accept("a", new byte[1]); await(entered);
            subscription.close();
            var fence = dispatcher.fence("a").toCompletableFuture();
            assertFalse(fence.isDone(), "retired running delivery is not yet quiescent");
            assertEquals(1, dispatcher.controlGroups());
            release.countDown(); fence.get(5, TimeUnit.SECONDS).release();
            waitFor(() -> dispatcher.controlGroups() == 0 && dispatcher.retainedMessages() == 0);
            var delivered = new CountDownLatch(1);
            dispatcher.register("a", bytes -> delivered.countDown());
            dispatcher.accept("a", new byte[1]); await(delivered);
        } finally { release.countDown(); }
    }

    @Test
    void positiveOptionsAndOverflowSafeByteAccounting() {
        assertEquals(new PubSubDispatchOptions(2, 1024, 16_777_216), PubSubDispatchOptions.DEFAULT);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new PubSubDispatchOptions(0, 1, 1))
                .getMessage().contains("dispatch-threads"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new PubSubDispatchOptions(1, -1, 1))
                .getMessage().contains("max-pending-messages"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new PubSubDispatchOptions(1, 1, 0))
                .getMessage().contains("max-pending-bytes"));
        assertEquals(Long.MAX_VALUE, new PubSubDispatchOptions(1, 1, Long.MAX_VALUE).maxPendingBytes());
    }

    static RecoveryResult safe() { return new RecoveryResult(RecoveryResult.Status.RESET_SAFE, "1-0", 1); }
    static CompletionStage<RecoveryResult> success() { return CompletableFuture.completedFuture(safe()); }
    static InvalidationGapHandler recovery(java.util.function.Function<String, CompletionStage<RecoveryResult>> work) {
        return new InvalidationGapHandler() {
            public CompletionStage<RecoveryResult> reset(String cache) { return work.apply(cache); }
            public RecoveryResult registrationBaseline(String cache) { return null; }
            public boolean isCurrent(String cache, RecoveryResult result) { return true; }
        };
    }
    static void await(CountDownLatch latch) {
        try { assertTrue(latch.await(5, TimeUnit.SECONDS)); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
    }
    interface Check { boolean get(); }
    static void waitFor(Check check) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!check.get()) { if (System.nanoTime() > end) fail("condition timed out"); Thread.sleep(1); }
    }
}
