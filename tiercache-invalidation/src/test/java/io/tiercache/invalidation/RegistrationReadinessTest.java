package io.tiercache.invalidation;

import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.InMemoryJournal;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

class RegistrationReadinessTest {
    static final class Transport implements InvalidationTransport {
        volatile CompletableFuture<InvalidationDeliveryFence> fence = CompletableFuture.completedFuture(InvalidationDeliveryFence.NOOP);
        final AtomicInteger fenceCalls = new AtomicInteger(), subscriptions = new AtomicInteger();
        final ThreadLocal<Boolean> delivery = ThreadLocal.withInitial(() -> false);
        volatile InvalidationGapHandler gaps;
        volatile boolean failSubscribe;
        final Map<String, Consumer<InvalidationMessage>> handlers = new ConcurrentHashMap<>();
        public void publish(InvalidationMessage message) { }
        public AutoCloseable subscribe(String cache, Consumer<InvalidationMessage> handler) {
            if (failSubscribe) throw new IllegalStateException("subscription failed");
            subscriptions.incrementAndGet(); handlers.put(cache, handler); return () -> handlers.remove(cache, handler);
        }
        public CompletionStage<InvalidationDeliveryFence> fenceDelivery(String cache) { fenceCalls.incrementAndGet(); return fence; }
        public void setGapHandler(InvalidationGapHandler handler) { gaps = handler; }
        public boolean requiresRegistrationReset() { return true; }
        public boolean isDeliveryThread() { return delivery.get(); }
        public void close() { }
    }
    @Test void readinessWaitsForFenceBeforeBaselineAndClear() throws Exception {
        var transport = new Transport(); transport.fence = new CompletableFuture<>();
        var journal = new RecoveryProtocolTest.Journal();
        var target = new RecoveryProtocolTest.Target();
        try (var service = new InvalidationService(transport, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
            var ready = service.registerTargetAsync("c", target).toCompletableFuture();
            await(() -> transport.fenceCalls.get() == 1);
            assertFalse(ready.isDone()); assertEquals(0, journal.ends.get()); assertEquals(0, target.clears.get());
            transport.fence.complete(InvalidationDeliveryFence.NOOP);
            ready.get(5, TimeUnit.SECONDS);
            assertEquals(1, target.clears.get()); assertEquals(1, transport.subscriptions.get());
        }
    }
    @Test void replacementCoalescesFenceAndRetiresOldReadiness() throws Exception {
        var transport = new Transport(); transport.fence = new CompletableFuture<>();
        var old = new RecoveryProtocolTest.Target(); var current = new RecoveryProtocolTest.Target();
        try (var service = new InvalidationService(transport, new InMemoryJournal(100), UUID.randomUUID(), InvalidationListener.NOOP)) {
            var first = service.registerTargetAsync("c", old).toCompletableFuture();
            await(() -> transport.fenceCalls.get() == 1);
            var second = service.registerTargetAsync("c", current).toCompletableFuture();
            assertInstanceOf(CancellationException.class, cause(first));
            transport.fence.complete(InvalidationDeliveryFence.NOOP);
            second.get(5, TimeUnit.SECONDS);
            assertEquals(1, transport.fenceCalls.get()); assertEquals(0, old.clears.get()); assertEquals(1, current.clears.get());
        }
    }
    @Test void closedPendingRegistrationCannotClearOrSubscribeLater() throws Exception {
        var transport = new Transport(); transport.fence = new CompletableFuture<>();
        var target = new RecoveryProtocolTest.Target();
        var service = new InvalidationService(transport, new InMemoryJournal(100), UUID.randomUUID(), InvalidationListener.NOOP);
        var ready = service.registerTargetAsync("c", target).toCompletableFuture();
        await(() -> transport.fenceCalls.get() == 1);
        service.close(); assertInstanceOf(CancellationException.class, cause(ready));
        transport.fence.complete(InvalidationDeliveryFence.NOOP);
        assertEquals(0, target.clears.get()); assertEquals(0, transport.subscriptions.get());
    }
    @Test void reentrantSyncRegistrationIsRejectedAndAsyncRemainsAvailable() throws Exception {
        var transport = new Transport();
        try (var service = new InvalidationService(transport, null, UUID.randomUUID(), InvalidationListener.NOOP)) {
            transport.delivery.set(true);
            try {
                assertTrue(assertThrows(IllegalStateException.class, () -> service.registerTarget("c", new RecoveryProtocolTest.Target()))
                        .getMessage().contains("registerTargetAsync"));
                var ready = service.registerTargetAsync("c", new RecoveryProtocolTest.Target());
                ready.toCompletableFuture().get(5, TimeUnit.SECONDS);
            } finally { transport.delivery.remove(); }
        }
    }
    @Test void baselineAndSubscribeFailuresAreVisibleAndRetryable() throws Exception {
        for (boolean baseline : new boolean[]{true, false}) {
            var transport = new Transport(); var journal = new RecoveryProtocolTest.Journal();
            if (baseline) journal.end = cache -> { throw new IllegalStateException("baseline failed"); };
            else transport.failSubscribe = true;
            try (var service = new InvalidationService(transport, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
                assertInstanceOf(IllegalStateException.class,
                        cause(service.registerTargetAsync("c", new RecoveryProtocolTest.Target()).toCompletableFuture()));
                journal.end = journal.data::endCursor; transport.failSubscribe = false;
                service.registerTargetAsync("c", new RecoveryProtocolTest.Target()).toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertEquals(1, transport.subscriptions.get());
            }
        }
    }
    @Test void intactLongLocalCatchupDoesNotClearAndDoesNotAuthorizeStreamsSkipping() throws Exception {
        var transport = new Transport(); var journal = new InMemoryJournal(20_000); var target = new RecoveryProtocolTest.Target();
        try (var service = new InvalidationService(transport, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
            service.registerTarget("c", target);
            target.values.put("unaffected", RecoveryProtocolTest.update("c", "unaffected", 1));
            int clears = target.clears.get();
            for (int i = 0; i < 5000; i++) journal.append("c", RecoveryProtocolTest.update("c", "key-" + i, i + 2));
            var result = transport.gaps.recoverLocalGap("c").toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(RecoveryResult.Status.CAUGHT_UP, result.status());
            assertTrue(transport.gaps.isLocalRecoveryCurrent("c", result));
            assertFalse(transport.gaps.isCurrent("c", result));
            assertEquals(clears, target.clears.get()); assertEquals(5001, target.values.size());
        }
    }
    @Test void resetDoesNotAcquireBaselineBeforeFenceAndNoJournalClearIsAValidLocalProof() throws Exception {
        var transport = new Transport(); var target = new RecoveryProtocolTest.Target();
        try (var service = new InvalidationService(transport, null, UUID.randomUUID(), InvalidationListener.NOOP)) {
            service.registerTarget("c", target); int clears = target.clears.get();
            transport.fence = new CompletableFuture<>();
            var repair = transport.gaps.recoverLocalGap("c").toCompletableFuture();
            await(() -> transport.fenceCalls.get() == 2);
            assertEquals(clears, target.clears.get()); assertFalse(repair.isDone());
            transport.fence.complete(InvalidationDeliveryFence.NOOP);
            var result = repair.get(5, TimeUnit.SECONDS);
            assertEquals(RecoveryResult.Status.NO_JOURNAL, result.status());
            assertTrue(transport.gaps.isLocalRecoveryCurrent("c", result));
            assertEquals(clears + 1, target.clears.get());
        }
    }
    @Test void serviceDeliveryCallbackRejectsSyncRegistrationEvenForLegacyTransport() throws Exception {
        var transport = new Transport();
        var rejection = new AtomicReference<Throwable>();
        var asynchronous = new AtomicReference<CompletionStage<Void>>();
        try (var service = new InvalidationService(transport, null, UUID.randomUUID(), InvalidationListener.NOOP)) {
            service.registerTarget("c", new RecoveryProtocolTest.Target());
            service.setEventListener((cache, message) -> {
                try { service.registerTarget("other", new RecoveryProtocolTest.Target()); }
                catch (Throwable failure) { rejection.set(failure); }
                asynchronous.set(service.registerTargetAsync("other", new RecoveryProtocolTest.Target()));
            });
            transport.handlers.get("c").accept(RecoveryProtocolTest.update("c", "key", 1));
            assertInstanceOf(IllegalStateException.class, rejection.get());
            asynchronous.get().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    static Throwable cause(CompletableFuture<?> result) {
        Throwable error = assertThrows(Exception.class, () -> result.get(5, TimeUnit.SECONDS));
        while (error instanceof CompletionException || error instanceof ExecutionException) error = error.getCause();
        return error;
    }
    interface Check { boolean get(); }
    static void await(Check check) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!check.get()) { if (System.nanoTime() > deadline) fail("condition timed out"); Thread.sleep(1); }
    }
}
