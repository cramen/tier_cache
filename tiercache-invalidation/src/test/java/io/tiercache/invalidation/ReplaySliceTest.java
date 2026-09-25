package io.tiercache.invalidation;

import io.tiercache.*;
import io.tiercache.spi.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;
import static io.tiercache.invalidation.RecoveryProtocolTest.*;

class ReplaySliceTest {
    static final class Transport implements InvalidationTransport {
        Consumer<InvalidationMessage> live;
        InvalidationGapHandler gaps;
        public void publish(InvalidationMessage message) { live.accept(message); }
        public AutoCloseable subscribe(String cache, Consumer<InvalidationMessage> handler) { live = handler; return () -> { }; }
        public void setGapHandler(InvalidationGapHandler handler) { gaps = handler; }
        public void close() { }
    }
    @Test void liveDeliveryEntersAfter32RowsWithoutAnotherJournalRead() throws Exception {
        var journal = new Journal(); var target = new Target(); var transport = new Transport();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var first = new AtomicBoolean(true);
        try (var service = new InvalidationService(transport, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
            service.registerTarget("c", target);
            var cursors = new ArrayList<String>();
            for (int i = 1; i <= 256; i++) cursors.add(journal.append("c", update("c", "key-" + i, i)));
            service.setEventListener((cache, message) -> {
                if (first.compareAndSet(true, false)) { entered.countDown(); gate(release); }
            });
            var recovery = service.recoverAsync(Runnable::run).toCompletableFuture();
            gate(entered);
            try {
                assertEquals(32, target.values.size(), "a notification boundary must precede the full read batch");
                assertEquals(cursors.get(31), field(state(service, "c"), "cursor"));
                assertEquals(1, journal.reads.get());
                assertEquals(true, field(state(service, "c"), "pending")); assertFalse(recovery.isDone());
                var live = update("c", "live", 1000); journal.append("c", live); transport.publish(live);
                assertTrue(target.values.containsKey("live"));
            } finally { release.countDown(); }
            assertTrue(recovery.get(5, TimeUnit.SECONDS));
            assertEquals(257, target.values.size());
            assertEquals(3, journal.reads.get(), "slice boundaries must not multiply checked reads");
            assertEquals(journal.endCursor("c"), field(state(service, "c"), "cursor"));
        } finally { release.countDown(); }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"clear", "replace", "restart", "close"})
    void boundaryMutationDiscardsOnlyUncommittedTail(String mutation) throws Exception {
        var journal = new Journal(); var changed = new AtomicBoolean(); var staleApplication = new AtomicBoolean();
        class WatchingTarget extends Target {
            @Override public long applyRecovery(InvalidationMessage message, long epoch) {
                if (changed.get() && journal.reads.get() == 1) staleApplication.set(true);
                return super.applyRecovery(message, epoch);
            }
        }
        var target = new WatchingTarget(); var replacement = new WatchingTarget();
        var notifications = new AtomicInteger(); var outsideGate = new AtomicBoolean(true);
        try (var service = new InvalidationService(new Transport(), journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
            service.registerTarget("c", target);
            Object state = state(service, "c");
            for (int i = 1; i <= 256; i++) journal.append("c", update("c", "k" + i, i));
            var once = new AtomicBoolean();
            service.setEventListener((cache, message) -> {
                notifications.incrementAndGet();
                if (Thread.holdsLock(state)) outsideGate.set(false);
                if (once.compareAndSet(false, true)) {
                    changed.set(true);
                    switch (mutation) {
                        case "clear" -> target.evictAllL1();
                        case "replace" -> service.registerTargetAsync("c", replacement);
                        case "restart" -> service.recoverAsync(Runnable::run);
                        case "close" -> service.close();
                        default -> throw new AssertionError(mutation);
                    }
                    // Listener failures must not suppress the rest of this committed prefix.
                    throw new IllegalStateException("intentional listener failure");
                }
            });
            var recovery = service.recoverAsync(Runnable::run).toCompletableFuture();
            assertEquals(!mutation.equals("close"), recovery.get(5, TimeUnit.SECONDS));
            await(() -> notifications.get() >= (mutation.equals("close") ? 32 : 256));
            assertTrue(outsideGate.get()); assertFalse(staleApplication.get());
            if (mutation.equals("close")) {
                assertEquals(32, target.values.size()); assertEquals(32, notifications.get());
                assertEquals("32", field(state, "cursor")); assertEquals(1, journal.reads.get());
            } else {
                assertEquals("256", field(state, "cursor")); assertEquals(3, journal.reads.get());
                assertEquals(256, notifications.get());
                if (mutation.equals("replace")) { assertEquals(32, target.values.size()); assertEquals(224, replacement.values.size()); }
                else assertEquals(mutation.equals("clear") ? 224 : 256, target.values.size());
            }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void committedNotificationsSurviveRejectedOrThrowingRow(boolean throwsError) throws Exception {
        var journal = new Journal(); var once = new AtomicBoolean(); var notified = new CopyOnWriteArrayList<String>();
        var target = new Target() {
            @Override public long applyRecovery(InvalidationMessage message, long epoch) {
                if (message.key().equals("k17") && once.compareAndSet(false, true)) {
                    if (throwsError) throw new IllegalStateException("intentional target failure");
                    generation.incrementAndGet(); return -1;
                }
                return super.applyRecovery(message, epoch);
            }
        };
        try (var service = new InvalidationService(new Transport(), journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
            service.registerTarget("c", target);
            for (int i = 1; i <= 64; i++) journal.append("c", update("c", "k" + i, i));
            service.setEventListener((cache, message) -> notified.add((String) message.key()));
            var recovery = service.recoverAsync(Runnable::run).toCompletableFuture();
            assertEquals(!throwsError, recovery.get(5, TimeUnit.SECONDS));
            if (throwsError) {
                assertEquals(16, notified.size(), "failure must retain committed notifications");
                assertEquals("16", field(state(service, "c"), "cursor"));
                await(() -> notified.size() == 64);
            }
            assertEquals(64, notified.size()); assertEquals(64, new HashSet<>(notified).size());
            assertEquals("k1", notified.get(0)); assertEquals("k64", notified.get(63));
            assertEquals(64, target.values.size()); assertEquals("64", field(state(service, "c"), "cursor"));
        }
    }

    @Test void tickSpansSlicesButStopsAtFirstUnknownRow() throws Exception {
        var journal = new Journal(); var target = new Target(); var transport = new Transport(); var origin = UUID.randomUUID();
        try (var scheduler = new Manual();
             var service = new InvalidationService(transport, journal, origin, InvalidationListener.NOOP)) {
            service.configureRecoveryExecutor(scheduler); service.registerTarget("c", target);
            var rows = new ArrayList<InvalidationMessage>();
            for (int i = 1; i <= 105; i++) {
                var row = i <= 40 ? new InvalidationMessage("c", "k" + i, new Version(i, origin), origin,
                        InvalidationMessage.Type.UPDATE, "v" + i) : update("c", "k" + i, i);
                rows.add(row); journal.append("c", row);
            }
            for (int i = 41; i <= 105; i++) if (i != 81) transport.publish(rows.get(i - 1));
            scheduler.drain();
            assertEquals("80", field(state(service, "c"), "cursor")); assertEquals(1, journal.reads.get());
            assertFalse(target.values.containsKey("k81"));
            var recovery = service.recoverAsync(Runnable::run).toCompletableFuture(); scheduler.drain();
            assertTrue(recovery.get()); assertEquals("105", field(state(service, "c"), "cursor"));
            assertEquals(3, journal.reads.get()); assertEquals(65, target.values.size());
            assertEquals(0, target.clears.get());
        }
    }

    @Test void clearsAcrossSlicesPreserveEpochAndRequireEmptyProof() throws Exception {
        var journal = new Journal(); var target = new Target(); var origin = UUID.randomUUID();
        try (var service = new InvalidationService(new Transport(), journal, origin, InvalidationListener.NOOP)) {
            service.registerTarget("c", target);
            for (int i = 1; i <= 96; i++) {
                // Include both remote and own full clears in the retained response.
                UUID writer = i == 65 ? origin : WRITER;
                journal.append("c", i == 33 || i == 65
                        ? new InvalidationMessage("c", null, new Version(i, writer), writer, InvalidationMessage.Type.EVICT_ALL)
                        : update("c", "k" + i, i));
            }
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            journal.read = (c, p, n) -> {
                if ("96".equals(p)) { entered.countDown(); gate(release); }
                return journal.data.checkedRead(c, p, n);
            };
            var recovery = service.recoverAsync(Runnable::run).toCompletableFuture();
            try {
                gate(entered); assertFalse(recovery.isDone());
                assertEquals(true, field(state(service, "c"), "pending"));
                assertEquals(2, target.clears.get()); assertEquals(31, target.values.size());
            } finally { release.countDown(); }
            assertTrue(recovery.get(5, TimeUnit.SECONDS)); assertEquals(2, journal.reads.get());
            assertEquals("96", field(state(service, "c"), "cursor"));
        }
    }

    @Test void retainedProofSurvivesTrimUntilNextActualRead() throws Exception {
        var journal = new Journal(); var target = new Target(); var notifications = new AtomicInteger();
        var emptyProof = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var service = new InvalidationService(new Transport(), journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
            service.registerTarget("c", target);
            for (int i = 1; i <= 256; i++) journal.append("c", update("c", "k" + i, i));
            journal.read = (c, p, n) -> {
                if (!"256".equals(p)) return journal.data.checkedRead(c, p, n);
                emptyProof.countDown(); gate(release);
                throw new IllegalStateException("trim metadata unavailable after retained proof");
            };
            service.setEventListener((cache, message) -> {
                if (notifications.incrementAndGet() == 1) {
                    // Trim all rows from Redis after a checked response was fetched.
                    for (int i = 257; i <= 20512; i++) journal.append("c", update("c", "later", i));
                }
            });
            var recovery = service.recoverAsync(Runnable::run).toCompletableFuture();
            try {
                gate(emptyProof); assertFalse(recovery.isDone());
                assertEquals(256, notifications.get()); assertEquals(256, target.values.size());
                assertEquals("256", field(state(service, "c"), "cursor"));
                assertEquals(2, journal.reads.get());
            } finally { release.countDown(); }
            assertTrue(recovery.get(5, TimeUnit.SECONDS), "a failed next read uses the existing safe-reset fallback");
            assertEquals(RecoveryResult.Status.RESET_SAFE,
                    ((RecoveryResult) field(state(service, "c"), "lastResult")).status());
            assertEquals(1, target.clears.get()); assertTrue(target.values.isEmpty());
            assertEquals(journal.endCursor("c"), field(state(service, "c"), "cursor"));
            assertEquals(true, field(state(service, "c"), "pending"));
        } finally { release.countDown(); }
    }

    @Test void mixedAndDuplicateRowsRemainVersionOrderedAcrossSlices() throws Exception {
        var journal = new Journal(); var target = new Target();
        try (var service = new InvalidationService(new Transport(), journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
            service.registerTarget("c", target);
            for (int i = 1; i <= 64; i++) journal.append("c", update("c", "k" + i, i));
            for (int i = 1; i <= 64; i++) {
                journal.append("c", new InvalidationMessage("c", "k" + i, new Version(100 + i, WRITER), WRITER,
                        InvalidationMessage.Type.INVALIDATE));
                if (i % 2 == 0) {
                    var newer = update("c", "k" + i, 200 + i);
                    journal.append("c", newer); journal.append("c", newer);
                }
            }
            assertTrue(service.recoverAsync(Runnable::run).toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals(32, target.values.size()); assertEquals(2, journal.reads.get());
            for (int i = 1; i <= 64; i++) {
                assertEquals(i % 2 == 0 ? new Version(200 + i, WRITER) : null, target.versionOfL1Entry("k" + i));
            }
            assertEquals(journal.endCursor("c"), field(state(service, "c"), "cursor"));
        }
    }
}
