package io.tiercache.invalidation;

import io.tiercache.*;
import io.tiercache.internal.CircuitBreaker;
import io.tiercache.spi.*;
import io.tiercache.testkit.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import static org.junit.jupiter.api.Assertions.*;

class RecoveryProtocolTest {
    static final UUID WRITER = UUID.randomUUID();
    static InvalidationMessage update(String cache, String key, long version) {
        return new InvalidationMessage(cache, key, new Version(version, WRITER), WRITER,
                InvalidationMessage.Type.UPDATE, "v" + version);
    }
    static class Journal implements InvalidationJournal {
        final InMemoryJournal data = new InMemoryJournal(20000);
        final AtomicInteger reads = new AtomicInteger();
        volatile TriRead read = (c, p, n) -> data.checkedRead(c, p, n);
        volatile Function<String, String> end = data::endCursor;
        public String append(String c, InvalidationMessage m) { return data.append(c, m); }
        public List<JournalRow> readRange(String c, String p) { return data.readRange(c, p); }
        public CheckedRange checkedRead(String c, String p, int n) { reads.incrementAndGet(); return read.read(c, p, n); }
        public String endCursor(String c) { return end.apply(c); }
        public boolean isTrimmed(String c, String p) { return data.isTrimmed(c, p); }
    }
    interface TriRead { CheckedRange read(String cache, String cursor, int count); }
    static class Target implements InvalidationTarget {
        final Map<Object, InvalidationMessage> values = new ConcurrentHashMap<>();
        final AtomicLong generation = new AtomicLong();
        final AtomicInteger clears = new AtomicInteger();
        public Version versionOfL1Entry(Object k) { var v = values.get(k); return v == null ? null : v.version(); }
        public void evictL1IfNewer(Object k, Version v) { values.computeIfPresent(k, (key, old) -> v.compareTo(old.version()) > 0 ? null : old); }
        public void evictAllL1() { generation.incrementAndGet(); values.clear(); clears.incrementAndGet(); }
        public long recoveryGeneration() { return generation.get(); }
        public void applyUpdateL1(Object k, Object value, Version v) {
            values.compute(k, (key, old) -> old == null || old.version().compareTo(v) < 0
                    ? new InvalidationMessage("c", k, v, v.instanceId(), InvalidationMessage.Type.UPDATE, value) : old);
        }
    }
    static class Harness implements AutoCloseable {
        final Journal journal = new Journal();
        final Target target = new Target();
        final InMemoryInvalidationTransport.Hub hub = new InMemoryInvalidationTransport.Hub();
        final InMemoryInvalidationTransport transport = new InMemoryInvalidationTransport(hub);
        final AtomicReference<BooleanSupplier> pending = new AtomicReference<>();
        final AtomicInteger removed = new AtomicInteger();
        final InvalidationService service;
        Harness() { this(null); }
        Harness(Manual scheduler) {
            CacheMetricsListener metrics = new CacheMetricsListener() {
                public AutoCloseable registerRecovery(String cache, BooleanSupplier state) {
                    if (cache.equals("c")) pending.set(state);
                    return removed::incrementAndGet;
                }
            };
            service = scheduler == null
                    ? new InvalidationService(transport, journal, UUID.randomUUID(), InvalidationListener.NOOP, metrics)
                    : new InvalidationService(transport, journal, UUID.randomUUID(), InvalidationListener.NOOP, metrics, scheduler.now::get);
            if (scheduler != null) service.configureRecoveryExecutor(scheduler);
            service.registerTarget("c", target);
        }
        CompletableFuture<Boolean> recover() { return service.recoverAsync(Runnable::run).toCompletableFuture(); }
        public void close() { service.close(); }
    }
    static void await(BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < until) Thread.sleep(5);
        assertTrue(condition.getAsBoolean(), "condition did not settle");
    }
    static void gate(CountDownLatch release) {
        try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("gate timeout"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
    }
    static Object state(InvalidationService service, String cache) throws Exception {
        var field = InvalidationService.class.getDeclaredField("states"); field.setAccessible(true);
        return ((Map<?, ?>) field.get(service)).get(cache);
    }
    static Object field(Object state, String name) throws Exception {
        synchronized (state) { var field = state.getClass().getDeclaredField(name); field.setAccessible(true); return field.get(state); }
    }

    @Test void liveDeliveryCanProgressWhileJournalIsParked() throws Exception {
        try (Harness h = new Harness()) {
            h.journal.append("c", update("c", "k", 1));
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var once = new AtomicBoolean();
            h.journal.read = (c, p, n) -> {
                var rows = h.journal.data.checkedRead(c, p, n);
                if (once.compareAndSet(false, true)) { entered.countDown(); gate(release); }
                return rows;
            };
            var recovery = h.recover();
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                var newer = update("c", "k", 2); h.journal.append("c", newer);
                try (var sender = new InMemoryInvalidationTransport(h.hub)) { sender.publish(newer); }
                assertEquals(newer.version(), h.target.versionOfL1Entry("k"));
                assertFalse(recovery.isDone());
            } finally { release.countDown(); }
            assertTrue(recovery.get(5, TimeUnit.SECONDS));
            assertEquals(new Version(2, WRITER), h.target.versionOfL1Entry("k"));
            assertEquals("2", field(state(h.service, "c"), "cursor"));
        }
    }

    @Test void secondRecoveryDiscardsTheOlderRead() throws Exception {
        staleReadIsDiscarded(false);
    }
    @Test void localClearDiscardsTheOlderRead() throws Exception {
        staleReadIsDiscarded(true);
    }
    private void staleReadIsDiscarded(boolean localClear) throws Exception {
        try (Harness h = new Harness()) {
            h.journal.append("c", update("c", "current", 1));
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var once = new AtomicBoolean();
            h.journal.read = (c, p, n) -> {
                if (once.compareAndSet(false, true)) {
                    entered.countDown(); gate(release);
                    return new CheckedRange(true, List.of(new JournalRow("999", update("c", "obsolete", 999))));
                }
                return h.journal.data.checkedRead(c, p, n);
            };
            var completion = h.recover();
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                if (localClear) h.target.evictAllL1(); else assertSame(completion, h.recover());
            } finally { release.countDown(); }
            assertTrue(completion.get(5, TimeUnit.SECONDS));
            assertNull(h.target.versionOfL1Entry("obsolete"));
            assertNotNull(h.target.versionOfL1Entry("current"));
            assertEquals("1", field(state(h.service, "c"), "cursor"));
        }
    }

    @Test void closeCancelsCompletionAndRejectsLateJournalResponse() throws Exception {
        Harness h = new Harness();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        h.journal.read = (c, p, n) -> {
            entered.countDown();
            // Model a transport that finishes after cancellation, ignoring interruption.
            while (release.getCount() != 0) { try { release.await(); } catch (InterruptedException ignored) { } }
            return new CheckedRange(true, List.of(new JournalRow("1", update("c", "late", 1))));
        };
        var completion = h.recover(); assertTrue(entered.await(5, TimeUnit.SECONDS));
        h.close(); assertFalse(completion.get(1, TimeUnit.SECONDS)); release.countDown();
        assertEquals(1, h.removed.get());
        assertEquals(RecoveryResult.Status.CLOSED, h.service.resetAsync("c").toCompletableFuture().get().status());
        assertFalse(h.recover().get());
        await(() -> Thread.getAllStackTraces().keySet().stream().noneMatch(t -> t.getName().equals("tiercache-recovery") && t.isAlive()));
        assertNull(h.target.versionOfL1Entry("late"));
        h.close(); assertEquals(1, h.removed.get());
    }

    @Test void resetCapturesBaselineBeforeClearAndReplaysRowsAfterIt() throws Exception {
        try (Harness h = new Harness()) {
            h.target.values.put("old", update("c", "old", 1));
            var captured = new CountDownLatch(1); var release = new CountDownLatch(1);
            h.journal.end = c -> {
                try { assertFalse(Thread.holdsLock(state(h.service, c)), "baseline I/O must be outside the state gate"); }
                catch (Exception e) { throw new AssertionError(e); }
                String baseline = h.journal.data.endCursor(c); captured.countDown(); gate(release); return baseline;
            };
            var reset = h.service.resetAsync("c").toCompletableFuture();
            try {
                assertTrue(captured.await(5, TimeUnit.SECONDS));
                assertEquals(0, h.target.clears.get());
                h.journal.append("c", update("c", "after", 2));
            } finally { release.countDown(); }
            RecoveryResult result = reset.get(5, TimeUnit.SECONDS);
            assertEquals(RecoveryResult.Status.RESET_SAFE, result.status());
            assertEquals("0", result.baseline()); assertTrue(result.generation() > 0);
            assertNull(h.target.versionOfL1Entry("old"));
            await(() -> h.target.versionOfL1Entry("after") != null);
            await(() -> !h.pending.get().getAsBoolean());
            assertEquals("1", field(state(h.service, "c"), "cursor"));
        }
    }

    @Test void failedBaselineClearsButRetainsCursorAndDoesNotCloseBreaker() throws Exception {
        try (Harness h = new Harness()) {
            h.target.values.put("old", update("c", "old", 1));
            h.journal.read = (c, p, n) -> { throw new IllegalStateException("journal offline"); };
            h.journal.end = c -> { throw new IllegalStateException("baseline offline"); };
            var b = new CircuitBreaker(new CircuitBreaker.Config(2, 1, 1, Duration.ZERO, 1),
                    new CircuitBreaker.Listener() { public void onOpen() { } public void onClose() { fail("unsafe recovery"); } });
            var executor = Executors.newSingleThreadExecutor();
            try {
                b.configureRecovery(executor, h::recover); b.onFailure();
                var probe = b.tryAcquirePermit(); assertNotNull(probe); probe.success();
                await(() -> h.target.clears.get() == 1);
                await(() -> b.state() == BreakerState.OPEN);
                assertEquals("0", field(state(h.service, "c"), "cursor"));
                assertTrue(h.pending.get().getAsBoolean());
                assertNull(h.target.versionOfL1Entry("old"));
                var result = (RecoveryResult) field(state(h.service, "c"), "lastResult");
                assertEquals(RecoveryResult.Status.FAILED, result.status()); assertNull(result.baseline());
            } finally { b.detachRecovery(); executor.shutdownNow(); }
        }
    }

    @Test void observerFailuresCannotCancelApplicationCursorOrSafeReset() throws Exception {
        var hub = new InMemoryInvalidationTransport.Hub(); var journal = new Journal(); var target = new Target();
        CacheMetricsListener metrics = new CacheMetricsListener() {
            public void onInvalidation(String c, Direction d) { throw new IllegalStateException("metric"); }
            public Object onInvalidationStart(String c) { throw new AssertionError("span"); }
            public void onInvalidationEnd(String c, Object span) { throw new IllegalStateException("end"); }
        };
        try (var service = new InvalidationService(new InMemoryInvalidationTransport(hub), journal, UUID.randomUUID(),
                c -> { throw new IllegalStateException("listener"); }, metrics)) {
            service.registerTarget("c", target);
            service.setEventListener((c, e) -> { throw new IllegalStateException("event listener"); });
            journal.append("c", update("c", "k", 1));
            assertTrue(service.recoverAsync(Runnable::run).toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertNotNull(target.versionOfL1Entry("k")); assertEquals("1", field(state(service, "c"), "cursor"));
            var reset = service.resetAsync("c").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(RecoveryResult.Status.RESET_SAFE, reset.status()); assertEquals(1, target.clears.get());
        }
    }

    @Test void mailboxesCoalesceAndLargePassYieldsToAnotherCache() throws Exception {
        try (Manual scheduler = new Manual(); Harness h = new Harness(scheduler)) {
            Target other = new Target(); h.service.registerTarget("z", other);
            for (int i = 1; i <= 5000; i++) h.journal.append("c", update("c", "k" + i, i));
            h.journal.append("z", update("z", "small", 1));
            var completion = h.recover();
            for (int i = 0; i < 1000; i++) assertSame(completion, h.recover());
            assertEquals(2, scheduler.queue.size());
            scheduler.next();
            assertTrue(h.journal.reads.get() <= 16);
            assertTrue(scheduler.queue.size() <= 2);
            scheduler.next();
            assertNotNull(other.versionOfL1Entry("small"), "the first continuation goes behind other ready caches");
            scheduler.drain();
            assertTrue(completion.get()); assertEquals(5000, h.target.values.size());
            assertTrue(scheduler.queue.isEmpty()); assertFalse(h.pending.get().getAsBoolean());
            int calls = h.journal.reads.get(); scheduler.advance(60); scheduler.drain(); assertEquals(calls, h.journal.reads.get());
        }
    }

    @Test void failedRetriesUseOneTokenAndOneTwoFourEightSixteenThirtySecondBackoff() throws Exception {
        try (Manual scheduler = new Manual(); Harness h = new Harness(scheduler)) {
            h.journal.read = (c, p, n) -> { throw new IllegalStateException("read"); };
            h.journal.end = c -> { throw new IllegalStateException("baseline"); };
            var initial = h.recover(); scheduler.next(); assertFalse(initial.get());
            for (int delay : new int[]{1, 2, 4, 8, 16, 30, 30}) {
                assertEquals(1, scheduler.queue.size());
                assertEquals(TimeUnit.SECONDS.toNanos(delay), scheduler.queue.peek().due - scheduler.now.get());
                int before = h.journal.reads.get();
                for (int i = 0; i < 100; i++) h.service.onL2Recovery();
                assertEquals(1, scheduler.queue.size()); assertEquals(before, h.journal.reads.get());
                scheduler.advance(delay); scheduler.next();
                assertEquals(before + 1, h.journal.reads.get()); assertTrue(h.pending.get().getAsBoolean());
            }
            h.journal.read = (c, p, n) -> h.journal.data.checkedRead(c, p, n);
            h.journal.end = h.journal.data::endCursor;
            scheduler.advance(30); scheduler.drain(); assertFalse(h.pending.get().getAsBoolean());
        }
    }

    @Test void actualWorkersBoundConcurrentReadsAndCoalesceRunningCaches() throws Exception {
        var release = new CountDownLatch(1);
        try (Harness h = new Harness()) {
            h.service.registerTarget("d", new Target()); h.service.registerTarget("e", new Target());
            var entered = new CountDownLatch(2); var active = new AtomicInteger(); var maximum = new AtomicInteger();
            h.journal.read = (c, p, n) -> {
                int running = active.incrementAndGet(); maximum.accumulateAndGet(running, Math::max);
                try { entered.countDown(); gate(release); return h.journal.data.checkedRead(c, p, n); }
                finally { active.decrementAndGet(); }
            };
            var completion = h.recover();
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                for (int i = 0; i < 100; i++) h.service.onL2Recovery();
                assertEquals(2, h.journal.reads.get());
                assertEquals(2, maximum.get());
            } finally { release.countDown(); }
            assertTrue(completion.get(5, TimeUnit.SECONDS)); assertTrue(maximum.get() <= 2);
        } finally { release.countDown(); }
    }

    @Test void closingCancelsQueuedTokensAndUnregistersGaugeWithoutFakeSuccess() throws Exception {
        try (Manual scheduler = new Manual()) {
            Harness h = new Harness(scheduler); var completion = h.recover();
            assertEquals(1, scheduler.queue.size()); assertTrue(h.pending.get().getAsBoolean());
            h.close(); assertEquals(0, scheduler.queue.size()); assertFalse(completion.get());
            assertEquals(1, h.removed.get());
            assertTrue(h.pending.get().getAsBoolean(), "unregister rather than rewriting pending as successful");
            scheduler.drain(); assertEquals(0, h.journal.reads.get());
        }
    }

    @Test void noJournalClearDoesNotPretendToBeJournalOverflowOrReplay() throws Exception {
        var overflows = new AtomicInteger(); var signals = new AtomicInteger(); var target = new Target();
        var metrics = new CacheMetricsListener() {
            public void onInvalidation(String cache, Direction direction) { signals.incrementAndGet(); }
        };
        try (var service = new InvalidationService(new InMemoryInvalidationTransport(new InMemoryInvalidationTransport.Hub()),
                null, UUID.randomUUID(), cache -> overflows.incrementAndGet(), metrics)) {
            service.registerTarget("c", target);
            var result = service.resetAsync("c").toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(RecoveryResult.Status.NO_JOURNAL, result.status()); assertNull(result.baseline());
            assertEquals(1, target.clears.get()); assertEquals(0, overflows.get()); assertEquals(0, signals.get());
        }
    }

    /** Deterministic two-worker scheduler seam: no task runs inline on submission. */
    static final class Manual extends ScheduledThreadPoolExecutor implements AutoCloseable {
        final AtomicLong now = new AtomicLong(1);
        final PriorityQueue<Task> queue = new PriorityQueue<>(); long sequence;
        Manual() { super(2); }
        class Task implements ScheduledFuture<Object> {
            final Runnable runnable; final long due, order = sequence++; boolean cancelled, done;
            Task(Runnable runnable, long due) { this.runnable = runnable; this.due = due; }
            public long getDelay(TimeUnit unit) { return unit.convert(due - now.get(), TimeUnit.NANOSECONDS); }
            public int compareTo(Delayed other) { Task t = (Task) other; int c = Long.compare(due, t.due); return c == 0 ? Long.compare(order, t.order) : c; }
            public boolean cancel(boolean interrupt) { cancelled = true; queue.remove(this); return true; }
            public boolean isCancelled() { return cancelled; }
            public boolean isDone() { return done || cancelled; }
            public Object get() { throw new UnsupportedOperationException(); }
            public Object get(long timeout, TimeUnit unit) { throw new UnsupportedOperationException(); }
        }
        @Override public ScheduledFuture<?> schedule(Runnable runnable, long delay, TimeUnit unit) {
            if (isShutdown()) throw new RejectedExecutionException();
            Task task = new Task(runnable, now.get() + unit.toNanos(delay)); queue.add(task); return task;
        }
        void next() {
            Task task = queue.remove(); assertTrue(task.due <= now.get());
            if (!task.cancelled) task.runnable.run(); task.done = true;
        }
        void drain() { int count = 0; while (!queue.isEmpty() && queue.peek().due <= now.get()) { assertTrue(count++ < 100); next(); } }
        void advance(long seconds) { now.addAndGet(TimeUnit.SECONDS.toNanos(seconds)); }
        public void close() { shutdownNow(); }
    }
}
