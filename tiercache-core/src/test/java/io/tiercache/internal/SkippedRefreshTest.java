package io.tiercache.internal;

import io.tiercache.CacheSettings;
import io.tiercache.InvalidationMode;
import io.tiercache.NullPolicy;
import io.tiercache.VersionGenerator;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.DistributedLock;
import io.tiercache.spi.DistributedLockProvider;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/** A skipped refresh is coordination control flow, not evidence of a missing value. */
class SkippedRefreshTest {
    private static final String KEY = "key";
    private static final Duration L2_TTL = Duration.ofMinutes(10);

    @Test
    void swrForegroundJoinDoesNotReturnSkippedRefreshAsNull() throws Exception {
        assertForegroundJoinObtainsValue(false);
    }

    @Test
    void xfetchForegroundJoinDoesNotReturnSkippedRefreshAsNull() throws Exception {
        assertForegroundJoinObtainsValue(true);
    }

    private void assertForegroundJoinObtainsValue(boolean xfetch) throws Exception {
        try (Rig rig = new Rig(xfetch, NullPolicy.deny(), 1)) {
            AtomicInteger refreshLoads = new AtomicInteger();
            rig.queueRefresh(key -> {
                refreshLoads.incrementAndGet();
                return "refresh-value";
            });
            Runnable background = rig.refresh.take();
            rig.removeCachedValue();
            AtomicInteger foregroundLoads = new AtomicInteger();
            rig.metrics.joined = new CountDownLatch(1);
            Future<String> caller = rig.workers.submit(() -> rig.cache.getOrCompute(KEY, key -> {
                foregroundLoads.incrementAndGet();
                return "current";
            }));
            await(rig.metrics.joined);
            background.run();
            assertEquals("current", caller.get(5, TimeUnit.SECONDS));
            assertEquals(1, foregroundLoads.get());
            assertEquals(0, refreshLoads.get());
            assertEquals(2, rig.acquisitions.get());
            assertEquals(1, rig.releases.get());
            assertTrue(rig.claims.isEmpty());
        }
    }

    @Test
    void terminalSkipWithEmptySlotCreatesForegroundOwner() throws Exception {
        assertLateJoin(Replacement.ABSENT);
    }

    @Test
    void oldRefreshCleanupCannotRemoveForegroundReplacement() throws Exception {
        assertLateJoin(Replacement.SAME_SKIPPED);
    }

    @Test
    void terminalSkipPromotesCompetingActiveRefresh() throws Exception {
        assertLateJoin(Replacement.ACTIVE_REFRESH);
    }

    @Test
    void terminalSkipReplacesAnotherAlreadySkippedRefresh() throws Exception {
        assertLateJoin(Replacement.SKIPPED_REFRESH);
    }

    @Test
    void terminalSkipJoinsCompetingForegroundOwner() throws Exception {
        assertLateJoin(Replacement.FOREGROUND);
    }

    private enum Replacement { ABSENT, SAME_SKIPPED, ACTIVE_REFRESH, SKIPPED_REFRESH, FOREGROUND }

    /**
     * Park the first foreground reader after it obtained the old claim but
     * before it can promote it. Then change the map behind that exact reference.
     * The map seam controls scheduling; assertions concern real readers/loaders.
     */
    private void assertLateJoin(Replacement replacement) throws Exception {
        int skips = replacement == Replacement.ACTIVE_REFRESH
                || replacement == Replacement.SKIPPED_REFRESH ? 2 : 1;
        CountDownLatch releaseCompletion = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        try (Rig rig = new Rig(false, NullPolicy.deny(), skips)) {
            AtomicInteger loads = new AtomicInteger();
            CountDownLatch loading = new CountDownLatch(1);
            Function<String, String> loader = key -> {
                loads.incrementAndGet();
                loading.countDown();
                await(releaseLoad);
                return "current";
            };
            rig.queueRefresh(key -> fail("a skipped refresh must not invoke its loader"));
            Runnable oldRefresh = rig.refresh.take();
            LoadClaim<String, String> old = rig.claims.get(KEY);
            rig.removeCachedValue();
            rig.claims.parkNextJoin.set(true);
            Future<String> caller = rig.workers.submit(() -> rig.cache.getOrCompute(KEY, loader));
            await(rig.claims.captured);

            CountDownLatch completed = new CountDownLatch(1);
            Runnable completionGate = () -> {
                completed.countDown();
                await(releaseCompletion);
            };
            Future<?> parkedRefresh = null;
            if (replacement == Replacement.SAME_SKIPPED) {
                rig.metrics.onCompleted = completionGate;
                parkedRefresh = rig.workers.submit(oldRefresh);
                await(completed);
            } else {
                oldRefresh.run();
            }

            Runnable competingRefresh = null;
            Future<String> competingForeground = null;
            if (replacement == Replacement.ACTIVE_REFRESH
                    || replacement == Replacement.SKIPPED_REFRESH) {
                rig.queueRefresh(key -> fail("promoted skip must use foreground demand"));
                competingRefresh = rig.refresh.take();
                rig.removeCachedValue();
                if (replacement == Replacement.SKIPPED_REFRESH) {
                    rig.metrics.onCompleted = completionGate;
                    parkedRefresh = rig.workers.submit(competingRefresh);
                    await(completed);
                }
            } else if (replacement == Replacement.FOREGROUND) {
                competingForeground = rig.workers.submit(() -> rig.cache.getOrCompute(KEY, loader));
                await(loading);
            }

            rig.claims.releaseJoin.countDown();
            await(rig.claims.recovered);
            Future<?> activeRefresh = replacement == Replacement.ACTIVE_REFRESH
                    ? rig.workers.submit(competingRefresh) : null;
            await(loading);
            LoadClaim<String, String> selected = rig.claims.get(KEY);
            assertNotNull(selected);
            assertNotSame(old, selected);
            releaseCompletion.countDown();
            if (parkedRefresh != null) {
                parkedRefresh.get(5, TimeUnit.SECONDS);
            }
            assertSame(selected, rig.claims.get(KEY), "old cleanup must retain the replacement");

            rig.metrics.joined = new CountDownLatch(1);
            Future<String> follower = rig.workers.submit(() -> rig.cache.getOrCompute(KEY, loader));
            await(rig.metrics.joined);
            assertEquals(1, loads.get(), "replacement keeps one local owner");
            releaseLoad.countDown();
            assertEquals("current", caller.get(5, TimeUnit.SECONDS));
            assertEquals("current", follower.get(5, TimeUnit.SECONDS));
            if (competingForeground != null) {
                assertEquals("current", competingForeground.get(5, TimeUnit.SECONDS));
            }
            if (activeRefresh != null) {
                activeRefresh.get(5, TimeUnit.SECONDS);
            }
            assertEquals(1, loads.get());
            assertTrue(rig.claims.isEmpty());
        } finally {
            releaseLoad.countDown();
            releaseCompletion.countDown();
        }
    }

    @Test
    void manyForegroundWaitersShareOnePromotedRefresh() throws Exception {
        try (Rig rig = new Rig(false, NullPolicy.deny(), 1)) {
            rig.queueRefresh(key -> fail("refresh must not fabricate a result"));
            Runnable refresh = rig.refresh.take();
            rig.removeCachedValue();
            int count = 6;
            rig.metrics.joined = new CountDownLatch(count);
            AtomicInteger loads = new AtomicInteger();
            List<Future<String>> callers = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                callers.add(rig.workers.submit(() -> rig.cache.getOrCompute(KEY, key -> {
                    loads.incrementAndGet();
                    return "current";
                })));
            }
            await(rig.metrics.joined);
            refresh.run();
            for (Future<String> caller : callers) {
                assertEquals("current", caller.get(5, TimeUnit.SECONDS));
            }
            assertEquals(1, loads.get());
        }
    }

    @Test
    void realRefreshNullUnderDenyIsNotRetried() throws Exception {
        assertRealNullIsShared(NullPolicy.deny());
    }

    @Test
    void realRefreshNullUnderAllowIsCachedWithoutRetry() throws Exception {
        assertRealNullIsShared(NullPolicy.allow(Duration.ofMinutes(1)));
    }

    private void assertRealNullIsShared(NullPolicy policy) throws Exception {
        try (Rig rig = new Rig(false, policy, 0)) {
            AtomicInteger refreshLoads = new AtomicInteger();
            rig.queueRefresh(key -> {
                refreshLoads.incrementAndGet();
                return null;
            });
            Runnable refresh = rig.refresh.take();
            rig.removeCachedValue();
            rig.metrics.joined = new CountDownLatch(1);
            Future<String> caller = rig.workers.submit(() -> rig.cache.getOrCompute(KEY,
                    key -> fail("genuine null is a result, not skipped coordination")));
            await(rig.metrics.joined);
            refresh.run();
            assertNull(caller.get(5, TimeUnit.SECONDS));
            assertEquals(1, refreshLoads.get());
            if (policy.markerTtl() == null) {
                assertNull(rig.l2.get(KEY));
            } else {
                assertTrue(rig.l2.get(KEY).isNullMarker());
                assertNull(rig.cache.getOrCompute(KEY, key -> fail("cached marker")));
            }
            assertTrue(rig.claims.isEmpty());
        }
    }

    @Test
    void newerL2ValueCompletesJoinedRefreshWithoutLoader() throws Exception {
        try (Rig rig = new Rig(false, NullPolicy.deny(), 0)) {
            rig.queueRefresh(key -> fail("newer L2 suppresses refresh loader"));
            Runnable refresh = rig.refresh.take();
            rig.removeCachedValue();
            rig.metrics.joined = new CountDownLatch(1);
            Future<String> caller = rig.workers.submit(() -> rig.cache.getOrCompute(KEY,
                    key -> fail("double-check supplies foreground result")));
            await(rig.metrics.joined);
            rig.l2.put(KEY, StoredEntry.ofValue("newer", null, System.currentTimeMillis()),
                    Duration.ofHours(1));
            refresh.run();
            assertEquals("newer", caller.get(5, TimeUnit.SECONDS));
            assertEquals("newer", rig.l1.get(KEY).value());
        }
    }

    @Test
    void loaderFailureUnblocksForegroundAndReleasesClaim() throws Exception {
        assertFailureShared(new IllegalArgumentException("loader failed"));
    }

    @Test
    void loaderErrorAlsoSettlesItsSharedClaim() throws Exception {
        assertFailureShared(new AssertionError("loader error"));
    }

    private void assertFailureShared(Throwable failure) throws Exception {
        try (Rig rig = new Rig(false, NullPolicy.deny(), 0)) {
            rig.queueRefresh(key -> {
                if (failure instanceof Error error) {
                    throw error;
                }
                throw (RuntimeException) failure;
            });
            Runnable refresh = rig.refresh.take();
            rig.removeCachedValue();
            rig.metrics.joined = new CountDownLatch(1);
            Future<String> caller = rig.workers.submit(() -> rig.cache.getOrCompute(KEY,
                    key -> fail("joined failure must not start a second loader")));
            await(rig.metrics.joined);
            if (failure instanceof Error) {
                assertSame(failure, assertThrows(Error.class, refresh::run));
            } else {
                refresh.run();
            }
            ExecutionException thrown = assertThrows(ExecutionException.class,
                    () -> caller.get(5, TimeUnit.SECONDS));
            assertSame(failure, assertInstanceOf(CompletionException.class, thrown.getCause()).getCause());
            assertTrue(rig.claims.isEmpty());
            assertEquals("retry", rig.cache.getOrCompute(KEY, key -> "retry"));
        }
    }

    @Test
    void rejectedSchedulingFailsJoinedReaderButKeepsStaleCallerResult() throws Exception {
        CountDownLatch submitted = new CountDownLatch(1);
        CountDownLatch reject = new CountDownLatch(1);
        RejectedExecutionException failure = new RejectedExecutionException("queue full");
        Executor rejected = task -> {
            submitted.countDown();
            await(reject);
            throw failure;
        };
        try (Rig rig = new Rig(false, NullPolicy.deny(), 0, rejected)) {
            rig.seed();
            Future<String> stale = rig.workers.submit(() -> rig.cache.getOrCompute(KEY,
                    key -> fail("rejected task cannot load")));
            await(submitted);
            rig.removeCachedValue();
            rig.metrics.joined = new CountDownLatch(1);
            Future<String> caller = rig.workers.submit(() -> rig.cache.getOrCompute(KEY,
                    key -> fail("a joined rejected claim must fail")));
            await(rig.metrics.joined);
            reject.countDown();
            assertEquals("cached", stale.get(5, TimeUnit.SECONDS));
            ExecutionException thrown = assertThrows(ExecutionException.class,
                    () -> caller.get(5, TimeUnit.SECONDS));
            assertSame(failure, assertInstanceOf(CompletionException.class, thrown.getCause()).getCause());
            assertTrue(rig.claims.isEmpty());
            assertEquals("retry", rig.cache.getOrCompute(KEY, key -> "retry"));
        } finally {
            reject.countDown();
        }
    }

    @Test
    void promotedRefreshKeepsTwoLoadBudgetAcrossTombstonesAndGenerationChanges() throws Exception {
        try (Rig rig = new Rig(false, NullPolicy.deny(), 1)) {
            rig.queueRefresh(key -> fail("skip has consumed no loader execution"));
            Runnable refresh = rig.refresh.take();
            rig.removeCachedValue();
            rig.l2.rejectWrites = true;
            rig.metrics.joined = new CountDownLatch(1);
            AtomicInteger loads = new AtomicInteger();
            Future<String> caller = rig.workers.submit(() -> rig.cache.getOrCompute(KEY, key -> {
                int attempt = loads.incrementAndGet();
                rig.cache.evictAllL1();
                rig.cache.evictAllL1();
                return "value-" + attempt;
            }));
            await(rig.metrics.joined);
            refresh.run();
            assertEquals("value-2", caller.get(5, TimeUnit.SECONDS));
            assertEquals(2, loads.get());
            assertEquals(2, rig.l2.conditionalWrites.get());
            assertNull(rig.l1.get(KEY));
            assertNull(rig.l2.get(KEY));
            assertTrue(rig.claims.isEmpty());
        }
    }

    @Test
    void promotionKeepsTheFirstForegroundDeadline() throws Exception {
        try (Rig rig = new Rig(false, NullPolicy.deny(), Integer.MAX_VALUE)) {
            rig.queueRefresh(key -> fail("no refresh loader after lock loss"));
            Runnable refresh = rig.refresh.take();
            rig.removeCachedValue();
            LoadClaim<String, String> claim = rig.claims.get(KEY);
            AtomicInteger loads = new AtomicInteger();
            // Simulate the original demand expiring while refresh was queued,
            // without sleeping through a real 30-second coordination budget.
            assertTrue(claim.requireResult(new LoadClaim.Demand<>(key -> {
                loads.incrementAndGet();
                return "expired-budget-result";
            }, System.nanoTime() - 1)));
            rig.metrics.joined = new CountDownLatch(1);
            Future<String> caller = rig.workers.submit(() -> rig.cache.getOrCompute(KEY,
                    key -> fail("a later waiter must not replace the first demand")));
            await(rig.metrics.joined);
            Future<?> background = rig.workers.submit(refresh);
            assertEquals("expired-budget-result", caller.get(5, TimeUnit.SECONDS));
            background.get(5, TimeUnit.SECONDS);
            assertEquals(1, rig.acquisitions.get(), "only the original refresh tries its lock");
            assertEquals(1, loads.get());
        }
    }

    @Test
    void terminalSkipRecoveryKeepsItsDeadlineAndTwoLoadBudget() throws Exception {
        try (Rig rig = new Rig(false, NullPolicy.deny(), Integer.MAX_VALUE)) {
            rig.queueRefresh(key -> fail("refresh is skipped"));
            Runnable refresh = rig.refresh.take();
            LoadClaim<String, String> skipped = rig.claims.get(KEY);
            rig.removeCachedValue();
            refresh.run();
            assertTrue(skipped.result.join().isSkipped());
            assertThrows(IllegalStateException.class, () -> skipped.result.join().resultEntry());
            rig.claims.put(KEY, skipped);
            rig.l2.rejectWrites = true;
            AtomicInteger loads = new AtomicInteger();
            LoadClaim.Demand<String, String> demand = new LoadClaim.Demand<>(key -> {
                rig.cache.evictAllL1();
                return "bounded-" + loads.incrementAndGet();
            }, System.nanoTime() - 1);
            Method recover = DefaultTierCache.class.getDeclaredMethod("recoverSkippedRefresh",
                    Object.class, LoadClaim.class, LoadClaim.Demand.class);
            recover.setAccessible(true);
            Future<Object> result = rig.workers.submit(() -> recover.invoke(rig.cache, KEY, skipped, demand));
            assertEquals("bounded-2", result.get(5, TimeUnit.SECONDS));
            assertEquals(1, rig.acquisitions.get(), "recovery must not restart coordination");
            assertEquals(2, loads.get());
            assertTrue(rig.claims.isEmpty());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS), "controlled step did not complete");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted controlled step", e);
        }
    }

    private static final class Rig implements AutoCloseable {
        final CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        final TestRemote l2 = new TestRemote();
        final QueuedExecutor refresh = new QueuedExecutor();
        final Metrics metrics = new Metrics();
        final ClaimMap claims = new ClaimMap();
        final AtomicInteger acquisitions = new AtomicInteger();
        final AtomicInteger releases = new AtomicInteger();
        final ExecutorService workers = Executors.newFixedThreadPool(10);
        final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor();
        final DefaultTierCache<String, String> cache;
        final boolean xfetch;

        Rig(boolean xfetch, NullPolicy policy, int refusedLocks) throws Exception {
            this(xfetch, policy, refusedLocks, null);
        }

        Rig(boolean xfetch, NullPolicy policy, int refusedLocks, Executor executor) throws Exception {
            this.xfetch = xfetch;
            DistributedLockProvider provider = (name, lease) ->
                    acquisitions.incrementAndGet() <= refusedLocks ? null : lock(releases);
            CacheSettings settings = new CacheSettings(10_000, Duration.ofMinutes(1), null,
                    L2_TTL, 0.0, policy, InvalidationMode.INVALIDATE, 64 * 1024,
                    Duration.ofMinutes(5), xfetch, Duration.ofNanos(1));
            cache = new DefaultTierCache<>("cache", l1, l2, settings, true, provider, watchdog,
                    new VersionGenerator(), null, null, metrics, executor == null ? refresh : executor);
            Field field = DefaultTierCache.class.getDeclaredField("inflight");
            field.setAccessible(true);
            field.set(cache, claims);
            if (xfetch) {
                field = DefaultTierCache.class.getDeclaredField("loaderDurationEmaNanos");
                field.setAccessible(true);
                ((AtomicLong) field.get(cache)).set(TimeUnit.SECONDS.toNanos(1));
            }
        }

        void seed() {
            long age = xfetch ? L2_TTL.toMillis() / 2 : L2_TTL.toMillis() + 60_000;
            l2.put(KEY, StoredEntry.ofValue("cached", null, System.currentTimeMillis() - age),
                    Duration.ofHours(1));
        }

        void queueRefresh(Function<String, String> loader) {
            seed();
            assertEquals("cached", cache.getOrCompute(KEY, loader));
        }

        void removeCachedValue() {
            l1.evict(KEY);
            l2.evict(KEY);
        }

        @Override
        public void close() {
            claims.releaseJoin.countDown();
            workers.shutdownNow();
            watchdog.shutdownNow();
        }
    }

    private static DistributedLock lock(AtomicInteger releases) {
        return new DistributedLock() {
            @Override
            public boolean extend(Duration lease) {
                return true;
            }

            @Override
            public void release() {
                releases.incrementAndGet();
            }
        };
    }

    private static final class QueuedExecutor implements Executor {
        private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        Runnable take() throws InterruptedException {
            Runnable task = tasks.poll(5, TimeUnit.SECONDS);
            assertNotNull(task, "refresh must be queued");
            return task;
        }
    }

    private static final class Metrics implements CacheMetricsListener {
        volatile CountDownLatch joined = new CountDownLatch(0);
        volatile Runnable onCompleted = () -> { };

        @Override
        public void onRequest(String cache, Outcome outcome) {
            if (outcome == Outcome.COALESCED) {
                joined.countDown();
            }
        }

        @Override
        public void onRevalidationCompleted(String cache) {
            onCompleted.run();
        }
    }

    private static final class ClaimMap extends ConcurrentHashMap<String, LoadClaim<String, String>> {
        final AtomicBoolean parkNextJoin = new AtomicBoolean();
        final CountDownLatch captured = new CountDownLatch(1);
        final CountDownLatch releaseJoin = new CountDownLatch(1);
        final CountDownLatch recovered = new CountDownLatch(1);

        @Override
        public LoadClaim<String, String> putIfAbsent(String key, LoadClaim<String, String> value) {
            LoadClaim<String, String> existing = super.putIfAbsent(key, value);
            if (existing != null && parkNextJoin.compareAndSet(true, false)) {
                captured.countDown();
                await(releaseJoin);
            }
            return existing;
        }

        @Override
        public LoadClaim<String, String> compute(String key,
                BiFunction<? super String, ? super LoadClaim<String, String>,
                        ? extends LoadClaim<String, String>> remappingFunction) {
            LoadClaim<String, String> selected = super.compute(key, remappingFunction);
            recovered.countDown();
            return selected;
        }
    }

    private static final class TestRemote implements RemoteCache<String, String> {
        final InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();
        final AtomicInteger conditionalWrites = new AtomicInteger();
        volatile boolean rejectWrites;

        @Override
        public StoredEntry<String> get(String key) {
            return delegate.get(key);
        }

        @Override
        public void put(String key, StoredEntry<String> value, Duration ttl) {
            delegate.put(key, value, ttl);
        }

        @Override
        public boolean putIfNewer(String key, StoredEntry<String> value, Duration ttl, Duration staleTtl) {
            conditionalWrites.incrementAndGet();
            if (rejectWrites) {
                return false;
            }
            delegate.put(key, value, ttl, staleTtl);
            return true;
        }

        @Override
        public void evict(String key) {
            delegate.evict(key);
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public boolean setIfAbsent(String key, StoredEntry<String> value, Duration ttl) {
            return delegate.setIfAbsent(key, value, ttl);
        }
    }
}
