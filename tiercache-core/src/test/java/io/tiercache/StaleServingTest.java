package io.tiercache;

import io.tiercache.internal.CircuitBreaker;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.DistributedLock;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.InMemoryLockProvider;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Spec: stale-serving — stale-window classification (fresh / stale / miss),
 * single asynchronous revalidation, failure keeps serving stale, and the
 * stale-serving metrics.
 */
class StaleServingTest {

    private static final Duration L2_TTL = Duration.ofMinutes(10);
    private static final Duration STALE_TTL = Duration.ofMinutes(5);

    private final List<ExecutorService> executors = new ArrayList<>();

    @AfterEach
    void tearDown() {
        executors.forEach(ExecutorService::shutdownNow);
    }

    /** Records the stale-serving events, attributable per cache. */
    private static final class RecordingMetrics implements CacheMetricsListener {
        private final Map<String, AtomicInteger> staleHits = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> triggers = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> completions = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> failures = new ConcurrentHashMap<>();

        @Override
        public void onStaleHit(String cache) {
            counter(staleHits, cache).incrementAndGet();
        }

        @Override
        public void onRevalidationTriggered(String cache) {
            counter(triggers, cache).incrementAndGet();
        }

        @Override
        public void onRevalidationCompleted(String cache) {
            counter(completions, cache).incrementAndGet();
        }

        @Override
        public void onRevalidationFailed(String cache) {
            counter(failures, cache).incrementAndGet();
        }

        int staleHits(String cache) {
            return count(staleHits, cache);
        }

        int triggers(String cache) {
            return count(triggers, cache);
        }

        int completions(String cache) {
            return count(completions, cache);
        }

        int failures(String cache) {
            return count(failures, cache);
        }

        private static AtomicInteger counter(Map<String, AtomicInteger> map, String cache) {
            return map.computeIfAbsent(cache, k -> new AtomicInteger());
        }

        private static int count(Map<String, AtomicInteger> map, String cache) {
            AtomicInteger c = map.get(cache);
            return c == null ? 0 : c.get();
        }
    }

    private static CacheSettings staleSettings() {
        return new CacheSettings(10_000, Duration.ofMinutes(5), null, L2_TTL, 0.0,
                NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                STALE_TTL, false, Duration.ofSeconds(1));
    }

    private static long freshTimestamp() {
        return System.currentTimeMillis();
    }

    private static long staleTimestamp() {
        // One minute into the stale window.
        return System.currentTimeMillis() - L2_TTL.toMillis() - 60_000;
    }

    private static long pastWindowTimestamp() {
        return System.currentTimeMillis() - L2_TTL.toMillis() - STALE_TTL.toMillis() - 60_000;
    }

    private DefaultTierCache<String, String> newCache(CacheSettings settings, Executor executor,
            RecordingMetrics metrics, CountingLocalCache<String, String> l1,
            InMemoryRemoteCache<String, String> l2) {
        return new DefaultTierCache<>("c", l1, l2, settings, true,
                null, null, null, null, null, metrics, executor);
    }

    private ExecutorService newExecutor(int threads) {
        ExecutorService executor = Executors.newFixedThreadPool(threads, daemonFactory("test-revalidation"));
        executors.add(executor);
        return executor;
    }

    private static ThreadFactory daemonFactory(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }

    // --- Requirement: Stale-while-revalidate window ---

    @Test
    void freshEntryIsServedAsNormalHit() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache =
                newCache(staleSettings(), Runnable::run, metrics, l1, l2);
        l2.put("k", StoredEntry.ofValue("v", null, freshTimestamp()), Duration.ofHours(1));

        assertEquals("v", cache.getOrCompute("k", key -> fail("loader must not run")));
        assertEquals(0, metrics.staleHits("c"));
        assertEquals(0, metrics.triggers("c"));
        assertEquals(1, l1.puts.get(), "an L2 hit warms L1 as before");
    }

    @Test
    void staleHitReturnsImmediatelyWhileRevalidationRuns() throws Exception {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache =
                newCache(staleSettings(), newExecutor(1), metrics, l1, l2);
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch loaderRelease = new CountDownLatch(1);
        String served = cache.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            loaderEntered.countDown();
            awaitQuietly(loaderRelease);
            return "fresh";
        });

        assertEquals("stale", served, "the stale value is served without waiting for the loader");
        assertTrue(loaderEntered.await(5, TimeUnit.SECONDS),
                "the revalidation must start on the executor");
        assertEquals(1, metrics.staleHits("c"));
        assertEquals(1, metrics.triggers("c"));

        loaderRelease.countDown();
        awaitTrue(() -> metrics.completions("c") == 1);
        assertEquals(1, loaderCalls.get());
        assertEquals("fresh", cache.get("k"), "the completed revalidation publishes fresh value");
    }

    @Test
    void pastTheStaleWindowIsHardMiss() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache =
                newCache(staleSettings(), Runnable::run, metrics, l1, l2);
        l2.put("k", StoredEntry.ofValue("ancient", null, pastWindowTimestamp()), Duration.ofHours(1));

        assertNull(cache.get("k"), "past the window the stale value is never returned");
        assertEquals("loaded", cache.getOrCompute("k", key -> "loaded"));
        assertEquals(0, metrics.staleHits("c"));
        assertEquals(0, metrics.triggers("c"), "a miss loads synchronously, no revalidation");
    }

    @Test
    void windowDisabledKeepsCurrentBehavior() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        CacheSettings noWindow = new CacheSettings(10_000, Duration.ofMinutes(5), null, L2_TTL, 0.0,
                NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                Duration.ZERO, false, Duration.ofSeconds(1));
        DefaultTierCache<String, String> cache =
                newCache(noWindow, Runnable::run, metrics, l1, l2);
        // Physically present but older than the TTL: with the window disabled
        // there is no age classification at all — presence means hit.
        l2.put("k", StoredEntry.ofValue("v", null, pastWindowTimestamp()), Duration.ofHours(1));

        assertEquals("v", cache.get("k"));
        assertEquals(0, metrics.staleHits("c"));
        assertEquals(0, metrics.triggers("c"));
    }

    @Test
    void entryWithoutWriteTimestampBehavesAsBefore() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache =
                newCache(staleSettings(), Runnable::run, metrics, l1, l2);
        l2.put("k", StoredEntry.ofValue("v"), Duration.ofHours(1)); // legacy frame

        assertEquals("v", cache.getOrCompute("k", key -> fail("loader must not run")));
        assertEquals(0, metrics.staleHits("c"));
        assertEquals(0, metrics.triggers("c"));
    }

    @Test
    void lookupClassifiesStaleEntries() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache =
                newCache(staleSettings(), Runnable::run, metrics, l1, l2);
        l2.put("stale", StoredEntry.ofValue("v", null, staleTimestamp()), Duration.ofHours(1));
        l2.put("ancient", StoredEntry.ofValue("v", null, pastWindowTimestamp()), Duration.ofHours(1));

        LookupResult<String> staleResult = cache.lookup("stale");
        assertEquals("v", assertInstanceOf(LookupResult.Hit.class, staleResult).value());
        assertEquals(1, metrics.staleHits("c"));
        assertEquals(0, metrics.triggers("c"), "lookup has no loader: no revalidation");
        assertInstanceOf(LookupResult.Miss.class, cache.lookup("ancient"));
    }

    @Test
    void staleNullMarkerIsServedAndRevalidated() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache =
                newCache(staleSettings(), Runnable::run, metrics, l1, l2);
        l2.put("k", StoredEntry.nullMarker(null, staleTimestamp()), Duration.ofHours(1));

        assertNull(cache.getOrCompute("k", key -> "recovered"),
                "the stale null-marker is served immediately");
        assertEquals(1, metrics.staleHits("c"));
        assertEquals(1, metrics.completions("c"), "inline executor revalidates synchronously");
        assertEquals("recovered", cache.get("k"));
    }

    // --- Requirement: Single asynchronous revalidation ---

    @Test
    void herdOnStaleKeyTriggersExactlyOneRevalidation() throws Exception {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache =
                newCache(staleSettings(), newExecutor(1), metrics, l1, l2);
        l2.put("hot", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        int threads = 16;
        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch loaderRelease = new CountDownLatch(1);
        ExecutorService readers = newExecutor(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<String>> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            results.add(readers.submit(() -> {
                start.await();
                return cache.getOrCompute("hot", key -> {
                    loaderCalls.incrementAndGet();
                    loaderEntered.countDown();
                    awaitQuietly(loaderRelease);
                    return "fresh";
                });
            }));
        }
        start.countDown();
        assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
        Thread.sleep(200); // let the herd pile onto the stale key

        // Every reader is served the stale value while the loader is blocked.
        for (Future<String> f : results) {
            assertEquals("stale", f.get(5, TimeUnit.SECONDS));
        }

        // A further stale read during the in-flight revalidation loses the
        // claim race and must not trigger a second one.
        l1.evict("hot");
        assertEquals("stale", cache.getOrCompute("hot", key -> fail("loader already in flight")));

        loaderRelease.countDown();
        awaitTrue(() -> metrics.completions("c") == 1);
        assertEquals(1, loaderCalls.get(), "exactly one revalidation runs the loader");
        assertEquals(1, metrics.triggers("c"), "exactly one revalidation is triggered");
        assertEquals("fresh", cache.get("hot"));
    }

    @Test
    void successfulRevalidationPublishesFreshValueWithStaleWindow() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache =
                newCache(staleSettings(), Runnable::run, metrics, l1, l2);
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        assertEquals("stale", cache.getOrCompute("k", key -> "fresh"));
        assertEquals(1, metrics.completions("c"));

        StoredEntry<String> refreshed = l2.get("k");
        assertNotNull(refreshed);
        assertEquals("fresh", refreshed.value());
        assertTrue(refreshed.hasWriteTimestamp(),
                "the refreshed entry keeps its stale window (extended write)");
        l1.evict("k");
        assertEquals("fresh", cache.get("k"));
        assertEquals(1, metrics.staleHits("c"), "the refreshed entry classifies as fresh");
    }

    @Test
    void versionedWritesKeepTheStaleWindow() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c", l1, l2,
                staleSettings(), true, null, null, new VersionGenerator(), null,
                null, metrics, Runnable::run);

        assertEquals("v", cache.getOrCompute("k", key -> "v"));
        StoredEntry<String> stored = l2.get("k");
        assertNotNull(stored);
        assertNotNull(stored.version(), "versioned write path stays version-conditional");
        assertTrue(stored.hasWriteTimestamp(),
                "versioned writes pass the stale window to the transport");
    }

    // --- Requirement: Revalidation failure keeps serving stale ---

    @Test
    void failingLoaderKeepsServingStaleAndCountsFailure() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache =
                newCache(staleSettings(), Runnable::run, metrics, l1, l2);
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        AtomicInteger loaderCalls = new AtomicInteger();
        String served = cache.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            throw new IllegalStateException("boom");
        });

        assertEquals("stale", served, "no loader exception reaches the caller");
        assertEquals(1, loaderCalls.get());
        assertEquals(1, metrics.failures("c"));
        assertEquals(0, metrics.completions("c"));
        assertEquals("stale", cache.get("k"), "the stale entry is untouched by the failure");
    }

    @Test
    void rejectedSubmissionKeepsStaleAndCountsFailure() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        ExecutorService dead = newExecutor(1);
        dead.shutdownNow();
        DefaultTierCache<String, String> cache =
                newCache(staleSettings(), dead, metrics, l1, l2);
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        assertEquals("stale", cache.getOrCompute("k", key -> fail("never submitted")));
        assertEquals(1, metrics.failures("c"));

        // The claim was dropped, so a later read retries the submission.
        l1.evict("k");
        assertEquals("stale", cache.getOrCompute("k", key -> fail("never submitted")));
        assertEquals(2, metrics.failures("c"));
        assertEquals(0, metrics.triggers("c") - metrics.failures("c"),
                "every rejected trigger is accounted as a failure");
    }

    @Test
    void staleServedWithoutRevalidationWhenNoExecutorWired() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        // Legacy 10-arg wiring: no revalidation executor.
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c", l1, l2,
                staleSettings(), true, null, null, null, null, null, metrics);
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        assertEquals("stale", cache.getOrCompute("k", key -> fail("no executor, no revalidation")));
        assertEquals(1, metrics.staleHits("c"));
        assertEquals(0, metrics.triggers("c"));
    }

    // --- Revalidation reuses the rebuild coordination ---

    @Test
    void lostLockRaceSkipsReloadAndKeepsServingStale() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        InMemoryLockProvider locks = new InMemoryLockProvider();
        ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(
                daemonFactory("test-watchdog"));
        executors.add(watchdog);
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c", l1, l2,
                staleSettings(), true, locks, watchdog, null, null, null, metrics, Runnable::run);
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        DistributedLock held = locks.tryLock("c:k", Duration.ofMinutes(1));
        assertNotNull(held);
        try {
            assertEquals("stale", cache.getOrCompute("k", key -> fail("lock lost: no reload")));
            assertEquals(1, metrics.completions("c"),
                    "a lost lock race is not a failure: the other instance refreshes");
            assertEquals(0, metrics.failures("c"));
        } finally {
            held.release();
        }
    }

    @Test
    void newerWriteSuppressesReload() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        InMemoryLockProvider locks = new InMemoryLockProvider();
        ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(
                daemonFactory("test-watchdog"));
        executors.add(watchdog);
        ExecutorService revalidation = newExecutor(1);
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c", l1, l2,
                staleSettings(), true, locks, watchdog, null, null, null, metrics, revalidation);
        long servedTimestamp = staleTimestamp();
        l2.put("k", StoredEntry.ofValue("stale", null, servedTimestamp), Duration.ofHours(1));

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch block = new CountDownLatch(1);
        revalidation.submit(() -> awaitQuietly(block)); // occupy the only worker
        assertEquals("stale", cache.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            return "unwanted";
        }));

        // A newer write lands before the queued revalidation runs.
        l2.put("k", StoredEntry.ofValue("newer", null, freshTimestamp()), Duration.ofHours(1));
        block.countDown();

        awaitTrue(() -> metrics.completions("c") == 1);
        assertEquals(0, loaderCalls.get(), "a newer write suppresses the reload");
        assertEquals("newer", cache.get("k"), "L1 converges to the newer write");
    }

    @Test
    void revalidationLoadsThroughCoordinationWhenLockIsFree() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        InMemoryLockProvider locks = new InMemoryLockProvider();
        ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(
                daemonFactory("test-watchdog"));
        executors.add(watchdog);
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c", l1, l2,
                staleSettings(), true, locks, watchdog, null, null, null, metrics, Runnable::run);
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        assertEquals("stale", cache.getOrCompute("k", key -> "fresh"));
        assertEquals(1, metrics.completions("c"));
        assertEquals("fresh", l2.get("k").value(), "the lock winner reloads and stores");
    }

    @Test
    void entryGoneBeforeRevalidationReloads() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        InMemoryLockProvider locks = new InMemoryLockProvider();
        ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(
                daemonFactory("test-watchdog"));
        executors.add(watchdog);
        ExecutorService revalidation = newExecutor(1);
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c", l1, l2,
                staleSettings(), true, locks, watchdog, null, null, null, metrics, revalidation);
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch block = new CountDownLatch(1);
        revalidation.submit(() -> awaitQuietly(block));
        assertEquals("stale", cache.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            return "fresh";
        }));

        l2.evict("k"); // the entry disappears before the revalidation runs
        block.countDown();

        awaitTrue(() -> metrics.completions("c") == 1);
        assertEquals(1, loaderCalls.get(), "an absent entry is reloaded");
        assertEquals("fresh", cache.get("k"));
    }

    @Test
    void legacyEntryInL2DoesNotSuppressReload() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        InMemoryLockProvider locks = new InMemoryLockProvider();
        ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(
                daemonFactory("test-watchdog"));
        executors.add(watchdog);
        ExecutorService revalidation = newExecutor(1);
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c", l1, l2,
                staleSettings(), true, locks, watchdog, null, null, null, metrics, revalidation);
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch block = new CountDownLatch(1);
        revalidation.submit(() -> awaitQuietly(block));
        assertEquals("stale", cache.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            return "fresh";
        }));

        // A legacy write (no write timestamp) cannot be compared by write
        // time, so the revalidation still reloads.
        l2.put("k", StoredEntry.ofValue("legacy"), Duration.ofHours(1));
        block.countDown();

        awaitTrue(() -> metrics.completions("c") == 1);
        assertEquals(1, loaderCalls.get());
    }

    @Test
    void lockProviderWithoutWatchdogFallsBackToPerInstanceLoad() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c", l1, l2,
                staleSettings(), true, new InMemoryLockProvider(), null, null, null,
                null, metrics, Runnable::run);
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        assertEquals("stale", cache.getOrCompute("k", key -> "fresh"));
        assertEquals(1, metrics.completions("c"));
        assertEquals("fresh", cache.get("k"));
    }

    @Test
    void openBreakerFallsBackToPerInstanceL1OnlyLoad() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        InMemoryLockProvider locks = new InMemoryLockProvider();
        ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(
                daemonFactory("test-watchdog"));
        executors.add(watchdog);
        CircuitBreaker breaker = new CircuitBreaker(
                new CircuitBreaker.Config(1, 1.0, 1, Duration.ofMinutes(10), 1),
                new CircuitBreaker.Listener() {
                    @Override
                    public void onOpen() {
                    }

                    @Override
                    public void onClose() {
                    }
                });
        ExecutorService revalidation = newExecutor(1);
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c", l1, l2,
                staleSettings(), true, locks, watchdog, null, null, breaker, metrics, revalidation);
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp()), Duration.ofHours(1));

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch block = new CountDownLatch(1);
        revalidation.submit(() -> awaitQuietly(block));
        assertEquals("stale", cache.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            return "fresh";
        }));

        breaker.onFailure(); // L2 goes down before the revalidation runs
        assertTrue(breaker.isOpen());
        block.countDown();

        awaitTrue(() -> metrics.completions("c") == 1);
        assertEquals(1, loaderCalls.get(), "degradation falls back to a per-instance load");
        assertEquals("stale", l2.get("k").value(), "L2 is untouched while the breaker is open");
        assertEquals("fresh", cache.get("k"), "the fresh value is stored L1-only");
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void awaitTrue(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("condition not met within 5s");
            }
            try {
                TimeUnit.NANOSECONDS.sleep(TimeUnit.MILLISECONDS.toNanos(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while waiting");
            }
        }
    }
}
