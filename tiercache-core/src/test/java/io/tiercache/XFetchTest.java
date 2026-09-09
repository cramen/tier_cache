package io.tiercache;

import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Spec: stale-serving — XFetch probabilistic early refresh and the per-cache
 * loader-duration EMA that drives it.
 */
class XFetchTest {

    private static final Duration L2_TTL = Duration.ofHours(1);
    private static final int SAMPLE_READS = 1_500;

    private final List<ExecutorService> executors = new ArrayList<>();

    @AfterEach
    void tearDown() {
        executors.forEach(ExecutorService::shutdownNow);
    }

    /** Counts revalidation triggers per cache. */
    private static final class TriggerCounter implements CacheMetricsListener {
        private final Map<String, AtomicInteger> triggers = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> completions = new ConcurrentHashMap<>();

        @Override
        public void onRevalidationTriggered(String cache) {
            triggers.computeIfAbsent(cache, k -> new AtomicInteger()).incrementAndGet();
        }

        @Override
        public void onRevalidationCompleted(String cache) {
            completions.computeIfAbsent(cache, k -> new AtomicInteger()).incrementAndGet();
        }

        int triggers(String cache) {
            AtomicInteger c = triggers.get(cache);
            return c == null ? 0 : c.get();
        }

        int completions(String cache) {
            AtomicInteger c = completions.get(cache);
            return c == null ? 0 : c.get();
        }
    }

    private static CacheSettings xfetchSettings(Duration beta) {
        return new CacheSettings(10_000, Duration.ofMinutes(5), null, L2_TTL, 0.0,
                NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                Duration.ZERO, true, beta);
    }

    private ExecutorService newExecutor(int threads) {
        ExecutorService executor = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "test-xfetch");
            t.setDaemon(true);
            return t;
        });
        executors.add(executor);
        return executor;
    }

    private static Function<String, String> sleepingLoader(long nanos) {
        return key -> {
            sleepQuietly(nanos);
            return "v";
        };
    }

    private static void sleepQuietly(long nanos) {
        try {
            TimeUnit.NANOSECONDS.sleep(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Writes {@code count} entries with the given age directly into L2. */
    private static void seedEntries(InMemoryRemoteCache<String, String> l2, String prefix,
            int count, long ageMillis) {
        long writeTimestamp = System.currentTimeMillis() - ageMillis;
        for (int i = 0; i < count; i++) {
            l2.put(prefix + i, StoredEntry.ofValue("v", null, writeTimestamp),
                    Duration.ofHours(2));
        }
    }

    private static void readAll(DefaultTierCache<String, String> cache, String prefix, int count,
            Function<String, String> loader) {
        for (int i = 0; i < count; i++) {
            cache.getOrCompute(prefix + i, loader);
        }
    }

    // --- Requirement: loader-duration EMA ---

    @Test
    void emaConvergesTowardConstantLoaderDurationWithoutMetricsBinding() {
        // No metrics listener bound: the EMA must still be tracked.
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), new InMemoryRemoteCache<>(),
                CacheSettings.defaults(), true, null, null, null, null,
                null, CacheMetricsListener.NOOP, Runnable::run);
        assertEquals(-1, cache.loaderDurationEmaNanos(),
                "uninitialized before the first measured load");

        long expected = TimeUnit.MILLISECONDS.toNanos(3);
        for (int i = 0; i < 12; i++) {
            cache.getOrCompute("k" + i, sleepingLoader(expected));
        }
        long ema = cache.loaderDurationEmaNanos();
        assertTrue(ema >= TimeUnit.MILLISECONDS.toNanos(1) && ema <= TimeUnit.MILLISECONDS.toNanos(50),
                "EMA should converge toward the ~3ms loader duration, was " + ema + "ns");
    }

    @Test
    void uninitializedEmaMeansZeroXFetchProbability() {
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        TriggerCounter metrics = new TriggerCounter();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, xfetchSettings(Duration.ofMillis(1)), true,
                null, null, null, null, null, metrics, newExecutor(2));
        seedEntries(l2, "fresh-", 500, 0);

        // No loader has ever run: the EMA is uninitialized and no draw happens.
        readAll(cache, "fresh-", 500, key -> fail("fresh entry: loader must not run"));
        for (int i = 0; i < 500; i++) {
            l2.put("g" + i, StoredEntry.ofValue("v", null, System.currentTimeMillis()),
                    Duration.ofHours(2));
            cache.get("g" + i); // plain get has no loader: never a gate
        }
        assertEquals(0, metrics.triggers("c"));
    }

    // --- Requirement: Probabilistic early refresh (XFetch) ---

    @Test
    void nearExpiryEntriesRefreshMoreOftenThanFreshOnes() {
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        TriggerCounter metrics = new TriggerCounter();
        ExecutorService executor = newExecutor(8);
        Duration beta = Duration.ofMillis(2);
        Function<String, String> loader = sleepingLoader(TimeUnit.MILLISECONDS.toNanos(2));

        DefaultTierCache<String, String> near = new DefaultTierCache<>("near",
                new CountingLocalCache<>(), l2, xfetchSettings(beta), true,
                null, null, null, null, null, metrics, executor);
        DefaultTierCache<String, String> fresh = new DefaultTierCache<>("fresh",
                new CountingLocalCache<>(), l2, xfetchSettings(beta), true,
                null, null, null, null, null, metrics, executor);

        // Prime both EMAs with ~2ms loads so delta/beta ~= 1.
        for (int i = 0; i < 8; i++) {
            near.getOrCompute("prime-n" + i, loader);
            fresh.getOrCompute("prime-f" + i, loader);
        }

        long nearExpiryAge = (long) (L2_TTL.toMillis() * 0.99);
        seedEntries(l2, "near-", SAMPLE_READS, nearExpiryAge);
        seedEntries(l2, "fresh-", SAMPLE_READS, 0);
        readAll(near, "near-", SAMPLE_READS, loader);
        readAll(fresh, "fresh-", SAMPLE_READS, loader);

        // Near-expiry p = 1 - exp(-0.99 * delta/beta) ~ 0.63..0.92 -> mean
        // 945+ of 1500; fresh p ~ 0. Assertions carry >20-sigma margins, and
        // trigger counting is synchronous, so this cannot flake.
        assertTrue(metrics.triggers("near") >= 400,
                "near-expiry refresh rate too low: " + metrics.triggers("near"));
        assertTrue(metrics.triggers("fresh") <= 150,
                "fresh refresh rate too high: " + metrics.triggers("fresh"));
    }

    @Test
    void negligibleLoaderDurationStaysNearZero() {
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        TriggerCounter metrics = new TriggerCounter();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, xfetchSettings(Duration.ofSeconds(1)), true,
                null, null, null, null, null, metrics, newExecutor(2));
        Function<String, String> instantLoader = key -> "v";

        // Prime with instant loads: delta stays negligible against beta=1s.
        for (int i = 0; i < 30; i++) {
            cache.getOrCompute("prime-" + i, instantLoader);
        }
        seedEntries(l2, "near-", 2_000, (long) (L2_TTL.toMillis() * 0.99));
        readAll(cache, "near-", 2_000, instantLoader);

        assertTrue(metrics.triggers("c") <= 50,
                "cheap entries must be refreshed ~never, got " + metrics.triggers("c"));
    }

    @Test
    void disabledXFetchNeverRefreshes() {
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        TriggerCounter metrics = new TriggerCounter();
        // Stale window on (so age is classified) but XFetch off.
        CacheSettings settings = new CacheSettings(10_000, Duration.ofMinutes(5), null, L2_TTL, 0.0,
                NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                Duration.ofMinutes(30), false, Duration.ofSeconds(1));
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, settings, true,
                null, null, null, null, null, metrics, newExecutor(2));
        for (int i = 0; i < 8; i++) {
            cache.getOrCompute("prime-" + i, sleepingLoader(TimeUnit.MILLISECONDS.toNanos(2)));
        }
        seedEntries(l2, "near-", 500, (long) (L2_TTL.toMillis() * 0.99));
        readAll(cache, "near-", 500, key -> fail("fresh entry: loader must not run"));
        assertEquals(0, metrics.triggers("c"));
    }

    @Test
    void winningDrawReloadsThroughTheRevalidationPath() {
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        TriggerCounter metrics = new TriggerCounter();
        // beta = 1ns with a measured ~20ms delta: p = 1 - exp(-huge) == 1.0,
        // so every near-expiry read wins the draw — fully deterministic.
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, xfetchSettings(Duration.ofNanos(1)), true,
                null, null, null, null, null, metrics, Runnable::run);
        for (int i = 0; i < 3; i++) {
            cache.getOrCompute("prime-" + i, sleepingLoader(TimeUnit.MILLISECONDS.toNanos(20)));
        }

        AtomicInteger loaderCalls = new AtomicInteger();
        l2.put("k", StoredEntry.ofValue("old", null,
                System.currentTimeMillis() - (long) (L2_TTL.toMillis() * 0.99)),
                Duration.ofHours(2));
        assertEquals("old", cache.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            return "fresh";
        }));
        assertEquals(1, metrics.triggers("c"));
        assertEquals(1, metrics.completions("c"));
        assertEquals(1, loaderCalls.get(), "a won draw refreshes through the revalidation path");
        assertEquals("fresh", cache.get("k"));
    }

    @Test
    void futureWriteTimestampNeverTriggersRefresh() {
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        TriggerCounter metrics = new TriggerCounter();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, xfetchSettings(Duration.ofNanos(1)), true,
                null, null, null, null, null, metrics, newExecutor(2));
        for (int i = 0; i < 3; i++) {
            cache.getOrCompute("prime-" + i, sleepingLoader(TimeUnit.MILLISECONDS.toNanos(20)));
        }
        // Clock skew: the write timestamp is ahead of this node's clock.
        l2.put("k", StoredEntry.ofValue("v", null, System.currentTimeMillis() + 60_000),
                Duration.ofHours(2));
        assertEquals("v", cache.getOrCompute("k", key -> fail("fresh entry: loader must not run")));
        assertEquals(0, metrics.triggers("c"));
    }

    @Test
    void lostDrawIsPlainHit() {
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        TriggerCounter metrics = new TriggerCounter();
        // delta ~ 2ms against beta = 1h: p ~ 1e-9 even at ageFraction 0.99,
        // so draws are lost deterministically.
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, xfetchSettings(Duration.ofHours(1)), true,
                null, null, null, null, null, metrics, newExecutor(2));
        Function<String, String> loader = sleepingLoader(TimeUnit.MILLISECONDS.toNanos(2));
        for (int i = 0; i < 8; i++) {
            cache.getOrCompute("prime-" + i, loader);
        }
        seedEntries(l2, "near-", 200, (long) (L2_TTL.toMillis() * 0.99));
        readAll(cache, "near-", 200, loader);
        assertEquals(0, metrics.triggers("c"));
    }
}
