package io.tiercache;

import io.tiercache.internal.CircuitBreaker;
import io.tiercache.internal.CircuitBreakerRemoteCache;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Degradation stale window (load-test review): while the breaker rejects
 * L2 calls, a physically retained but logically expired L1 entry is served
 * stale without a loader call — breaker-gated, deadline-honest, and never
 * changing normal-mode freshness.
 */
class DegradationStaleTest {

    private static final Duration L1_TTL = Duration.ofMillis(200);
    private static final Duration WINDOW = Duration.ofMillis(500);

    private static final class RecordingMetrics implements CacheMetricsListener {
        final Map<Outcome, AtomicInteger> counts = new ConcurrentHashMap<>();

        @Override
        public void onRequest(String cache, Outcome outcome) {
            counts.computeIfAbsent(outcome, o -> new AtomicInteger()).incrementAndGet();
        }

        int count(Outcome outcome) {
            var counter = counts.get(outcome);
            return counter == null ? 0 : counter.get();
        }
    }

    private static CircuitBreaker.Config fastBreaker() {
        // Opens on the first failure; half-opens after 200 ms; one probe closes.
        return new CircuitBreaker.Config(1, 1.0, 1, Duration.ofMillis(200), 1);
    }

    private record Rig(DefaultTierCache<String, String> cache, CircuitBreaker breaker,
            InMemoryRemoteCache<String, String> l2Delegate, RecordingMetrics metrics) {
    }

    private static Rig rig(CacheSettings settings) {
        return rig(settings, new CountingLocalCache<>());
    }

    private static Rig rig(CacheSettings settings, Duration halfOpenAfter) {
        return rig(settings, new CountingLocalCache<>(), halfOpenAfter);
    }

    /**
     * Test rigs use {@link CountingLocalCache} (no physical expiry) so
     * stale behavior is driven deterministically by the engine's stamped
     * deadlines; the knob-off test passes a real {@link
     * io.tiercache.internal.CaffeineLocalCache} for physical expiry.
     */
    private static Rig rig(CacheSettings settings, io.tiercache.spi.LocalCache<String, String> l1) {
        return rig(settings, l1, Duration.ofMillis(200));
    }

    private static Rig rig(CacheSettings settings, io.tiercache.spi.LocalCache<String, String> l1,
            Duration halfOpenAfter) {
        CircuitBreaker breaker = new CircuitBreaker(
                new CircuitBreaker.Config(1, 1.0, 1, halfOpenAfter, 1), new CircuitBreaker.Listener() {
            @Override
            public void onOpen() {
            }

            @Override
            public void onClose() {
            }
        });
        InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        RemoteCache<String, String> l2 = new CircuitBreakerRemoteCache<>(delegate, breaker);
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                l1, l2, settings, true, null, null, null, null, breaker, metrics);
        return new Rig(cache, breaker, delegate, metrics);
    }

    private static CacheSettings settings(Duration l1Ttl, Duration accessTtl, Duration window) {
        return new CacheSettings(10_000, l1Ttl, accessTtl, Duration.ofHours(1), 0.0,
                NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                Duration.ZERO, false, Duration.ofSeconds(1), window);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static <V> java.util.function.Function<String, V> throwingLoader() {
        return key -> {
            throw new AssertionError("the loader must not be invoked");
        };
    }

    /**
     * Outage serving without a source storm: logically expired but retained
     * entry, breaker OPEN → stale serve, zero loader calls, metric counted.
     */
    @Test
    void openBreakerServesStaleWithinWindowWithoutLoader() {
        Rig rig = rig(settings(L1_TTL, null, WINDOW));
        rig.cache().put("k", "v1");
        sleep(250); // past the logical L1 TTL (200 ms), inside the window (700 ms)
        rig.breaker().onFailure(); // OPEN

        assertEquals("v1", rig.cache().getOrCompute("k", throwingLoader()),
                "the retained entry is served stale while the breaker rejects");
        assertEquals("v1", rig.cache().get("k"), "plain get also serves stale");
        assertEquals(2, rig.metrics().count(CacheMetricsListener.Outcome.STALE_DEGRADED),
                "every degraded-stale serve is counted");
    }

    /**
     * Window end falls back to the loader: past L1 TTL + window the stale
     * horizon is over and today's degraded loader fallback applies.
     */
    @Test
    void pastWindowFallsBackToLoader() {
        Rig rig = rig(settings(L1_TTL, null, Duration.ofMillis(300)));
        rig.cache().put("k", "v1");
        rig.cache().put("k2", "v2"); // never reloaded: exercises get/lookup past-window
        sleep(550); // past 200 + 300
        rig.breaker().onFailure();
        AtomicInteger loads = new AtomicInteger();

        assertEquals("fresh", rig.cache().getOrCompute("k", key -> {
            loads.incrementAndGet();
            return "fresh";
        }), "past the window the loader fallback applies");
        assertEquals(1, loads.get());
        assertNull(rig.cache().get("k2"),
                "plain get past the window: rejected read, honest miss (no stale)");
        assertEquals(LookupResult.Miss.instance(), rig.cache().lookup("k2"),
                "lookup past the window is a miss too");
        assertEquals(0, rig.metrics().count(CacheMetricsListener.Outcome.STALE_DEGRADED));
    }

    /**
     * Closed breaker never serves the extra retention: an admitted L2 read
     * converges to the L2 truth instead of serving the retained entry.
     */
    @Test
    void closedBreakerConvergesFromL2InsteadOfServingRetention() {
        Rig rig = rig(settings(L1_TTL, null, WINDOW));
        rig.cache().put("k", "v1");
        sleep(250);
        // L2 now holds a NEWER value; the breaker is CLOSED.
        rig.l2Delegate().put("k", io.tiercache.spi.StoredEntry.ofValue("v2"),
                Duration.ofHours(1));

        assertEquals("v2", rig.cache().get("k"),
                "plain get converges via the classified read instead of the retention");
        assertEquals("v2", ((LookupResult.Hit<String>) rig.cache().lookup("k")).value(),
                "lookup converges the same way");
        assertEquals("v2", rig.cache().getOrCompute("k", throwingLoader()),
                "an admitted L2 read converges instead of serving the retention");
        assertEquals(0, rig.metrics().count(CacheMetricsListener.Outcome.STALE_DEGRADED));
    }

    /**
     * Default (knob off) behaves exactly as before: no stale machinery, the
     * degraded loader fallback on expiry.
     */
    @Test
    void defaultKnobOffBehavesAsBefore() {
        CacheSettings knobOff = new CacheSettings(10_000, L1_TTL, null, Duration.ofHours(1),
                0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                Duration.ZERO, false, Duration.ofSeconds(1), Duration.ZERO);
        Rig rig = rig(knobOff, new io.tiercache.internal.CaffeineLocalCache<>(knobOff));
        rig.cache().put("k", "v1");
        sleep(250);
        rig.breaker().onFailure();

        assertEquals("reloaded", rig.cache().getOrCompute("k", key -> "reloaded"),
                "knob off: the loader fallback is unchanged");
        assertEquals(0, rig.metrics().count(CacheMetricsListener.Outcome.STALE_DEGRADED));
    }

    /**
     * HALF_OPEN: probe permits flow through the normal path (they can close
     * the breaker) while requests without a permit serve stale; a closing
     * probe restores normal rules.
     */
    @Test
    void halfOpenProbesFlowAndNonProbesServeStale() {
        Rig rig = rig(settings(L1_TTL, null, WINDOW));
        rig.cache().put("k", "v1");
        sleep(250);
        rig.breaker().onFailure(); // OPEN
        sleep(250); // halfOpenAfter elapsed

        // Consume the single probe permit manually and hold it.
        assertTrue(rig.breaker().tryAcquire(), "the half-open transition admits a probe");
        assertEquals("v1", rig.cache().getOrCompute("k", throwingLoader()),
                "no permit left: the reader is served stale instead of queueing");

        rig.breaker().onSuccess(); // the held probe closes the breaker
        assertEquals(io.tiercache.BreakerState.CLOSED, rig.breaker().state());
        assertEquals("v1", rig.cache().getOrCompute("k", throwingLoader()),
                "closed breaker: admitted reads converge normally (no stale)");
    }

    /**
     * A probe failure in HALF_OPEN re-opens: stale rules resume.
     */
    @Test
    void halfOpenProbeFailureReopensAndStaleResumes() {
        Rig rig = rig(settings(L1_TTL, null, WINDOW));
        rig.cache().put("k", "v1");
        sleep(250);
        rig.breaker().onFailure();
        sleep(250);

        assertTrue(rig.breaker().tryAcquire(), "the half-open transition admits a probe");
        rig.breaker().onFailure(); // the probe failed: back to OPEN
        assertEquals("v1", rig.cache().getOrCompute("k", throwingLoader()),
                "the failed probe re-opens: stale rules resume");
    }

    /**
     * L1-local freshness: a late L2-hit warm gets its FULL L1 TTL from the
     * warm moment (never the L2 write timestamp), with zero extra L2 reads
     * inside that freshness window.
     */
    @Test
    void lateL2WarmGetsFullL1FreshnessWithZeroExtraReads() {
        Rig rig = rig(settings(L1_TTL, null, WINDOW));
        rig.cache().put("k", "v"); // written at t=0
        sleep(250);
        rig.cache().evictAllL1(); // L1 empty, L2 intact

        assertEquals("v", rig.cache().get("k"), "L2 hit warms L1 at t≈250");
        for (int i = 0; i < 3; i++) {
            sleep(50);
            assertEquals("v", rig.cache().get("k"),
                    "warm-time freshness: served from L1 until t≈450, no L2 re-read");
        }
    }

    /**
     * Fresh-access slides freshness and the stale horizon; a constant
     * stale-read stream never extends them; the store-time retention floor
     * is never shortened by a short access TTL.
     */
    @Test
    void accessRulesSlideFreshnessButNeverTheRetentionFloor() {
        CacheSettings settings = new CacheSettings(10_000, Duration.ofSeconds(10),
                Duration.ofMillis(100), Duration.ofHours(1), 0.0,
                NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                Duration.ZERO, false, Duration.ofSeconds(1), WINDOW);
        Rig rig = rig(settings, Duration.ofSeconds(30)); // no half-open during the test
        rig.cache().put("k", "v");
        sleep(50);
        assertEquals("v", rig.cache().get("k"),
                "fresh access slides the logical deadline forward by the access TTL");

        sleep(150); // past the slid deadline (50+100), inside the slid horizon
        rig.breaker().onFailure();
        assertEquals("v", rig.cache().getOrCompute("k", throwingLoader()),
                "the slid stale horizon still allows stale serving");
        for (int i = 0; i < 3; i++) {
            sleep(100);
            rig.cache().get("k"); // stale accesses must not extend the horizon
        }
        sleep(200); // total elapsed since the slide (~700 ms) exceeds the slid horizon
        assertEquals("reloaded", rig.cache().getOrCompute("k", key -> "reloaded"),
                "stale reads never extend the stale horizon");
    }

    /**
     * The null-marker keeps its own (short) TTL under the window: within
     * marker TTL + window the stale marker answers null without a loader;
     * past it the loader runs.
     */
    @Test
    void nullMarkerFollowsItsOwnTtlUnderTheWindow() {
        CacheSettings settings = new CacheSettings(10_000, Duration.ofSeconds(10), null,
                Duration.ofHours(1), 0.0, NullPolicy.allow(Duration.ofMillis(150)),
                InvalidationMode.INVALIDATE, 64 * 1024,
                Duration.ZERO, false, Duration.ofSeconds(1), Duration.ofMillis(400));
        Rig rig = rig(settings);
        rig.cache().putNull("k");
        sleep(200); // past the marker TTL (150), inside 150+400
        rig.breaker().onFailure();

        assertNull(rig.cache().getOrCompute("k", throwingLoader()),
                "the stale null-marker answers without a loader call");

        sleep(400); // past 150+400
        assertEquals("real", rig.cache().getOrCompute("k", key -> "real"),
                "past the marker horizon the loader runs");
    }

    /**
     * Lookup also serves stale within the window (the metric and the
     * returned result mirror get/getOrCompute).
     */
    @Test
    void lookupServesStaleTooWithinWindow() {
        Rig rig = rig(settings(L1_TTL, null, WINDOW));
        rig.cache().put("k", "v1");
        sleep(250);
        rig.breaker().onFailure();

        LookupResult<String> result = rig.cache().lookup("k");
        assertTrue(result instanceof LookupResult.Hit<String>,
                "lookup serves the retained entry stale, got " + result);
        assertEquals("v1", ((LookupResult.Hit<String>) result).value());
        assertEquals(1, rig.metrics().count(CacheMetricsListener.Outcome.STALE_DEGRADED));
    }

    /**
     * A FAILED L2 read (the call errored, not a breaker rejection) takes
     * today's degraded fallback instead of the stale path.
     */
    @Test
    void failedL2ReadFallsBackToLoader() {
        CircuitBreaker breaker = new CircuitBreaker(
                new CircuitBreaker.Config(10, 1.0, 5, Duration.ofSeconds(30), 1),
                new CircuitBreaker.Listener() {
                    @Override
                    public void onOpen() {
                    }

                    @Override
                    public void onClose() {
                    }
                });
        InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();
        RemoteCache<String, String> failingGet = new RemoteCache<>() {
            @Override
            public StoredEntry<String> get(String key) {
                throw new io.tiercache.internal.L2UnavailableException("boom");
            }

            @Override
            public void put(String key, StoredEntry<String> entry, Duration ttl) {
                delegate.put(key, entry, ttl);
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
            public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) {
                return delegate.setIfAbsent(key, entry, ttl);
            }
        };
        RemoteCache<String, String> l2 = new CircuitBreakerRemoteCache<>(failingGet, breaker);
        RecordingMetrics metrics = new RecordingMetrics();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, settings(L1_TTL, null, WINDOW), true,
                null, null, null, null, breaker, metrics);
        cache.put("k", "v1");
        sleep(250);

        // The delegate throws on every get: the read is FAILED (the single
        // failure does not open this breaker), so the loader fallback runs.
        assertEquals("fresh", cache.getOrCompute("k", key -> "fresh"),
                "a failed read is not a stale-serve trigger: loader fallback");
        assertEquals(0, metrics.count(CacheMetricsListener.Outcome.STALE_DEGRADED));
    }

    /**
     * An entry without stamped metadata (stored through a path that does
     * not stamp, e.g. the degraded setIfAbsent) is never stale-served.
     */
    @Test
    void degradedSetIfAbsentWinnerIsImmediatelyAvailable() {
        Rig rig = rig(settings(L1_TTL, null, WINDOW));
        rig.breaker().onFailure(); // outage first: putIfAbsent goes L1-only
        assertTrue(rig.cache().putIfAbsent("k", "v1"),
                "the degraded set-if-absent wins");

        assertEquals("v1", rig.cache().get("k"),
                "the winning insert is immediately readable");
        assertEquals("v1", rig.cache().getOrCompute("k", throwingLoader()),
                "and needs no reload — the winner got full metadata and retention");
        assertEquals(0, rig.metrics().count(CacheMetricsListener.Outcome.STALE_DEGRADED),
                "it is served as fresh within its logical TTL, not as stale");

        // The loser still changes nothing.
        org.junit.jupiter.api.Assertions.assertFalse(rig.cache().putIfAbsent("k", "v2"),
                "the existing entry wins");
        assertEquals("v1", rig.cache().get("k"));
    }

    /**
     * Freshness-aware coordinated path: with a healthy Redis (CLOSED
     * breaker) a logically expired retained entry is NOT a hit — the
     * coordinated load goes to the loader (the Docker healthy-stale probe).
     */
    @Test
    void coordinatedPathNeverServesLogicallyExpiredAsHit() {
        CircuitBreaker breaker = new CircuitBreaker(fastBreaker(), new CircuitBreaker.Listener() {
            @Override
            public void onOpen() {
            }

            @Override
            public void onClose() {
            }
        });
        InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        RemoteCache<String, String> l2 = new CircuitBreakerRemoteCache<>(delegate, breaker);
        java.util.concurrent.ScheduledExecutorService watchdog =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        try {
            DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                    new CountingLocalCache<>(), l2, settings(L1_TTL, null, WINDOW), true,
                    new io.tiercache.testkit.InMemoryLockProvider(), watchdog, null, null,
                    breaker, metrics);
            cache.put("k", "v0");
            // L2 expires quickly; L1 entry is retained (knob on) but logically dead.
            delegate.evict("k");
            sleep(250); // past the logical L1 TTL; breaker stays CLOSED
            AtomicInteger loads = new AtomicInteger();

            assertEquals("v1", cache.getOrCompute("k", key -> {
                loads.incrementAndGet();
                return "v1";
            }), "a healthy Redis must never serve the retained stale entry");
            assertEquals(1, loads.get(), "the coordinated path went to the loader");
            assertEquals("v1", cache.getOrCompute("k", throwingLoader()),
                "the fresh reloaded entry IS a hit on the coordinated double-check");
            assertEquals(0, metrics.count(CacheMetricsListener.Outcome.STALE_DEGRADED),
                    "no stale serving while the breaker is CLOSED");
        } finally {
            watchdog.shutdownNow();
        }
    }

    /**
     * Late capture fix: the generation is captured at operation start, so a
     * warm after an L2 read that raced a generation bump is refused — the
     * caller may receive the L2 value, but L1 must NOT keep it.
     */
    @Test
    void generationBumpDuringL2ReadRefusesTheWarm() throws Exception {
        CircuitBreaker breaker = new CircuitBreaker(fastBreaker(), new CircuitBreaker.Listener() {
            @Override
            public void onOpen() {
            }

            @Override
            public void onClose() {
            }
        });
        InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();
        java.util.concurrent.CountDownLatch getEntered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch releaseGet = new java.util.concurrent.CountDownLatch(1);
        RecordingMetrics metrics = new RecordingMetrics();
        // Wrap the delegate's read with a gate.
        RemoteCache<String, String> gated = new RemoteCache<>() {
            final InMemoryRemoteCache<String, String> inner = delegate;

            @Override
            public StoredEntry<String> get(String key) {
                getEntered.countDown();
                try {
                    releaseGet.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return inner.get(key);
            }

            @Override
            public void put(String key, StoredEntry<String> entry, Duration ttl) {
                inner.put(key, entry, ttl);
            }

            @Override
            public void evict(String key) {
                inner.evict(key);
            }

            @Override
            public void clear() {
                inner.clear();
            }

            @Override
            public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) {
                return inner.setIfAbsent(key, entry, ttl);
            }
        };
        RemoteCache<String, String> guarded = new CircuitBreakerRemoteCache<>(gated, breaker);
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), guarded, settings(L1_TTL, null, WINDOW), true,
                null, null, null, null, breaker, metrics);
        cache.put("k", "old");
        cache.evictAllL1(); // L1 empty, L2 still holds "old"

        java.util.concurrent.atomic.AtomicReference<String> returned = new java.util.concurrent.atomic.AtomicReference<>();
        Thread reader = new Thread(() -> returned.set(cache.get("k")));
        reader.start();
        if (!getEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
            throw new AssertionError("the L2 read was never reached");
        }
        cache.evictAllL1(); // generation bump mid-read
        releaseGet.countDown();
        reader.join(5_000);

        // The caller may receive what L2 said; L1 must NOT retain it.
        org.junit.jupiter.api.Assertions.assertNull(cache.versionOfL1Entry("k"),
                "the generation bump refuses the warm: L1 stays empty");
    }

    /**
     * Fresh-access slide can never cross-assign deadlines: replacing the
     * value with a short-lived marker while readers slide must keep the
     * marker's OWN TTL.
     */
    @Test
    void freshAccessSlideNeverCrossAssignsDeadlines() throws Exception {
        CacheSettings settings = new CacheSettings(10_000, Duration.ofSeconds(10),
                Duration.ofMillis(50), Duration.ofHours(1), 0.0,
                NullPolicy.allow(Duration.ofMillis(60)), InvalidationMode.INVALIDATE,
                64 * 1024, Duration.ZERO, false, Duration.ofSeconds(1), WINDOW);
        Rig rig = rig(settings);
        rig.cache().put("k", "v1");
        java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
        Thread slider = new Thread(() -> {
            while (!stop.get()) {
                rig.cache().get("k");
            }
        });
        slider.start();
        sleep(30);
        rig.cache().putNull("k"); // the marker must live by its own 60 ms TTL
        stop.set(true);
        slider.join(5_000);
        sleep(150); // well past the marker TTL, within any leaked long deadline

        assertEquals("real", rig.cache().getOrCompute("k", key -> "real"),
                "the marker expires on its own TTL, never on the previous value's");
    }

    /**
     * Admitted L2 HIT on the retained path goes through the normal
     * age/SWR classification: a stale SWR frame triggers revalidation
     * instead of being warmed as fresh.
     */
    @Test
    void retainedHitKeepsSwRClassification() throws Exception {
        CircuitBreaker breaker = new CircuitBreaker(fastBreaker(), new CircuitBreaker.Listener() {
            @Override
            public void onOpen() {
            }

            @Override
            public void onClose() {
            }
        });
        InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();
        RecordingMetrics metrics = new RecordingMetrics();
        RemoteCache<String, String> l2 = new CircuitBreakerRemoteCache<>(delegate, breaker);
        java.util.concurrent.ExecutorService revalidation =
                java.util.concurrent.Executors.newSingleThreadExecutor();
        CacheSettings settings = new CacheSettings(10_000, Duration.ofMillis(50), null,
                Duration.ofMillis(100), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE,
                64 * 1024, Duration.ofSeconds(1), false, Duration.ofSeconds(1), WINDOW);
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, settings, true, null, null, null, null,
                breaker, metrics, revalidation);
        try {
            AtomicInteger loads = new AtomicInteger();
            // L2 holds a SWR frame aged past the logical L2 TTL (100 ms).
            delegate.put("k", StoredEntry.ofValue("v0", null,
                    System.currentTimeMillis() - 500), Duration.ofMillis(100),
                    Duration.ofSeconds(1));
            cache.put("k", "v0"); // L1 retained entry (its own freshness by the engine)
            sleep(150); // L1 logical TTL (50 ms) expired; breaker CLOSED

            String served = cache.getOrCompute("k", key -> {
                loads.incrementAndGet();
                return "v1";
            });
            assertEquals("v0", served,
                    "the stale SWR frame is served stale (SWR semantics), not warmed as fresh");
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (loads.get() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, loads.get(), "the retained-hit path triggered the SWR revalidation");
        } finally {
            revalidation.shutdownNow();
        }
    }

    /**
     * Full-outage residual: an L1-only write during the outage is local

    /**
     * The non-singleflight getOrCompute variant takes the same stale path
     * (and the marker-stale branch in lookup).
     */
    @Test
    void staleServingWithoutSingleflightAndWithMarkers() {
        CacheSettings allowMarkers = new CacheSettings(10_000, L1_TTL, null,
                Duration.ofHours(1), 0.0, NullPolicy.allow(Duration.ofMillis(150)),
                InvalidationMode.INVALIDATE, 64 * 1024,
                Duration.ZERO, false, Duration.ofSeconds(1), WINDOW);
        Rig rig = rig(allowMarkers);
        DefaultTierCache<String, String> plain = new DefaultTierCache<>("c2",
                new CountingLocalCache<>(), new CircuitBreakerRemoteCache<>(rig.l2Delegate(), rig.breaker()),
                allowMarkers, false, null, null, null, null, rig.breaker(), rig.metrics());
        plain.put("k", "v1");
        rig.cache().putNull("m");
        sleep(250);
        rig.breaker().onFailure();

        assertEquals("v1", plain.getOrCompute("k", throwingLoader()),
                "the non-singleflight path serves stale the same way");
        LookupResult<String> marker = rig.cache().lookup("m");
        assertTrue(marker instanceof LookupResult.CachedNull<String>,
                "lookup serves the stale null-marker without a loader");
        assertNull(rig.cache().get("m"),
                "plain get also serves the stale null-marker without a loader");
    }

    /**
     * Full-outage residual: an L1-only write during the outage is local
     * truth until its TTL; afterwards the stale L2 copy re-warms L1 (no
     * journal row exists to replay) — the documented layered bound.
     */
    @Test
    void fullOutageWritesAreNotHealedByReplay() {
        Rig rig = rig(settings(L1_TTL, null, WINDOW));
        rig.cache().put("k", "old");
        rig.breaker().onFailure(); // outage starts

        rig.cache().put("k", "local-only"); // L1-only write during the outage
        assertEquals("local-only", rig.cache().get("k"),
                "during the outage the local write is local truth");

        // Recovery: half-open probe succeeds and closes the breaker.
        sleep(250);
        assertTrue(rig.breaker().tryAcquire());
        rig.breaker().onSuccess();

        sleep(250); // L1 logical TTL of the local write expires
        assertEquals("old", rig.cache().getOrCompute("k", throwingLoader()),
                "the stale L2 copy re-warms L1 — no replay heals the outage write");
    }
}
