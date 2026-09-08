package io.tiercache;

import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.CacheMetricsListener.Outcome;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every request outcome is classified and emitted. */
class MetricsEmissionTest {

    private static final class Recording implements CacheMetricsListener {
        final Map<Outcome, AtomicInteger> outcomes = new ConcurrentHashMap<>();
        final AtomicLong l2LatencyCalls = new AtomicLong();
        final AtomicInteger nullEntries = new AtomicInteger();

        @Override
        public void onRequest(String cache, Outcome outcome) {
            outcomes.computeIfAbsent(outcome, o -> new AtomicInteger()).incrementAndGet();
        }

        @Override
        public void onLatency(String cache, Level level, long nanos) {
            if (level == io.tiercache.spi.CacheMetricsListener.Level.L2) {
                l2LatencyCalls.incrementAndGet();
            }
        }

        @Override
        public void onNullEntry(String cache) {
            nullEntries.incrementAndGet();
        }
    }

    private int count(Recording r, Outcome o) {
        return r.outcomes.getOrDefault(o, new AtomicInteger()).get();
    }

    @Test
    void allOutcomesClassified() {
        Recording recording = new Recording();
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .metricsListener(recording)
                .build();
        TierCache<String, String> cache = factory.getCache("c");

        cache.put("k", "v");
        cache.get("k");          // L1_HIT
        // A different named cache (its own L1, same shared L2 namespace in
        // the in-memory test double) sees the key via L2.
        TierCache<String, String> cold = factory.getCache("c2");
        cold.get("k");           // L2_HIT
        cold.get("k");           // L1_HIT
        cache.get("absent");     // MISS (deny policy)
        assertEquals(2, count(recording, Outcome.L1_HIT));
        assertEquals(1, count(recording, Outcome.L2_HIT));
        assertEquals(1, count(recording, Outcome.MISS));

        cache.getOrCompute("new", key -> "loaded"); // LOAD
        assertEquals(1, count(recording, Outcome.LOAD));
        assertTrue(recording.l2LatencyCalls.get() > 0, "L2 reads are timed");
        factory.close();
    }

    @Test
    void coalescedJoinIsClassified() throws Exception {
        Recording recording = new Recording();
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .metricsListener(recording)
                .build();
        TierCache<String, String> cache = factory.getCache("c");

        var release = new java.util.concurrent.CountDownLatch(1);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(4);
        var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
        var first = pool.submit(() -> cache.getOrCompute("hot", key -> {
            try {
                release.await(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "v";
        }));
        Thread.sleep(100); // let the leader start
        for (int i = 0; i < 3; i++) {
            futures.add(pool.submit(() -> cache.getOrCompute("hot", key -> "x")));
        }
        Thread.sleep(100); // let followers join the in-flight load
        release.countDown();
        first.get(5, java.util.concurrent.TimeUnit.SECONDS);
        for (var f : futures) {
            f.get(5, java.util.concurrent.TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertTrue(count(recording, Outcome.COALESCED) >= 1, "joins recorded as coalesced");
        assertEquals(1, count(recording, Outcome.LOAD));
        factory.close();
    }

    @Test
    void nullMarkerEmitsEvent() {
        Recording recording = new Recording();
        CacheSettings allow = new CacheSettings(10_000, java.time.Duration.ofMinutes(5), null,
                java.time.Duration.ofHours(1), 0.0, NullPolicy.allow(java.time.Duration.ofMinutes(1)), InvalidationMode.INVALIDATE, 64 * 1024);
        TierCacheFactory factory = TierCacheFactory.builder()
                .defaults(allow)
                .remoteCache(new InMemoryRemoteCache<>())
                .metricsListener(recording)
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        cache.getOrCompute("k", key -> null); // stores marker under allow
        assertEquals(1, recording.nullEntries.get());
        assertEquals(1, count(recording, Outcome.MISS), "loader null classifies as miss");
        factory.close();
    }
}
