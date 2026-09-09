package io.tiercache;

import io.tiercache.internal.CircuitBreaker;
import io.tiercache.spi.DegradationListener;
import io.tiercache.spi.InvalidationHandler;
import io.tiercache.spi.InvalidationTarget;
import io.tiercache.testkit.FailingRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: l2-degradation — L1-only mode, honest signaling, recovery.
 */
class DegradationTest {

    /** Breaker that opens after 2 failures and probes after 50 ms. */
    private static final CircuitBreaker.Config FAST =
            new CircuitBreaker.Config(10, 0.5, 2, Duration.ofMillis(50), 1);

    private record Harness(TierCacheFactory factory, TierCache<String, String> cache,
            FailingRemoteCache<String, String> l2, List<String> events) {
    }

    private Harness harness() {
        FailingRemoteCache<String, String> l2 = new FailingRemoteCache<>();
        List<String> events = new ArrayList<>();
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(l2)
                .circuitBreakerConfig(FAST)
                .degradationListener(new DegradationListener() {
                    @Override
                    public void onDegraded() {
                        events.add("degraded");
                    }

                    @Override
                    public void onRecovered() {
                        events.add("recovered");
                    }
                })
                .build();
        return new Harness(factory, factory.getCache("c"), l2, events);
    }

    private static void degrade(Harness h) {
        h.l2().fail();
        h.cache().get("probe-1"); // failure 1
        h.cache().get("probe-2"); // failure 2 -> open
        assertTrue(h.factory().isDegraded(), "breaker must be open");
    }

    @Test
    void degradedReadsResolveWithoutExceptions() {
        Harness h = harness();
        degrade(h);
        AtomicInteger loaderCalls = new AtomicInteger();
        String value = h.cache().getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            return "loaded";
        });
        assertEquals("loaded", value);
        assertEquals(1, loaderCalls.get());
        assertEquals("loaded", h.cache().get("k"), "served from L1");
        assertNull(h.cache().get("never-existed"));
        assertEquals(List.of("degraded"), h.events());
    }

    @Test
    void degradedWritesApplyToL1Only() {
        Harness h = harness();
        degrade(h);
        h.cache().put("k", "v");
        assertEquals("v", h.cache().get("k"));
        h.cache().evict("k");
        assertNull(h.cache().get("k"));
        h.cache().evictAll();
        assertNull(h.cache().get("k"));
    }

    @Test
    void degradedPutIfAbsentIsPerInstance() {
        Harness a = harness();
        Harness b = harness();
        degrade(a);
        degrade(b);
        assertTrue(a.cache().putIfAbsent("shared", "a-wins"));
        assertTrue(b.cache().putIfAbsent("shared", "b-wins"),
                "degraded: both instances may win (per-instance atomicity)");
        assertTrue(a.factory().isDegraded());
        assertTrue(b.factory().isDegraded());
    }

    @Test
    void degradedCoordinationFallsBackToSingleflight() throws Exception {
        Harness h = harness();
        degrade(h);
        AtomicInteger loaderCalls = new AtomicInteger();
        int threads = 8;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        var futures = new java.util.ArrayList<java.util.concurrent.Future<String>>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> h.cache().getOrCompute("hot", key -> {
                loaderCalls.incrementAndGet();
                return "v";
            })));
        }
        // Await every caller before asserting on the loader count.
        for (var f : futures) {
            assertEquals("v", f.get(), "every caller gets the coalesced value");
        }
        pool.shutdown();
        assertEquals(1, loaderCalls.get(), "singleflight still coalesces per instance");
    }

    @Test
    void recoveryClosesBreakerAndSignals() throws Exception {
        Harness h = harness();
        degrade(h);
        h.l2().heal();
        Thread.sleep(100); // past halfOpenAfter
        String probe = h.cache().getOrCompute("k", key -> "v");
        assertEquals("v", probe); // probe succeeds -> close
        assertFalse(h.factory().isDegraded());
        assertEquals(List.of("degraded", "recovered"), h.events());
    }

    @Test
    void recoveryKeepsL1() {
        Harness h = harness();
        h.cache().put("warm", "v");
        degrade(h);
        h.l2().heal();
        h.cache().get("warm"); // L1 hit — no L2 access at all
        assertEquals("v", h.cache().get("warm"));
    }

    @Test
    void recoveryTriggersInvalidationReplayHookBeforeRecovered() throws Exception {
        FailingRemoteCache<String, String> l2 = new FailingRemoteCache<>();
        List<String> events = new ArrayList<>();
        InvalidationHandler handler = new InvalidationHandler() {
            @Override
            public void onLocalWrite(String cache, Object key, Version version,
                    InvalidationMessage.Type type) {
            }

            @Override
            public void registerTarget(String cache, InvalidationTarget target) {
            }

            @Override
            public void onL2Recovery() {
                events.add("replay");
            }

            @Override
            public void close() {
            }
        };
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(l2)
                .circuitBreakerConfig(FAST)
                .invalidation(versions -> handler)
                .degradationListener(new DegradationListener() {
                    @Override
                    public void onRecovered() {
                        events.add("recovered");
                    }
                })
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        l2.fail();
        cache.get("p1");
        cache.get("p2");
        assertTrue(factory.isDegraded());
        l2.heal();
        Thread.sleep(100);
        cache.getOrCompute("k", key -> "v"); // probe -> close
        assertFalse(factory.isDegraded());
        assertEquals(List.of("replay", "recovered"), events,
                "journal replay runs before the recovered signal");
        factory.close();
    }

    @Test
    void optOutLetsExceptionsEscape() {
        FailingRemoteCache<String, String> l2 = new FailingRemoteCache<>();
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(l2)
                .disableCircuitBreaker()
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        l2.fail();
        assertThrows(RuntimeException.class, () -> cache.get("k"));
        assertFalse(factory.isDegraded(), "no breaker -> no degraded state");
        factory.close();
    }
}
