package io.tiercache.tck;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.tiercache.CacheSettings;
import io.tiercache.NullPolicy;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.micrometer.MicrometerCacheMetrics;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Diagnosability guarantee: every chaos scenario is visible in metrics.
 * (The stampede/degradation/loss mechanics are covered by their dedicated
 * tests; here we assert the metrics see them.)
 */
class MetricsDiagnosabilityTest {

    @Test
    void stampedeIsVisibleInMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerCacheMetrics metrics = new MicrometerCacheMetrics(registry);
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .metricsListener(metrics)
                .build();
        TierCache<String, String> cache = factory.getCache("stampede-metrics");

        int threads = 32;
        var leaderStarted = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
        var leader = pool.submit(() -> cache.getOrCompute("hot", key -> {
            // Inside the loader the in-flight entry already exists, so any
            // follower arriving from now on must coalesce onto this load.
            leaderStarted.countDown();
            try {
                release.await(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "v";
        }));
        try {
            assertTrue(leaderStarted.await(5, java.util.concurrent.TimeUnit.SECONDS),
                    "leader must be inside the loader before followers arrive");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (int i = 0; i < threads - 1; i++) {
            futures.add(pool.submit(() -> cache.getOrCompute("hot", key -> "x")));
        }
        // The join records the coalesced metric synchronously; poll for it
        // instead of sleeping a fixed interval before releasing the loader.
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (registry.get("tiercache.requests")
                        .tags("cache", "stampede-metrics", "result", "coalesced").counter().count() < 1
                && System.nanoTime() < deadline) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        release.countDown();
        try {
            leader.get(5, java.util.concurrent.TimeUnit.SECONDS);
            for (var f : futures) {
                f.get(5, java.util.concurrent.TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        pool.shutdown();

        assertEquals(1.0, registry.get("tiercache.requests")
                .tags("cache", "stampede-metrics", "result", "load").counter().count());
        assertTrue(registry.get("tiercache.requests")
                        .tags("cache", "stampede-metrics", "result", "coalesced").counter().count() >= 1,
                "coalesced joins must be visible");
        factory.close();
    }

    @Test
    void degradationIsVisibleInMetrics() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerCacheMetrics metrics = new MicrometerCacheMetrics(registry);
        io.tiercache.testkit.FailingRemoteCache<String, String> l2 =
                new io.tiercache.testkit.FailingRemoteCache<>();
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(l2)
                .circuitBreakerConfig(new io.tiercache.internal.CircuitBreaker.Config(
                        10, 0.5, 2, Duration.ofMillis(50), 1))
                .metricsListener(metrics)
                .degradationListener(metrics)
                .build();
        metrics.registerGauges(factory, null, java.util.List.of("c"));
        TierCache<String, String> cache = factory.getCache("c");

        l2.fail();
        cache.get("a");
        cache.get("b");
        assertTrue(factory.isDegraded());
        assertEquals(1.0, registry.get("tiercache.degraded").gauge().value());

        l2.heal();
        // The half-open delay must elapse before a probe is admitted; probe
        // with cold keys (each reaches L2 and counts as a probe) until the
        // breaker closes, instead of sleeping a fixed interval.
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        int probe = 0;
        while (factory.isDegraded() && System.nanoTime() < deadline) {
            cache.get("probe-" + probe++);
            Thread.sleep(20);
        }
        assertFalse(factory.isDegraded(), "breaker must close once L2 is healthy");
        assertEquals(0.0, registry.get("tiercache.degraded").gauge().value());
        factory.close();
    }
}
