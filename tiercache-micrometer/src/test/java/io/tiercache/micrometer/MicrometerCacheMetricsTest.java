package io.tiercache.micrometer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.tiercache.CacheSettings;
import io.tiercache.InvalidationMode;
import io.tiercache.NullPolicy;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Binder coverage: every metric name/tag; spans on L2, none on L1. */
class MicrometerCacheMetricsTest {

    @Test
    void requestMetricsAreExported() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerCacheMetrics metrics = new MicrometerCacheMetrics(registry);
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .metricsListener(metrics)
                .build();
        TierCache<String, String> cache = factory.getCache("c");

        cache.put("k", "v");
        cache.get("k");
        factory.getCache("c2").get("k"); // another cache name: L1 miss -> L2 hit
        cache.get("missing");
        cache.getOrCompute("new", key -> "loaded");

        assertEquals(1.0, registry.get("tiercache.requests")
                .tags("cache", "c", "result", "l1_hit").counter().count());
        assertEquals(1.0, registry.get("tiercache.requests")
                .tags("cache", "c2", "result", "l2_hit").counter().count());
        assertEquals(1.0, registry.get("tiercache.requests")
                .tags("cache", "c", "result", "miss").counter().count());
        assertEquals(1.0, registry.get("tiercache.requests")
                .tags("cache", "c", "result", "load").counter().count());
        assertTrue(registry.get("tiercache.latency")
                        .tags("cache", "c2", "level", "l2").timer().count() > 0);
        factory.close();
    }

    @Test
    void gaugesAndFailureModeMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerCacheMetrics metrics = new MicrometerCacheMetrics(registry);
        io.tiercache.testkit.InMemoryJournal journal = new io.tiercache.testkit.InMemoryJournal(100);
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .metricsListener(metrics)
                .build();
        metrics.registerGauges(factory, journal, java.util.List.of("c"));

        assertEquals(0.0, registry.get("tiercache.degraded").gauge().value());
        assertEquals(0.0, registry.get("tiercache.journal.size").tags("cache", "c").gauge().value());
        assertTrue(registry.get("tiercache.breaker.state").gauge().value() == 0.0);
        factory.getCache("c").getOrCompute("k", key -> "v");
        assertTrue(registry.get("tiercache.entry.age.max").tags("cache", "c").gauge().value() >= 0.0);

        metrics.onNullEntry("c");
        assertEquals(1.0, registry.get("tiercache.null.entries").tags("cache", "c").counter().count());
        metrics.onInvalidation("c", CacheMetricsListener.Direction.SENT);
        assertEquals(1.0, registry.get("tiercache.invalidation")
                .tags("cache", "c", "direction", "sent").counter().count());
        factory.close();
    }

    @Test
    void staleServingCountersAreExported() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerCacheMetrics metrics = new MicrometerCacheMetrics(registry);

        metrics.onStaleHit("c");
        metrics.onStaleHit("c");
        metrics.onStaleHit("other");
        metrics.onRevalidationTriggered("c");
        metrics.onRevalidationCompleted("c");
        metrics.onRevalidationFailed("c");

        assertEquals(2.0, registry.get("tiercache.l2.stale.hits").tags("cache", "c").counter().count());
        assertEquals(1.0, registry.get("tiercache.l2.stale.hits").tags("cache", "other").counter().count());
        assertEquals(1.0, registry.get("tiercache.l2.revalidation.triggers")
                .tags("cache", "c").counter().count());
        assertEquals(1.0, registry.get("tiercache.l2.revalidation.completions")
                .tags("cache", "c").counter().count());
        assertEquals(1.0, registry.get("tiercache.l2.revalidation.failures")
                .tags("cache", "c").counter().count());
    }

    @Test
    void staleHitSurfacesAsCounterEndToEnd() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerCacheMetrics metrics = new MicrometerCacheMetrics(registry);
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        Duration l2Ttl = Duration.ofMinutes(10);
        CacheSettings settings = new CacheSettings(10_000, Duration.ofMinutes(5), null, l2Ttl, 0.0,
                NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                Duration.ofMinutes(5), false, Duration.ofSeconds(1));
        TierCacheFactory factory = TierCacheFactory.builder()
                .defaults(settings)
                .remoteCache(l2)
                .metricsListener(metrics)
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        // Written one minute into the stale window: past l2Ttl, still servable.
        long staleTimestamp = System.currentTimeMillis() - l2Ttl.toMillis() - 60_000;
        l2.put("k", StoredEntry.ofValue("stale", null, staleTimestamp), Duration.ofHours(1));

        assertEquals("stale", cache.getOrCompute("k", key -> "fresh"));
        assertEquals(1.0, registry.get("tiercache.l2.stale.hits").tags("cache", "c").counter().count());
        assertEquals(1.0, registry.get("tiercache.l2.revalidation.triggers")
                .tags("cache", "c").counter().count());
        awaitTrue(() -> {
            var completions = registry.find("tiercache.l2.revalidation.completions")
                    .tags("cache", "c").counter();
            return completions != null && completions.count() == 1.0;
        });
        assertNull(registry.find("tiercache.l2.revalidation.failures").tags("cache", "c").counter(),
                "no revalidation failure is recorded");
        assertEquals("fresh", cache.get("k"), "the completed revalidation publishes fresh value");
        factory.close();
    }

    private static void awaitTrue(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("condition not met within 5s");
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void spansOnL2Only() {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        OpenTelemetrySdk otel = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
        TiercacheTracing tracing = new TiercacheTracing(otel);

        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .metricsListener(tracing)
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        cache.put("k", "v");
        cache.get("k");                    // L1 hit: no span
        factory.getCache("c2").get("k");   // L1 miss -> L2 hit: span
        provider.forceFlush();

        assertEquals(1, exporter.getFinishedSpanItems().size());
        var span = exporter.getFinishedSpanItems().get(0);
        assertEquals("tiercache.l2.get", span.getName());
        assertEquals("c2", span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("cache.name")));
        assertEquals("redis", span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("db.system")));
        assertEquals(true, span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.booleanKey("cache.hit")));
        factory.close();
    }
}
