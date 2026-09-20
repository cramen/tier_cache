package io.tiercache.tck;

import io.lettuce.core.RedisClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.tiercache.micrometer.MicrometerCacheMetrics;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Cursor tracking on reconnect (reviewer-reported): a from-the-beginning
 * cursor must not read as trimmed, and the replay position must track live
 * consumption — otherwise reconnects force full L1 flushes with nothing
 * actually lost. The discriminator is the `dropped` invalidation metric: it
 * increments exactly when the flush fallback fires.
 */
class CursorTrackingTest extends AbstractInvalidationChaosTest {

    private static double dropped(SimpleMeterRegistry registry) {
        var counter = registry.find("tiercache.invalidation").tags("direction", "dropped")
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    @Test
    void beginningCursorIsNotALoss() throws Exception {
        try (var server = startServer(DockerImageName.parse("redis:6.2-alpine"))) {
            String uri = uri(server);
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            MicrometerCacheMetrics metrics = new MicrometerCacheMetrics(registry);
            Side a = new Side(RedisClient.create(uri), uri, 1000);
            Side b = new Side(RedisClient.create(uri), uri, 1000, metrics, metrics);
            try {
                // B registered against an empty journal (cursor 0-0).
                a.cache.put("k", "v");
                assertEquals("v", b.cache.get("k")); // warm B's L1 via live event

                b.transport.disconnect();
                a.cache.put("k2", "v2");
                Thread.sleep(300);

                b.transport.reconnect();
                waitFor(() -> "v2".equals(b.cache.get("k2")));
                assertEquals(0.0, dropped(registry),
                        "a beginning cursor must not trigger the flush fallback");
            } finally {
                a.close();
                b.close();
            }
        }
    }

    @Test
    void liveConsumptionAdvancesReplayPosition() throws Exception {
        try (var server = startServer(DockerImageName.parse("redis:6.2-alpine"))) {
            String uri = uri(server);
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            MicrometerCacheMetrics metrics = new MicrometerCacheMetrics(registry);
            // Tiny journal (4 rows): normal operation trims it constantly.
            Side a = new Side(RedisClient.create(uri), uri, 4);
            Side b = new Side(RedisClient.create(uri), uri, 4, metrics, metrics);
            try {
                // B consumes 70 events live — far past the journal window.
                for (int i = 0; i < 70; i++) {
                    a.cache.put("k" + i, "v" + i);
                }
                waitFor(() -> "v69".equals(b.cache.get("k69")));

                b.transport.disconnect();
                a.cache.put("fresh", "x");
                a.cache.put("fresh2", "y");
                Thread.sleep(300);

                b.transport.reconnect();
                waitFor(() -> "y".equals(b.cache.get("fresh2")));
                assertEquals(0.0, dropped(registry),
                        "live consumption must advance the cursor: no flush after a brief disconnect");
                assertEquals("x", b.cache.get("fresh"), "only the missed rows were replayed");
            } finally {
                a.close();
                b.close();
            }
        }
    }
}
