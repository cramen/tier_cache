package io.tiercache.tck;

import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.tiercache.micrometer.MicrometerCacheMetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pub/Sub loss: a disconnected receiver misses invalidations. Within the
 * journal window, replay on reconnect heals L1; beyond the window, the
 * receiver flushes L1 entirely.
 */
abstract class AbstractPubSubLossTest extends AbstractInvalidationChaosTest {

    abstract DockerImageName image();

    @Test
    void shortDisconnectHealsByReplay() throws Exception {
        try (var server = startServer(image())) {
            String uri = uri(server);
            Side a = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000);
            Side b = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000);
            try {
                a.cache.put("k", "old");
                assertEquals("old", b.cache.get("k")); // warm B's L1

                b.transport.disconnect();
                a.cache.put("k", "new");
                Thread.sleep(300); // the event is lost for B
                assertEquals("old", b.cache.get("k"), "sanity: B is stale while disconnected");

                b.transport.reconnect();
                waitFor(() -> "new".equals(b.cache.get("k")));
            } finally {
                a.close();
                b.close();
            }
        }
    }

    @Test
    void windowOverflowFlushesL1() throws Exception {
        try (var server = startServer(image())) {
            String uri = uri(server);
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            MicrometerCacheMetrics metricsB = new MicrometerCacheMetrics(registry);
            // Tiny journal window: 2 entries.
            Side a = new Side(io.lettuce.core.RedisClient.create(uri), uri, 2);
            Side b = new Side(io.lettuce.core.RedisClient.create(uri), uri, 2, metricsB, metricsB);
            try {
                a.cache.put("keep", "v");
                assertEquals("v", b.cache.get("keep")); // warm B's L1

                b.transport.disconnect();
                for (int i = 0; i < 5; i++) { // overflow the window
                    a.cache.put("flood-" + i, "v" + i);
                }
                Thread.sleep(300);
                assertEquals("v", b.cache.get("keep"), "sanity: stale while disconnected");

                // Overflow -> synchronous full L1 flush on reconnect.
                b.transport.reconnect();
                // After the flush, reads re-resolve from L2: B agrees with L2.
                assertEquals(l2Truth(b, "keep"), b.cache.get("keep"));
                assertEquals("v0", b.cache.get("flood-0"));
            } finally {
                a.close();
                b.close();
            }
        }
    }
}

class RedisPubSubLossTest extends AbstractPubSubLossTest {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("redis:6.2-alpine");
    }
}

class ValkeyPubSubLossTest extends AbstractPubSubLossTest {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("valkey/valkey:8.0-alpine");
    }
}
