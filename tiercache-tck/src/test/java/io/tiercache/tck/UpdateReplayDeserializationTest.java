package io.tiercache.tck;

import io.lettuce.core.RedisClient;
import io.tiercache.CacheOverride;
import io.tiercache.InvalidationMode;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Regression: a replayed UPDATE from the journal must apply the typed value,
 * not its serialized bytes. Before the fix, the journal's replay path passed
 * the raw payload bytes to L1 while the live Pub/Sub path deserialized them.
 *
 * <p>The journal is pre-populated before the receiver subscribes so its
 * replay cursor starts at a real stream position — otherwise a stale
 * beginning cursor masks this path by flushing L1 (tracked separately).
 */
class UpdateReplayDeserializationTest extends AbstractInvalidationChaosTest {

    @Test
    void replayedUpdateAppliesTypedValue() throws Exception {
        try (var server = startServer(DockerImageName.parse("redis:6.2-alpine"))) {
            String uri = uri(server);
            CacheOverride update = new CacheOverride().invalidationMode(InvalidationMode.UPDATE);
            Side a = new Side(RedisClient.create(uri), uri, 1000,
                    io.tiercache.spi.CacheMetricsListener.NOOP,
                    io.tiercache.spi.CacheMetricsListener.NOOP, update);
            a.cache.put("warmup", "w"); // journal row 1: B will subscribe past it
            Side b = new Side(RedisClient.create(uri), uri, 1000,
                    io.tiercache.spi.CacheMetricsListener.NOOP,
                    io.tiercache.spi.CacheMetricsListener.NOOP, update);
            try {
                b.transport.disconnect();
                a.cache.put("k", "new"); // journaled UPDATE with payload, missed live
                Thread.sleep(300);

                b.transport.reconnect();
                waitFor(() -> "new".equals(b.cache.get("k")));

                Object replayed = b.cache.get("k");
                assertInstanceOf(String.class, replayed,
                        "replayed UPDATE must apply the typed value, not byte[]");
                assertEquals("new", replayed);
            } finally {
                a.close();
                b.close();
            }
        }
    }
}
