package io.tiercache.tck;

import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tag and batch invalidation across instances on a real server; UPDATE-mode
 * cross-instance warm-up; oversized-payload fallback.
 */
abstract class AbstractTagAndUpdateTest extends AbstractInvalidationChaosTest {

    abstract DockerImageName image();

    @Test
    void evictByTagAcrossInstances() throws Exception {
        try (var server = startServer(image())) {
            String uri = uri(server);
            Side a = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000);
            Side b = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000);
            try {
                a.cache.put("a1", "v1", "group");
                a.cache.put("a2", "v2", "group");
                a.cache.put("other", "v3", "other");
                assertEquals("v1", b.cache.get("a1")); // warm B's L1
                assertEquals("v3", b.cache.get("other"));

                a.cache.evictByTag("group");

                waitFor(() -> b.cache.get("a1") == null && b.cache.get("a2") == null);
                assertEquals("v3", b.cache.get("other"), "untagged entry survives");
                assertNull(l2Truth(a, "a1"), "evicted from L2");
            } finally {
                a.close();
                b.close();
            }
        }
    }

    @Test
    void batchEvictAcrossInstances() throws Exception {
        try (var server = startServer(image())) {
            String uri = uri(server);
            Side a = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000);
            Side b = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000);
            try {
                a.cache.put("x", "1");
                a.cache.put("y", "2");
                assertEquals("1", b.cache.get("x"));
                assertEquals("2", b.cache.get("y"));

                a.cache.evictAll(List.of("x", "y"));
                waitFor(() -> b.cache.get("x") == null && b.cache.get("y") == null);
            } finally {
                a.close();
                b.close();
            }
        }
    }

    @Test
    void updateModeCarriesPayloadAcrossInstances() throws Exception {
        try (var server = startServer(image())) {
            String uri = uri(server);
            var update = new io.tiercache.CacheOverride()
                    .invalidationMode(io.tiercache.InvalidationMode.UPDATE);
            Side a = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000,
                    io.tiercache.spi.CacheMetricsListener.NOOP,
                    io.tiercache.spi.CacheMetricsListener.NOOP, update);
            Side b = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000,
                    io.tiercache.spi.CacheMetricsListener.NOOP,
                    io.tiercache.spi.CacheMetricsListener.NOOP, update);
            try {
                a.cache.put("k", "old");
                assertEquals("old", b.cache.get("k"));

                a.cache.put("k", "new");
                waitFor(() -> "new".equals(b.cache.get("k")));

                // The journal row carried the payload (UPDATE semantics).
                var rows = a.journal.readRange(CACHE, "0-0");
                assertTrue(rows.stream().anyMatch(m -> m.payload() != null),
                        "UPDATE events must carry the payload in the journal");
            } finally {
                a.close();
                b.close();
            }
        }
    }

    @Test
    void oversizedPayloadFallsBackToInvalidate() throws Exception {
        try (var server = startServer(image())) {
            String uri = uri(server);
            var update = new io.tiercache.CacheOverride()
                    .invalidationMode(io.tiercache.InvalidationMode.UPDATE)
                    .payloadCapBytes(1024);
            Side a = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000,
                    io.tiercache.spi.CacheMetricsListener.NOOP,
                    io.tiercache.spi.CacheMetricsListener.NOOP, update);
            Side b = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000,
                    io.tiercache.spi.CacheMetricsListener.NOOP,
                    io.tiercache.spi.CacheMetricsListener.NOOP, update);
            try {
                String big = "x".repeat(64 * 1024); // over the 1KB cap
                a.cache.put("k", big);
                waitFor(() -> big.equals(b.cache.get("k")));
                var rows = a.journal.readRange(CACHE, "0-0");
                assertTrue(rows.stream().allMatch(m -> m.payload() == null),
                        "oversized payloads fall back to INVALIDATE (no payload)");
            } finally {
                a.close();
                b.close();
            }
        }
    }

}

class RedisTagAndUpdateTest extends AbstractTagAndUpdateTest {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("redis:6.2-alpine");
    }
}

class ValkeyTagAndUpdateTest extends AbstractTagAndUpdateTest {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("valkey/valkey:8.0-alpine");
    }
}
