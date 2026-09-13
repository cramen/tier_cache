package io.tiercache.tck;

import io.tiercache.InvalidationMode;

import io.lettuce.core.RedisClient;
import io.tiercache.CacheSettings;
import io.tiercache.NullPolicy;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.redis.LettuceRemoteCache;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Per-cache L2 key-space isolation: two named caches over one real server
 * must never collide in L2. Regression coverage for the live-cluster defect
 * where a value written to one cache was served to another cache under the
 * same key (single shared L2 namespace).
 *
 * <p>Each instance wires {@code remoteCacheFactory(name -> ...)} so every
 * named cache gets its own {@link LettuceRemoteCache} (its own key prefix)
 * over a shared client. Cross-instance reads are done from a second instance
 * with a cold L1, so every read reflects L2 truth.
 */
abstract class AbstractPerCacheIsolationTest {

    static final String CACHE_A = "alpha";
    static final String CACHE_B = "beta";
    static final String KEY = "shared-key";

    abstract DockerImageName image();

    /**
     * One cache instance: own factory, own L1s, per-name L2 instances over
     * one shared client.
     */
    static final class Instance implements AutoCloseable {
        private final List<LettuceRemoteCache<String, String>> l2s = new CopyOnWriteArrayList<>();
        private final RedisClient client;
        private final TierCacheFactory factory;

        Instance(String uri) {
            this.client = RedisClient.create(uri);
            this.factory = TierCacheFactory.builder()
                    .defaults(new CacheSettings(10_000, Duration.ofMinutes(5), null,
                            Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                    .remoteCacheFactory(name -> {
                        LettuceRemoteCache<String, String> l2 = LettuceRemoteCache
                                .<String, String>builder(uri)
                                .client(client)
                                .cacheName(name)
                                .build();
                        l2s.add(l2);
                        return l2;
                    })
                    .build();
        }

        TierCache<String, String> cache(String name) {
            return factory.getCache(name);
        }

        @Override
        public void close() {
            factory.close();
            l2s.forEach(LettuceRemoteCache::close);
            client.shutdown();
        }
    }

    @Test
    void sameKeyDifferentValuesStayIsolated() {
        try (var server = startServer(image());
                Instance one = new Instance(uri(server));
                Instance two = new Instance(uri(server))) {
            one.cache(CACHE_A).put(KEY, "alpha-value");
            one.cache(CACHE_B).put(KEY, "beta-value");

            // Cross-instance cold reads: each cache must serve its own value.
            assertEquals("alpha-value", two.cache(CACHE_A).get(KEY));
            assertEquals("beta-value", two.cache(CACHE_B).get(KEY));
        }
    }

    @Test
    void evictInOneCacheLeavesTheOtherIntact() {
        try (var server = startServer(image());
                Instance one = new Instance(uri(server));
                Instance two = new Instance(uri(server))) {
            one.cache(CACHE_A).put(KEY, "alpha-value");
            one.cache(CACHE_B).put(KEY, "beta-value");

            one.cache(CACHE_A).evict(KEY);

            assertNull(two.cache(CACHE_A).get(KEY));
            assertEquals("beta-value", two.cache(CACHE_B).get(KEY));
        }
    }

    @Test
    void evictAllInOneCacheLeavesTheOtherIntact() {
        try (var server = startServer(image());
                Instance one = new Instance(uri(server));
                Instance two = new Instance(uri(server))) {
            one.cache(CACHE_A).put("k1", "a1");
            one.cache(CACHE_A).put("k2", "a2");
            one.cache(CACHE_B).put("k1", "b1");

            one.cache(CACHE_A).evictAll();

            assertNull(two.cache(CACHE_A).get("k1"));
            assertNull(two.cache(CACHE_A).get("k2"));
            assertEquals("b1", two.cache(CACHE_B).get("k1"));
        }
    }

    @Test
    void readingAnotherCachesKeyMisses() {
        // The original collision scenario: a value written via one cache must
        // NOT be visible through another cache under the same key.
        try (var server = startServer(image());
                Instance one = new Instance(uri(server));
                Instance two = new Instance(uri(server))) {
            one.cache(CACHE_A).put(KEY, "alpha-value");

            assertNull(two.cache(CACHE_B).get(KEY));
        }
    }

    static GenericContainer<?> startServer(DockerImageName image) {
        GenericContainer<?> server = new GenericContainer<>(image).withExposedPorts(6379);
        server.start();
        return server;
    }

    static String uri(GenericContainer<?> server) {
        return "redis://" + server.getHost() + ":" + server.getMappedPort(6379);
    }
}

class RedisPerCacheIsolationTest extends AbstractPerCacheIsolationTest {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("redis:6.2-alpine");
    }
}

class ValkeyPerCacheIsolationTest extends AbstractPerCacheIsolationTest {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("valkey/valkey:8.0-alpine");
    }
}
