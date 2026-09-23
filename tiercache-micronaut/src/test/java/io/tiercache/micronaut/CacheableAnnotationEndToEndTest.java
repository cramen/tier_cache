package io.tiercache.micronaut;

import io.lettuce.core.RedisClient;
import io.micronaut.cache.CacheManager;
import io.micronaut.cache.annotation.CacheInvalidate;
import io.micronaut.cache.annotation.CachePut;
import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import io.tiercache.LookupResult;
import io.tiercache.TierCache;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spec: micronaut-integration — annotation end-to-end: Micronaut's
 * {@code @Cacheable}/{@code @CachePut}/{@code @CacheInvalidate} served by
 * the two-level cache over a real Redis container.
 */
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CacheableAnnotationEndToEndTest implements TestPropertyProvider {

    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);

    @Override
    public Map<String, String> getProperties() {
        if (!REDIS.isRunning()) {
            REDIS.start();
        }
        return Map.of(
                "tiercache.enabled", "true",
                "tiercache.redis-uri",
                "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
    }

    /** Counting service: the method body records every execution. */
    @Singleton
    static class CounterService {
        final AtomicInteger calls = new AtomicInteger();

        @Cacheable("numbers")
        public String compute(String key) {
            calls.incrementAndGet();
            return "value-" + key;
        }

        @CachePut(cacheNames = "numbers", parameters = "key")
        public String update(String key, String value) {
            calls.incrementAndGet();
            return value;
        }

        @CacheInvalidate("numbers")
        public void evict(String key) {
            calls.incrementAndGet();
        }
    }

    @Inject
    CounterService service;

    @Inject
    CacheManager<TierCache<Object, Object>> cacheManager;

    private String redisUri() {
        return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    @BeforeEach
    void resetCounter() {
        service.calls.set(0);
    }

    @Test
    void cacheableSkipsTheMethodBodyOnSecondCall() {
        String first = service.compute("a");
        String second = service.compute("a");
        assertThat(first).isEqualTo("value-a");
        assertThat(second).isEqualTo("value-a");
        assertThat(service.calls.get()).isEqualTo(1);

        // The entry lives in L2 under the cache's namespace.
        try (RedisClient probe = RedisClient.create(redisUri());
                io.lettuce.core.api.StatefulRedisConnection<String, String> conn = probe.connect()) {
            assertThat(conn.sync().keys(new String(io.tiercache.redis.RedisKeyspace.dataPrefix("micronaut:numbers"), java.nio.charset.StandardCharsets.US_ASCII) + "*")).isNotEmpty();
        }
    }

    @Test
    void cachePutAndCacheInvalidateFlowThroughBothLevels() {
        service.compute("b"); // body runs, entry cached
        assertThat(service.calls.get()).isEqualTo(1);

        // @CachePut replaces the cached value.
        assertThat(service.update("b", "updated")).isEqualTo("updated");
        assertThat(service.compute("b")).isEqualTo("updated");

        // @CacheInvalidate removes the entry from both levels: the next
        // call executes the method body again.
        int callsBeforeInvalidate = service.calls.get();
        service.evict("b");
        assertThat(service.compute("b")).isEqualTo("value-b");
        assertThat(service.calls.get()).isEqualTo(callsBeforeInvalidate + 2);

        // Direct evidence at the engine level: after eviction the tri-state
        // lookup reports a miss in both levels.
        service.evict("b");
        TierCache<Object, Object> nativeCache =
                cacheManager.getCache("numbers").getNativeCache();
        assertThat(nativeCache.lookup("b")).isInstanceOf(LookupResult.Miss.class);
    }
}
