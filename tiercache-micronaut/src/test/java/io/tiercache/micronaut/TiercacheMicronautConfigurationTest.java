package io.tiercache.micronaut;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.micronaut.cache.CacheManager;
import io.micronaut.cache.DefaultCacheManager;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.tiercache.CacheConfigurationException;
import io.tiercache.TierCacheFactory;
import io.tiercache.redis.LettuceRemoteCache;
import io.tiercache.redis.RedisStreamJournal;
import io.tiercache.testkit.InMemoryRemoteCache;
import jakarta.inject.Singleton;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Spec: micronaut-integration — activation, fail-fast validation, and
 * default wiring.
 */
class TiercacheMicronautConfigurationTest {

    /** Provides an in-memory L2 so no Redis is needed in context tests. */
    @Factory
    @Requires(env = "tiercache-inmemory-l2")
    static class InMemoryL2Config {
        @Singleton
        io.tiercache.spi.RemoteCache<Object, Object> testRemoteCache() {
            return new InMemoryRemoteCache<>();
        }
    }

    private Map<String, Object> enabledWithInMemoryL2() {
        Map<String, Object> config = new HashMap<>();
        config.put("tiercache.enabled", "true");
        return config;
    }

    @Test
    void enabledActivatesCacheManager() {
        try (ApplicationContext context = ApplicationContext.run(
                enabledWithInMemoryL2(), "tiercache-inmemory-l2")) {
            assertThat(context.containsBean(TierCacheMicronautManager.class)).isTrue();
            assertThat(context.containsBean(TierCacheFactory.class)).isTrue();
            assertThat(context.getBean(CacheManager.class))
                    .isInstanceOf(TierCacheMicronautManager.class);
            // The replacement leaves ours as the only CacheManager candidate.
            assertThat(context.getBeanDefinitions(CacheManager.class))
                    .allSatisfy(d -> assertThat(d.getBeanType())
                            .isEqualTo(TierCacheMicronautManager.class));
        }
    }

    @Test
    void disabledStaysOutOfTheWay() {
        try (ApplicationContext context = ApplicationContext.run()) {
            assertThat(context.containsBean(TierCacheMicronautManager.class)).isFalse();
            assertThat(context.containsBean(TierCacheFactory.class)).isFalse();
            // Micronaut's default cache manager remains in effect.
            assertThat(context.getBean(CacheManager.class))
                    .isInstanceOf(DefaultCacheManager.class);
        }
        try (ApplicationContext context = ApplicationContext.run(
                Map.of("tiercache.enabled", "false"))) {
            assertThat(context.containsBean(TierCacheMicronautManager.class)).isFalse();
            assertThat(context.containsBean(TierCacheFactory.class)).isFalse();
        }
    }

    @Test
    void missingRedisUriFailsWithActionableError() {
        assertThatThrownBy(() -> ApplicationContext.run(Map.of("tiercache.enabled", "true")))
                .rootCause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tiercache.redis-uri");
    }

    @Test
    void invalidTtlOrderingAbortsStartup() {
        Map<String, Object> config = enabledWithInMemoryL2();
        config.put("tiercache.caches.bad.l1-expire-after-write", "2h");
        config.put("tiercache.caches.bad.l2-ttl", "1h");
        assertThatThrownBy(() -> ApplicationContext.run(config, "tiercache-inmemory-l2"))
                .rootCause()
                .isInstanceOf(CacheConfigurationException.class)
                .hasMessageContaining("bad")
                .hasMessageContaining("PT2H")
                .hasMessageContaining("PT1H");
    }

    @Test
    void perCacheOverrideAppliesFromProperties() {
        Map<String, Object> config = enabledWithInMemoryL2();
        config.put("tiercache.caches.catalog.l2-ttl", "30m");
        config.put("tiercache.caches.catalog.null-policy", "allow");
        config.put("tiercache.caches.catalog.null-marker-ttl", "45s");
        try (ApplicationContext context = ApplicationContext.run(config, "tiercache-inmemory-l2")) {
            TierCacheFactory factory = context.getBean(TierCacheFactory.class);
            var cache = factory.getCache("catalog");
            cache.putNull("nk"); // would be a no-op under deny
            assertThat(cache.lookup("nk"))
                    .isInstanceOf(io.tiercache.LookupResult.CachedNull.class);
        }
    }

    @Test
    void staleServingSettingsApplyFromProperties() {
        Map<String, Object> config = enabledWithInMemoryL2();
        config.put("tiercache.defaults.stale-ttl", "10m");
        config.put("tiercache.caches.hot.stale-ttl", "2m");
        config.put("tiercache.caches.hot.xfetch-enabled", "true");
        config.put("tiercache.caches.hot.xfetch-beta", "500ms");
        config.put("tiercache.caches.plain.l2-ttl", "30m");
        try (ApplicationContext context = ApplicationContext.run(config, "tiercache-inmemory-l2")) {
            TiercacheProperties properties = context.getBean(TiercacheProperties.class);
            io.tiercache.CacheSettings base = properties.getDefaults()
                    .toSettings(io.tiercache.CacheSettings.defaults());

            Map<String, TiercacheCacheProperties> byName = new HashMap<>();
            context.getBeansOfType(TiercacheCacheProperties.class)
                    .forEach(c -> byName.put(c.getName(), c));

            io.tiercache.CacheSettings hot = byName.get("hot").toSettings(base);
            assertThat(hot.staleTtl()).isEqualTo(java.time.Duration.ofMinutes(2));
            assertThat(hot.xfetchEnabled()).isTrue();
            assertThat(hot.xfetchBeta()).isEqualTo(java.time.Duration.ofMillis(500));

            // No stale-serving overrides: inherits the global default.
            io.tiercache.CacheSettings plain = byName.get("plain").toSettings(base);
            assertThat(plain.staleTtl()).isEqualTo(java.time.Duration.ofMinutes(10));
            assertThat(plain.xfetchEnabled()).isFalse();
            assertThat(plain.xfetchBeta()).isEqualTo(base.xfetchBeta());
        }
    }

    /**
     * The default Lettuce wiring builds one L2 per cache name over the
     * shared client: over real Redis, the same key in two caches holds
     * independent values and one cache's {@code evictAll} leaves the other
     * cache's L2 entries intact (keys are namespaced
     * {@code micronaut:<name>}). The Pub/Sub invalidation profile is wired
     * by default; {@code tiercache.invalidation.enabled=false} opts out.
     */
    @Test
    void defaultWiringOverRealRedis() {
        try (GenericContainer<?> redis = new GenericContainer<>(
                DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);

            Map<String, Object> config = Map.of(
                    "tiercache.enabled", "true",
                    "tiercache.redis-uri", uri);
            try (ApplicationContext context = ApplicationContext.run(config)) {
                // Invalidation wiring is on by default.
                assertThat(context.containsBean(RedisStreamJournal.class)).isTrue();

                TierCacheFactory factory = context.getBean(TierCacheFactory.class);
                io.tiercache.TierCache<Object, Object> greetings = factory.getCache("greetings");
                io.tiercache.TierCache<Object, Object> demo = factory.getCache("demo");

                demo.put("shared-key", "marker");
                // L1 miss -> the greetings L2 namespace must not hold demo's value.
                assertThat(greetings.get("shared-key")).isNull();

                greetings.put("shared-key", "hello");
                greetings.evictAll();
                assertThat(greetings.get("shared-key")).isNull();
                assertThat(demo.get("shared-key")).isEqualTo("marker");

                try (RedisClient probe = RedisClient.create(uri);
                        io.lettuce.core.api.StatefulRedisConnection<String, String> conn =
                                probe.connect()) {
                    assertThat(conn.sync().keys("micronaut:demo:*")).isNotEmpty();
                    assertThat(conn.sync().keys("micronaut:greetings:*")).isEmpty();
                }
            }

            // Opting out of invalidation skips the journal/transport beans.
            Map<String, Object> optOut = Map.of(
                    "tiercache.enabled", "true",
                    "tiercache.redis-uri", uri,
                    "tiercache.invalidation.enabled", "false");
            try (ApplicationContext context = ApplicationContext.run(optOut)) {
                assertThat(context.containsBean(RedisStreamJournal.class)).isFalse();
                assertThat(context.containsBean(TierCacheFactory.class)).isTrue();
            }
        }
    }

    /**
     * The shared Redis client must carry the fast timeouts (100 ms connect,
     * 250 ms command) so an L2 outage trips the circuit breaker instead of
     * hanging on Lettuce's 60 s default.
     */
    @Test
    void redisClientCarriesFastTimeouts() {
        TiercacheProperties properties = new TiercacheProperties(
                true, "redis://localhost:6379", null, null, null);
        RedisClient client =
                new TiercacheMicronautConfiguration().tiercacheRedisClient(properties);
        try {
            assertThat(client.getOptions()).isNotEqualTo(ClientOptions.create());
            assertThat(client.getOptions().getSocketOptions().getConnectTimeout())
                    .isEqualTo(LettuceRemoteCache.DEFAULT_CONNECT_TIMEOUT);
        } finally {
            client.shutdown();
        }
    }
}
