package io.tiercache.spring;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.tiercache.CacheConfigurationException;
import io.tiercache.TierCacheFactory;
import io.tiercache.redis.LettuceRemoteCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spec: spring-cache-integration — activation and fail-fast validation.
 */
class TiercacheAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TiercacheAutoConfiguration.class));

    /** Provides an in-memory L2 so no Redis is needed in context tests. */
    @Configuration(proxyBeanMethods = false)
    static class InMemoryL2Config {
        @Bean
        io.tiercache.spi.RemoteCache<Object, Object> testRemoteCache() {
            return new InMemoryRemoteCache<>();
        }
    }

    /**
     * Per-name L2 factory form, mirroring the wiring the auto-configuration
     * builds for the default Lettuce transport (one L2 namespace per cache).
     */
    @Configuration(proxyBeanMethods = false)
    static class PerCacheL2Config {
        @Bean
        TierCacheFactory testTierCacheFactory() {
            return TierCacheFactory.builder()
                    .remoteCacheFactory(InMemoryRemoteCache.perName())
                    .build();
        }
    }

    @Test
    void enabledActivatesCacheManager() {
        runner.withUserConfiguration(InMemoryL2Config.class)
                .withPropertyValues("tiercache.enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(TierCacheManager.class);
                    assertThat(context).hasSingleBean(TierCacheFactory.class);
                    assertThat(context.getBean(CacheManager.class))
                            .isInstanceOf(TierCacheManager.class);
                });
    }

    @Test
    void disabledStaysOutOfTheWay() {
        runner.run(context -> assertThat(context)
                .doesNotHaveBean(TierCacheManager.class)
                .doesNotHaveBean(TierCacheFactory.class));
        runner.withPropertyValues("tiercache.enabled=false")
                .run(context -> assertThat(context)
                        .doesNotHaveBean(TierCacheManager.class));
    }

    @Test
    void missingRedisUriFailsWithActionableError() {
        runner.withPropertyValues("tiercache.enabled=true")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("tiercache.redis-uri");
                });
    }

    @Test
    void invalidTtlOrderingAbortsStartup() {
        runner.withUserConfiguration(InMemoryL2Config.class)
                .withPropertyValues(
                        "tiercache.enabled=true",
                        "tiercache.caches.bad.l1-expire-after-write=2h",
                        "tiercache.caches.bad.l2-ttl=1h")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .rootCause()
                            .isInstanceOf(CacheConfigurationException.class)
                            .hasMessageContaining("bad")
                            .hasMessageContaining("PT2H")
                            .hasMessageContaining("PT1H");
                });
    }

    @Test
    void perCacheOverrideAppliesFromProperties() {
        runner.withUserConfiguration(InMemoryL2Config.class)
                .withPropertyValues(
                        "tiercache.enabled=true",
                        "tiercache.caches.catalog.l2-ttl=30m",
                        "tiercache.caches.catalog.null-policy=allow",
                        "tiercache.caches.catalog.null-marker-ttl=45s")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    TierCacheFactory factory = context.getBean(TierCacheFactory.class);
                    var cache = factory.getCache("catalog");
                    cache.putNull("nk"); // would be a no-op under deny
                    assertThat(cache.lookup("nk"))
                            .isInstanceOf(io.tiercache.LookupResult.CachedNull.class);
                });
    }

    @Test
    void staleServingSettingsApplyFromProperties() {
        runner.withUserConfiguration(InMemoryL2Config.class)
                .withPropertyValues(
                        "tiercache.enabled=true",
                        "tiercache.defaults.stale-ttl=10m",
                        "tiercache.caches.hot.stale-ttl=2m",
                        "tiercache.caches.hot.xfetch-enabled=true",
                        "tiercache.caches.hot.xfetch-beta=500ms",
                        "tiercache.caches.plain.l2-ttl=30m")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    TiercacheProperties properties = context.getBean(TiercacheProperties.class);
                    io.tiercache.CacheSettings base = properties.getDefaults()
                            .toSettings(io.tiercache.CacheSettings.defaults());

                    io.tiercache.CacheSettings hot =
                            properties.getCaches().get("hot").toSettings(base);
                    assertThat(hot.staleTtl()).isEqualTo(java.time.Duration.ofMinutes(2));
                    assertThat(hot.xfetchEnabled()).isTrue();
                    assertThat(hot.xfetchBeta()).isEqualTo(java.time.Duration.ofMillis(500));

                    // No stale-serving overrides: inherits the global default.
                    io.tiercache.CacheSettings plain =
                            properties.getCaches().get("plain").toSettings(base);
                    assertThat(plain.staleTtl()).isEqualTo(java.time.Duration.ofMinutes(10));
                    assertThat(plain.xfetchEnabled()).isFalse();
                    assertThat(plain.xfetchBeta()).isEqualTo(base.xfetchBeta());
                });
    }

    @Test
    void negativeStaleTtlAbortsStartup() {
        runner.withUserConfiguration(InMemoryL2Config.class)
                .withPropertyValues(
                        "tiercache.enabled=true",
                        "tiercache.caches.bad.stale-ttl=-1s")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .rootCause()
                            .isInstanceOf(CacheConfigurationException.class)
                            .hasMessageContaining("bad")
                            .hasMessageContaining("staleTtl");
                });
    }

    /**
     * Per-cache L2 keyspaces: the same key in two caches holds independent
     * values (a value written via one cache is not served to the other from
     * L2), and {@code evictAll} through the Spring Cache SPI clears only its
     * own cache. Regression: a single shared L2 namespace served the other
     * cache's value and wiped both caches on clear.
     */
    @Test
    void perCacheL2IsolatesSameKeyAndEvictAll() {
        runner.withUserConfiguration(PerCacheL2Config.class)
                .withPropertyValues(
                        "tiercache.enabled=true",
                        "tiercache.redis-uri=redis://localhost:6379",
                        "tiercache.invalidation.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    CacheManager manager = context.getBean(CacheManager.class);
                    org.springframework.cache.Cache greetings = manager.getCache("greetings");
                    org.springframework.cache.Cache demo = manager.getCache("demo");

                    demo.put("shared-key", "marker");
                    // L1 miss -> the greetings L2 namespace must not hold demo's value.
                    assertThat(greetings.get("shared-key")).isNull();

                    greetings.put("shared-key", "hello");
                    assertThat(greetings.get("shared-key", String.class)).isEqualTo("hello");
                    assertThat(demo.get("shared-key", String.class)).isEqualTo("marker");

                    greetings.clear();
                    assertThat(greetings.get("shared-key")).isNull();
                    assertThat(demo.get("shared-key", String.class)).isEqualTo("marker");
                });
    }

    /**
     * The default Lettuce wiring builds one L2 per cache name over the
     * shared client: over real Redis, the same key in two caches holds
     * independent values and one cache's {@code evictAll} leaves the other
     * cache's L2 entries intact (keys are namespaced {@code spring:<name>}).
     */
    @Test
    void defaultWiringIsolatesPerCacheL2Namespaces() {
        try (GenericContainer<?> redis = new GenericContainer<>(
                DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            runner.withPropertyValues("tiercache.enabled=true", "tiercache.redis-uri=" + uri)
                    .run(context -> {
                        assertThat(context).hasNotFailed();
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
                            assertThat(conn.sync().keys("spring:demo:*")).isNotEmpty();
                            assertThat(conn.sync().keys("spring:greetings:*")).isEmpty();
                        }
                    });
        }
    }

    /**
     * The shared Redis client must carry the fast timeouts (100 ms connect,
     * 250 ms command) so an L2 outage trips the circuit breaker instead of
     * hanging on Lettuce's 60 s default. Regression: the starter previously
     * created the client with default options, and LettuceRemoteCache skips
     * its own timeout setup for caller-provided clients.
     */
    @Test
    void redisClientCarriesFastTimeouts() {
        TiercacheProperties properties = new TiercacheProperties();
        properties.setRedisUri("redis://localhost:6379");
        RedisClient client = new TiercacheAutoConfiguration().tiercacheRedisClient(properties);
        try {
            assertThat(client.getOptions()).isNotEqualTo(ClientOptions.create());
            assertThat(client.getOptions().getSocketOptions().getConnectTimeout())
                    .isEqualTo(LettuceRemoteCache.DEFAULT_CONNECT_TIMEOUT);
        } finally {
            client.shutdown();
        }
    }
}
