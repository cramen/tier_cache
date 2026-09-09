package io.tiercache.spring;

import io.tiercache.CacheConfigurationException;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
}
