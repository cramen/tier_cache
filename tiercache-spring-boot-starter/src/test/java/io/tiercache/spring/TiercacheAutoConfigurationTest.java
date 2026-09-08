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
}
