package io.tiercache.spring;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/** Spec: observability — starter metrics binding. */
class TiercacheMetricsAutoConfigurationTest {

    @Configuration(proxyBeanMethods = false)
    static class RegistryConfig {
        @Bean
        SimpleMeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        io.tiercache.spi.RemoteCache<Object, Object> testRemoteCache() {
            return new InMemoryRemoteCache<>();
        }
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    TiercacheAutoConfiguration.class, TiercacheMetricsAutoConfiguration.class));

    @Test
    void metricsBoundWhenRegistryPresent() {
        runner.withUserConfiguration(RegistryConfig.class)
                .withPropertyValues("tiercache.enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    SimpleMeterRegistry registry = context.getBean(SimpleMeterRegistry.class);
                    context.getBean(TierCacheFactory.class).getCache("c").getOrCompute("k", k -> "v");
                    assertThat(registry.get("tiercache.requests")
                            .tags("cache", "c", "result", "load").counter().count()).isEqualTo(1.0);
                });
    }

    @Test
    void metricsSkippedWithoutRegistry() {
        runner.withUserConfiguration(TiercacheAutoConfigurationTest.InMemoryL2Config.class)
                .withPropertyValues("tiercache.enabled=true")
                .run(context -> {
                    // No MeterRegistry -> no metrics beans, factory still works.
                    assertThat(context).hasNotFailed();
                    context.getBean(TierCacheFactory.class).getCache("c").put("k", "v");
                });
    }

    @Test
    void metricsOptOut() {
        runner.withUserConfiguration(RegistryConfig.class)
                .withPropertyValues("tiercache.enabled=true", "tiercache.metrics.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBeanNamesForType(
                            io.tiercache.spi.CacheMetricsListener.class)).isEmpty();
                });
    }
}
