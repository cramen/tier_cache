package io.tiercache.micronaut;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.tiercache.TierCacheFactory;
import io.tiercache.micrometer.TiercacheInspection;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.testkit.InMemoryRemoteCache;
import jakarta.inject.Singleton;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Spec: micronaut-integration — conditional metrics wiring. */
class TiercacheMicronautMetricsConfigurationTest {

    /** Registry + in-memory L2, active only under the dedicated env. */
    @Factory
    @Requires(env = "tiercache-metrics-test")
    static class RegistryConfig {
        @Singleton
        SimpleMeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Singleton
        io.tiercache.spi.RemoteCache<Object, Object> testRemoteCache() {
            return new InMemoryRemoteCache<>();
        }
    }

    private Map<String, Object> enabled() {
        Map<String, Object> config = new HashMap<>();
        config.put("tiercache.enabled", "true");
        return config;
    }

    @Test
    void metricsBoundWhenRegistryPresent() {
        try (ApplicationContext context = ApplicationContext.run(
                enabled(), "tiercache-metrics-test")) {
            assertThat(context.containsBean(CacheMetricsListener.class)).isTrue();
            SimpleMeterRegistry registry = context.getBean(SimpleMeterRegistry.class);
            context.getBean(TierCacheFactory.class).getCache("c").getOrCompute("k", k -> "v");
            assertThat(registry.get("tiercache.requests")
                    .tags("cache", "c", "result", "load").counter().count()).isEqualTo(1.0);
        }
    }

    @Test
    void factoryGaugesRegisterEagerly() {
        Map<String, Object> config = enabled();
        config.put("tiercache.caches.demo.l2-ttl", "1h");
        try (ApplicationContext context = ApplicationContext.run(config, "tiercache-metrics-test")) {
            assertThat(context.containsBean(TiercacheInspection.class)).isTrue();
            SimpleMeterRegistry registry = context.getBean(SimpleMeterRegistry.class);
            assertThat(registry.find("tiercache.degraded").gauge()).isNotNull();
            assertThat(registry.find("tiercache.breaker.state").gauge().value()).isEqualTo(0.0);
            assertThat(registry.find("tiercache.journal.size").tags("cache", "demo").gauge())
                    .isNotNull();
            assertThat(registry.find("tiercache.last.load.age").tags("cache", "demo").gauge())
                    .isNotNull();
        }
    }

    @Test
    void metricsSkippedWithoutRegistry() {
        // No MeterRegistry bean -> no metrics beans, factory still works.
        try (ApplicationContext context = ApplicationContext.run(
                enabled(), "tiercache-inmemory-l2")) {
            assertThat(context.containsBean(CacheMetricsListener.class)).isFalse();
            context.getBean(TierCacheFactory.class).getCache("c").put("k", "v");
        }
    }

    @Test
    void metricsDisabledByProperty() {
        Map<String, Object> config = enabled();
        config.put("tiercache.metrics.enabled", "false");
        try (ApplicationContext context = ApplicationContext.run(config, "tiercache-metrics-test")) {
            assertThat(context.containsBean(CacheMetricsListener.class)).isFalse();
            assertThat(context.containsBean(TiercacheInspection.class)).isFalse();
        }
    }
}
