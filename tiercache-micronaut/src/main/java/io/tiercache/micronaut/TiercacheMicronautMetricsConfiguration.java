package io.tiercache.micronaut;

import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.BeanProvider;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.tiercache.TierCacheFactory;
import io.tiercache.micrometer.MicrometerCacheMetrics;
import io.tiercache.micrometer.TiercacheInspection;
import io.tiercache.redis.RedisStreamJournal;
import io.tiercache.spi.CacheMetricsListener;
import jakarta.inject.Singleton;

import java.util.List;

/**
 * Metrics auto-binding: when a {@link MeterRegistry} exists and the metrics
 * module is on the classpath, the factory's metrics listener is wired and the
 * JMX inspection view is registered. {@code tiercache.metrics.enabled=false}
 * opts out.
 *
 * <p>The metrics module is an optional dependency of this module; the
 * class-level {@code @Requires(classes = ...)} guard backs this factory off
 * without ever loading (and failing to link) the bean-method signatures
 * below when Micrometer or {@code io.tiercache.micrometer} is absent.
 *
 * <p><strong>Internal:</strong> not part of the supported public API.
 * Micronaut loads this factory through compile-time DI; applications control
 * it via {@code tiercache.metrics.enabled}. It may change in any release
 * without notice.
 *
 * @since 1.1.0
 */
@Factory
@Requires(property = "tiercache.enabled", value = "true")
@Requires(classes = {MeterRegistry.class, MicrometerCacheMetrics.class})
@Requires(beans = MeterRegistry.class)
@Requires(property = "tiercache.metrics.enabled", notEquals = "false")
public class TiercacheMicronautMetricsConfiguration {

    /**
     * Creates the metrics wiring factory.
     *
     * @since 1.1.0
     */
    public TiercacheMicronautMetricsConfiguration() {
    }

    // The listener, exposed as a bean the factory wiring looks up. A custom
    // CacheMetricsListener bean takes precedence.
    @Singleton
    @Requires(missingBeans = CacheMetricsListener.class)
    MicrometerCacheMetrics tiercacheMetricsListener(MeterRegistry registry) {
        return new MicrometerCacheMetrics(registry);
    }

    // Eagerly registers the factory-level gauges (degraded, breaker state,
    // journal size, last load age) and the JMX inspection view
    // (io.tiercache:type=Inspection) at startup; covers the configured cache
    // names. The factory bean is created first as a dependency.
    @Context
    @Singleton
    @Bean(preDestroy = "close")
    @Requires(beans = TierCacheFactory.class)
    TiercacheInspection tiercacheInspection(MeterRegistry registry, TierCacheFactory factory,
            BeanProvider<RedisStreamJournal> journal,
            java.util.Collection<TiercacheCacheProperties> caches,
            MicrometerCacheMetrics metrics) {
        List<String> cacheNames = caches.stream().map(TiercacheCacheProperties::getName).toList();
        RedisStreamJournal sharedJournal = journal.isPresent() ? journal.get() : null;
        metrics.registerGauges(factory, sharedJournal, cacheNames);
        TiercacheInspection inspection =
                new TiercacheInspection(registry, factory, sharedJournal, cacheNames);
        inspection.register();
        return inspection;
    }
}
