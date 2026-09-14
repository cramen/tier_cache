package io.tiercache.spring;

import io.micrometer.core.instrument.MeterRegistry;
import io.tiercache.TierCacheFactory;
import io.tiercache.micrometer.MicrometerCacheMetrics;
import io.tiercache.micrometer.TiercacheInspection;
import io.tiercache.redis.RedisStreamJournal;
import io.tiercache.spi.CacheMetricsListener;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Metrics auto-binding: when a {@link MeterRegistry} exists and the metrics
 * module is on the classpath, the factory's metrics listener is wired and the
 * JMX inspection view is registered. {@code tiercache.metrics.enabled=false}
 * opts out.
 *
 * <p>The metrics module is an optional dependency of the starter, so the
 * class-level guard names it in string form: without
 * {@code io.tiercache.micrometer} this configuration backs off before the
 * JVM ever loads (and fails to link) the bean-method signatures below.
 *
 * <p><strong>Internal:</strong> not part of the supported public API.
 * Spring Boot loads this class through its auto-configuration mechanism;
 * applications control it via {@code tiercache.metrics.enabled}. It may
 * change in any release without notice.
 *
 * @since 0.1.0
 */
@AutoConfiguration(after = TiercacheAutoConfiguration.class)
@ConditionalOnClass(value = MeterRegistry.class,
        name = "io.tiercache.micrometer.MicrometerCacheMetrics")
@ConditionalOnBean(MeterRegistry.class)
@ConditionalOnProperty(name = "tiercache.metrics.enabled", havingValue = "true", matchIfMissing = true)
public class TiercacheMetricsAutoConfiguration {

    /**
     * The listener, exposed as a bean the factory wiring looks up. A custom
     * {@link CacheMetricsListener} bean takes precedence.
     */
    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean(CacheMetricsListener.class)
    MicrometerCacheMetrics tiercacheMetricsListener(MeterRegistry registry) {
        return new MicrometerCacheMetrics(registry);
    }

    /**
     * Eagerly registers the factory-level gauges (degraded, breaker state,
     * journal size, last load age) once all singletons are up. Runs after
     * context refresh so the factory bean exists; covers the configured
     * cache names, matching the inspection view.
     */
    @Bean
    @ConditionalOnBean({TierCacheFactory.class, MicrometerCacheMetrics.class})
    org.springframework.beans.factory.SmartInitializingSingleton tiercacheGaugeRegistration(
            MicrometerCacheMetrics metrics, TierCacheFactory factory,
            ObjectProvider<RedisStreamJournal> journal, TiercacheProperties properties) {
        return () -> metrics.registerGauges(factory, journal.getIfAvailable(),
                properties.getCaches().keySet().stream().toList());
    }

    /**
     * JMX inspection view over the metrics registry, registered with the
     * platform MBean server at startup ({@code io.tiercache:type=Inspection})
     * and unregistered on shutdown.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnBean(TierCacheFactory.class)
    TiercacheInspection tiercacheInspection(MeterRegistry registry, TierCacheFactory factory,
            ObjectProvider<RedisStreamJournal> journal, TiercacheProperties properties) {
        TiercacheInspection inspection = new TiercacheInspection(registry, factory,
                journal.getIfAvailable(), properties.getCaches().keySet().stream().toList());
        inspection.register();
        return inspection;
    }
}
