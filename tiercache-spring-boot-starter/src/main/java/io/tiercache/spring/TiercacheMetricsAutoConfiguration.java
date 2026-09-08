package io.tiercache.spring;

import io.micrometer.core.instrument.MeterRegistry;
import io.tiercache.TierCacheFactory;
import io.tiercache.micrometer.MicrometerCacheMetrics;
import io.tiercache.redis.RedisStreamJournal;
import io.tiercache.spi.CacheMetricsListener;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.util.function.Consumer;

/**
 * Metrics auto-binding: when a {@link MeterRegistry} exists and the metrics
 * module is on the classpath, the factory's metrics listener is wired.
 * {@code tiercache.metrics.enabled=false} opts out.
 */
@AutoConfiguration(after = TiercacheAutoConfiguration.class)
@ConditionalOnClass(MeterRegistry.class)
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
     * Applies the listener to the factory and registers gauges + JMX.
     */
    @Bean
    Consumer<TierCacheFactory> tiercacheMetricsWiring(MicrometerCacheMetrics metrics,
            ObjectProvider<RedisStreamJournal> journal,
            TiercacheProperties properties) {
        return factory -> {
            metrics.registerGauges(factory, journal.getIfAvailable(),
                    properties.getCaches().keySet().stream().toList());
        };
    }
}
