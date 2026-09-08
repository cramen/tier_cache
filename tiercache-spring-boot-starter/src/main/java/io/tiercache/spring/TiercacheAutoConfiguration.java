package io.tiercache.spring;

import io.tiercache.TierCacheFactory;
import io.tiercache.redis.LettuceRemoteCache;
import io.tiercache.spi.RemoteCache;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for Tiercache. Active with the starter on the
 * classpath and {@code tiercache.enabled=true}; contributes a
 * {@link TierCacheManager} backed by the two-level cache.
 *
 * <p>The L2 bean is a plain {@link RemoteCache}: by default a
 * {@link LettuceRemoteCache} is created from {@code tiercache.redis-uri},
 * but an application may provide its own implementation (it takes
 * precedence). Bean destroy methods are inferred (e.g. {@code close()}).
 */
@AutoConfiguration
@ConditionalOnClass(TierCacheFactory.class)
@ConditionalOnProperty(name = "tiercache.enabled", havingValue = "true")
@EnableConfigurationProperties(TiercacheProperties.class)
@EnableCaching
public class TiercacheAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(RemoteCache.class)
    @SuppressWarnings("unchecked")
    RemoteCache<Object, Object> tiercacheRemoteCache(TiercacheProperties properties) {
        if (properties.getRedisUri() == null || properties.getRedisUri().isBlank()) {
            throw new IllegalStateException(
                    "tiercache.redis-uri is required when tiercache.enabled=true "
                            + "(or provide your own RemoteCache bean)");
        }
        return (RemoteCache<Object, Object>) LettuceRemoteCache.builder(properties.getRedisUri())
                .cacheName("spring")
                .build();
    }

    @Bean
    @ConditionalOnMissingBean
    TierCacheFactory tierCacheFactory(TiercacheProperties properties,
            RemoteCache<Object, Object> remoteCache) {
        TierCacheFactory.Builder builder = TierCacheFactory.builder()
                .defaults(properties.getDefaults().toSettings(io.tiercache.CacheSettings.defaults()))
                .remoteCache(remoteCache);
        properties.getCaches().forEach((name, props) -> builder.cache(name, props.toOverride()));
        // build() runs core's fail-fast startup validation: invalid
        // configuration aborts application startup with an actionable error.
        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean(CacheManager.class)
    TierCacheManager tierCacheManager(TierCacheFactory factory) {
        return new TierCacheManager(factory);
    }
}
