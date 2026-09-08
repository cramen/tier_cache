package io.tiercache.spring;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.TierCacheFactory;
import io.tiercache.VersionGenerator;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.redis.JdkCacheSerializer;
import io.tiercache.redis.LettucePubSubInvalidationTransport;
import io.tiercache.redis.LettuceRemoteCache;
import io.tiercache.redis.RedisStreamJournal;
import io.tiercache.spi.InvalidationHandler;
import io.tiercache.spi.RemoteCache;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

/**
 * Auto-configuration for Tiercache. Active with the starter on the
 * classpath and {@code tiercache.enabled=true}; contributes a
 * {@link TierCacheManager} backed by the two-level cache.
 *
 * <p>The L2 bean is a plain {@link RemoteCache}: by default a
 * {@link LettuceRemoteCache} is created from {@code tiercache.redis-uri},
 * but an application may provide its own implementation (it takes
 * precedence; cross-instance invalidation is then skipped unless the app
 * wires it itself). With the default transport, the Pub/Sub invalidation
 * profile is wired automatically ({@code tiercache.invalidation.enabled=false}
 * opts out). Bean destroy methods are inferred.
 */
@AutoConfiguration
@ConditionalOnClass(TierCacheFactory.class)
@ConditionalOnProperty(name = "tiercache.enabled", havingValue = "true")
@EnableConfigurationProperties(TiercacheProperties.class)
@EnableCaching
public class TiercacheAutoConfiguration {

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(RemoteCache.class)
    RedisClient tiercacheRedisClient(TiercacheProperties properties) {
        if (properties.getRedisUri() == null || properties.getRedisUri().isBlank()) {
            throw new IllegalStateException(
                    "tiercache.redis-uri is required when tiercache.enabled=true "
                            + "(or provide your own RemoteCache bean)");
        }
        return RedisClient.create(properties.getRedisUri());
    }

    @Bean
    @ConditionalOnMissingBean(RemoteCache.class)
    @SuppressWarnings("unchecked")
    RemoteCache<Object, Object> tiercacheRemoteCache(TiercacheProperties properties,
            RedisClient tiercacheRedisClient, ObjectProvider<RedisStreamJournal> journal) {
        return (RemoteCache<Object, Object>) LettuceRemoteCache.builder(properties.getRedisUri())
                .client(tiercacheRedisClient)
                .cacheName("spring")
                .journal(journal.getIfAvailable())
                .build();
    }

    @Bean
    @ConditionalOnBean(RedisClient.class)
    @ConditionalOnProperty(name = "tiercache.invalidation.enabled",
            havingValue = "true", matchIfMissing = true)
    RedisStreamJournal tiercacheInvalidationJournal(RedisClient tiercacheRedisClient,
            TiercacheProperties properties) {
        return new RedisStreamJournal(tiercacheRedisClient.connect(ByteArrayCodec.INSTANCE),
                properties.getInvalidation().getJournalCapacity(), new JdkCacheSerializer<>());
    }

    @Bean
    @ConditionalOnBean(RedisClient.class)
    @ConditionalOnProperty(name = "tiercache.invalidation.enabled",
            havingValue = "true", matchIfMissing = true)
    Function<VersionGenerator, InvalidationHandler> tiercacheInvalidationHandlerFactory(
            RedisClient tiercacheRedisClient, RedisStreamJournal journal) {
        LettucePubSubInvalidationTransport transport =
                new LettucePubSubInvalidationTransport(tiercacheRedisClient, new JdkCacheSerializer<>());
        return versions -> new InvalidationService(transport, journal,
                versions.instanceId(), io.tiercache.spi.InvalidationListener.NOOP);
    }

    @Bean
    @ConditionalOnMissingBean
    TierCacheFactory tierCacheFactory(TiercacheProperties properties,
            RemoteCache<Object, Object> remoteCache,
            ObjectProvider<Function<VersionGenerator, InvalidationHandler>> invalidation,
            ObjectProvider<io.tiercache.spi.CacheMetricsListener> metrics) {
        TierCacheFactory.Builder builder = TierCacheFactory.builder()
                .defaults(properties.getDefaults().toSettings(io.tiercache.CacheSettings.defaults()))
                .remoteCache(remoteCache);
        properties.getCaches().forEach((name, props) -> builder.cache(name, props.toOverride()));
        Function<VersionGenerator, InvalidationHandler> handlerFactory = invalidation.getIfAvailable();
        if (handlerFactory != null) {
            builder.invalidation(handlerFactory);
        }
        io.tiercache.spi.CacheMetricsListener metricsListener = metrics.getIfAvailable();
        if (metricsListener != null) {
            builder.metricsListener(metricsListener);
        }
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
