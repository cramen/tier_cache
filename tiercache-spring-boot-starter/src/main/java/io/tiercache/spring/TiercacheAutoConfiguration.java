package io.tiercache.spring;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.TierCacheFactory;
import io.tiercache.VersionGenerator;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.redis.JdkCacheSerializer;
import io.tiercache.redis.LettuceLockProvider;
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
 * <p>By default, each named cache gets its own {@link LettuceRemoteCache}
 * built from {@code tiercache.redis-uri} over one shared {@link RedisClient},
 * namespaced as {@code spring:<cache-name>} so the same key in two caches
 * never collides in L2 and {@code evictAll} is scoped per cache. An
 * application may instead provide its own {@link RemoteCache} bean (it takes
 * precedence and is then shared by all caches — see the shared-namespace
 * warning on {@link TierCacheFactory.Builder#remoteCache}; cross-instance
 * invalidation is also skipped unless the app wires it itself). With the
 * default transport, the Pub/Sub invalidation profile is wired automatically
 * ({@code tiercache.invalidation.enabled=false} opts out). Bean destroy
 * methods are inferred.
 *
 * <p><strong>Internal:</strong> not part of the supported public API.
 * Spring Boot loads this class through its auto-configuration mechanism;
 * applications configure it via the {@code tiercache.*} properties
 * ({@link TiercacheProperties}). It may change in any release without
 * notice.
 *
 * @since 0.1.0
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
        // The shared client must carry the same timeouts LettuceRemoteCache
        // applies to self-created clients: they keep L2 outages below business
        // timeouts so the circuit breaker trips fast instead of requests
        // hanging on Lettuce's 60 s default. A caller-provided RemoteCache
        // takes over this responsibility.
        RedisClient client = RedisClient.create(properties.getRedisUri());
        client.setOptions(ClientOptions.builder()
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(LettuceRemoteCache.DEFAULT_CONNECT_TIMEOUT)
                        .build())
                .timeoutOptions(TimeoutOptions.enabled(LettuceRemoteCache.DEFAULT_COMMAND_TIMEOUT))
                .build());
        return client;
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
            RedisClient tiercacheRedisClient, RedisStreamJournal journal,
            TiercacheProperties properties) {
        io.tiercache.spi.InvalidationTransport transport;
        if ("streams".equalsIgnoreCase(properties.getInvalidation().getProfile())) {
            transport = new io.tiercache.redis.LettuceStreamsInvalidationTransport(
                    tiercacheRedisClient, new JdkCacheSerializer<>(), new JdkCacheSerializer<>());
        } else {
            transport = new LettucePubSubInvalidationTransport(tiercacheRedisClient,
                    new JdkCacheSerializer<>());
        }
        io.tiercache.spi.InvalidationTransport selected = transport;
        return versions -> new InvalidationService(selected, journal,
                versions.instanceId(), io.tiercache.spi.InvalidationListener.NOOP);
    }

    @Bean
    @ConditionalOnMissingBean
    TierCacheFactory tierCacheFactory(TiercacheProperties properties,
            ObjectProvider<RemoteCache<Object, Object>> remoteCache,
            ObjectProvider<RedisClient> redisClient,
            ObjectProvider<RedisStreamJournal> journal,
            ObjectProvider<Function<VersionGenerator, InvalidationHandler>> invalidation,
            ObjectProvider<io.tiercache.spi.CacheMetricsListener> metrics) {
        TierCacheFactory.Builder builder = TierCacheFactory.builder()
                .defaults(properties.getDefaults().toSettings(io.tiercache.CacheSettings.defaults()));
        properties.getCaches().forEach((name, props) -> builder.cache(name, props.toOverride()));
        if (properties.getAsyncExecutorThreads() > 0) {
            builder.asyncExecutorThreads(properties.getAsyncExecutorThreads());
        }
        RemoteCache<Object, Object> sharedRemoteCache = remoteCache.getIfAvailable();
        if (sharedRemoteCache != null) {
            // Application-provided L2 takes precedence; it is shared by all
            // caches (shared namespace — see TierCacheFactory.Builder#remoteCache).
            builder.remoteCache(sharedRemoteCache);
        } else {
            RedisClient client = redisClient.getObject();
            RedisStreamJournal sharedJournal = journal.getIfAvailable();
            builder.remoteCacheFactory(name -> perCacheRemoteCache(properties, client, sharedJournal, name))
                    // LockProviderSource auto-derivation does not apply to the
                    // factory form: the rebuild-lock provider must be explicit
                    // or coordination silently degrades to per-instance.
                    .lockProvider(new LettuceLockProvider(client));
        }
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

    /**
     * One L2 per cache name over the shared client, data keys namespaced
     * {@code spring:<name>} so equal keys in different caches never collide.
     * The invalidation journal stays under the logical name ({@code users},
     * not {@code spring:users}) so the write and replay paths address the
     * same stream. Per-cache invalidation settings resolve the same way the
     * factory resolves them: the named override against the global defaults
     * (a cache without configured overrides uses the defaults).
     */
    private static LettuceRemoteCache<Object, Object> perCacheRemoteCache(
            TiercacheProperties properties, RedisClient client, RedisStreamJournal journal, String name) {
        io.tiercache.CacheSettings base = properties.getDefaults()
                .toSettings(io.tiercache.CacheSettings.defaults());
        TiercacheProperties.CacheProps override = properties.getCaches().get(name);
        io.tiercache.CacheSettings settings = override != null ? override.toSettings(base) : base;
        return LettuceRemoteCache.builder(properties.getRedisUri())
                .client(client)
                .cacheName("spring:" + name)
                .journalName(name)
                .journal(journal)
                .invalidationMode(settings.invalidationMode(), settings.payloadCapBytes())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(CacheManager.class)
    TierCacheManager tierCacheManager(TierCacheFactory factory) {
        return new TierCacheManager(factory);
    }
}
