package io.tiercache.micronaut;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.codec.ByteArrayCodec;
import io.micronaut.cache.DefaultCacheManager;
import io.micronaut.context.BeanProvider;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.tiercache.TierCacheFactory;
import io.tiercache.VersionGenerator;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.redis.JdkCacheSerializer;
import io.tiercache.redis.LettuceLockProvider;
import io.tiercache.redis.LettucePubSubInvalidationTransport;
import io.tiercache.redis.LettuceRemoteCache;
import io.tiercache.redis.RedisStreamJournal;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.InvalidationHandler;
import io.tiercache.spi.RemoteCache;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Micronaut wiring for Tiercache. Active with the module on the classpath
 * and {@code tiercache.enabled=true}; contributes a
 * {@link TierCacheMicronautManager} backed by the two-level cache, replacing
 * Micronaut's {@link DefaultCacheManager}.
 *
 * <p>By default, each named cache gets its own {@link LettuceRemoteCache}
 * built from {@code tiercache.redis-uri} over one shared {@link RedisClient},
 * namespaced as {@code micronaut:<cache-name>} so the same key in two caches
 * never collides in L2 and {@code invalidateAll} is scoped per cache. An
 * application may instead provide its own {@link RemoteCache} bean (it takes
 * precedence and is then shared by all caches — see the shared-namespace
 * warning on {@link TierCacheFactory.Builder#remoteCache}; cross-instance
 * invalidation is also skipped unless the app wires it itself). With the
 * default transport, the Pub/Sub invalidation profile is wired automatically
 * ({@code tiercache.invalidation.enabled=false} opts out).
 *
 * <p><strong>Internal:</strong> not part of the supported public API.
 * Micronaut loads this factory through compile-time DI; applications
 * configure it via the {@code tiercache.*} properties
 * ({@link TiercacheProperties}, {@link TiercacheCacheProperties}). It may
 * change in any release without notice.
 *
 * @since 1.1.0
 */
@Factory
@Requires(property = "tiercache.enabled", value = "true")
public class TiercacheMicronautConfiguration {

    /**
     * Creates the wiring factory.
     *
     * @since 1.1.0
     */
    public TiercacheMicronautConfiguration() {
    }

    // The shared client must carry the same timeouts LettuceRemoteCache
    // applies to self-created clients: they keep L2 outages below business
    // timeouts so the circuit breaker trips fast instead of requests
    // hanging on Lettuce's 60 s default. A caller-provided RemoteCache
    // takes over this responsibility.
    @Singleton
    @Bean(preDestroy = "shutdown")
    @Requires(missingBeans = RemoteCache.class)
    RedisClient tiercacheRedisClient(TiercacheProperties properties) {
        if (properties.getRedisUri() == null || properties.getRedisUri().isBlank()) {
            throw new IllegalStateException(
                    "tiercache.redis-uri is required when tiercache.enabled=true "
                            + "(or provide your own RemoteCache bean)");
        }
        RedisClient client = RedisClient.create(properties.getRedisUri());
        client.setOptions(ClientOptions.builder()
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(LettuceRemoteCache.DEFAULT_CONNECT_TIMEOUT)
                        .build())
                .timeoutOptions(TimeoutOptions.enabled(LettuceRemoteCache.DEFAULT_COMMAND_TIMEOUT))
                .build());
        return client;
    }

    @Singleton
    @Requires(beans = RedisClient.class)
    @Requires(property = "tiercache.invalidation.enabled", notEquals = "false")
    RedisStreamJournal tiercacheInvalidationJournal(RedisClient tiercacheRedisClient,
            TiercacheProperties properties) {
        return new RedisStreamJournal(tiercacheRedisClient.connect(ByteArrayCodec.INSTANCE),
                properties.getInvalidation().getJournalCapacity(), new JdkCacheSerializer<>());
    }

    @Singleton
    @Requires(beans = RedisClient.class)
    @Requires(property = "tiercache.invalidation.enabled", notEquals = "false")
    Function<VersionGenerator, InvalidationHandler> tiercacheInvalidationHandlerFactory(
            RedisClient tiercacheRedisClient, RedisStreamJournal journal,
            TiercacheProperties properties, BeanProvider<CacheMetricsListener> metrics) {
        io.tiercache.spi.InvalidationTransport transport;
        if ("streams".equalsIgnoreCase(properties.getInvalidation().getProfile())) {
            transport = new io.tiercache.redis.LettuceStreamsInvalidationTransport(
                    tiercacheRedisClient, new JdkCacheSerializer<>(), new JdkCacheSerializer<>());
        } else {
            transport = new LettucePubSubInvalidationTransport(tiercacheRedisClient,
                    new JdkCacheSerializer<>());
        }
        io.tiercache.spi.InvalidationTransport selected = transport;
        CacheMetricsListener selectedMetrics = metrics.isPresent()
                ? metrics.get() : CacheMetricsListener.NOOP;
        return versions -> new InvalidationService(selected, journal,
                versions.instanceId(), io.tiercache.spi.InvalidationListener.NOOP, selectedMetrics);
    }

    // build() runs core's fail-fast startup validation: invalid
    // configuration aborts application startup with an actionable error.
    // @Context makes the bean eager: Micronaut singletons are lazy by
    // default, and validation must run at startup, not on first cache use.
    @Context
    @Singleton
    @Bean(preDestroy = "close")
    @Requires(missingBeans = TierCacheFactory.class)
    TierCacheFactory tierCacheFactory(TiercacheProperties properties,
            Collection<TiercacheCacheProperties> caches,
            BeanProvider<RemoteCache<Object, Object>> remoteCache,
            BeanProvider<RedisClient> redisClient,
            BeanProvider<RedisStreamJournal> journal,
            BeanProvider<Function<VersionGenerator, InvalidationHandler>> invalidation,
            BeanProvider<CacheMetricsListener> metrics) {
        Map<String, TiercacheCacheProperties> overridesByName = overridesByName(caches);
        TierCacheFactory.Builder builder = TierCacheFactory.builder()
                .defaults(properties.getDefaults().toSettings(io.tiercache.CacheSettings.defaults()));
        overridesByName.forEach((name, props) -> builder.cache(name, props.toOverride()));
        if (properties.getAsyncExecutorThreads() > 0) {
            builder.asyncExecutorThreads(properties.getAsyncExecutorThreads());
        }
        RemoteCache<Object, Object> sharedRemoteCache = remoteCache.isPresent() ? remoteCache.get() : null;
        if (sharedRemoteCache != null) {
            // Application-provided L2 takes precedence; it is shared by all
            // caches (shared namespace — see TierCacheFactory.Builder#remoteCache).
            builder.remoteCache(sharedRemoteCache);
        } else {
            RedisClient client = redisClient.get();
            RedisStreamJournal sharedJournal = journal.isPresent() ? journal.get() : null;
            builder.remoteCacheFactory(name ->
                            perCacheRemoteCache(properties, overridesByName, client, sharedJournal, name))
                    // LockProviderSource auto-derivation does not apply to the
                    // factory form: the rebuild-lock provider must be explicit
                    // or coordination silently degrades to per-instance.
                    .lockProvider(new LettuceLockProvider(client));
        }
        Function<VersionGenerator, InvalidationHandler> handlerFactory =
                invalidation.isPresent() ? invalidation.get() : null;
        if (handlerFactory != null) {
            builder.invalidation(handlerFactory);
        }
        CacheMetricsListener metricsListener = metrics.isPresent() ? metrics.get() : null;
        if (metricsListener != null) {
            builder.metricsListener(metricsListener);
        }
        return builder.build();
    }

    /**
     * One L2 per cache name over the shared client, namespaced
     * {@code micronaut:<name>} so equal keys in different caches never
     * collide. Per-cache invalidation settings resolve the same way the
     * factory resolves them: the named override against the global defaults
     * (a cache without configured overrides uses the defaults).
     */
    private static LettuceRemoteCache<Object, Object> perCacheRemoteCache(
            TiercacheProperties properties, Map<String, TiercacheCacheProperties> overridesByName,
            RedisClient client, RedisStreamJournal journal, String name) {
        io.tiercache.CacheSettings base = properties.getDefaults()
                .toSettings(io.tiercache.CacheSettings.defaults());
        TiercacheProperties.CacheProps override = overridesByName.get(name);
        io.tiercache.CacheSettings settings = override != null ? override.toSettings(base) : base;
        return LettuceRemoteCache.builder(properties.getRedisUri())
                .client(client)
                .cacheName("micronaut:" + name)
                .journalName(name)
                .journal(journal)
                .invalidationMode(settings.invalidationMode(), settings.payloadCapBytes())
                .build();
    }

    private static Map<String, TiercacheCacheProperties> overridesByName(
            Collection<TiercacheCacheProperties> caches) {
        Map<String, TiercacheCacheProperties> byName = new LinkedHashMap<>();
        for (TiercacheCacheProperties cache : caches) {
            byName.put(cache.getName(), cache);
        }
        return byName;
    }

    @Singleton
    @Primary
    @Replaces(DefaultCacheManager.class)
    TierCacheMicronautManager tierCacheManager(TierCacheFactory factory) {
        return new TierCacheMicronautManager(factory);
    }
}
