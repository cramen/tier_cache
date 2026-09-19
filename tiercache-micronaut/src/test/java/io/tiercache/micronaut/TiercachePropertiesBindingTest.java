package io.tiercache.micronaut;

import io.micronaut.context.ApplicationContext;
import io.tiercache.CacheSettings;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spec: micronaut-integration — configuration binding parity with the
 * Spring starter (same keys, same defaults).
 */
class TiercachePropertiesBindingTest {

    @Test
    void bindsNothingByDefault() {
        try (ApplicationContext context = ApplicationContext.run(Map.of())) {
            TiercacheProperties properties = context.getBean(TiercacheProperties.class);
            assertThat(properties.isEnabled()).isFalse();
            assertThat(properties.getRedisUri()).isNull();
            assertThat(context.getBeansOfType(TiercacheCacheProperties.class)).isEmpty();
            assertThat(properties.getInvalidation().isEnabled()).isTrue();
            assertThat(properties.getInvalidation().getProfile()).isEqualTo("pubsub");
            assertThat(properties.getInvalidation().getJournalCapacity()).isEqualTo(10_000);
            assertThat(properties.getAsyncExecutorThreads()).isEqualTo(0);
            // An empty defaults level resolves onto the core defaults.
            CacheSettings resolved = properties.getDefaults()
                    .toSettings(CacheSettings.defaults());
            assertThat(resolved).isEqualTo(CacheSettings.defaults());
        }
    }

    @Test
    void bindsEnabledAndRedisUri() {
        // With an application-provided RemoteCache the eager factory builds
        // without connecting to Redis.
        Map<String, Object> config = Map.of(
                "tiercache.enabled", "true",
                "tiercache.redis-uri", "redis://localhost:6379");
        try (ApplicationContext context = ApplicationContext.run(config, "tiercache-inmemory-l2")) {
            TiercacheProperties properties = context.getBean(TiercacheProperties.class);
            assertThat(properties.isEnabled()).isTrue();
            assertThat(properties.getRedisUri()).isEqualTo("redis://localhost:6379");
        }
    }

    @Test
    void bindsAsyncExecutorThreads() {
        Map<String, Object> config = Map.of(
                "tiercache.enabled", "true",
                "tiercache.async-executor-threads", "7");
        try (ApplicationContext context = ApplicationContext.run(config, "tiercache-inmemory-l2")) {
            assertThat(context.getBean(TiercacheProperties.class).getAsyncExecutorThreads())
                    .isEqualTo(7);
        }
    }

    @Test
    void bindsGlobalDefaultsAndPerCacheOverrides() {
        Map<String, Object> config = new HashMap<>();
        config.put("tiercache.defaults.l1-max-size", "5000");
        config.put("tiercache.defaults.l1-expire-after-write", "5m");
        config.put("tiercache.defaults.stale-ttl", "10m");
        config.put("tiercache.caches.orders.l2-ttl", "30m");
        config.put("tiercache.caches.orders.null-policy", "allow");
        config.put("tiercache.caches.orders.null-marker-ttl", "45s");
        config.put("tiercache.caches.orders.invalidation-mode", "update");
        config.put("tiercache.caches.orders.payload-cap-bytes", "32768");
        config.put("tiercache.caches.orders.xfetch-enabled", "true");
        config.put("tiercache.caches.orders.xfetch-beta", "500ms");
        config.put("tiercache.invalidation.profile", "streams");
        config.put("tiercache.invalidation.journal-capacity", "2000");
        try (ApplicationContext context = ApplicationContext.run(config)) {
            TiercacheProperties properties = context.getBean(TiercacheProperties.class);
            assertThat(properties.getDefaults().getL1MaxSize()).isEqualTo(5_000L);
            assertThat(properties.getDefaults().getL1ExpireAfterWrite())
                    .isEqualTo(Duration.ofMinutes(5));
            assertThat(properties.getDefaults().getStaleTtl()).isEqualTo(Duration.ofMinutes(10));

            List<TiercacheCacheProperties> caches =
                    List.copyOf(context.getBeansOfType(TiercacheCacheProperties.class));
            assertThat(caches).hasSize(1);
            TiercacheCacheProperties orders = caches.get(0);
            assertThat(orders.getName()).isEqualTo("orders");
            assertThat(orders.getL2Ttl()).isEqualTo(Duration.ofMinutes(30));
            assertThat(orders.getNullPolicy())
                    .isEqualTo(TiercacheProperties.CacheProps.Kind.ALLOW);
            assertThat(orders.getNullMarkerTtl()).isEqualTo(Duration.ofSeconds(45));
            assertThat(orders.getInvalidationMode()).isEqualTo(io.tiercache.InvalidationMode.UPDATE);
            assertThat(orders.getPayloadCapBytes()).isEqualTo(32_768L);
            assertThat(orders.getXfetchEnabled()).isTrue();
            assertThat(orders.getXfetchBeta()).isEqualTo(Duration.ofMillis(500));

            assertThat(properties.getInvalidation().getProfile()).isEqualTo("streams");
            assertThat(properties.getInvalidation().getJournalCapacity()).isEqualTo(2_000);

            // Unset fields of a named cache inherit the global defaults.
            CacheSettings base = properties.getDefaults()
                    .toSettings(CacheSettings.defaults());
            CacheSettings resolved = orders.toSettings(base);
            assertThat(resolved.l2Ttl()).isEqualTo(Duration.ofMinutes(30));
            assertThat(resolved.l1MaxSize()).isEqualTo(5_000L);
            assertThat(resolved.l1ExpireAfterWrite()).isEqualTo(Duration.ofMinutes(5));
            assertThat(resolved.staleTtl()).isEqualTo(Duration.ofMinutes(10));
        }
    }

    @Test
    void bindsPerCacheOverrideFromKebabCaseKeys() {
        // kebab-case keys, as in docs/configuration.md examples.
        Map<String, Object> config = Map.of(
                "tiercache.caches.users.l2-ttl", "10m",
                "tiercache.caches.users.jitter-amplitude", "0.1");
        try (ApplicationContext context = ApplicationContext.run(config)) {
            List<TiercacheCacheProperties> caches =
                    List.copyOf(context.getBeansOfType(TiercacheCacheProperties.class));
            assertThat(caches).hasSize(1);
            TiercacheCacheProperties users = caches.get(0);
            assertThat(users.getName()).isEqualTo("users");
            assertThat(users.getL2Ttl()).isEqualTo(Duration.ofMinutes(10));
            assertThat(users.getJitterAmplitude()).isEqualTo(0.1);
        }
    }

    @Test
    void bindsMultiplePerCacheOverrides() {
        Map<String, Object> config = Map.of(
                "tiercache.caches.users.l2-ttl", "10m",
                "tiercache.caches.catalog.l2-ttl", "2h");
        try (ApplicationContext context = ApplicationContext.run(config)) {
            Map<String, Duration> ttls = new HashMap<>();
            context.getBeansOfType(TiercacheCacheProperties.class)
                    .forEach(c -> ttls.put(c.getName(), c.getL2Ttl()));
            assertThat(ttls).containsExactlyInAnyOrderEntriesOf(Map.of(
                    "users", Duration.ofMinutes(10),
                    "catalog", Duration.ofHours(2)));
        }
    }
}
