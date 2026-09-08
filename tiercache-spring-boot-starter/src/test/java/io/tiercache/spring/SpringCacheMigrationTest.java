package io.tiercache.spring;

import io.tiercache.NullPolicy;
import io.tiercache.CacheOverride;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.InMemoryLockProvider;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spring Cache migration gate: the same annotation-driven service behaves
 * identically under the standard {@link ConcurrentMapCacheManager} and
 * under {@link TierCacheManager}. The service class is shared — the swap is
 * configuration-only by construction.
 */
class SpringCacheMigrationTest {

    /** External counter: service state must live in a bean, not in fields
     *  (Spring AOP proxies skip the target's constructor via Objenesis). */
    public static class CallCounter {
        final AtomicInteger computeCalls = new AtomicInteger();
    }

    /**
     * The reference service: unchanged whether standard or Tiercache
     * infrastructure backs it.
     */
    public static class ReferenceService {

        private final CallCounter counter;

        public ReferenceService(CallCounter counter) {
            this.counter = counter;
        }

        int computeCalls() {
            return counter.computeCalls.get();
        }

        @Cacheable("items")
        public String getItem(String id) {
            counter.computeCalls.incrementAndGet();
            return "item-" + id;
        }

        @CachePut(cacheNames = "items", key = "#id")
        public String updateItem(String id, String value) {
            return value;
        }

        @CacheEvict(cacheNames = "items", key = "#id")
        public void deleteItem(String id) {
        }

        @Cacheable("nullable")
        public String getNullable(String id) {
            counter.computeCalls.incrementAndGet();
            return null;
        }
    }

    /** "Before migration": standard Spring cache manager. */
    @Configuration(proxyBeanMethods = false)
    @EnableCaching
    static class StandardConfig {
        @Bean
        CacheManager cacheManager() {
            return new ConcurrentMapCacheManager();
        }

        @Bean
        CallCounter callCounter() {
            return new CallCounter();
        }

        @Bean
        ReferenceService referenceService(CallCounter counter) {
            return new ReferenceService(counter);
        }
    }

    /** "After migration": Tiercache manager (config-only swap). */
    @Configuration(proxyBeanMethods = false)
    @EnableCaching
    static class TiercacheConfig {
        @Bean
        CacheManager cacheManager() {
            TierCacheFactory factory = TierCacheFactory.builder()
                    .remoteCache(new InMemoryRemoteCache<>())
                    .lockProvider(new InMemoryLockProvider())
                    .cache("nullable", new CacheOverride()
                            .nullPolicy(NullPolicy.allow(Duration.ofMinutes(1))))
                    .build();
            return new TierCacheManager(factory);
        }

        @Bean
        CallCounter callCounter() {
            return new CallCounter();
        }

        @Bean
        ReferenceService referenceService(CallCounter counter) {
            return new ReferenceService(counter);
        }
    }

    @Test
    void behavioralSuiteOnStandardManager() {
        try (var context = new AnnotationConfigApplicationContext(StandardConfig.class)) {
            runBehavioralSuite(context.getBean(ReferenceService.class));
        }
    }

    @Test
    void behavioralSuiteOnTiercacheManager() {
        try (var context = new AnnotationConfigApplicationContext(TiercacheConfig.class)) {
            assertThat(context.getBean(CacheManager.class)).isInstanceOf(TierCacheManager.class);
            runBehavioralSuite(context.getBean(ReferenceService.class));
        }
    }

    private void runBehavioralSuite(ReferenceService service) {
        // @Cacheable: second call served from cache
        assertThat(service.getItem("1")).isEqualTo("item-1");
        assertThat(service.getItem("1")).isEqualTo("item-1");
        assertThat(service.computeCalls()).isEqualTo(1);

        // @CachePut: overwrites the cached value
        assertThat(service.updateItem("1", "updated")).isEqualTo("updated");
        assertThat(service.getItem("1")).isEqualTo("updated");
        assertThat(service.computeCalls()).isEqualTo(1);

        // @CacheEvict: next call recomputes
        service.deleteItem("1");
        assertThat(service.getItem("1")).isEqualTo("item-1");
        assertThat(service.computeCalls()).isEqualTo(2);

        // null result caching: under the standard manager ConcurrentMapCache
        // does not cache nulls; under Tiercache the "nullable" cache has
        // allow policy — but through the adapter a null result is only
        // cached when the interceptor calls put(key, null). Both must at
        // least return null correctly.
        assertThat(service.getNullable("x")).isNull();
        assertThat(service.getNullable("x")).isNull();
    }
}
