package io.tiercache.compatibility;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.tiercache.spring.TierCacheManager;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.*;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class ConsumerTest {
    @Configuration(proxyBeanMethods=false)
    @EnableAutoConfiguration
    static class App {
        @Bean MeterRegistry registry() { return new SimpleMeterRegistry(); }
        @Bean Service service() { return new Service(); }
    }
    @Configuration(proxyBeanMethods=false)
    static class CustomManager {
        @Bean CacheManager customManager() { return new ConcurrentMapCacheManager("sync"); }
    }
    public static class Service {
        final AtomicInteger syncCalls = new AtomicInteger(), nullCalls = new AtomicInteger(), asyncCalls = new AtomicInteger();
        final CompletableFuture<String> result = new CompletableFuture<>();
        final CountDownLatch entered = new CountDownLatch(1);
        @Cacheable(cacheNames="sync", sync=true)
        public String sync(String key) { syncCalls.incrementAndGet(); return "value:" + key; }
        @Cacheable(cacheNames="nullable", sync=true)
        public String nullable(String key) { nullCalls.incrementAndGet(); return null; }
        @Cacheable(cacheNames="async", sync=true)
        public CompletableFuture<String> async(String key) { asyncCalls.incrementAndGet(); entered.countDown(); return result; }
    }
    ConfigurableApplicationContext start(boolean enabled, Class<?>... extras) {
        SpringApplication app = new SpringApplication(App.class);
        app.addPrimarySources(java.util.List.of(extras));
        app.setWebApplicationType(WebApplicationType.NONE);
        app.setDefaultProperties(Map.of("tiercache.enabled", enabled,
            "tiercache.redis-uri", System.getProperty("tiercache.test.redisUri"),
            "tiercache.caches.nullable.null-policy", "allow",
            "spring.main.banner-mode", "off"));
        return app.run();
    }
    @Test void shippedStarterActivatesAndExercisesAnnotationsAndMetrics() throws Exception {
        assertEquals(System.getProperty("tiercache.test.bootVersion"), SpringBootVersion.getVersion());
        assertEquals(17, Runtime.version().feature());
        System.out.println("CONSUMER_JVM " + System.getProperty("java.runtime.version"));
        try (var context = start(true)) {
            assertInstanceOf(TierCacheManager.class, context.getBean(CacheManager.class));
            Service service = context.getBean(Service.class);
            // CGLIB proxies do not expose target fields; obtain counters via the target bean methods below.
            String key = UUID.randomUUID().toString();
            assertEquals("value:"+key, service.sync(key));
            assertEquals("value:"+key, service.sync(key));
            assertNull(service.nullable(key)); assertNull(service.nullable(key));
            var first = service.async(key); var second = service.async(key);
            Service target = (Service) ((org.springframework.aop.framework.Advised)service).getTargetSource().getTarget();
            assertTrue(target.entered.await(5, TimeUnit.SECONDS));
            target.result.complete("async-value");
            assertEquals("async-value", first.get(5, TimeUnit.SECONDS));
            assertEquals("async-value", second.get(5, TimeUnit.SECONDS));
            assertEquals("async-value", service.async(key).get(5, TimeUnit.SECONDS));
            assertEquals(1, target.syncCalls.get()); assertEquals(1, target.nullCalls.get()); assertEquals(1, target.asyncCalls.get());
            assertFalse(context.getBean(MeterRegistry.class).find("tiercache.requests").counters().isEmpty());
        }
    }
    @Test void disabledStarterBacksOff() {
        try (var context = start(false)) { assertTrue(context.getBeansOfType(TierCacheManager.class).isEmpty()); }
    }
    @Test void applicationCacheManagerTakesPrecedence() {
        try (var context = start(true, CustomManager.class)) {
            assertEquals(1, context.getBeansOfType(CacheManager.class).size());
            assertInstanceOf(ConcurrentMapCacheManager.class, context.getBean(CacheManager.class));
        }
    }
}
