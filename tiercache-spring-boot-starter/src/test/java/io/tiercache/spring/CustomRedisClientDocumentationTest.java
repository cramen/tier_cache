package io.tiercache.spring;

import io.lettuce.core.*;
import io.tiercache.TierCacheFactory;
import io.tiercache.TierCache;
import io.tiercache.redis.RedisStreamJournal;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import static org.assertj.core.api.Assertions.assertThat;

class CustomRedisClientDocumentationTest {
    @Configuration(proxyBeanMethods = false)
    static class CacheClientConfiguration {
        @Bean(destroyMethod = "shutdown")
        @Primary
        RedisClient applicationRedisClient(@Value("${tiercache.redis-uri}") String uri) {
            RedisClient client = RedisClient.create(uri);
            client.setOptions(ClientOptions.builder()
                    .socketOptions(SocketOptions.builder()
                            .connectTimeout(Duration.ofMillis(150)).build())
                    .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(350)))
                    .build());
            return client;
        }
    }
    @Test void primaryClientPreservesJournalAndCrossInstanceInvalidation() {
        try (var redis = new GenericContainer<>(DockerImageName.parse("redis:6.2.24-alpine")).withExposedPorts(6379)) {
            redis.start();
            var runner = new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(TiercacheAutoConfiguration.class))
                    .withUserConfiguration(CacheClientConfiguration.class)
                    .withPropertyValues("tiercache.enabled=true",
                        "tiercache.redis-uri=redis://"+redis.getHost()+":"+redis.getMappedPort(6379));
            runner.run(a -> runner.run(b -> {
                assertThat(a).hasNotFailed(); assertThat(b).hasNotFailed();
                assertThat(a.getBeansOfType(RedisClient.class)).hasSize(2);
                assertThat(a.getBean(RedisClient.class)).isSameAs(a.getBean("applicationRedisClient"));
                assertThat(a.getBean(RedisClient.class).getOptions().getSocketOptions().getConnectTimeout())
                        .isEqualTo(Duration.ofMillis(150));
                var timeout = a.getBean(RedisClient.class).getOptions().getTimeoutOptions().getSource();
                assertThat(timeout.getTimeUnit().toMillis(timeout.getTimeout(null))).isEqualTo(350);
                TierCache<String,String> left=a.getBean(TierCacheFactory.class).getCache("client-doc");
                TierCache<String,String> right=b.getBean(TierCacheFactory.class).getCache("client-doc");
                left.put("key","old"); assertThat(right.get("key")).isEqualTo("old");
                left.put("key","new");
                long deadline=System.nanoTime()+Duration.ofSeconds(5).toNanos();
                while (!"new".equals(right.get("key")) && System.nanoTime()<deadline) Thread.sleep(10);
                assertThat(right.get("key")).isEqualTo("new");
                assertThat(a.getBean(RedisStreamJournal.class).size("client-doc")).isGreaterThanOrEqualTo(2);
                left.put("p1","first","category:books");
                left.evictByTag("category:books"); assertThat(left.get("p1")).isNull();
                left.put("p3","third"); left.evictAll(java.util.List.of("p3","p4"));
                assertThat(left.get("p3")).isNull(); left.evictAll();
            }));
        }
    }
}
