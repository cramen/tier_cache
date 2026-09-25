package io.tiercache.spring;

import io.tiercache.TierCacheFactory;
import io.tiercache.redis.PubSubDispatchOptions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.GenericContainer;
import static org.junit.jupiter.api.Assertions.*;

class PubSubDispatchConfigurationTest {
    final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TiercacheAutoConfiguration.class)).withPropertyValues("tiercache.enabled=true");
    @Test void invalidAndOverflowingPropertiesFailBeforeRedisConnection() {
        for (String name : new String[]{"dispatch-threads", "max-pending-messages", "max-pending-bytes"}) {
            for (String value : new String[]{"0", "-1", "999999999999999999999999"}) {
                String property = "tiercache.invalidation.pubsub." + name;
                runner.withPropertyValues("tiercache.redis-uri=redis://127.0.0.1:1", property + "=" + value).run(context -> {
                    assertNotNull(context.getStartupFailure());
                    assertTrue(JournalCapacityConfigurationTest.messages(context.getStartupFailure()).contains(property));
                });
            }
        }
    }
    @Test void defaultsAndOverridesBindAndValidOptionsDoNotChangeStreams() {
        try (var redis = new GenericContainer<>("redis:6.2-alpine").withExposedPorts(6379)) {
            redis.start();
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            for (String profile : new String[]{"pubsub", "streams"}) {
                runner.withPropertyValues("tiercache.redis-uri=" + uri, "tiercache.invalidation.profile=" + profile,
                        "tiercache.invalidation.pubsub.dispatch-threads=3", "tiercache.invalidation.pubsub.max-pending-messages=17",
                        "tiercache.invalidation.pubsub.max-pending-bytes=1024").run(context -> {
                    assertNull(context.getStartupFailure());
                    assertEquals(new PubSubDispatchOptions(3, 17, 1024), context.getBean(TiercacheProperties.class)
                            .getInvalidation().getPubsub().toOptions());
                    var cache = context.getBean(TierCacheFactory.class).getCache("dispatch");
                    cache.put("key", "value"); assertEquals("value", cache.get("key"));
                });
            }
        }
        assertEquals(PubSubDispatchOptions.DEFAULT, new TiercacheProperties().getInvalidation().getPubsub().toOptions());
    }
}
