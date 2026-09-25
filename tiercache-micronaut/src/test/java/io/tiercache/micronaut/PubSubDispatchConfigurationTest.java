package io.tiercache.micronaut;

import io.tiercache.TierCacheFactory;
import io.tiercache.redis.PubSubDispatchOptions;
import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class PubSubDispatchConfigurationTest {
    @Test void invalidAndOverflowingPropertiesFailBeforeRedisConnection() {
        for (String name : new String[]{"dispatch-threads", "max-pending-messages", "max-pending-bytes"}) {
            for (String value : new String[]{"0", "-1", "999999999999999999999999"}) {
                String property = "tiercache.invalidation.pubsub." + name;
                var failure = assertThrows(RuntimeException.class, () -> {
                    try (var context = ApplicationContext.run(Map.of("tiercache.enabled", true,
                            "tiercache.redis-uri", "redis://127.0.0.1:1", property, value))) {
                        context.getBean(TierCacheFactory.class);
                    }
                });
                assertTrue(JournalCapacityConfigurationTest.messages(failure).contains(property),
                        property + "=" + value + "\n" + JournalCapacityConfigurationTest.messages(failure));
            }
        }
    }
    @Test void defaultsAndOverridesBindAndValidOptionsDoNotChangeStreams() {
        try (var redis = new GenericContainer<>("redis:6.2-alpine").withExposedPorts(6379)) {
            redis.start();
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            for (String profile : new String[]{"pubsub", "streams"}) {
                try (var context = ApplicationContext.run(Map.of("tiercache.enabled", true, "tiercache.redis-uri", uri,
                        "tiercache.invalidation.profile", profile, "tiercache.invalidation.pubsub.dispatch-threads", 3,
                        "tiercache.invalidation.pubsub.max-pending-messages", 17,
                        "tiercache.invalidation.pubsub.max-pending-bytes", 1024))) {
                    assertEquals(new PubSubDispatchOptions(3, 17, 1024), context.getBean(TiercacheProperties.class)
                            .getInvalidation().getPubsub().toOptions());
                    var cache = context.getBean(TierCacheFactory.class).getCache("dispatch");
                    cache.put("key", "value"); assertEquals("value", cache.get("key"));
                }
            }
        }
        assertEquals(PubSubDispatchOptions.DEFAULT, new TiercacheProperties.InvalidationProps(null, null, null).getPubsub().toOptions());
    }
}
