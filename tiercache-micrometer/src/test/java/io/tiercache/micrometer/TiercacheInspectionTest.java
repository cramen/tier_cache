package io.tiercache.micrometer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import javax.management.ObjectName;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** JMX registration and state exposure. */
class TiercacheInspectionTest {

    @Test
    void jmxExposesCacheState() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerCacheMetrics metrics = new MicrometerCacheMetrics(registry);
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .metricsListener(metrics)
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        cache.put("k", "v");
        cache.get("k");                 // L1 hit
        factory.getCache("c2").get("k"); // L2 hit (cache c2, fresh L1)
        cache.get("absent");            // miss

        try (TiercacheInspection inspection = new TiercacheInspection(registry, factory,
                null, List.of("c"))) {
            inspection.register();
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("io.tiercache:type=Inspection");
            assertArrayEquals(new String[]{"c"},
                    (String[]) server.getAttribute(name, "CacheNames"));
            assertEquals("closed", server.getAttribute(name, "BreakerState"));
            // c: one L1 hit + one miss; c2: one L2 hit.
            Object l1 = server.invoke(name, "getL1HitRatio",
                    new Object[]{"c"}, new String[]{String.class.getName()});
            assertEquals(0.5, (double) l1, 0.001);
            Object l2 = server.invoke(name, "getL2HitRatio",
                    new Object[]{"c2"}, new String[]{String.class.getName()});
            assertEquals(1.0, (double) l2, 0.001);
            Object journalSize = server.invoke(name, "getJournalSize",
                    new Object[]{"c"}, new String[]{String.class.getName()});
            assertEquals(-1L, (long) journalSize);
        }
        factory.close();
    }
}
