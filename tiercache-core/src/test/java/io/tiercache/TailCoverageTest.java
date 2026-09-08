package io.tiercache;

import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.CountingRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tail-end branch coverage. */
class TailCoverageTest {

    @Test
    void negativeDurationsRejected() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(-1), 0.0,
                        NullPolicy.deny()));
        assertThrows(IllegalArgumentException.class, () ->
                NullPolicy.allow(Duration.ofMinutes(-1)));
    }

    @Test
    void coordinationDisabledWithoutProviderIsFine() {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new io.tiercache.testkit.InMemoryRemoteCache<>())
                .disableDistributedCoordination()
                .build();
        factory.getCache("c").put("k", "v");
        factory.close();
    }

    @Test
    void getSeesMarkerFromL2AsNull() {
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        l2.put("k", StoredEntry.nullMarker(new Version(1, UUID.randomUUID())), Duration.ofMinutes(1));
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, CacheSettings.defaults(), true,
                null, null, new VersionGenerator(), null);
        assertNull(cache.get("k"));
        assertNull(cache.getOrCompute("k", key -> {
            throw new AssertionError("marker must suppress the loader");
        }));
    }

    @Test
    void getOrComputeWithoutSingleflightClassifies() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                l1, new CountingRemoteCache<>(), CacheSettings.defaults(), false,
                null, null, new VersionGenerator(), null);
        cache.getOrCompute("k", key -> "v"); // non-singleflight load path
        assertNull(cache.getOrCompute("m", key -> null)); // miss path
        assertNull(cache.get("nope"));
    }
}
