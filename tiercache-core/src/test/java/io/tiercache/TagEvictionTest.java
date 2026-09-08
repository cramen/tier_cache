package io.tiercache;

import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Spec: core-read-path — tagged writes and tag/batch eviction. */
class TagEvictionTest {

    @Test
    void taggedPutThenEvictByTag() {
        TierCache<String, String> cache = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build()
                .getCache("c");
        cache.put("a", "1", "group");
        cache.put("b", "2", "group");
        cache.put("c", "3", "other");

        cache.evictByTag("group");
        assertNull(cache.get("a"));
        assertNull(cache.get("b"));
        assertEquals("3", cache.get("c"), "differently tagged entry survives");
    }

    @Test
    void batchEvict() {
        TierCache<String, String> cache = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build()
                .getCache("c");
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");
        cache.evictAll(List.of("a", "b"));
        assertNull(cache.get("a"));
        assertNull(cache.get("b"));
        assertEquals("3", cache.get("c"));
    }

    @Test
    void putWithoutTagsBehavesLikePlainPut() {
        TierCache<String, String> cache = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build()
                .getCache("c");
        cache.put("k", "v");
        assertEquals("v", cache.get("k"));
    }
}
