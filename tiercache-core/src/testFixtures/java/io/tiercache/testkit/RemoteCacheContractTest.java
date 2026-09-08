package io.tiercache.testkit;

import io.tiercache.spi.RemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Contract test for {@link RemoteCache} implementations. Subclass and
 * implement {@link #newCache()} to run the suite against an implementation.
 */
public abstract class RemoteCacheContractTest {

    protected abstract RemoteCache<String, String> newCache();

    @Test
    void getReturnsNullWhenAbsent() {
        assertNull(newCache().get("missing"));
    }

    @Test
    void putThenGetReturnsValue() {
        RemoteCache<String, String> cache = newCache();
        cache.put("k", "v", Duration.ofMinutes(1));
        assertEquals("v", cache.get("k"));
    }

    @Test
    void evictRemovesValue() {
        RemoteCache<String, String> cache = newCache();
        cache.put("k", "v", Duration.ofMinutes(1));
        cache.evict("k");
        assertNull(cache.get("k"));
    }

    @Test
    void entryExpiresAfterTtl() throws InterruptedException {
        RemoteCache<String, String> cache = newCache();
        cache.put("k", "v", Duration.ofMillis(50));
        Thread.sleep(150);
        assertNull(cache.get("k"));
    }

    @Test
    void entriesOfOneCacheMayHaveDifferentTtls() throws InterruptedException {
        // F-06: per-entry TTL in L2.
        RemoteCache<String, String> cache = newCache();
        cache.put("short", "v1", Duration.ofMillis(50));
        cache.put("long", "v2", Duration.ofMinutes(1));
        Thread.sleep(150);
        assertNull(cache.get("short"));
        assertEquals("v2", cache.get("long"));
    }
}
