package io.tiercache.testkit;

import io.tiercache.spi.LocalCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Contract test for {@link LocalCache} implementations. Subclass and
 * implement {@link #newCache()} to run the suite against an implementation.
 */
public abstract class LocalCacheContractTest {

    protected abstract LocalCache<String, String> newCache();

    @Test
    void getReturnsNullWhenAbsent() {
        assertNull(newCache().get("missing"));
    }

    @Test
    void putThenGetReturnsValue() {
        LocalCache<String, String> cache = newCache();
        cache.put("k", "v", Duration.ofMinutes(1));
        assertEquals("v", cache.get("k"));
    }

    @Test
    void evictRemovesValue() {
        LocalCache<String, String> cache = newCache();
        cache.put("k", "v", Duration.ofMinutes(1));
        cache.evict("k");
        assertNull(cache.get("k"));
    }

    @Test
    void entryExpiresAfterTtl() throws InterruptedException {
        LocalCache<String, String> cache = newCache();
        cache.put("k", "v", Duration.ofMillis(50));
        Thread.sleep(150);
        assertNull(cache.get("k"));
    }

    @Test
    void putOverwritesExistingValue() {
        LocalCache<String, String> cache = newCache();
        cache.put("k", "v1", Duration.ofMinutes(1));
        cache.put("k", "v2", Duration.ofMinutes(1));
        assertEquals("v2", cache.get("k"));
    }
}
