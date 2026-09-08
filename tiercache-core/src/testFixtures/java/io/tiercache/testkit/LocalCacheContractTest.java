package io.tiercache.testkit;

import io.tiercache.spi.LocalCache;
import io.tiercache.spi.StoredEntry;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        cache.put("k", StoredEntry.ofValue("v"), Duration.ofMinutes(1));
        assertEquals("v", cache.get("k").value());
    }

    @Test
    void evictRemovesValue() {
        LocalCache<String, String> cache = newCache();
        cache.put("k", StoredEntry.ofValue("v"), Duration.ofMinutes(1));
        cache.evict("k");
        assertNull(cache.get("k"));
    }

    @Test
    void entryExpiresAfterTtl() throws InterruptedException {
        LocalCache<String, String> cache = newCache();
        cache.put("k", StoredEntry.ofValue("v"), Duration.ofMillis(50));
        Thread.sleep(150);
        assertNull(cache.get("k"));
    }

    @Test
    void putOverwritesExistingValue() {
        LocalCache<String, String> cache = newCache();
        cache.put("k", StoredEntry.ofValue("v1"), Duration.ofMinutes(1));
        cache.put("k", StoredEntry.ofValue("v2"), Duration.ofMinutes(1));
        assertEquals("v2", cache.get("k").value());
    }

    @Test
    void setIfAbsentStoresOnlyWhenAbsent() {
        LocalCache<String, String> cache = newCache();
        assertTrue(cache.setIfAbsent("k", StoredEntry.ofValue("v"), Duration.ofMinutes(1)));
        assertFalse(cache.setIfAbsent("k", StoredEntry.ofValue("x"), Duration.ofMinutes(1)));
        assertEquals("v", cache.get("k").value());
    }

    @Test
    void clearRemovesAllEntries() {
        LocalCache<String, String> cache = newCache();
        cache.put("a", StoredEntry.ofValue("1"), Duration.ofMinutes(1));
        cache.put("b", StoredEntry.ofValue("2"), Duration.ofMinutes(1));
        cache.clear();
        assertNull(cache.get("a"));
        assertNull(cache.get("b"));
    }

    @Test
    void nullMarkerRoundTrips() {
        // Markers are stored like any other entry.
        LocalCache<String, String> cache = newCache();
        cache.put("k", StoredEntry.nullMarker(), Duration.ofMinutes(1));
        StoredEntry<String> entry = cache.get("k");
        assertTrue(entry != null && entry.isNullMarker());
    }
}
