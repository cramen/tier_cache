package io.tiercache;

import io.tiercache.internal.DefaultTierCache;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Remaining branch coverage for message validation and target edge cases. */
class InvalidationEdgeTest {

    @Test
    void messageValidation() {
        UUID id = UUID.randomUUID();
        Version v = new Version(1, id);
        // INVALIDATE without a key is rejected.
        assertThrows(IllegalArgumentException.class, () ->
                new InvalidationMessage("c", null, v, id, InvalidationMessage.Type.INVALIDATE));
        // EVICT_ALL without a key is fine; null components rejected.
        assertDoesNotThrow(() ->
                new InvalidationMessage("c", null, v, id, InvalidationMessage.Type.EVICT_ALL));
        assertThrows(NullPointerException.class, () ->
                new InvalidationMessage(null, "k", v, id, InvalidationMessage.Type.INVALIDATE));
        assertThrows(NullPointerException.class, () ->
                new InvalidationMessage("c", "k", null, id, InvalidationMessage.Type.INVALIDATE));
        assertThrows(NullPointerException.class, () ->
                new InvalidationMessage("c", "k", v, null, InvalidationMessage.Type.INVALIDATE));
        assertThrows(NullPointerException.class, () ->
                new InvalidationMessage("c", "k", v, id, null));
    }

    @Test
    void evictL1IfNewerOnAbsentKeyIsNoop() {
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), new InMemoryRemoteCache<>(),
                CacheSettings.defaults(), true, null, null, new VersionGenerator(), null);
        assertDoesNotThrow(() -> cache.evictL1IfNewer("absent", new Version(1, UUID.randomUUID())));
    }

    @Test
    void cacheSettingsRejectsNullNullPolicy() {
        assertThrows(NullPointerException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1), 0.0, null,
                        InvalidationMode.INVALIDATE, 64 * 1024));
    }

    @Test
    void putIfAbsentVersionedPublishes() {
        java.util.List<InvalidationMessage> published = new java.util.ArrayList<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), new InMemoryRemoteCache<>(),
                CacheSettings.defaults(), true, null, null, new VersionGenerator(),
                new io.tiercache.spi.InvalidationHandler() {
                    @Override
                    public void onLocalWrite(String c, Object k, Version v, InvalidationMessage.Type t) {
                        published.add(new InvalidationMessage(c, k, v, v.instanceId(), t));
                    }

                    @Override
                    public void registerTarget(String c, io.tiercache.spi.InvalidationTarget t) {
                    }

                    @Override
                    public void close() {
                    }
                });
        cache.putIfAbsent("k", "v");
        cache.putIfAbsent("k", "v2"); // loses: no publish
        assertEquals(1, published.size());
        assertEquals(InvalidationMessage.Type.INVALIDATE, published.get(0).type());
    }
}
