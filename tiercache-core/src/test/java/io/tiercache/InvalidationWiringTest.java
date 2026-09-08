package io.tiercache;

import io.tiercache.spi.InvalidationHandler;
import io.tiercache.spi.InvalidationTarget;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: invalidation — factory wiring: local writes publish when the engine
 * is configured; single-node factories publish nothing.
 */
class InvalidationWiringTest {

    private static final class RecordingHandler implements InvalidationHandler {
        final List<InvalidationMessage> published = new CopyOnWriteArrayList<>();

        @Override
        public void onLocalWrite(String cache, Object key, Version version,
                InvalidationMessage.Type type) {
            published.add(new InvalidationMessage(cache, key, version, version.instanceId(), type));
        }

        @Override
        public void registerTarget(String cache, InvalidationTarget target) {
        }

        @Override
        public void close() {
        }
    }

    @Test
    void writesPublishWithVersionsWhenEngineConfigured() {
        RecordingHandler handler = new RecordingHandler();
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .invalidation(versions -> handler)
                .build();
        TierCache<String, String> cache = factory.getCache("c");

        cache.put("k", "v");
        cache.evict("k");
        cache.evictAll();
        factory.close();

        assertEquals(3, publishedCount(handler));
        // Versions are monotonically increasing within the instance.
        assertTrue(handler.published.get(0).version()
                .compareTo(handler.published.get(1).version()) < 0);
        assertTrue(handler.published.get(1).version()
                .compareTo(handler.published.get(2).version()) < 0);
        assertEquals(InvalidationMessage.Type.EVICT_ALL, handler.published.get(2).type());
    }

    @Test
    void nothingPublishedWithoutEngine() {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        cache.put("k", "v"); // must not throw, must not publish
        assertEquals("v", cache.get("k"));
        factory.close();
    }

    private static int publishedCount(RecordingHandler handler) {
        return handler.published.size();
    }
}
