package io.tiercache;

import io.tiercache.spi.InvalidationEventListener;
import io.tiercache.spi.InvalidationHandler;
import io.tiercache.spi.InvalidationTarget;
import io.tiercache.testkit.InMemoryRemoteCache;
import io.tiercache.testkit.RecordingLocalCache;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Spec: invalidation — application-facing event hook: the factory hands the
 * registered {@link InvalidationEventListener} to the invalidation engine;
 * the default is a no-op.
 */
class InvalidationEventListenerTest {

    /**
     * Engine stand-in: applies inbound events to registered targets, then
     * notifies the event listener (the engine-side contract).
     */
    private static final class ApplyingHandler implements InvalidationHandler {
        final Map<String, InvalidationTarget> targets = new ConcurrentHashMap<>();
        InvalidationEventListener eventListener;

        @Override
        public void onLocalWrite(String cache, Object key, Version version,
                InvalidationMessage.Type type) {
        }

        @Override
        public void registerTarget(String cache, InvalidationTarget target) {
            targets.put(cache, target);
        }

        @Override
        public void setEventListener(InvalidationEventListener listener) {
            this.eventListener = listener;
        }

        @Override
        public void close() {
        }

        void deliver(InvalidationMessage message) {
            InvalidationTarget target = targets.get(message.cache());
            switch (message.type()) {
                case INVALIDATE -> target.evictL1IfNewer(message.key(), message.version());
                case UPDATE -> target.applyUpdateL1(message.key(), message.payload(),
                        message.version());
                case EVICT_ALL -> target.evictAllL1();
            }
            eventListener.onEvent(message.cache(), message);
        }
    }

    @Test
    void listenerReceivesAppliedEventsInOrder() {
        ApplyingHandler handler = new ApplyingHandler();
        RecordingLocalCache<String, String> l1 = new RecordingLocalCache<>();
        List<InvalidationMessage> received = new CopyOnWriteArrayList<>();
        List<Boolean> l1HadK1AtCallback = new CopyOnWriteArrayList<>();
        InvalidationEventListener listener = (cache, event) -> {
            received.add(event);
            l1HadK1AtCallback.add(l1.get("k1") != null);
        };
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .localCacheFactory((name, settings) -> l1)
                .invalidation(versions -> handler)
                .invalidationEventListener(listener)
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        cache.put("k1", "v1");
        cache.put("k2", "v2");

        assertSame(listener, handler.eventListener,
                "factory must hand the registered listener to the engine");

        UUID remote = UUID.randomUUID();
        handler.deliver(new InvalidationMessage("c", "k1", new Version(1000, remote),
                remote, InvalidationMessage.Type.INVALIDATE));
        handler.deliver(new InvalidationMessage("c", null, new Version(1001, remote),
                remote, InvalidationMessage.Type.EVICT_ALL));
        factory.close();

        assertEquals(2, received.size());
        assertEquals(InvalidationMessage.Type.INVALIDATE, received.get(0).type());
        assertEquals("k1", received.get(0).key());
        assertEquals(InvalidationMessage.Type.EVICT_ALL, received.get(1).type());
        assertEquals(List.of(false, false), l1HadK1AtCallback,
                "the event must already be applied when the listener runs");
    }

    @Test
    void noListenerDefaultsToNoop() {
        ApplyingHandler handler = new ApplyingHandler();
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .invalidation(versions -> handler)
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        cache.put("k", "v");

        assertNotNull(handler.eventListener, "factory must install the NOOP default");
        UUID remote = UUID.randomUUID();
        // Applying an event with the default listener must not throw.
        handler.deliver(new InvalidationMessage("c", "k", new Version(1000, remote),
                remote, InvalidationMessage.Type.INVALIDATE));
        assertEquals("v", cache.get("k"), "behavior without a listener is unchanged");
        factory.close();
    }
}
