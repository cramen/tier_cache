package io.tiercache.reactor;

import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.VersionGenerator;
import io.tiercache.spi.InvalidationEventListener;
import io.tiercache.spi.InvalidationHandler;
import io.tiercache.spi.InvalidationTarget;
import io.tiercache.testkit.InMemoryInvalidationTransport;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spec: reactor-api — invalidation Flux (design D3): subscribers receive
 * inbound events in order, cancellation unregisters the listener, and
 * slow subscribers never block producers — overflow is dropped for that
 * subscriber and signalled.
 */
class ReactorInvalidationFluxTest {

    @Test
    void subscriberReceivesEventsInOrder() {
        try (Fixture f = new Fixture()) {
            f.factory.reactorCache("events");
            StepVerifier.create(f.factory.invalidationEvents("events")
                            .map(InvalidationMessage::key)
                            .take(3))
                    .then(() -> {
                        f.publish("events", "k1");
                        f.publish("events", "k2");
                        f.publish("events", "k3");
                    })
                    .expectNext("k1", "k2", "k3")
                    .verifyComplete();
        }
    }

    @Test
    void cancellationUnregistersTheListenerAndStopsDelivery() throws InterruptedException {
        try (Fixture f = new Fixture()) {
            f.factory.reactorCache("events");
            ConcurrentLinkedQueue<Object> first = new ConcurrentLinkedQueue<>();
            Disposable sub1 = f.factory.invalidationEvents("events")
                    .subscribe(e -> first.add(e.key()));
            awaitTrue(() -> f.factory.subscriberCount("events") == 1);
            f.publish("events", "k1");
            awaitTrue(() -> first.size() == 1);

            sub1.dispose();
            awaitTrue(() -> f.factory.subscriberCount("events") == 0);

            // Probe via a second subscription: it sees only events that
            // arrive after its own registration.
            ConcurrentLinkedQueue<Object> second = new ConcurrentLinkedQueue<>();
            Disposable sub2 = f.factory.invalidationEvents("events")
                    .subscribe(e -> second.add(e.key()));
            awaitTrue(() -> f.factory.subscriberCount("events") == 1);
            f.publish("events", "k2");
            awaitTrue(() -> second.size() == 1);
            sub2.dispose();

            assertThat(first).containsExactly("k1");
            assertThat(second).containsExactly("k2");
        }
    }

    @Test
    void slowSubscriberDoesNotBlockProducersAndDropsAreSignalled() throws InterruptedException {
        try (Fixture f = new Fixture()) {
            f.factory.reactorCache("events");
            ConcurrentLinkedQueue<Object> received = new ConcurrentLinkedQueue<>();
            AtomicLong drops = new AtomicLong();
            BaseSubscriber<InvalidationMessage> subscriber = new BaseSubscriber<>() {
                @Override
                protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) {
                    // Deliberately request nothing: the subscriber is parked.
                }

                @Override
                protected void hookOnNext(InvalidationMessage event) {
                    received.add(event.key());
                }
            };
            f.factory.invalidationEvents("events", 8, e -> drops.incrementAndGet())
                    .subscribe(subscriber);
            awaitTrue(() -> f.factory.subscriberCount("events") == 1);

            int total = 500;
            // Synchronous return proves the producer is never blocked by the parked subscriber.
            for (int i = 0; i < total; i++) {
                f.publish("events", "k" + i);
            }
            assertThat(drops.get()).isEqualTo(total - 8L);

            subscriber.request(total);
            awaitTrue(() -> received.size() == 8);
            assertThat(received.size() + drops.get()).isEqualTo(total);
            subscriber.cancel();
        }
    }

    /**
     * Two transports on one hub: the factory's engine subscribes through
     * one, a raw publisher drives events through the other (different
     * origin).
     */
    private static final class Fixture implements AutoCloseable {
        private final InMemoryInvalidationTransport.Hub hub = new InMemoryInvalidationTransport.Hub();
        private final InMemoryInvalidationTransport factoryTransport =
                new InMemoryInvalidationTransport(hub);
        private final InMemoryInvalidationTransport publisherTransport =
                new InMemoryInvalidationTransport(hub);
        private final VersionGenerator publisherVersions = new VersionGenerator();

        final ReactorCacheFactory factory = ReactorCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<String, String>())
                .invalidation(versions ->
                        new TestInvalidationHandler(factoryTransport, versions.instanceId()))
                .build();

        void publish(String cache, String key) {
            publisherTransport.publish(new InvalidationMessage(cache, key, publisherVersions.next(),
                    publisherVersions.instanceId(), InvalidationMessage.Type.INVALIDATE));
        }

        @Override
        public void close() {
            factory.close();
            publisherTransport.close();
        }
    }

    /**
     * Minimal in-test invalidation engine over
     * {@link InMemoryInvalidationTransport}, mirroring the real one's
     * receive path: ignore own origin, apply to the registered target,
     * then notify the event listener.
     */
    private static final class TestInvalidationHandler implements InvalidationHandler {

        private final InMemoryInvalidationTransport transport;
        private final UUID originInstanceId;
        private volatile InvalidationEventListener eventListener = InvalidationEventListener.NOOP;
        private final Map<String, InvalidationTarget> targets = new ConcurrentHashMap<>();
        private final List<AutoCloseable> subscriptions = new CopyOnWriteArrayList<>();

        TestInvalidationHandler(InMemoryInvalidationTransport transport, UUID originInstanceId) {
            this.transport = transport;
            this.originInstanceId = originInstanceId;
        }

        @Override
        public void onLocalWrite(String cache, Object key, Version version,
                                 InvalidationMessage.Type type) {
            transport.publish(new InvalidationMessage(cache, key, version, originInstanceId, type));
        }

        @Override
        public void registerTarget(String cache, InvalidationTarget target) {
            targets.put(cache, target);
            subscriptions.add(transport.subscribe(cache, message -> {
                if (message.originInstanceId().equals(originInstanceId)) {
                    return;
                }
                InvalidationTarget registered = targets.get(message.cache());
                if (registered == null) {
                    return;
                }
                switch (message.type()) {
                    case INVALIDATE -> registered.evictL1IfNewer(message.key(), message.version());
                    case UPDATE ->
                            registered.applyUpdateL1(message.key(), message.payload(), message.version());
                    case EVICT_ALL -> registered.evictAllL1();
                }
                eventListener.onEvent(message.cache(), message);
            }));
        }

        @Override
        public void setEventListener(InvalidationEventListener listener) {
            eventListener = listener;
        }

        @Override
        public void close() {
            for (AutoCloseable subscription : subscriptions) {
                try {
                    subscription.close();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            transport.close();
        }
    }

    private interface BoolProbe {
        boolean getAsBoolean();
    }

    private static void awaitTrue(BoolProbe probe) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!probe.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not met within 10000ms");
            }
            Thread.sleep(5);
        }
    }
}
