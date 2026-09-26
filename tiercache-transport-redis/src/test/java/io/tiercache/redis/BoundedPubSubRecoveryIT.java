package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.*;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.spi.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.tiercache.redis.StreamsRecoveryTest.await;
import static io.tiercache.redis.StreamsRecoveryTest.gate;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BoundedPubSubRecoveryIT {
    GenericContainer<?> redis;
    RedisClient client;
    static final JdkCacheSerializer<Object> CODEC = new JdkCacheSerializer<>();
    final UUID writer = UUID.randomUUID();
    @BeforeAll void start() {
        redis = new GenericContainer<>(ServerProfile.image()).withExposedPorts(6379); redis.start();
        client = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
    }
    @AfterAll void close() { client.shutdown(); redis.stop(); }
    static PubSubDispatcher dispatcher(LettucePubSubInvalidationTransport transport) throws Exception {
        var field = LettucePubSubInvalidationTransport.class.getDeclaredField("dispatcher"); field.setAccessible(true);
        return (PubSubDispatcher) field.get(transport);
    }
    InvalidationMessage message(String cache, long version, String value) {
        return new InvalidationMessage(cache, "key", new Version(version, writer), writer,
                InvalidationMessage.Type.UPDATE, value);
    }
    void send(LettucePubSubInvalidationTransport publisher, RedisStreamJournal journal, InvalidationMessage message) throws Exception {
        if (journal != null) journal.append(message.cache(), message);
        publisher.publishAsync(message).toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
    @Test void anotherCacheProgressesDuringOverflowAndIntactReplayPreservesUnaffectedValues() throws Exception {
        String a = "slow-" + UUID.randomUUID(), b = "fast-" + UUID.randomUUID();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var connection = client.connect(ByteArrayCodec.INSTANCE);
             var publisher = new LettucePubSubInvalidationTransport(client, CODEC);
             var receiver = new LettucePubSubInvalidationTransport(client, CODEC, CODEC, 65536, new PubSubDispatchOptions(2, 8, 4096))) {
            var journal = new RedisStreamJournal(connection, 10000, CODEC);
            var first = new StreamsRecoveryTest.Target(); var other = new StreamsRecoveryTest.Target();
            try (var service = new InvalidationService(receiver, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
                service.registerTarget(a, first); service.registerTarget(b, other);
                first.values.put("unaffected", "keep"); int initialClears = first.clears.get();
                service.setEventListener((cache, event) -> {
                    if (cache.equals(a) && event.version().sequence() == 1) { entered.countDown(); gate(release); }
                });
                send(publisher, journal, message(a, 1, "first")); gate(entered);
                send(publisher, journal, message(b, 1, "independent"));
                await(() -> "independent".equals(other.values.get("key")));
                for (int i = 2; i <= 200; i++) send(publisher, journal, message(a, i, "v" + i));
                var dispatch = dispatcher(receiver);
                await(() -> dispatch.pending(a));
                assertTrue(dispatch.retainedMessages() <= 8); assertTrue(dispatch.retainedBytes() <= 4096);
                assertEquals(1, release.getCount()); assertEquals(initialClears, first.clears.get());
                release.countDown();
                await(() -> !dispatch.pending(a) && "v200".equals(first.values.get("key")));
                assertEquals("keep", first.values.get("unaffected")); assertEquals(initialClears, first.clears.get());
                await(() -> dispatch.retainedMessages() == 0 && dispatch.controlGroups() == 0);
            }
        } finally { release.countDown(); }
    }
    @Test void recoveryClearWaitsForHandlerAndCannotBeUndoneByQueuedUpdate() throws Exception {
        String cache = "clear-" + UUID.randomUUID();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var publisher = new LettucePubSubInvalidationTransport(client, CODEC);
             var receiver = new LettucePubSubInvalidationTransport(client, CODEC)) {
            var target = new StreamsRecoveryTest.Target();
            try (var service = new InvalidationService(receiver, null, UUID.randomUUID(), InvalidationListener.NOOP)) {
                service.registerTarget(cache, target); int initialClears = target.clears.get();
                service.setEventListener((name, event) -> {
                    if (event.version().sequence() == 1) { entered.countDown(); gate(release); }
                });
                send(publisher, null, message(cache, 1, "first")); gate(entered);
                send(publisher, null, message(cache, 2, "obsolete queued payload"));
                var dispatch = dispatcher(receiver);
                await(() -> dispatch.retainedMessages() == 2);
                var recovery = service.recoverAsync(Runnable::run).toCompletableFuture();
                await(() -> dispatch.pending(cache));
                assertFalse(recovery.isDone()); assertEquals(initialClears, target.clears.get());
                release.countDown(); assertTrue(recovery.get(5, TimeUnit.SECONDS));
                await(() -> !dispatch.pending(cache)); assertTrue(target.values.isEmpty());
                send(publisher, null, message(cache, 3, "fresh"));
                await(() -> "fresh".equals(target.values.get("key")));
            }
        } finally { release.countDown(); }
    }
    @Test void lossDuringCompletedButUnacknowledgedRepairRequiresAnotherRepair() throws Exception {
        String cache = "epoch-" + UUID.randomUUID();
        var allowCompletion = new CompletableFuture<Void>(); var repaired = new CountDownLatch(1);
        try (var connection = client.connect(ByteArrayCodec.INSTANCE);
             var publisher = new LettucePubSubInvalidationTransport(client, CODEC);
             var receiver = new LettucePubSubInvalidationTransport(client, CODEC, CODEC, 65536, new PubSubDispatchOptions(2, 8, 4096))) {
            var journal = new RedisStreamJournal(connection, 10000, CODEC);
            var target = new StreamsRecoveryTest.Target();
            try (var service = new InvalidationService(receiver, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
                service.registerTarget(cache, target);
                var dispatch = dispatcher(receiver);
                var field = PubSubDispatcher.class.getDeclaredField("recovery"); field.setAccessible(true);
                var delegate = (InvalidationGapHandler) field.get(dispatch);
                var attempts = new AtomicInteger();
                receiver.setGapHandler(new InvalidationGapHandler() {
                    public CompletionStage<RecoveryResult> reset(String name) { return delegate.reset(name); }
                    public RecoveryResult registrationBaseline(String name) { return delegate.registrationBaseline(name); }
                    public boolean isCurrent(String name, RecoveryResult result) { return delegate.isCurrent(name, result); }
                    public boolean isLocalRecoveryCurrent(String name, RecoveryResult result) { return delegate.isLocalRecoveryCurrent(name, result); }
                    public CompletionStage<RecoveryResult> recoverLocalGap(String name) {
                        boolean first = attempts.incrementAndGet() == 1;
                        return delegate.recoverLocalGap(name).thenCompose(result -> {
                            if (!first) return CompletableFuture.completedFuture(result);
                            repaired.countDown(); return allowCompletion.thenApply(ignored -> result);
                        });
                    }
                });
                var pendingLosses = new AtomicLong();
                receiver.setMetricsListener(new CacheMetricsListener() {
                    @Override public void onDispatchRejected(DispatchReason reason, long messages) {
                        if (reason == DispatchReason.PENDING) pendingLosses.addAndGet(messages);
                    }
                });
                // Malformed frame is attributed through the real subscribed channel.
                connection.sync().publish(RedisKeyspace.channel(cache), new byte[]{1}); gate(repaired);
                send(publisher, journal, message(cache, 2, "latest"));
                // Pending was already true before publish. Observe this frame's actual
                // rejection before acknowledging repair, rather than racing delivery.
                await(() -> pendingLosses.get() == 1);
                assertTrue(dispatch.pending(cache));
                allowCompletion.complete(null);
                await(() -> !dispatch.pending(cache) && "latest".equals(target.values.get("key")));
                assertTrue(attempts.get() >= 2);
            }
        } finally { allowCompletion.complete(null); }
    }
    @Test void replayedEvictAllFencesAnOlderPausedDeserializer() throws Exception {
        String cache = "replay-clear-" + UUID.randomUUID();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var first = new AtomicBoolean(true);
        var outside = new AtomicBoolean();
        var dispatcherRef = new AtomicReference<PubSubDispatcher>();
        CacheSerializer<Object> gated = new CacheSerializer<>() {
            public byte[] toBytes(Object value) { return CODEC.toBytes(value); }
            public Object fromBytes(byte[] value) {
                if (first.compareAndSet(true, false)) {
                    try {
                        var field = PubSubDispatcher.class.getDeclaredField("gate"); field.setAccessible(true);
                        outside.set(!Thread.holdsLock(field.get(dispatcherRef.get()))
                                && Thread.currentThread().getName().startsWith("tiercache-invalidation-"));
                    } catch (Exception error) { throw new AssertionError(error); }
                    entered.countDown(); gate(release);
                }
                return CODEC.fromBytes(value);
            }
        };
        try (var connection = client.connect(ByteArrayCodec.INSTANCE);
             var publisher = new LettucePubSubInvalidationTransport(client, CODEC);
             var receiver = new LettucePubSubInvalidationTransport(client, gated)) {
            var journal = new RedisStreamJournal(connection, 10000, CODEC);
            var target = new StreamsRecoveryTest.Target();
            var dispatch = dispatcher(receiver); dispatcherRef.set(dispatch);
            try (var service = new InvalidationService(receiver, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
                service.registerTarget(cache, target); int clears = target.clears.get();
                send(publisher, journal, message(cache, 1, "old-running")); gate(entered);
                send(publisher, journal, message(cache, 2, "old-queued"));
                journal.append(cache, new InvalidationMessage(cache, null, new Version(3, writer), writer, InvalidationMessage.Type.EVICT_ALL));
                journal.append(cache, message(cache, 4, "after-clear"));
                var recovered = service.recoverAsync(Runnable::run).toCompletableFuture();
                await(() -> dispatch.pending(cache));
                assertFalse(recovered.isDone()); assertEquals(clears, target.clears.get());
                assertTrue(outside.get(), "application deserialization must run off the callback and outside the gate");
                release.countDown(); assertTrue(recovered.get(5, TimeUnit.SECONDS));
                await(() -> !dispatch.pending(cache));
                assertEquals("after-clear", target.values.get("key"));
            }
        } finally { release.countDown(); }
    }
}
