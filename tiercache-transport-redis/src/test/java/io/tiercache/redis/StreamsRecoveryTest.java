package io.tiercache.redis;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.*;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.spi.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class StreamsRecoveryTest {
    abstract String image();
    GenericContainer<?> server; RedisClient client;
    static final JdkCacheSerializer<Object> CODEC = new JdkCacheSerializer<>();
    static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    @BeforeAll void start() {
        server = new GenericContainer<>(DockerImageName.parse(image())).withExposedPorts(6379); server.start();
        client = RedisClient.create("redis://" + server.getHost() + ":" + server.getMappedPort(6379));
    }
    @AfterAll void stop() { client.shutdown(); server.stop(); }
    static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
        assertTrue(condition.getAsBoolean(), "condition did not settle");
    }
    static void gate(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("gate timeout"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
    static class Target implements InvalidationTarget {
        final Map<Object, Object> values = new ConcurrentHashMap<>(); final AtomicLong generation = new AtomicLong();
        final AtomicInteger clears = new AtomicInteger(); volatile boolean failClear; volatile Runnable beforeClear = () -> { };
        public Version versionOfL1Entry(Object key) { return null; }
        public long recoveryGeneration() { return generation.get(); }
        public void evictL1IfNewer(Object key, Version v) { values.remove(key); }
        public void applyUpdateL1(Object key, Object value, Version v) { values.put(key, value); }
        public void evictAllL1() {
            beforeClear.run(); if (failClear) throw new IllegalStateException("clear failed");
            values.clear(); generation.incrementAndGet(); clears.incrementAndGet();
        }
    }
    static class Metrics implements CacheMetricsListener {
        final Map<StreamResult, AtomicInteger> failures = new ConcurrentHashMap<>();
        final List<BooleanSupplier> pending = new CopyOnWriteArrayList<>();
        public void onStreamFailure(String cache, StreamResult result) { failures.computeIfAbsent(result, r -> new AtomicInteger()).incrementAndGet(); }
        public AutoCloseable registerRecovery(String cache, BooleanSupplier value) { pending.add(value); return () -> pending.remove(value); }
        int count(StreamResult result) { return failures.getOrDefault(result, new AtomicInteger()).get(); }
        boolean pending() { return pending.stream().anyMatch(BooleanSupplier::getAsBoolean); }
    }
    final class Harness implements AutoCloseable {
        final String cache = "recovery-" + UUID.randomUUID(); final UUID id = UUID.randomUUID();
        final byte[] stream = RedisKeyspace.journal(cache), group = RedisKeyspace.group(cache, id);
        final StatefulRedisConnection<byte[], byte[]> connection = client.connect(ByteArrayCodec.INSTANCE);
        final RedisStreamJournal journal = new RedisStreamJournal(connection, 1000, CODEC);
        final Target target = new Target(); final Metrics metrics = new Metrics();
        final AtomicLong sequence = new AtomicLong(); final AtomicReference<String> baseline = new AtomicReference<>();
        volatile boolean failBaseline; volatile Runnable afterBaseline = () -> { };
        final LettuceStreamsInvalidationTransport transport = new LettuceStreamsInvalidationTransport(client, CODEC, CODEC, id);
        final InvalidationService service;
        Harness() {
            InvalidationJournal observed = new InvalidationJournal() {
                public String append(String c, InvalidationMessage m) { return journal.append(c, m); }
                public List<JournalRow> readRange(String c, String p) { return journal.readRange(c, p); }
                public CheckedRange checkedRead(String c, String p, int n) { return journal.checkedRead(c, p, n); }
                public boolean isTrimmed(String c, String p) { return journal.isTrimmed(c, p); }
                public String endCursor(String c) {
                    String value = journal.endCursor(c); baseline.set(value);
                    if (failBaseline) throw new IllegalStateException("baseline unavailable");
                    afterBaseline.run(); return value;
                }
            };
            service = new InvalidationService(transport, observed, UUID.randomUUID(), InvalidationListener.NOOP, metrics);
            service.registerTarget(cache, target);
        }
        String put(String key, String value) {
            var version = new Version(sequence.incrementAndGet(), id);
            return journal.append(cache, new InvalidationMessage(cache, key, version, id, InvalidationMessage.Type.UPDATE, value));
        }
        List<byte[]> poisonBatch() {
            target.values.put("victim", "stale");
            return connection.sync().eval("local a=redis.call('xadd',KEYS[1],'*','t',string.char(99),'k',ARGV[1],'v',ARGV[3]); "
                    + "local b=redis.call('xadd',KEYS[1],'*','t',string.char(2),'k',ARGV[2],'v',ARGV[4],'p',ARGV[5]); return {a,b}",
                    ScriptOutputType.MULTI, new byte[][]{stream}, CODEC.toBytes("victim"), CODEC.toBytes("covered"),
                    bytes(new Version(sequence.incrementAndGet(), id).toWire()), bytes(new Version(sequence.incrementAndGet(), id).toWire()), CODEC.toBytes("obsolete"));
        }
        long pending() { return connection.sync().xpending(stream, group).getCount(); }
        public void close() { service.close(); connection.close(); }
    }

    @Test void baselineAndClearMustFinishBeforeAckAndLaterRowsStillApply() throws Exception {
        try (Harness h = new Harness()) {
            var captured = new CountDownLatch(1); var releaseBaseline = new CountDownLatch(1);
            var clearing = new CountDownLatch(1); var releaseClear = new CountDownLatch(1);
            h.afterBaseline = () -> { captured.countDown(); gate(releaseBaseline); };
            h.target.beforeClear = () -> { clearing.countDown(); gate(releaseClear); };
            int before = h.target.clears.get(); h.poisonBatch();
            try {
                assertTrue(captured.await(5, TimeUnit.SECONDS)); assertEquals(2, h.pending());
                assertEquals("stale", h.target.values.get("victim")); assertTrue(h.metrics.pending());
                String after = h.put("after-baseline", "new");
                assertTrue(RedisStreamJournal.compareIds(after, h.baseline.get()) > 0);
                releaseBaseline.countDown(); assertTrue(clearing.await(5, TimeUnit.SECONDS));
                assertEquals(2, h.pending(), "clear is not yet committed, so poison cannot be ACKed");
            } finally { releaseBaseline.countDown(); releaseClear.countDown(); }
            await(() -> h.pending() == 0 && "new".equals(h.target.values.get("after-baseline")));
            assertFalse(h.target.values.containsKey("victim")); assertFalse(h.target.values.containsKey("covered"));
            assertEquals(before + 1, h.target.clears.get());
            assertEquals(1, h.metrics.count(CacheMetricsListener.StreamResult.DECODE_FAILED));
        }
    }

    @Test void failedBaselineAndFailedClearDoNotAuthorizeAck() throws Exception {
        for (boolean baselineFailure : new boolean[]{true, false}) {
            try (Harness h = new Harness()) {
                int initialClears = h.target.clears.get();
                h.failBaseline = baselineFailure; h.target.failClear = !baselineFailure;
                h.poisonBatch();
                await(() -> h.metrics.count(CacheMetricsListener.StreamResult.RESYNC_FAILED) > 0);
                assertEquals(2, h.pending()); assertTrue(h.metrics.pending());
                assertEquals(initialClears + (baselineFailure ? 1 : 0), h.target.clears.get());
                if (baselineFailure) assertFalse(h.target.values.containsKey("victim"));
                h.failBaseline = false; h.target.failClear = false;
                await(() -> h.pending() == 0);
                assertFalse(h.target.values.containsKey("victim"));
                h.put("after", "ok"); await(() -> "ok".equals(h.target.values.get("after")));
            }
        }
    }

    @Test void corruptTailIsAnOpaqueCheckedReadAnchorAndReplayRecovers() throws Exception {
        String cache = "anchor-" + UUID.randomUUID(); var target = new Target();
        try (var connection = client.connect(ByteArrayCodec.INSTANCE)) {
            var journal = new RedisStreamJournal(connection, 1000, CODEC);
            var pubsub = new LettucePubSubInvalidationTransport(client, CODEC);
            try (var service = new InvalidationService(pubsub, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
                service.registerTarget(cache, target); target.values.put("victim", "stale");
                String bad = connection.sync().xadd(RedisKeyspace.journal(cache), Map.of(bytes("t"), new byte[]{99}, bytes("k"), CODEC.toBytes("victim"), bytes("v"), bytes("broken-secret")));
                var error = assertThrows(StreamRowCorruptionException.class, () -> journal.checkedRead(cache, "0-0", 10));
                assertEquals(cache, error.cache()); assertEquals(bad, error.rowId()); assertNull(error.getCause());
                assertFalse(error.toString().contains("broken-secret"));
                assertTrue(service.recoverAsync(Runnable::run).toCompletableFuture().get(5, TimeUnit.SECONDS));
                assertFalse(target.values.containsKey("victim"));
                CheckedRange atTail = journal.checkedRead(cache, bad, 1);
                assertTrue(atTail.startIntact()); assertTrue(atTail.rows().isEmpty());
                var v = new Version(1, UUID.randomUUID());
                String next = journal.append(cache, new InvalidationMessage(cache, "later", v, v.instanceId(), InvalidationMessage.Type.UPDATE, "typed"));
                CheckedRange after = journal.checkedRead(cache, bad, 1);
                assertTrue(after.startIntact()); assertEquals(1, after.rows().size());
                assertEquals(next, after.rows().get(0).cursor()); assertEquals("typed", after.rows().get(0).message().payload());
                assertTrue(service.recoverAsync(Runnable::run).toCompletableFuture().get(5, TimeUnit.SECONDS));
                assertEquals("typed", target.values.get("later")); assertEquals(1, target.clears.get());
                connection.sync().xdel(RedisKeyspace.journal(cache), bad);
                assertFalse(journal.checkedRead(cache, bad, 1).startIntact());
            }
        }
    }

    @Test void stableResumeClaimsOnlyItsOwnGroupAndSkipsCoveredHistoricalUpdates() throws Exception {
        for (String oldConsumer : List.of("main", "previous")) {
            String cache = "resume-" + UUID.randomUUID(); UUID owner = UUID.randomUUID(), spectator = UUID.randomUUID();
            byte[] stream = RedisKeyspace.journal(cache), own = RedisKeyspace.group(cache, owner), other = RedisKeyspace.group(cache, spectator);
            try (var connection = client.connect(ByteArrayCodec.INSTANCE)) {
                var commands = connection.sync(); var journal = new RedisStreamJournal(connection, 1000, CODEC);
                var v = new Version(1, UUID.randomUUID());
                journal.append(cache, new InvalidationMessage(cache, "historical", v, v.instanceId(), InvalidationMessage.Type.UPDATE, "stale"));
                commands.xgroupCreate(XReadArgs.StreamOffset.from(stream, "0-0"), own);
                commands.xgroupCreate(XReadArgs.StreamOffset.from(stream, "0-0"), other);
                commands.xreadgroup(io.lettuce.core.Consumer.from(own, bytes(oldConsumer)), XReadArgs.StreamOffset.lastConsumed(stream));
                commands.xreadgroup(io.lettuce.core.Consumer.from(other, bytes("spectator")), XReadArgs.StreamOffset.lastConsumed(stream));
                var transport = new LettuceStreamsInvalidationTransport(client, CODEC, CODEC, owner);
                var target = new Target();
                try (var service = new InvalidationService(transport, journal, UUID.randomUUID(), InvalidationListener.NOOP)) {
                    service.registerTarget(cache, target);
                    await(() -> commands.xpending(stream, own).getCount() == 0);
                    assertFalse(target.values.containsKey("historical"));
                    assertEquals(1, commands.xpending(stream, other).getCount(), "another receiver's PEL must be untouched");
                    journal.append(cache, new InvalidationMessage(cache, "new", new Version(2, v.instanceId()), v.instanceId(), InvalidationMessage.Type.UPDATE, "fresh"));
                    await(() -> "fresh".equals(target.values.get("new")));
                }
                assertNotNull(commands.xpending(stream, own), "stable group survives close");
                assertEquals(1, commands.xpending(stream, other).getCount());
            }
        }
    }

    @Test void missingPendingPayloadIsNotRemovedBeforeSafeReset() throws Exception {
        for (String oldConsumer : List.of("main", "previous")) {
            String cache = "missing-" + UUID.randomUUID(); UUID id = UUID.randomUUID();
            byte[] stream = RedisKeyspace.journal(cache), group = RedisKeyspace.group(cache, id);
            try (var connection = client.connect(ByteArrayCodec.INSTANCE)) {
                var commands = connection.sync(); var journal = new RedisStreamJournal(connection, 1000, CODEC);
                var v = new Version(1, UUID.randomUUID());
                String missing = journal.append(cache, new InvalidationMessage(cache, "victim", v, v.instanceId(), InvalidationMessage.Type.INVALIDATE));
                commands.xgroupCreate(XReadArgs.StreamOffset.from(stream, "0-0"), group);
                commands.xreadgroup(io.lettuce.core.Consumer.from(group, bytes(oldConsumer)), XReadArgs.StreamOffset.lastConsumed(stream));
                commands.xdel(stream, missing);
                String baseline = journal.append(cache, new InvalidationMessage(cache, "tail", v, v.instanceId(), InvalidationMessage.Type.INVALIDATE));
                var future = new CompletableFuture<RecoveryResult>(); var asked = new CountDownLatch(1);
                var initial = new RecoveryResult(RecoveryResult.Status.RESET_SAFE, "0-0", 1);
                var current = new AtomicReference<>(initial);
                try (var transport = new LettuceStreamsInvalidationTransport(client, CODEC, CODEC, id)) {
                    transport.setGapHandler(new InvalidationGapHandler() {
                        public CompletionStage<RecoveryResult> reset(String c) { asked.countDown(); return future; }
                        public RecoveryResult registrationBaseline(String c) { return initial; }
                        public boolean isCurrent(String c, RecoveryResult r) { return r == current.get(); }
                    });
                    transport.subscribe(cache, message -> { });
                    assertTrue(asked.await(5, TimeUnit.SECONDS));
                    assertEquals(1, commands.xpending(stream, group).getCount(), "claim must not silently delete the missing PEL entry");
                    var safe = new RecoveryResult(RecoveryResult.Status.RESET_SAFE, baseline, 2); current.set(safe); future.complete(safe);
                    await(() -> commands.xpending(stream, group).getCount() == 0);
                }
            }
        }
    }

    @Test void standalonePoisonRemainsPendingWhileAnotherCacheMakesProgress() throws Exception {
        String bad = "blocked-" + UUID.randomUUID(), good = "healthy-" + UUID.randomUUID(); UUID id = UUID.randomUUID();
        Metrics metrics = new Metrics(); var delivered = new CountDownLatch(1);
        try (var connection = client.connect(ByteArrayCodec.INSTANCE);
             var transport = new LettuceStreamsInvalidationTransport(client, CODEC, CODEC, id)) {
            transport.setMetricsListener(metrics); transport.subscribe(bad, message -> fail("poison dispatched"));
            transport.subscribe(good, message -> delivered.countDown());
            connection.sync().xadd(RedisKeyspace.journal(bad), Map.of(bytes("t"), new byte[]{99}));
            await(() -> metrics.count(CacheMetricsListener.StreamResult.RESYNC_FAILED) > 0);
            assertTrue(metrics.pending());
            var journal = new RedisStreamJournal(connection, 1000, CODEC); var v = new Version(1, UUID.randomUUID());
            journal.append(good, new InvalidationMessage(good, "k", v, v.instanceId(), InvalidationMessage.Type.INVALIDATE));
            assertTrue(delivered.await(1, TimeUnit.SECONDS));
            Thread.sleep(250);
            assertEquals(1, metrics.count(CacheMetricsListener.StreamResult.DECODE_FAILED), "known poison is not repeatedly deserialized");
            assertEquals(1, connection.sync().xpending(RedisKeyspace.journal(bad), RedisKeyspace.group(bad, id)).getCount());
        }
        assertTrue(metrics.pending.isEmpty(), "both gauge registrations must be removed");
    }

    @Test void lateAndSupersededResetResultsCannotAcknowledgeRows() throws Exception {
        for (boolean close : new boolean[]{true, false}) {
            String cache = "late-" + UUID.randomUUID(); UUID id = UUID.randomUUID();
            byte[] stream = RedisKeyspace.journal(cache), group = RedisKeyspace.group(cache, id);
            var first = new CompletableFuture<RecoveryResult>(); var second = new CompletableFuture<RecoveryResult>();
            var count = new AtomicInteger(); var initial = new RecoveryResult(RecoveryResult.Status.RESET_SAFE, "0-0", 1);
            var current = new AtomicReference<>(initial); Metrics metrics = new Metrics();
            try (var connection = client.connect(ByteArrayCodec.INSTANCE);
                 var transport = new LettuceStreamsInvalidationTransport(client, CODEC, CODEC, id)) {
                transport.setMetricsListener(metrics);
                transport.setGapHandler(new InvalidationGapHandler() {
                    public CompletionStage<RecoveryResult> reset(String c) { return count.incrementAndGet() == 1 ? first : second; }
                    public RecoveryResult registrationBaseline(String c) { return initial; }
                    public boolean isCurrent(String c, RecoveryResult r) { return r == current.get(); }
                });
                transport.subscribe(cache, message -> { });
                String row = connection.sync().xadd(stream, Map.of(bytes("t"), new byte[]{99}));
                await(() -> count.get() == 1);
                var old = new RecoveryResult(RecoveryResult.Status.RESET_SAFE, row, 2);
                if (close) { current.set(old); transport.close(); }
                first.complete(old);
                if (!close) await(() -> metrics.count(CacheMetricsListener.StreamResult.RESYNC_FAILED) > 0);
                assertEquals(1, connection.sync().xpending(stream, group).getCount());
                if (!close) {
                    await(() -> count.get() == 2);
                    var safe = new RecoveryResult(RecoveryResult.Status.RESET_SAFE, row, 3); current.set(safe); second.complete(safe);
                    await(() -> connection.sync().xpending(stream, group).getCount() == 0);
                }
            }
        }
    }

    @Test void ackFailureDoesNotReapplyOrLoseTheRestOfTheBatch() throws Exception {
        for (boolean replyLost : new boolean[]{false, true}) {
            String cache = "ack-" + UUID.randomUUID(); UUID id = UUID.randomUUID();
            byte[] stream = RedisKeyspace.journal(cache), group = RedisKeyspace.group(cache, id);
            try (var probe = client.connect(ByteArrayCodec.INSTANCE)) {
                var real = client.connect(ByteArrayCodec.INSTANCE); var once = new AtomicBoolean();
                var applied = new ConcurrentHashMap<Object, AtomicInteger>(); Metrics metrics = new Metrics();
                var wrapped = ackFailureConnection(real, once, replyLost);
                try (var transport = new LettuceStreamsInvalidationTransport(wrapped, CODEC, CODEC, id, true)) {
                    transport.setMetricsListener(metrics);
                    transport.subscribe(cache, message -> applied.computeIfAbsent(message.key(), k -> new AtomicInteger()).incrementAndGet());
                    var v = new Version(1, UUID.randomUUID());
                    probe.sync().eval("redis.call('xadd',KEYS[1],'*','t',string.char(0),'k',ARGV[1],'v',ARGV[3]); "
                                    + "redis.call('xadd',KEYS[1],'*','t',string.char(0),'k',ARGV[2],'v',ARGV[3]); return 1",
                            ScriptOutputType.INTEGER, new byte[][]{stream}, CODEC.toBytes("first"), CODEC.toBytes("second"), bytes(v.toWire()));
                    await(() -> applied.containsKey("second") && probe.sync().xpending(stream, group).getCount() == 0);
                    assertEquals(1, applied.get("first").get()); assertEquals(1, applied.get("second").get());
                    assertEquals(1, metrics.count(CacheMetricsListener.StreamResult.ACK_FAILED));
                }
            }
        }
    }

    @Test void throwingObserverDoesNotRetryCommittedApplicationAsPoison() throws Exception {
        try (Harness h = new Harness()) {
            h.service.setEventListener((cache, message) -> { throw new IllegalStateException("observer failed"); });
            int clears = h.target.clears.get(); h.put("k", "value");
            await(() -> "value".equals(h.target.values.get("k")) && h.pending() == 0);
            assertEquals(0, h.metrics.count(CacheMetricsListener.StreamResult.APPLY_FAILED));
            assertEquals(clears, h.target.clears.get());
        }
    }

    @Test void applicationFailureGetsThreeAttemptsThenSafeResync() throws Exception {
        String cache = "apply-" + UUID.randomUUID(); UUID id = UUID.randomUUID();
        byte[] stream = RedisKeyspace.journal(cache), group = RedisKeyspace.group(cache, id);
        var times = new CopyOnWriteArrayList<Long>(); var resets = new AtomicInteger();
        var current = new AtomicReference<>(new RecoveryResult(RecoveryResult.Status.RESET_SAFE, "0-0", 1));
        Metrics metrics = new Metrics();
        try (var connection = client.connect(ByteArrayCodec.INSTANCE);
             var transport = new LettuceStreamsInvalidationTransport(client, CODEC, CODEC, id)) {
            var journal = new RedisStreamJournal(connection, 1000, CODEC);
            transport.setMetricsListener(metrics);
            transport.setGapHandler(new InvalidationGapHandler() {
                public CompletionStage<RecoveryResult> reset(String c) {
                    resets.incrementAndGet(); var safe = new RecoveryResult(RecoveryResult.Status.RESET_SAFE, journal.endCursor(c), 2);
                    current.set(safe); return CompletableFuture.completedFuture(safe);
                }
                public RecoveryResult registrationBaseline(String c) { return current.get(); }
                public boolean isCurrent(String c, RecoveryResult r) { return r == current.get(); }
            });
            transport.subscribe(cache, message -> { times.add(System.nanoTime()); throw new IllegalStateException("target temporarily failed"); });
            var v = new Version(1, UUID.randomUUID()); journal.append(cache, new InvalidationMessage(cache, "k", v, v.instanceId(), InvalidationMessage.Type.INVALIDATE));
            await(() -> resets.get() == 1 && connection.sync().xpending(stream, group).getCount() == 0);
            assertEquals(3, times.size()); assertEquals(3, metrics.count(CacheMetricsListener.StreamResult.APPLY_FAILED));
            assertTrue(times.get(1) - times.get(0) >= TimeUnit.MILLISECONDS.toNanos(900));
            assertTrue(times.get(2) - times.get(1) >= TimeUnit.MILLISECONDS.toNanos(1900));
        }
    }

    @Test void ephemeralCloseRetiresOnlyItsOwnGroup() throws Exception {
        String cache = "ephemeral-" + UUID.randomUUID(); byte[] stream = RedisKeyspace.journal(cache);
        try (var connection = client.connect(ByteArrayCodec.INSTANCE)) {
            var transport = new LettuceStreamsInvalidationTransport(client, CODEC, CODEC);
            var field = LettuceStreamsInvalidationTransport.class.getDeclaredField("instanceId"); field.setAccessible(true);
            byte[] own = RedisKeyspace.group(cache, (UUID) field.get(transport));
            var subscription = transport.subscribe(cache, message -> { });
            byte[] other = RedisKeyspace.group(cache, UUID.randomUUID());
            connection.sync().xgroupCreate(XReadArgs.StreamOffset.from(stream, "0-0"), other);
            subscription.close(); subscription.close(); transport.close();
            assertThrows(RedisCommandExecutionException.class, () -> connection.sync().xpending(stream, own));
            assertNotNull(connection.sync().xpending(stream, other));
        }
    }

    @SuppressWarnings("unchecked")
    static StatefulRedisConnection<byte[], byte[]> ackFailureConnection(StatefulRedisConnection<byte[], byte[]> real,
            AtomicBoolean once, boolean replyLost) {
        Object async = java.lang.reflect.Proxy.newProxyInstance(StreamsRecoveryTest.class.getClassLoader(),
                new Class<?>[]{io.lettuce.core.api.async.RedisAsyncCommands.class}, (proxy, method, args) -> {
                    if (method.getName().equals("xack") && once.compareAndSet(false, true)) {
                        if (replyLost) ((RedisFuture<?>) method.invoke(real.async(), args)).get();
                        var failed = CompletableFuture.failedFuture(new IllegalStateException("ACK unavailable"));
                        return java.lang.reflect.Proxy.newProxyInstance(StreamsRecoveryTest.class.getClassLoader(), new Class<?>[]{RedisFuture.class},
                                (p, m, a) -> { try { return CompletableFuture.class.getMethod(m.getName(), m.getParameterTypes()).invoke(failed, a); }
                                catch (java.lang.reflect.InvocationTargetException e) { throw e.getCause(); } });
                    }
                    try { return method.invoke(real.async(), args); } catch (java.lang.reflect.InvocationTargetException e) { throw e.getCause(); }
                });
        return (StatefulRedisConnection<byte[], byte[]>) java.lang.reflect.Proxy.newProxyInstance(StreamsRecoveryTest.class.getClassLoader(),
                new Class<?>[]{StatefulRedisConnection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("async")) return async;
                    try { return method.invoke(real, args); } catch (java.lang.reflect.InvocationTargetException e) { throw e.getCause(); }
                });
    }
}

class RedisStreamsRecoveryTest extends StreamsRecoveryTest { String image() { return "redis:6.2-alpine"; } }
class ValkeyStreamsRecoveryTest extends StreamsRecoveryTest { String image() { return "valkey/valkey:8.0-alpine"; } }
