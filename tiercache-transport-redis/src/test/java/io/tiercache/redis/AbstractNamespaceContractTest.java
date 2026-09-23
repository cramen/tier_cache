package io.tiercache.redis;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.XReadArgs;
import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.TierCacheFactory;
import io.tiercache.invalidation.MessageCodec;
import io.tiercache.spi.StoredEntry;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Namespace and cold-cutover contract, inherited by both supported server suites. */
abstract class AbstractNamespaceContractTest extends AbstractLettuceContractTest {
    private static final Duration TTL = Duration.ofMinutes(2);
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    private static final JdkCacheSerializer<String> STRINGS = new JdkCacheSerializer<>();

    @Test void allNamesClearOnlyTheirOwnDataAndPreserveControlFamilies() {
        String[] names = {"", "user", "user:roles", "a?", "a1", "a*", "ab", "a[12]", "a2", "a\\b",
                "tiercache", "tiercache:v2", "spring:users", "micronaut:users", "é", "e\u0301", "用户", "😀"};
        List<LettuceRemoteCache<String, String>> caches = new ArrayList<>();
        try (var connection = sharedClient().connect(ByteArrayCodec.INSTANCE)) {
            byte[][] control = {RedisKeyspace.journal("users"), RedisKeyspace.trims("users"),
                    RedisKeyspace.tagKey("users", "group"), RedisKeyspace.reverseKey("users", bytes("k")),
                    bytes(RedisKeyspace.lock("users:k")), bytes("tiercache:journal:users"),
                    bytes("tiercache:tags:users:group"), bytes("tiercache:rebuild:users:k"),
                    bytes("user:legacy"), RedisKeyspace.channel("users"), RedisKeyspace.group("users", UUID.randomUUID())};
            for (byte[] key : control) connection.sync().set(key, bytes("sentinel"));
            for (String name : names) {
                var cache = LettuceRemoteCache.<String, String>builder(redisUri()).client(sharedClient()).cacheName(name).build();
                caches.add(cache); cache.put("k", StoredEntry.ofValue(name), TTL);
            }
            for (int i = 0; i < caches.size(); i++) {
                caches.get(i).clear(); assertNull(caches.get(i).get("k"));
                for (int j = 0; j < caches.size(); j++) if (i != j) assertEquals(names[j], caches.get(j).get("k").value());
                for (byte[] key : control) assertArrayEquals(bytes("sentinel"), connection.sync().get(key));
                caches.get(i).put("k", StoredEntry.ofValue(names[i]), TTL);
            }
            for (var cache : caches) cache.clear();
            connection.sync().del(control);
        } finally { caches.forEach(LettuceRemoteCache::close); }
    }

    @Test void binaryApplicationKeysAndDelimiterLookingTagsRemainOpaque() {
        String name = "spring:user:?" + UUID.randomUUID();
        byte[] key = {0, (byte) 255, ':', '*', '[', ']', '\\', 0, ':'};
        CacheSerializer<byte[]> identity = new CacheSerializer<>() {
            public byte[] toBytes(byte[] value) { return value; }
            public byte[] fromBytes(byte[] value) { return value; }
        };
        try (var connection = sharedClient().connect(ByteArrayCodec.INSTANCE);
             var cache = LettuceRemoteCache.<byte[], String>builder(redisUri()).client(sharedClient())
                     .cacheName(name).keySerializer(identity).build()) {
            cache.putTagged(key, StoredEntry.ofValue("v"), TTL, new String[]{"A:B", "A?", "用户"});
            byte[] physical = RedisKeyspace.dataKey(name, key);
            assertNotNull(connection.sync().get(physical));
            assertTrue(connection.sync().exists(RedisKeyspace.reverseKey(name, key)) == 1);
            assertArrayEquals(key, cache.keysByTag("A:B").get(0));
            assertEquals("v", cache.get(key).value());
            cache.putTagged(key, StoredEntry.ofValue("new"), TTL, new String[]{"B:A"});
            assertTrue(cache.keysByTag("A:B").isEmpty());
            assertArrayEquals(physical, connection.sync().smembers(RedisKeyspace.tagKey(name, "B:A")).iterator().next());
            cache.evict(key);
            assertNull(connection.sync().get(physical));
            assertEquals(0, connection.sync().exists(RedisKeyspace.reverseKey(name, key)));
            assertTrue(cache.keysByTag("B:A").isEmpty());
        }
    }

    @Test void malformedNamesAndTagsFailBeforeMutation() {
        String name = "malformed-" + UUID.randomUUID();
        String bad = "x\ud800";
        assertThrows(IllegalArgumentException.class, () -> LettuceRemoteCache.builder(redisUri())
                .client(sharedClient()).cacheName(bad).build());
        assertThrows(IllegalArgumentException.class, () -> LettuceRemoteCache.builder(redisUri())
                .client(sharedClient()).cacheName(name).journalName(bad).build());
        try (var connection = sharedClient().connect(ByteArrayCodec.INSTANCE);
             var cache = LettuceRemoteCache.<String, String>builder(redisUri()).client(sharedClient()).cacheName(name).build();
             var pubsub = new LettucePubSubInvalidationTransport(sharedClient(), new JdkCacheSerializer<>());
             var locks = new LettuceLockProvider(sharedClient())) {
            cache.putTagged("k", StoredEntry.ofValue("old"), TTL, new String[]{"A"});
            assertThrows(IllegalArgumentException.class, () -> cache.putTagged("k", StoredEntry.ofValue("bad"), TTL, new String[]{"B", bad}));
            assertEquals("old", cache.get("k").value()); assertEquals(List.of("k"), cache.keysByTag("A"));
            assertTrue(cache.keysByTag("B").isEmpty());
            assertThrows(IllegalArgumentException.class, () -> pubsub.subscribe(bad, m -> fail("unexpected event")));
            assertThrows(IllegalArgumentException.class, () -> locks.tryLock(bad, TTL));
            var journal = new RedisStreamJournal(connection, 1000, new JdkCacheSerializer<>());
            var v = new Version(1, UUID.randomUUID());
            assertThrows(IllegalArgumentException.class, () -> journal.append(bad,
                    new InvalidationMessage(bad, "k", v, v.instanceId(), InvalidationMessage.Type.INVALIDATE)));
        }
    }

    @Test void legacyDataIsNeitherReadNorDeletedDuringColdCutover() {
        String name = "cutover-" + UUID.randomUUID();
        byte[] v2 = RedisKeyspace.dataKey(name, STRINGS.toBytes("k"));
        byte[] legacy = RedisKeyspace.join(bytes(name + ":"), STRINGS.toBytes("k"));
        try (var connection = sharedClient().connect(ByteArrayCodec.INSTANCE);
             var remote = LettuceRemoteCache.<Object, Object>builder(redisUri()).client(sharedClient()).cacheName(name).build()) {
            remote.put("k", StoredEntry.ofValue("retired"), TTL);
            byte[] frame = connection.sync().get(v2);
            connection.sync().set(legacy, frame); connection.sync().del(v2);
            AtomicInteger loads = new AtomicInteger();
            try (var factory = TierCacheFactory.builder().remoteCache(remote).build()) {
                var cache = factory.<String, String>getCache(name);
                assertEquals("source", cache.getOrCompute("k", k -> { loads.incrementAndGet(); return "source"; }));
                assertEquals(1, loads.get());
                cache.evictAll();
                assertArrayEquals(frame, connection.sync().get(legacy));
                assertNull(connection.sync().get(v2));
            }
            connection.sync().del(legacy);
        }
    }

    @Test void pubsubUsesV2ChannelAndIgnoresLegacyEvents() throws Exception {
        String name = "channel:?" + UUID.randomUUID();
        var id = UUID.randomUUID();
        var old = new InvalidationMessage(name, "old", new Version(1, id), id, InvalidationMessage.Type.INVALIDATE);
        var current = new InvalidationMessage(name, "new", new Version(2, id), id, InvalidationMessage.Type.INVALIDATE);
        var received = new CopyOnWriteArrayList<InvalidationMessage>();
        var delivered = new CountDownLatch(1);
        try (var connection = sharedClient().connect(ByteArrayCodec.INSTANCE);
             var transport = new LettucePubSubInvalidationTransport(sharedClient(), new JdkCacheSerializer<>());
             var subscription = transport.subscribe(name, message -> { received.add(message); delivered.countDown(); })) {
            assertEquals(0, connection.sync().publish(bytes("tiercache:inv:" + name), MessageCodec.encode(old, STRINGS.toBytes("old"))));
            transport.publish(current);
            assertTrue(delivered.await(5, TimeUnit.SECONDS));
            assertEquals(List.of(current), received);
            assertEquals(1, connection.sync().pubsubNumsub(RedisKeyspace.channel(name)).values().iterator().next());
        }
    }

    @Test void streamsStartInV2AndLeaveLegacyJournalAndGroupsUntouched() throws Exception {
        String name = "stream:?" + UUID.randomUUID(); UUID id = UUID.randomUUID();
        byte[] legacy = bytes("tiercache:journal:" + name);
        byte[] legacyGroup = bytes("tiercache:cg:" + name + ":" + id);
        var delivered = new CountDownLatch(1); var received = new CopyOnWriteArrayList<InvalidationMessage>();
        try (var connection = sharedClient().connect(ByteArrayCodec.INSTANCE)) {
            var cmd = connection.sync();
            cmd.xadd(legacy, Map.of(bytes("retired"), bytes("event")));
            cmd.xgroupCreate(XReadArgs.StreamOffset.from(legacy, "0-0"), legacyGroup);
            byte[] before = cmd.dump(legacy);
            var journal = new RedisStreamJournal(connection, 1000, new JdkCacheSerializer<>());
            try (var transport = new LettuceStreamsInvalidationTransport(sharedClient(), new JdkCacheSerializer<>(), new JdkCacheSerializer<>(), id);
                 var subscription = transport.subscribe(name, message -> { received.add(message); delivered.countDown(); })) {
                var v = new Version(1, UUID.randomUUID());
                var event = new InvalidationMessage(name, "new", v, v.instanceId(), InvalidationMessage.Type.INVALIDATE);
                journal.append(name, event);
                assertTrue(delivered.await(5, TimeUnit.SECONDS)); assertEquals(List.of(event), received);
                // An exact group operation proves subscription used this v2 identity.
                assertNotNull(cmd.xpending(RedisKeyspace.journal(name), RedisKeyspace.group(name, id)));
                assertArrayEquals(before, cmd.dump(legacy));
                assertEquals(1, journal.size(name));
            }
            cmd.del(legacy, RedisKeyspace.journal(name));
        }
    }

    @Test void lockAcquireExtendAndReleaseUseV2WithoutTouchingLegacyLock() {
        String name = "lock:?" + UUID.randomUUID(); String legacy = "tiercache:rebuild:" + name;
        try (var connection = sharedClient().connect(); var provider = new LettuceLockProvider(connection)) {
            connection.sync().set(legacy, "legacy-owner");
            var lock = provider.tryLock(name, Duration.ofSeconds(5)); assertNotNull(lock);
            String key = RedisKeyspace.lock(name);
            assertNotNull(connection.sync().get(key));
            assertNull(provider.tryLock(name, Duration.ofSeconds(5)));
            assertTrue(lock.extend(TTL)); assertTrue(connection.sync().pttl(key) > 60000);
            lock.release(); assertNull(connection.sync().get(key));
            assertEquals("legacy-owner", connection.sync().get(legacy));
            connection.sync().del(legacy);
        }
    }
    @Test
    @SuppressWarnings("unchecked")
    void uncertainLockAcquireCompensatesOnlyTheV2Token() throws Exception {
        String name = "compensate:?" + UUID.randomUUID();
        String legacy = "tiercache:rebuild:" + name;
        try (var connection = sharedClient().connect()) {
            connection.sync().set(legacy, "old-owner");
            var captured = new java.util.concurrent.atomic.AtomicReference<String>();
            var commands = (io.lettuce.core.api.sync.RedisCommands<String, String>) java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{io.lettuce.core.api.sync.RedisCommands.class},
                    (proxy, method, args) -> {
                        Object result;
                        try { result = method.invoke(connection.sync(), args); }
                        catch (java.lang.reflect.InvocationTargetException e) { throw e.getCause(); }
                        if ("set".equals(method.getName()) && args.length == 3 && args[2] instanceof io.lettuce.core.SetArgs) {
                            captured.set((String) args[0]);
                            throw new io.lettuce.core.RedisCommandTimeoutException("accepted SET, lost reply");
                        }
                        return result;
                    });
            var wrapped = (io.lettuce.core.api.StatefulRedisConnection<String, String>) java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{io.lettuce.core.api.StatefulRedisConnection.class},
                    (proxy, method, args) -> {
                        if ("sync".equals(method.getName())) return commands;
                        throw new UnsupportedOperationException(method.getName());
                    });
            try (var provider = new LettuceLockProvider(wrapped)) {
                assertThrows(io.lettuce.core.RedisCommandTimeoutException.class, () -> provider.tryLock(name, TTL));
                assertEquals(RedisKeyspace.lock(name), captured.get());
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (connection.sync().get(captured.get()) != null && System.nanoTime() < deadline) Thread.sleep(10);
                assertNull(connection.sync().get(captured.get()));
                assertEquals("old-owner", connection.sync().get(legacy));
            }
            connection.sync().del(legacy);
        }
    }

}
