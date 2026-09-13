package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.spi.InvalidationTransport;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Durable invalidation transport profile: receivers consume the per-cache
 * journal stream through a consumer group PER INSTANCE (groups distribute,
 * they do not fan out — one group per instance is the broadcast shape).
 * The group cursor survives disconnects, so arbitrarily long partitions
 * (within stream retention) heal without a full L1 flush.
 *
 * <p><b>Incubating:</b> 0.x API, may change before 1.0.
 */
public final class LettuceStreamsInvalidationTransport implements InvalidationTransport {

    /** Consumer-group keyspace: one group per instance per cache. */
    public static final String GROUP_PREFIX = "tiercache:cg:";

    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final io.lettuce.core.api.sync.RedisCommands<byte[], byte[]> commands;
    private final CacheSerializer<Object> keySerializer;
    private final CacheSerializer<Object> valueSerializer;
    private final UUID instanceId;
    private final Map<String, java.util.function.Consumer<InvalidationMessage>> handlers = new ConcurrentHashMap<>();
    private final Map<String, Thread> readers = new ConcurrentHashMap<>();
    private volatile boolean closed;
    volatile Throwable lastReaderError; // test diagnostics

    public LettuceStreamsInvalidationTransport(RedisClient client,
            CacheSerializer<Object> keySerializer, CacheSerializer<Object> valueSerializer) {
        this(client, keySerializer, valueSerializer, UUID.randomUUID());
    }

    /**
     * Stable instance identity: the same id reconnects to the same consumer
     * group (durable cursor). Random per default (new instance).
     */
    public LettuceStreamsInvalidationTransport(RedisClient client,
            CacheSerializer<Object> keySerializer, CacheSerializer<Object> valueSerializer,
            UUID instanceId) {
        this.instanceId = instanceId;
        this.connection = client.connect(ByteArrayCodec.INSTANCE);
        this.commands = connection.sync();
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void publish(InvalidationMessage message) {
        // The journal row IS the durable event (written atomically with the
        // data write by the L2 transport). Streams profile needs no channel.
    }

    @Override
    public AutoCloseable subscribe(String cache, java.util.function.Consumer<InvalidationMessage> handler) {
        // Create the consumer group eagerly: its cursor starts at creation
        // time, so entries published immediately after subscription are
        // delivered. Lazy creation in the read loop left a permanent gap for
        // anything published between subscribe() and the loop's first pass.
        ensureGroup(RedisStreamJournal.streamKeyBytes(cache), group(cache), cache);
        handlers.put(cache, handler);
        readers.computeIfAbsent(cache, this::startReader);
        return () -> {
            handlers.remove(cache);
            Thread reader = readers.remove(cache);
            if (reader != null) {
                reader.interrupt();
            }
        };
    }

    private Thread startReader(String cache) {
        Thread thread = new Thread(() -> readLoop(cache), "tiercache-streams-" + cache);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private byte[] group(String cache) {
        return (GROUP_PREFIX + cache + ":" + instanceId).getBytes(StandardCharsets.UTF_8);
    }

    private void readLoop(String cache) {
        try {
            readLoopInner(cache);
        } catch (Throwable t) {
            lastReaderError = t; // surfaced for diagnostics (JMX/logs)
        }
    }

    private void readLoopInner(String cache) {
        
        byte[] stream = RedisStreamJournal.streamKeyBytes(cache);
        byte[] group = group(cache);
        byte[] consumerName = "main".getBytes(StandardCharsets.UTF_8);
        // Short-poll instead of BLOCK: a blocked sync call can outlive the
        // connection's command timeout and never return on some stacks.
        XReadArgs args = XReadArgs.Builder.count(50);
        boolean groupReady = false;
        while (!closed && handlers.containsKey(cache)) {
            try {
                if (!groupReady) {
                    ensureGroup(stream, group, cache);
                    claimDeadPending(stream, cache, group, consumerName);
                    groupReady = true;
                }
                List<StreamMessage<byte[], byte[]>> messages = commands.xreadgroup(
                        io.lettuce.core.Consumer.<byte[]>from(group, consumerName), args,
                        XReadArgs.StreamOffset.lastConsumed(stream));

                if (messages == null || messages.isEmpty()) {
                    sleepQuietly(50);
                    continue;
                }
                for (StreamMessage<byte[], byte[]> message : messages) {
                    apply(cache, message);
                    commands.xack(stream, group, message.getId());
                }
            } catch (Throwable e) {
                lastReaderError = e;
                if (closed) {
                    return;
                }
                sleepQuietly(200); // transient failure: retry
            }
        }
    }

    private void apply(String cache, StreamMessage<byte[], byte[]> entry) {
        java.util.function.Consumer<InvalidationMessage> handler = handlers.get(cache);
        if (handler == null) {
            return;
        }
        Map<byte[], byte[]> body = entry.getBody();
        byte[] type = field(body, "t");
        byte[] key = field(body, "k");
        byte[] version = field(body, "v");
        byte[] payload = field(body, "p");
        Version v = Version.fromWire(new String(version, StandardCharsets.UTF_8));
        Object k = key.length > 0 ? keySerializer.fromBytes(key) : null;
        Object p = payload != null && payload.length > 0 ? valueSerializer.fromBytes(payload) : null;
        InvalidationMessage.Type t = InvalidationMessage.Type.values()[type[0]];
        if (p != null && t == InvalidationMessage.Type.INVALIDATE) {
            t = InvalidationMessage.Type.UPDATE;
        }
        handler.accept(new InvalidationMessage(cache, k, v, v.instanceId(), t, p));
    }

    private static byte[] field(Map<byte[], byte[]> body, String name) {
        byte[] wanted = name.getBytes(StandardCharsets.UTF_8);
        for (Map.Entry<byte[], byte[]> e : body.entrySet()) {
            if (java.util.Arrays.equals(e.getKey(), wanted)) {
                return e.getValue();
            }
        }
        return new byte[0];
    }

    private void ensureGroup(byte[] stream, byte[] group, String cache) {
        try {
            commands.xgroupCreate(XReadArgs.StreamOffset.latest(stream), group,
                    XGroupCreateArgs.Builder.mkstream());
        } catch (RuntimeException e) {
            if (!String.valueOf(e.getMessage()).contains("BUSYGROUP")) {
                throw e; // real failure: the read loop retries
            }
            // BUSYGROUP: group exists — fine.
        }
        // Consumers are registered implicitly on first XREADGROUP; no
        // explicit CREATECONSUMER needed (it fails on a missing key).
    }

    /**
     * Claims pending entries from dead groups (zero registered consumers) of
     * this cache, applies them, then destroys those groups. Conservative:
     * live groups always have a registered consumer, so they are untouched.
     */
    private void claimDeadPending(byte[] stream, String cache, byte[] group, byte[] consumerName) {
        try {
            List<Object> groups = commands.xinfoGroups(stream);
            for (Object g : groups) {
                List<Object> row = (List<Object>) g;
                Map<String, Object> info = kvMap(row);
                byte[] name = info.get("name") instanceof byte[]
                        ? (byte[]) info.get("name") : str(info.get("name")).getBytes(StandardCharsets.UTF_8);
                long consumers = num(info.get("consumers"));
                long pending = num(info.get("pending"));
                if (name == null || java.util.Arrays.equals(name, group) || consumers > 0 || pending == 0) {
                    continue;
                }
                // Dead group with unprocessed entries: claim and apply.
                var claimed = commands.xautoclaim(stream, new XAutoClaimArgs<byte[]>()
                        .minIdleTime(1)
                        .startId("0-0")
                        .count(1000)
                        .consumer(io.lettuce.core.Consumer.<byte[]>from(group, consumerName)));
                for (StreamMessage<byte[], byte[]> m : claimed.getMessages()) {
                    apply(cache, m);
                    commands.xack(stream, group, m.getId());
                }
                commands.xgroupDestroy(stream, name);
            }
        } catch (RuntimeException e) {
            // Janitor is best-effort; the stream trim bounds residue.
        }
    }

    private static Map<String, Object> kvMap(List<Object> row) {
        Map<String, Object> map = new java.util.HashMap<>();
        for (int i = 0; i + 1 < row.size(); i += 2) {
            Object k = row.get(i);
            map.put(k instanceof byte[] ? new String((byte[]) k, StandardCharsets.UTF_8) : String.valueOf(k),
                    row.get(i + 1));
        }
        return map;
    }

    private static String str(Object o) {
        return o instanceof byte[] ? new String((byte[]) o, StandardCharsets.UTF_8)
                : o != null ? String.valueOf(o) : null;
    }

    private static long num(Object o) {
        return o instanceof Number ? ((Number) o).longValue()
                : o != null ? Long.parseLong(str(o)) : 0;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        closed = true;
        readers.values().forEach(Thread::interrupt);
        readers.clear();
        handlers.clear();
        connection.close();
    }
}
