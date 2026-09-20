package io.tiercache.redis;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.spi.InvalidationJournal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bounded invalidation journal on Redis Streams: one stream per cache
 * ({@code tiercache:journal:<cache>}), capacity-capped by approximate
 * MAXLEN trimming. Writers append inside the same MULTI as the data write
 * (see {@link LettuceRemoteCache}), so there is no "wrote but didn't
 * journal" window.
 *
 * <p>Cursors are stream entry IDs ({@code millis-seq}); replay reads
 * {@code XRANGE (cursor, +]}.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @since 0.1.0
 */
public final class RedisStreamJournal implements InvalidationJournal {

    /**
     * Stream keyspace prefix.
     *
     * @since 0.1.0
     */
    public static final String JOURNAL_KEYSPACE = "tiercache:journal:";

    private static final byte[] FIELD_TYPE = "t".getBytes();
    private static final byte[] FIELD_KEY = "k".getBytes();
    private static final byte[] FIELD_VERSION = "v".getBytes();
    private static final byte[] FIELD_PAYLOAD = "p".getBytes();

    private final RedisCommands<byte[], byte[]> commands;
    private final int capacity;
    private final CacheSerializer<Object> keySerializer;
    private final CacheSerializer<Object> valueSerializer;

    /**
     * Creates a journal over an existing byte-codec connection. The caller
     * keeps ownership of the connection. The key serializer doubles as the
     * value serializer (the common case: both are JDK serialization).
     *
     * @param connection    the connection to issue stream commands on
     * @param capacity      maximum entries kept per cache stream (approximate
     *                      MAXLEN trimming)
     * @param keySerializer serializer for message keys
     * @since 0.1.0
     */
    public RedisStreamJournal(StatefulRedisConnection<byte[], byte[]> connection, int capacity,
            CacheSerializer<Object> keySerializer) {
        this(connection, capacity, keySerializer, keySerializer);
    }

    /**
     * Creates a journal over an existing byte-codec connection with separate
     * serializers for keys and UPDATE payloads. The value serializer MUST
     * match the one the L2 uses to write payloads, or replayed UPDATEs will
     * not deserialize. The caller keeps ownership of the connection.
     *
     * @param connection      the connection to issue stream commands on
     * @param capacity        maximum entries kept per cache stream
     *                        (approximate MAXLEN trimming)
     * @param keySerializer   serializer for message keys
     * @param valueSerializer serializer for UPDATE payloads
     * @since 1.2.1
     */
    public RedisStreamJournal(StatefulRedisConnection<byte[], byte[]> connection, int capacity,
            CacheSerializer<Object> keySerializer, CacheSerializer<Object> valueSerializer) {
        this.commands = connection.sync();
        this.capacity = capacity;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    static byte[] streamKey(String cache) {
        return (JOURNAL_KEYSPACE + cache).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Stream key bytes for a cache (public for the transport's Lua script).
     *
     * @param cache the cache name
     * @return the stream key bytes ({@code tiercache:journal:<cache>})
     * @since 0.1.0
     */
    public static byte[] streamKeyBytes(String cache) {
        return streamKey(cache);
    }

    /**
     * Configured journal capacity (entries per cache stream).
     *
     * @return the capacity passed to the constructor
     * @since 0.1.0
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Appends the journal row inside an open MULTI transaction (called by the
     * transport between {@code multi()} and {@code exec()}).
     */
    void appendQueued(RedisCommands<byte[], byte[]> tx, String cache, byte[] keyBytes,
            Version version, InvalidationMessage.Type type) {
        appendQueued(tx, cache, keyBytes, version, type, null);
    }

    void appendQueued(RedisCommands<byte[], byte[]> tx, String cache, byte[] keyBytes,
            Version version, InvalidationMessage.Type type, byte[] payload) {
        tx.xadd(streamKey(cache), XAddArgs.Builder.maxlen(capacity).approximateTrimming(),
                fields(keyBytes, version, type, payload));
    }

    @Override
    public String append(String cache, InvalidationMessage message) {
        byte[] keyBytes = message.key() != null ? keySerializer.toBytes(message.key()) : null;
        return commands.xadd(streamKey(cache),
                XAddArgs.Builder.maxlen(capacity).approximateTrimming(),
                fields(keyBytes, message.version(), message.type(), message.payload() != null ? keySerializer.toBytes(message.payload()) : null));
    }

    @Override
    public List<InvalidationMessage> readRange(String cache, String cursorExclusive) {
        List<StreamMessage<byte[], byte[]>> entries = commands.xrange(streamKey(cache),
                Range.from(Range.Boundary.excluding(cursorExclusive), Range.Boundary.unbounded()));
        List<InvalidationMessage> out = new ArrayList<>(entries.size());
        for (StreamMessage<byte[], byte[]> entry : entries) {
            out.add(toMessage(cache, entry.getBody()));
        }
        return out;
    }

    /**
     * Last applied journal cursor per cache for replay bookkeeping: we return
     * the id of the newest entry, so readRange is exclusive-consistent.
     *
     * @param cache the cache name
     * @return the id of the newest journal entry, or {@code "0-0"} when the
     *         stream is empty
     * @since 0.1.0
     */
    public String lastCursor(String cache) {
        return endCursor(cache);
    }

    @Override
    public String endCursor(String cache) {
        List<StreamMessage<byte[], byte[]>> last = commands.xrevrange(streamKey(cache),
                Range.unbounded(), Limit.from(1));
        return last.isEmpty() ? "0-0" : last.get(0).getId();
    }

    @Override
    public long size(String cache) {
        Long size = commands.xlen(streamKey(cache));
        return size != null ? size : 0;
    }

    @Override
    public boolean isTrimmed(String cache, String cursor) {
        if ("0-0".equals(cursor)) {
            // From-the-beginning cursor (recorded only against an empty
            // stream): a loss happened only if rows were trimmed since.
            // Redis offers no exact trim counter here, so the guard is
            // "the stream outgrew the window": a small journal after a few
            // writes (the reviewer's case) is NOT a loss, while a stream
            // that had to be trimmed past capacity is. Documented heuristic
            // — the failure mode in the narrow slack edge is one bounded
            // flush, and replay itself is always idempotent.
            Long length = commands.xlen(streamKey(cache));
            return length != null && length > capacity;
        }
        List<StreamMessage<byte[], byte[]>> first = commands.xrange(streamKey(cache),
                Range.unbounded(), Limit.from(1));
        if (first.isEmpty()) {
            return false;
        }
        return compareIds(cursor, first.get(0).getId()) < 0;
    }

    private Map<byte[], byte[]> fields(byte[] keyBytes, Version version,
            InvalidationMessage.Type type, byte[] payload) {
        Map<byte[], byte[]> fields = new LinkedHashMap<>();
        fields.put(FIELD_TYPE, new byte[]{(byte) type.ordinal()});
        fields.put(FIELD_KEY, keyBytes != null ? keyBytes : new byte[0]);
        fields.put(FIELD_VERSION, version.toWire().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        fields.put(FIELD_PAYLOAD, payload != null ? payload : new byte[0]);
        return fields;
    }

    private InvalidationMessage toMessage(String cache, Map<byte[], byte[]> body) {
        byte[] typeOrd = get(body, FIELD_TYPE);
        byte[] keyBytes = get(body, FIELD_KEY);
        Version version = Version.fromWire(new String(get(body, FIELD_VERSION),
                java.nio.charset.StandardCharsets.UTF_8));
        Object key = keyBytes.length > 0 ? keySerializer.fromBytes(keyBytes) : null;
        byte[] payloadBytes = body.entrySet().stream()
                .filter(e -> java.util.Arrays.equals(e.getKey(), FIELD_PAYLOAD))
                .map(Map.Entry::getValue).findFirst().orElse(new byte[0]);
        // Replay must apply the same typed value as the live path (which
        // deserializes in the transport): never hand raw bytes to L1.
        Object payload = payloadBytes.length > 0 ? valueSerializer.fromBytes(payloadBytes) : null;
        InvalidationMessage.Type type = InvalidationMessage.Type.values()[typeOrd[0]];
        if (payload != null && type == InvalidationMessage.Type.INVALIDATE) {
            type = InvalidationMessage.Type.UPDATE; // payload implies update semantics
        }
        return new InvalidationMessage(cache, key, version, version.instanceId(), type, payload);
    }

    private static byte[] get(Map<byte[], byte[]> body, byte[] field) {
        for (Map.Entry<byte[], byte[]> e : body.entrySet()) {
            if (java.util.Arrays.equals(e.getKey(), field)) {
                return e.getValue();
            }
        }
        throw new IllegalStateException("journal entry missing field");
    }

    static int compareIds(String a, String b) {
        long[] pa = parse(a);
        long[] pb = parse(b);
        int byMillis = Long.compare(pa[0], pb[0]);
        return byMillis != 0 ? byMillis : Long.compare(pa[1], pb[1]);
    }

    private static long[] parse(String id) {
        int dash = id.indexOf('-');
        if (dash < 0) {
            return new long[]{Long.parseLong(id), 0};
        }
        return new long[]{Long.parseLong(id.substring(0, dash)), Long.parseLong(id.substring(dash + 1))};
    }
}
