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
 * <p><b>Incubating:</b> 0.x API, may change before 1.0.
 */
public final class RedisStreamJournal implements InvalidationJournal {

    /** Stream keyspace prefix. */
    public static final String JOURNAL_KEYSPACE = "tiercache:journal:";

    private static final byte[] FIELD_TYPE = "t".getBytes();
    private static final byte[] FIELD_KEY = "k".getBytes();
    private static final byte[] FIELD_VERSION = "v".getBytes();

    private final RedisCommands<byte[], byte[]> commands;
    private final int capacity;
    private final CacheSerializer<Object> keySerializer;

    public RedisStreamJournal(StatefulRedisConnection<byte[], byte[]> connection, int capacity,
            CacheSerializer<Object> keySerializer) {
        this.commands = connection.sync();
        this.capacity = capacity;
        this.keySerializer = keySerializer;
    }

    static byte[] streamKey(String cache) {
        return (JOURNAL_KEYSPACE + cache).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** Stream key bytes for a cache (public for the transport's Lua script). */
    public static byte[] streamKeyBytes(String cache) {
        return streamKey(cache);
    }

    /** Configured journal capacity (entries per cache stream). */
    public int capacity() {
        return capacity;
    }

    /**
     * Appends the journal row inside an open MULTI transaction (called by the
     * transport between {@code multi()} and {@code exec()}).
     */
    void appendQueued(RedisCommands<byte[], byte[]> tx, String cache, byte[] keyBytes,
            Version version, InvalidationMessage.Type type) {
        tx.xadd(streamKey(cache), XAddArgs.Builder.maxlen(capacity).approximateTrimming(),
                fields(keyBytes, version, type));
    }

    @Override
    public String append(String cache, InvalidationMessage message) {
        byte[] keyBytes = message.key() != null ? keySerializer.toBytes(message.key()) : null;
        return commands.xadd(streamKey(cache),
                XAddArgs.Builder.maxlen(capacity).approximateTrimming(),
                fields(keyBytes, message.version(), message.type()));
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
    public boolean isTrimmed(String cache, String cursor) {
        List<StreamMessage<byte[], byte[]>> first = commands.xrange(streamKey(cache),
                Range.unbounded(), Limit.from(1));
        if (first.isEmpty()) {
            return false;
        }
        return compareIds(cursor, first.get(0).getId()) < 0;
    }

    private Map<byte[], byte[]> fields(byte[] keyBytes, Version version,
            InvalidationMessage.Type type) {
        Map<byte[], byte[]> fields = new LinkedHashMap<>();
        fields.put(FIELD_TYPE, new byte[]{(byte) type.ordinal()});
        fields.put(FIELD_KEY, keyBytes != null ? keyBytes : new byte[0]);
        fields.put(FIELD_VERSION, version.toWire().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return fields;
    }

    private InvalidationMessage toMessage(String cache, Map<byte[], byte[]> body) {
        byte[] type = get(body, FIELD_TYPE);
        byte[] keyBytes = get(body, FIELD_KEY);
        Version version = Version.fromWire(new String(get(body, FIELD_VERSION),
                java.nio.charset.StandardCharsets.UTF_8));
        Object key = keyBytes.length > 0 ? keySerializer.fromBytes(keyBytes) : null;
        return new InvalidationMessage(cache, key, version, version.instanceId(),
                InvalidationMessage.Type.values()[type[0]]);
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
