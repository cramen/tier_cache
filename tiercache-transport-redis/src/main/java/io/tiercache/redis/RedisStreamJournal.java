package io.tiercache.redis;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.spi.CheckedRange;
import io.tiercache.spi.InvalidationJournal;
import io.tiercache.spi.JournalRow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bounded invalidation journal on Redis Streams: one stream per cache
 * ({@code tiercache:v2:journal:<token(cache)>}), capacity-capped by approximate
 * MAXLEN trimming. Writers append inside the same atomic Lua unit as the
 * data write (see {@link LettuceRemoteCache}), so there is no "wrote but
 * didn't journal" window; every append path also keeps the exact trim
 * counter ({@code tiercache:v2:journal-trims:<token(cache)>}) for the
 * beginning-cursor trim check.
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
    public static final String JOURNAL_KEYSPACE = RedisKeyspace.JOURNAL;

    /**
     * Keyspace prefix of the atomic trim counter (one per cache stream):
     * incremented inside every journal-appending script when the capped
     * XADD actually removed rows, so a beginning cursor ({@code 0-0}) can
     * tell "journal still small" from "history trimmed".
     *
     * @since 1.3.0
     */
    public static final String TRIMS_KEYSPACE = RedisKeyspace.TRIMS;

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(RedisStreamJournal.class);

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
        return RedisKeyspace.journal(cache);
    }

    /**
     * Stream key bytes for a cache (public for the transport's Lua script).
     *
     * @param cache the cache name
     * @return the stream key bytes ({@code tiercache:v2:journal:<token(cache)>})
     * @since 0.1.0
     */
    public static byte[] streamKeyBytes(String cache) {
        return streamKey(cache);
    }

    static byte[] trimCounterKey(String cache) {
        return RedisKeyspace.trims(cache);
    }

    /**
     * Trim-counter key bytes for a cache (public for the transport's Lua
     * scripts, which increment it on every capped XADD that removed rows).
     *
     * @param cache the cache name
     * @return the counter key bytes ({@code tiercache:v2:journal-trims:<token(cache)>})
     * @since 1.3.0
     */
    public static byte[] trimCounterKeyBytes(String cache) {
        return trimCounterKey(cache);
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
     * Append-with-cap, shared by every journal-appending path: the XADD
     * plus exact trim accounting in one atomic unit — when the capped
     * stream did not grow by one, rows were removed and the trim counter
     * is incremented. This is what makes the beginning-cursor ({@code 0-0})
     * trim check exact even when the stream length equals the capacity.
     */
    static final String APPEND_WITH_CAP =
            "local before = redis.call('xlen', KEYS[1]) "
                    + "local id = redis.call('xadd', KEYS[1], 'MAXLEN', '~', ARGV[1], '*', unpack(ARGV, 2)) "
                    + "if redis.call('xlen', KEYS[1]) < before + 1 then "
                    + "redis.call('incr', KEYS[2]) end "
                    + "return id";

    /**
     * Beginning-cursor checked read: the trim counter and the range come
     * from ONE Lua call, so a trim can never slip between the check and
     * the read. Returns {@code {counter-or-nil, xrange-result}}.
     */
    private static final String CHECKED_READ_FROM_BEGINNING =
            "local trims = redis.call('get', KEYS[2]) "
                    + "local rows = redis.call('xrange', KEYS[1], '-', '+', 'COUNT', tonumber(ARGV[1])) "
                    + "return {trims, rows}";

    @Override
    public String append(String cache, InvalidationMessage message) {
        byte[] keyBytes = message.key() != null ? keySerializer.toBytes(message.key()) : null;
        Map<byte[], byte[]> fields = fields(keyBytes, message.version(), message.type(),
                message.payload() != null ? valueSerializer.toBytes(message.payload()) : null);
        byte[][] argv = new byte[1 + fields.size() * 2][];
        argv[0] = String.valueOf(capacity).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int i = 1;
        for (Map.Entry<byte[], byte[]> field : fields.entrySet()) {
            argv[i++] = field.getKey();
            argv[i++] = field.getValue();
        }
        Object id = commands.eval(APPEND_WITH_CAP, io.lettuce.core.ScriptOutputType.VALUE,
                new byte[][]{streamKey(cache), trimCounterKey(cache)}, argv);
        return new String((byte[]) id, java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public List<JournalRow> readRange(String cache, String cursorExclusive) {
        List<StreamMessage<byte[], byte[]>> entries = commands.xrange(streamKey(cache),
                Range.from(Range.Boundary.excluding(cursorExclusive), Range.Boundary.unbounded()));
        return toRows(cache, entries);
    }

    @Override
    public CheckedRange checkedRead(String cache, String cursor, int maxRows) {
        if (maxRows < 1) throw new IllegalArgumentException("maxRows must be positive");
        if ("0-0".equals(cursor)) {
            return checkedReadFromBeginning(cache, maxRows);
        }
        // One inclusive read: the integrity proof (first row IS the cursor
        // row) and the range come from the same response.
        List<StreamMessage<byte[], byte[]>> entries = commands.xrange(streamKey(cache),
                Range.from(Range.Boundary.including(cursor), Range.Boundary.unbounded()),
                Limit.from((long) maxRows + 1));
        boolean intact = !entries.isEmpty() && entries.get(0).getId().equals(cursor);
        // The anchor's raw ID proves integrity. Its payload was already accounted
        // for and may be the poison row covered by the last safe reset.
        return new CheckedRange(intact, intact ? toRows(cache, entries.subList(1, entries.size())) : List.of());
    }

    private CheckedRange checkedReadFromBeginning(String cache, int maxRows) {
        List<Object> reply = commands.eval(CHECKED_READ_FROM_BEGINNING,
                io.lettuce.core.ScriptOutputType.MULTI,
                new byte[][]{streamKey(cache), trimCounterKey(cache)},
                String.valueOf(maxRows).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Object trims = reply.get(0);
        boolean intact = trims == null
                || Long.parseLong(new String((byte[]) trims, java.nio.charset.StandardCharsets.UTF_8)) == 0;
        if (!intact) return new CheckedRange(false, List.of());
        List<JournalRow> rows = new ArrayList<>();
        for (Object entry : (List<?>) reply.get(1)) {
            List<?> pair = (List<?>) entry;
            String id = new String((byte[]) pair.get(0), java.nio.charset.StandardCharsets.UTF_8);
            List<?> flatFields = (List<?>) pair.get(1);
            Map<byte[], byte[]> body = new LinkedHashMap<>();
            for (int f = 0; f + 1 < flatFields.size(); f += 2) {
                body.put((byte[]) flatFields.get(f), (byte[]) flatFields.get(f + 1));
            }
            rows.add(new JournalRow(id, StreamRowDecoder.decode(cache, id, body, keySerializer, valueSerializer)));
        }
        return new CheckedRange(intact, rows);
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
            // stream): a loss happened only if rows were trimmed since,
            // counted atomically at write time on every append path — exact
            // even when the stream length equals the capacity exactly.
            try {
                byte[] trims = commands.get(trimCounterKey(cache));
                return trims != null
                        && Long.parseLong(new String(trims, java.nio.charset.StandardCharsets.UTF_8)) > 0;
            } catch (RuntimeException e) {
                // Counter unreadable: treat as a possible loss (flush path),
                // never as "no loss".
                log.warn("Trim counter read failed for cache '{}'; treating as a possible loss.",
                        cache, e);
                return true;
            }
        }
        List<StreamMessage<byte[], byte[]>> first = commands.xrange(streamKey(cache),
                Range.unbounded(), Limit.from(1));
        if (first.isEmpty()) {
            return false;
        }
        return compareIds(cursor, first.get(0).getId()) < 0;
    }

    private List<JournalRow> toRows(String cache, List<StreamMessage<byte[], byte[]>> entries) {
        List<JournalRow> out = new ArrayList<>(entries.size());
        for (StreamMessage<byte[], byte[]> entry : entries) {
            out.add(new JournalRow(entry.getId(), StreamRowDecoder.decode(cache, entry.getId(), entry.getBody(), keySerializer, valueSerializer)));
        }
        return out;
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

    static int compareIds(String a, String b) {
        return StreamRowDecoder.compareIds(a, b);
    }
}
