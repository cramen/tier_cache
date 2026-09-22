package io.tiercache.redis;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.InvalidationMessage;
import io.tiercache.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Durable per-receiver Streams consumption. Pending rows are drained before
 * new rows; unreadable history requires an authorized baseline-before-clear.
 * Stable identities require one live owner. Internal transport API.
 */
public final class LettuceStreamsInvalidationTransport implements InvalidationTransport {
    public static final String GROUP_PREFIX = RedisKeyspace.GROUP;
    private static final Logger log = LoggerFactory.getLogger(LettuceStreamsInvalidationTransport.class);
    private static final int BATCH = 50;
    private static final byte[] MAIN = "main".getBytes(StandardCharsets.US_ASCII);
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final CacheSerializer<Object> keySerializer, valueSerializer;
    private final UUID instanceId;
    private final boolean stable;
    private final Map<String, Reader> readers = new ConcurrentHashMap<>();
    private volatile boolean closed;
    private volatile InvalidationGapHandler gaps;
    private volatile CacheMetricsListener metrics = CacheMetricsListener.NOOP;
    volatile Throwable lastReaderError; // sanitized test/diagnostic state

    private final class Reader {
        final String cache;
        final byte[] stream, group;
        final Consumer<InvalidationMessage> handler;
        volatile boolean active = true, pending;
        Thread thread;
        RecoveryResult covered;
        AutoCloseable gauge;
        long nextLog;
        Reader(String cache, Consumer<InvalidationMessage> handler) {
            this.cache = cache; this.handler = handler;
            stream = RedisKeyspace.journal(cache); group = RedisKeyspace.group(cache, instanceId);
        }
    }

    /** New volatile L1 identity; its group is retired best-effort on close. */
    public LettuceStreamsInvalidationTransport(RedisClient client, CacheSerializer<Object> keys, CacheSerializer<Object> values) {
        this(client.connect(ByteArrayCodec.INSTANCE), keys, values, UUID.randomUUID(), false);
    }
    /** Stable identity. The caller guarantees exclusive ownership across process restarts. */
    public LettuceStreamsInvalidationTransport(RedisClient client, CacheSerializer<Object> keys,
            CacheSerializer<Object> values, UUID instanceId) {
        this(client.connect(ByteArrayCodec.INSTANCE), keys, values, instanceId, true);
    }
    // Connection seam for real-server ACK/failure tests; transport owns the supplied connection.
    LettuceStreamsInvalidationTransport(StatefulRedisConnection<byte[], byte[]> connection,
            CacheSerializer<Object> keys, CacheSerializer<Object> values, UUID instanceId, boolean stable) {
        this.connection = connection; keySerializer = keys; valueSerializer = values;
        this.instanceId = Objects.requireNonNull(instanceId); this.stable = stable;
    }
    @Override public void publish(InvalidationMessage message) { /* the journal row is the durable event */ }
    @Override public java.util.concurrent.CompletionStage<io.tiercache.spi.PublicationOutcome> publishAsync(InvalidationMessage message) {
        return java.util.concurrent.CompletableFuture.completedFuture(io.tiercache.spi.PublicationOutcome.NOT_REQUIRED);
    }

    @Override public void setGapHandler(InvalidationGapHandler handler) { gaps = handler; }
    @Override public void setMetricsListener(CacheMetricsListener listener) { metrics = listener == null ? CacheMetricsListener.NOOP : listener; }
    @Override public boolean requiresRegistrationReset() { return true; }

    @Override
    public AutoCloseable subscribe(String cache, Consumer<InvalidationMessage> handler) {
        if (closed) throw new IllegalStateException("Streams transport is closed");
        Reader reader = new Reader(cache, handler);
        if (gaps != null) reader.covered = gaps.registrationBaseline(cache);
        ensureGroup(reader, reader.covered == null ? "$" : reader.covered.baseline());
        if (readers.putIfAbsent(cache, reader) != null) throw new IllegalStateException("Cache already subscribed: " + cache);
        synchronized (reader) {
            if (closed) reader.active = false;
            if (reader.active) {
                reader.thread = new Thread(() -> readLoop(reader), "tiercache-streams-" + cache);
                reader.thread.setDaemon(true);
                reader.thread.start();
            }
        }
        AutoCloseable gauge = null;
        try { gauge = metrics.registerRecovery(cache, () -> reader.pending); }
        catch (Throwable e) { log.warn("Streams metric registration failed ({})", e.getClass().getSimpleName()); }
        boolean discard;
        synchronized (reader) { discard = !reader.active || closed; if (!discard) reader.gauge = gauge; }
        if (discard) closeGauge(gauge);
        if (!reader.active) retire(reader);
        return () -> retire(reader);
    }

    private boolean active(Reader reader) { return !closed && reader.active; }

    /**
     * Read own pending IDs first. Before same-group XAUTOCLAIM, inspect missing
     * payloads atomically: Redis 7+ may otherwise remove their PEL entries while
     * claiming. This also handles the Redis 6.2 reply without deleted-ID fields.
     */
    private static final String PENDING = """
            redis.replicate_commands()
            local own = redis.call('xreadgroup','GROUP',ARGV[1],ARGV[2],'COUNT',50,'STREAMS',KEYS[1],'0-0')
            if own and #own > 0 and #own[1][2] > 0 then return own[1][2] end
            local pending = redis.call('xpending',KEYS[1],ARGV[1],'-','+',50)
            local missing = {}
            for _, p in ipairs(pending) do
                local row = redis.call('xrange',KEYS[1],p[1],p[1])
                if #row == 0 then missing[#missing+1] = {p[1],{}} end
            end
            if #missing > 0 then return missing end
            if #pending == 0 then return {} end
            local claimed = redis.call('xautoclaim',KEYS[1],ARGV[1],ARGV[2],0,'0-0','COUNT',50)
            return claimed[2]
            """;

    private List<StreamMessage<byte[], byte[]>> pending(Reader reader) {
        List<Object> rows = connection.sync().eval(PENDING, ScriptOutputType.MULTI,
                new byte[][]{reader.stream}, reader.group, MAIN);
        List<StreamMessage<byte[], byte[]>> result = new ArrayList<>(rows.size());
        for (Object raw : rows) {
            List<?> row = (List<?>) raw;
            String id = new String((byte[]) row.get(0), StandardCharsets.US_ASCII);
            Map<byte[], byte[]> body = new LinkedHashMap<>();
            if (row.get(1) instanceof List<?> fields) {
                for (int i = 0; i + 1 < fields.size(); i += 2) body.put((byte[]) fields.get(i), (byte[]) fields.get(i + 1));
            }
            result.add(new StreamMessage<>(reader.stream, id, body));
        }
        return result;
    }

    private void readLoop(Reader reader) {
        int connectionFailures = 0;
        try {
            while (active(reader)) {
                try {
                    List<StreamMessage<byte[], byte[]>> batch = pending(reader);
                    if (batch.isEmpty()) batch = connection.sync().xreadgroup(
                            io.lettuce.core.Consumer.from(reader.group, MAIN), XReadArgs.Builder.count(BATCH),
                            XReadArgs.StreamOffset.lastConsumed(reader.stream));
                    connectionFailures = 0;
                    if (batch == null || batch.isEmpty()) { reader.pending = false; pause(50); continue; }
                    // Retain this batch and its current row until application/settlement
                    // completes. Never abandon its remainder after Redis advanced >.
                    for (var row : batch) {
                        if (!active(reader)) return;
                        process(reader, row);
                    }
                } catch (RuntimeException error) {
                    if (!active(reader)) return;
                    failure(reader, null, CacheMetricsListener.StreamResult.RESYNC_FAILED, error);
                    reader.pending = true;
                    if (String.valueOf(error.getMessage()).contains("NOGROUP")) {
                        if (recover(reader, null)) ensureGroup(reader, reader.covered.baseline());
                    }
                    pause(backoff(++connectionFailures));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void process(Reader reader, StreamMessage<byte[], byte[]> row) throws InterruptedException {
        String id = row.getId();
        refreshBaseline(reader);
        if (gaps != null && reader.covered == null && !recover(reader, id)) return;
        if (covered(reader, id)) { acknowledge(reader, id, true); return; }
        InvalidationMessage message;
        try { message = StreamRowDecoder.decode(reader.cache, id, row.getBody(), keySerializer, valueSerializer); }
        catch (StreamRowCorruptionException error) {
            failure(reader, id, CacheMetricsListener.StreamResult.DECODE_FAILED, error);
            if (recover(reader, id)) acknowledge(reader, id, true);
            return;
        }
        boolean applied = false;
        for (int attempt = 1; active(reader) && attempt <= 3; attempt++) {
            try {
                refreshBaseline(reader);
                if (covered(reader, id)) { acknowledge(reader, id, true); return; }
                // Dispatch is admitted before close, without executing arbitrary
                // target/observer code under the transport state monitor.
                synchronized (reader) { if (!active(reader)) return; }
                reader.handler.accept(message); applied = true; break;
            } catch (RuntimeException error) {
                if (!active(reader)) return;
                failure(reader, id, CacheMetricsListener.StreamResult.APPLY_FAILED, error);
                if (attempt < 3) pause(attempt * 1000L);
            }
        }
        if (applied) acknowledge(reader, id, false);
        else if (active(reader) && recover(reader, id)) acknowledge(reader, id, true);
    }

    private void refreshBaseline(Reader reader) {
        InvalidationGapHandler handler = gaps;
        if (handler == null) return;
        RecoveryResult latest = handler.registrationBaseline(reader.cache);
        if (latest != null && handler.isCurrent(reader.cache, latest)) reader.covered = latest;
        // A newer journal replay epoch alone does not require clearing L1.
        // Only a row we intend to skip needs a still-current reset proof.
    }

    private boolean covered(Reader reader, String id) {
        return reader.covered != null && StreamRowDecoder.compareIds(id, reader.covered.baseline()) <= 0;
    }

    private boolean recover(Reader reader, String row) throws InterruptedException {
        reader.pending = true;
        int failures = 0;
        while (active(reader)) {
            InvalidationGapHandler handler = gaps;
            try {
                if (handler == null) throw new IllegalStateException("No capable gap handler");
                RecoveryResult result = handler.reset(reader.cache).toCompletableFuture().get();
                if (!active(reader)) return false;
                if (handler == gaps && result != null && result.status() == RecoveryResult.Status.RESET_SAFE
                        && result.generation() >= 0 && StreamRowDecoder.validId(result.baseline())
                        && handler.isCurrent(reader.cache, result)) {
                    reader.covered = result;
                    return true;
                }
                throw new IllegalStateException("Recovery did not establish a current safe baseline");
            } catch (ExecutionException | RuntimeException error) {
                if (!active(reader)) return false;
                failure(reader, row, CacheMetricsListener.StreamResult.RESYNC_FAILED, error);
                pause(backoff(++failures));
            }
        }
        return false;
    }

    private void acknowledge(Reader reader, String id, boolean needsProof) throws InterruptedException {
        int failures = 0;
        while (active(reader)) {
            if (needsProof && (gaps == null || !covered(reader, id) || !gaps.isCurrent(reader.cache, reader.covered))) {
                if (!recover(reader, id)) return;
                if (!covered(reader, id)) { // a reset must never cover a row after its baseline
                    failure(reader, id, CacheMetricsListener.StreamResult.RESYNC_FAILED, new IllegalStateException("Row is after baseline"));
                    pause(backoff(++failures)); continue;
                }
            }
            try {
                RedisFuture<Long> ack;
                synchronized (reader) {
                    if (!active(reader)) return;
                    if (needsProof && !gaps.isCurrent(reader.cache, reader.covered)) continue;
                    // Queue admission only. Waiting/network I/O is outside the gate.
                    ack = connection.async().xack(reader.stream, reader.group, id);
                }
                ack.get(); reader.pending = false; return;
            } catch (ExecutionException | RuntimeException error) {
                if (!active(reader)) return;
                failure(reader, id, CacheMetricsListener.StreamResult.ACK_FAILED, error);
                reader.pending = true; pause(backoff(++failures));
            }
        }
    }

    private void ensureGroup(Reader reader, String cursor) {
        try {
            connection.sync().xgroupCreate(XReadArgs.StreamOffset.from(reader.stream, cursor), reader.group,
                    XGroupCreateArgs.Builder.mkstream());
        } catch (RuntimeException error) {
            if (!String.valueOf(error.getMessage()).contains("BUSYGROUP")) throw error;
        }
    }

    private void failure(Reader reader, String row, CacheMetricsListener.StreamResult result, Throwable error) {
        lastReaderError = error instanceof StreamRowCorruptionException ? error
                : new IllegalStateException("Streams " + result + ": " + error.getClass().getSimpleName());
        try { metrics.onStreamFailure(reader.cache, result); }
        catch (Throwable ignored) { /* an observer cannot alter dispatch or ACK state */ }
        long now = System.nanoTime();
        if (reader.nextLog == 0 || now - reader.nextLog >= 0) {
            reader.nextLog = now + TimeUnit.SECONDS.toNanos(30);
            log.warn("Streams recovery: cache={}, row={}, result={}, failure={}", reader.cache, row, result, error.getClass().getSimpleName());
        }
    }
    private static long backoff(int failures) { return Math.min(30000, 1000L << Math.min(5, failures - 1)); }
    private static void pause(long millis) throws InterruptedException { Thread.sleep(millis); }
    private static void closeGauge(AutoCloseable gauge) {
        if (gauge != null) try { gauge.close(); } catch (Throwable ignored) { }
    }
    private void retire(Reader reader) {
        synchronized (reader) {
            reader.active = false;
            if (reader.thread != null) reader.thread.interrupt();
        }
        if (!readers.remove(reader.cache, reader)) return;
        closeGauge(reader.gauge);
        if (!stable) try { connection.sync().xgroupDestroy(reader.stream, reader.group); }
        catch (RuntimeException error) { log.debug("Ephemeral Streams group cleanup failed for cache {} ({})", reader.cache, error.getClass().getSimpleName()); }
    }
    @Override public void close() {
        closed = true;
        readers.values().forEach(this::retire);
        connection.close();
    }
}
