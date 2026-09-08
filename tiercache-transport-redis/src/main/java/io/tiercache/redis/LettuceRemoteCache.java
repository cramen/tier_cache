package io.tiercache.redis;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.spi.DistributedLockProvider;
import io.tiercache.spi.LockProviderSource;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

/**
 * {@link RemoteCache} over Redis/Valkey via Lettuce (the default L2
 * transport). Supports per-entry TTLs ({@code SET ... PX}), atomic
 * {@code setIfAbsent}, and — when a {@link RedisStreamJournal} is attached —
 * versioned writes with strict last-write-wins convergence:
 *
 * <ul>
 *   <li>entries carry the write version in the payload framing
 *       ({@code [tag][4B len][version string][payload]});</li>
 *   <li>writes are version-conditional in Lua: an older write never
 *       overwrites a newer entry, and data write + journal row commit
 *       atomically;</li>
 *   <li>evict leaves a short-lived versioned tombstone, so an older racing
 *       write cannot resurrect a deleted entry;</li>
 *   <li>legacy framing v1 (tag byte only) still decodes on read as
 *       unversioned — oldest in comparisons, safe for rolling upgrades.</li>
 * </ul>
 *
 * <p>Keys are namespaced as {@code <cacheName>:<serialized-key>}.
 *
 * <p>Default timeouts: 100 ms connect, 250 ms per command — both
 * configurable via the builder and deliberately below a typical business
 * timeout. Infrastructure failures surface as Lettuce unchecked exceptions;
 * degradation handling (circuit breaker) lives in core, not here.
 *
 * <p><b>Incubating:</b> 0.x API, may change before the public API freeze.
 */
public final class LettuceRemoteCache<K, V> implements RemoteCache<K, V>, LockProviderSource, AutoCloseable {

    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofMillis(100);
    public static final Duration DEFAULT_COMMAND_TIMEOUT = Duration.ofMillis(250);

    /** Tombstone lifetime after evict: bounds the resurrect-protection window. */
    private static final long TOMBSTONE_TTL_MILLIS = 60_000;

    private final RedisClient client;
    private final boolean ownsClient;
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final RedisCommands<byte[], byte[]> commands;
    private final String cacheName;
    private final byte[] keyPrefix;
    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;
    private final RedisStreamJournal journal; // null = unversioned mode
    private io.tiercache.InvalidationMode invalidationMode = io.tiercache.InvalidationMode.INVALIDATE;
    private long payloadCapBytes = 64 * 1024;

    private static final byte TAG_NULL_MARKER = 0x00;
    private static final byte TAG_VALUE = 0x01;
    private static final byte TAG_NULL_MARKER_V2 = 0x02;
    private static final byte TAG_VALUE_V2 = 0x03;
    private static final byte TAG_TOMBSTONE = 0x04;

    private LettuceRemoteCache(Builder<K, V> builder) {
        this.ownsClient = builder.sharedClient == null;
        this.client = ownsClient ? RedisClient.create(builder.redisUri) : builder.sharedClient;
        if (ownsClient) {
            // For a caller-provided client, options are the caller's responsibility.
            this.client.setOptions(ClientOptions.builder()
                    .socketOptions(SocketOptions.builder()
                            .connectTimeout(builder.connectTimeout)
                            .build())
                    .timeoutOptions(TimeoutOptions.enabled(builder.commandTimeout))
                    .build());
        }
        this.connection = client.connect(ByteArrayCodec.INSTANCE);
        this.commands = connection.sync();
        this.cacheName = builder.cacheName;
        this.keyPrefix = (builder.cacheName + ":").getBytes(StandardCharsets.UTF_8);
        this.keySerializer = builder.keySerializer;
        this.valueSerializer = builder.valueSerializer;
        this.journal = builder.journal;
        this.invalidationMode = builder.invalidationMode;
        this.payloadCapBytes = builder.payloadCapBytes;
    }

    public static <K, V> Builder<K, V> builder(String redisUri) {
        return new Builder<>(redisUri);
    }

    @Override
    public StoredEntry<V> get(K key) {
        byte[] bytes = commands.get(namespaced(key));
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        byte tag = bytes[0];
        if (tag == TAG_NULL_MARKER) {
            return StoredEntry.nullMarker();
        }
        if (tag == TAG_VALUE) {
            return StoredEntry.ofValue(valueSerializer.fromBytes(
                    Arrays.copyOfRange(bytes, 1, bytes.length)));
        }
        if (tag == TAG_TOMBSTONE) {
            return null; // tombstones read as absent
        }
        int versionLen = ByteBuffer.wrap(bytes, 1, 4).getInt();
        Version version = Version.fromWire(new String(bytes, 5, versionLen, StandardCharsets.UTF_8));
        if (tag == TAG_NULL_MARKER_V2) {
            return StoredEntry.nullMarker(version);
        }
        byte[] payload = Arrays.copyOfRange(bytes, 5 + versionLen, bytes.length);
        return StoredEntry.ofValue(valueSerializer.fromBytes(payload), version);
    }

    @Override
    public void put(K key, StoredEntry<V> entry, Duration ttl) {
        if (journal != null && entry.version() != null) {
            // Versioned entries always go through the conditional Lua write
            // so the journal row and the data write stay atomic.
            putIfNewer(key, entry, ttl);
            return;
        }
        commands.set(namespaced(key), encode(entry), SetArgs.Builder.px(ttl));
    }

    /**
     * Version-conditional write with atomic journal append (strict
     * last-write-wins). Requires an attached journal; falls back to plain
     * put in unversioned mode.
     */
    @Override
    public boolean putIfNewer(K key, StoredEntry<V> entry, Duration ttl) {
        if (journal == null || entry.version() == null) {
            put(key, entry, ttl);
            return true;
        }
        Long result = commands.eval(Lua.CONDITIONAL_WRITE, io.lettuce.core.ScriptOutputType.INTEGER,
                new byte[][]{namespaced(key), RedisStreamJournal.streamKeyBytes(cacheName)},
                entry.version().toWire().getBytes(StandardCharsets.UTF_8),
                encode(entry),
                String.valueOf(ttl.toMillis()).getBytes(StandardCharsets.UTF_8),
                String.valueOf(journal.capacity()).getBytes(StandardCharsets.UTF_8),
                new byte[]{(byte) InvalidationMessage.Type.INVALIDATE.ordinal()},
                keySerializer.toBytes(key),
                journalPayload(entry));
        return result != null && result == 1L;
    }

    @Override
    public void evict(K key) {
        byte[] namespaced = namespaced(key);
        commands.del(namespaced);
        pruneTags(namespaced);
    }

    /**
     * Versioned evict: leaves a tombstone so an older racing write cannot
     * resurrect the entry; the tombstone and journal row commit atomically.
     */
    @Override
    public void evict(K key, Version version) {
        if (journal == null || version == null) {
            evict(key);
            return;
        }
        byte[] namespaced = namespaced(key);
        pruneTags(namespaced);
        commands.eval(Lua.VERSIONED_EVICT, io.lettuce.core.ScriptOutputType.INTEGER,
                new byte[][]{namespaced, RedisStreamJournal.streamKeyBytes(cacheName)},
                version.toWire().getBytes(StandardCharsets.UTF_8),
                String.valueOf(TOMBSTONE_TTL_MILLIS).getBytes(StandardCharsets.UTF_8),
                String.valueOf(journal.capacity()).getBytes(StandardCharsets.UTF_8),
                new byte[]{(byte) InvalidationMessage.Type.INVALIDATE.ordinal()},
                keySerializer.toBytes(key));
    }

    @Override
    public void clear() {
        // Namespace-scoped clear: SCAN the prefix, UNLINK in batches.
        String pattern = new String(keyPrefix, StandardCharsets.UTF_8) + "*";
        io.lettuce.core.ScanCursor cursor = io.lettuce.core.ScanCursor.INITIAL;
        do {
            io.lettuce.core.KeyScanCursor<byte[]> scanResult = commands.scan(cursor,
                    io.lettuce.core.ScanArgs.Builder.matches(pattern).limit(200));
            java.util.List<byte[]> keys = scanResult.getKeys();
            if (!keys.isEmpty()) {
                commands.unlink(keys.toArray(new byte[0][]));
            }
            cursor = scanResult;
        } while (!cursor.isFinished());
    }

    /** Namespace-scoped clear plus a journal EVICT_ALL row (not transactional: SCAN can't be). */
    public void clearWithJournal(Version version) {
        clear();
        if (journal != null && version != null) {
            journal.append(cacheName, new InvalidationMessage(cacheName, null, version,
                    version.instanceId(), InvalidationMessage.Type.EVICT_ALL));
        }
    }

    @Override
    public boolean setIfAbsent(K key, StoredEntry<V> entry, Duration ttl) {
        if (journal == null || entry.version() == null) {
            String result = commands.set(namespaced(key), encode(entry), SetArgs.Builder.px(ttl).nx());
            return "OK".equals(result);
        }
        Long result = commands.eval(Lua.SET_IF_ABSENT, io.lettuce.core.ScriptOutputType.INTEGER,
                new byte[][]{namespaced(key), RedisStreamJournal.streamKeyBytes(cacheName)},
                encode(entry),
                String.valueOf(ttl.toMillis()).getBytes(StandardCharsets.UTF_8),
                String.valueOf(journal.capacity()).getBytes(StandardCharsets.UTF_8),
                new byte[]{(byte) InvalidationMessage.Type.INVALIDATE.ordinal()},
                keySerializer.toBytes(key),
                entry.version().toWire().getBytes(StandardCharsets.UTF_8),
                journalPayload(entry));
        return result != null && result == 1L;
    }

    /** Value bytes for the journal row in UPDATE mode (capped); empty otherwise. */
    private byte[] journalPayload(StoredEntry<V> entry) {
        if (invalidationMode != io.tiercache.InvalidationMode.UPDATE || entry.isNullMarker()) {
            return new byte[0];
        }
        byte[] bytes = valueSerializer.toBytes(entry.value());
        return bytes.length <= payloadCapBytes ? bytes : new byte[0];
    }

    private byte[] encode(StoredEntry<V> entry) {
        if (entry.version() == null) {
            if (entry.isNullMarker()) {
                return new byte[]{TAG_NULL_MARKER};
            }
            byte[] payload = valueSerializer.toBytes(entry.value());
            byte[] out = new byte[payload.length + 1];
            out[0] = TAG_VALUE;
            System.arraycopy(payload, 0, out, 1, payload.length);
            return out;
        }
        byte[] versionBytes = entry.version().toWire().getBytes(StandardCharsets.UTF_8);
        byte[] payload = entry.isNullMarker() ? new byte[0] : valueSerializer.toBytes(entry.value());
        byte[] out = new byte[5 + versionBytes.length + payload.length];
        out[0] = entry.isNullMarker() ? TAG_NULL_MARKER_V2 : TAG_VALUE_V2;
        ByteBuffer.wrap(out, 1, 4).putInt(versionBytes.length);
        System.arraycopy(versionBytes, 0, out, 5, versionBytes.length);
        System.arraycopy(payload, 0, out, 5 + versionBytes.length, payload.length);
        return out;
    }

    // --- Tag registry: tiercache:tags:<cache>:<tag> sets + reverse index
    // tiercache:tagkeys:<cache>:<key> (Redis sets have no per-member TTL;
    // stale members are pruned on eviction and are harmless otherwise). ---

    private byte[] tagSetKey(String tag) {
        return ("tiercache:tags:" + cacheName + ":" + tag).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] keyTagsKey(byte[] namespacedKey) {
        byte[] prefix = ("tiercache:tagkeys:").getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[prefix.length + namespacedKey.length];
        System.arraycopy(prefix, 0, out, 0, prefix.length);
        System.arraycopy(namespacedKey, 0, out, prefix.length, namespacedKey.length);
        return out;
    }

    @Override
    public void putTagged(K key, StoredEntry<V> entry, Duration ttl, String[] tags) {
        put(key, entry, ttl);
        byte[] namespaced = namespaced(key);
        byte[] keyTags = keyTagsKey(namespaced);
        if (tags.length > 0) {
            commands.del(keyTags);
            commands.sadd(keyTags, java.util.Arrays.stream(tags)
                    .map(t -> t.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new));
            for (String tag : tags) {
                commands.sadd(tagSetKey(tag), namespaced);
            }
        }
    }

    @Override
    public java.util.List<K> keysByTag(String tag) {
        java.util.Set<byte[]> members = commands.smembers(tagSetKey(tag));
        java.util.List<K> out = new java.util.ArrayList<>(members.size());
        for (byte[] namespaced : members) {
            byte[] raw = java.util.Arrays.copyOfRange(namespaced, keyPrefix.length, namespaced.length);
            out.add(keySerializer.fromBytes(raw));
        }
        return out;
    }

    /** Removes tag bookkeeping for an evicted key. */
    private void pruneTags(byte[] namespaced) {
        byte[] keyTags = keyTagsKey(namespaced);
        java.util.Set<byte[]> tags = commands.smembers(keyTags);
        if (tags != null && !tags.isEmpty()) {
            for (byte[] tag : tags) {
                commands.srem(tagSetKey(new String(tag, StandardCharsets.UTF_8)), namespaced);
            }
            commands.del(keyTags);
        }
    }

    private byte[] namespaced(K key) {
        byte[] serialized = keySerializer.toBytes(key);
        byte[] out = new byte[keyPrefix.length + serialized.length];
        System.arraycopy(keyPrefix, 0, out, 0, keyPrefix.length);
        System.arraycopy(serialized, 0, out, keyPrefix.length, serialized.length);
        return out;
    }

    @Override
    public DistributedLockProvider lockProvider() {
        // Shares the client; the provider owns its String-codec connection.
        return new LettuceLockProvider(client.connect());
    }

    /** The connection used by this transport (for journal/wiring sharing). */
    StatefulRedisConnection<byte[], byte[]> connection() {
        return connection;
    }

    @Override
    public void close() {
        connection.close();
        if (ownsClient) {
            client.shutdown();
        }
    }

    /**
     * Lua scripts for the versioned write paths. Version strings compare as
     * (numeric seq, lexicographic instance id) — identical to the Java side.
     */
    private static final class Lua {

        private static final String VERSION_COMPARE =
                "local function newer(a, b) "
                        + "local as, au = string.match(a, '^(%d+):(.+)$') "
                        + "local bs, bu = string.match(b, '^(%d+):(.+)$') "
                        + "local an, bn = tonumber(as), tonumber(bs) "
                        + "if an ~= bn then return an > bn end "
                        + "return au > bu end "
                        + "local function curVersion(bytes) "
                        + "if not bytes then return nil end "
                        + "local tag = string.byte(bytes, 1) "
                        + "if tag ~= 2 and tag ~= 3 and tag ~= 4 then return nil end "
                        + "local l = string.byte(bytes,2)*16777216 + string.byte(bytes,3)*65536"
                        + " + string.byte(bytes,4)*256 + string.byte(bytes,5) "
                        + "return string.sub(bytes, 6, 6 + l - 1) end ";

        /** Conditional value/marker write + journal row (payload in 'p' when present). */
        static final String CONDITIONAL_WRITE = VERSION_COMPARE
                + "local cur = redis.call('get', KEYS[1]) "
                + "local curVer = curVersion(cur) "
                + "if curVer and newer(curVer, ARGV[1]) then return 0 end "
                + "redis.call('set', KEYS[1], ARGV[2], 'PX', ARGV[3]) "
                + "redis.call('xadd', KEYS[2], 'MAXLEN', '~', ARGV[4], '*',"
                + " 't', ARGV[5], 'k', ARGV[6], 'v', ARGV[1], 'p', ARGV[7]) "
                + "return 1";

        /** Evict = versioned tombstone write + journal row. */
        static final String VERSIONED_EVICT = VERSION_COMPARE
                + "local cur = redis.call('get', KEYS[1]) "
                + "local curVer = curVersion(cur) "
                + "if curVer and newer(curVer, ARGV[1]) then return 0 end "
                + "local tombstone = string.char(4, 0, 0, 0, string.len(ARGV[1])) .. ARGV[1] "
                + "redis.call('set', KEYS[1], tombstone, 'PX', ARGV[2]) "
                + "redis.call('xadd', KEYS[2], 'MAXLEN', '~', ARGV[3], '*',"
                + " 't', ARGV[4], 'k', ARGV[5], 'v', ARGV[1]) "
                + "return 1";

        /** set-if-absent; a tombstone counts as absent. Payload in 'p' when present. */
        static final String SET_IF_ABSENT = VERSION_COMPARE
                + "local cur = redis.call('get', KEYS[1]) "
                + "if cur and string.byte(cur, 1) ~= 4 then return 0 end "
                + "redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2]) "
                + "redis.call('xadd', KEYS[2], 'MAXLEN', '~', ARGV[3], '*',"
                + " 't', ARGV[4], 'k', ARGV[5], 'v', ARGV[6], 'p', ARGV[7]) "
                + "return 1";
    }

    /**
     * @param <K> key type
     * @param <V> value type
     */
    public static final class Builder<K, V> {

        private final String redisUri;
        private RedisClient sharedClient;
        private String cacheName = "default";
        private CacheSerializer<K> keySerializer;
        private CacheSerializer<V> valueSerializer;
        private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        private Duration commandTimeout = DEFAULT_COMMAND_TIMEOUT;
        private RedisStreamJournal journal;
        private io.tiercache.InvalidationMode invalidationMode = io.tiercache.InvalidationMode.INVALIDATE;
        private long payloadCapBytes = 64 * 1024;

        private Builder(String redisUri) {
            this.redisUri = Objects.requireNonNull(redisUri, "redisUri");
        }

        /**
         * Reuses an existing client (e.g. shared across caches/instances)
         * instead of creating one from {@code redisUri}. The caller then owns
         * the client lifecycle.
         */
        @SuppressWarnings("unchecked")
        public Builder<K, V> client(RedisClient sharedClient) {
            this.sharedClient = Objects.requireNonNull(sharedClient, "sharedClient");
            return this;
        }

        /**
         * Namespace for keys (used as {@code <cacheName>:} prefix). Required
         * when several caches share one server.
         */
        public Builder<K, V> cacheName(String cacheName) {
            this.cacheName = Objects.requireNonNull(cacheName, "cacheName");
            return this;
        }

        @SuppressWarnings("unchecked")
        public Builder<K, V> keySerializer(CacheSerializer<?> keySerializer) {
            this.keySerializer = (CacheSerializer<K>) Objects.requireNonNull(keySerializer);
            return this;
        }

        @SuppressWarnings("unchecked")
        public Builder<K, V> valueSerializer(CacheSerializer<?> valueSerializer) {
            this.valueSerializer = (CacheSerializer<V>) Objects.requireNonNull(valueSerializer);
            return this;
        }

        public Builder<K, V> connectTimeout(Duration connectTimeout) {
            this.connectTimeout = Objects.requireNonNull(connectTimeout, "connectTimeout");
            return this;
        }

        public Builder<K, V> commandTimeout(Duration commandTimeout) {
            this.commandTimeout = Objects.requireNonNull(commandTimeout, "commandTimeout");
            return this;
        }

        /**
         * Attaches the invalidation journal: writes become version-conditional
         * with atomic journal rows, and evicts leave versioned tombstones
         * (strict last-write-wins convergence).
         */
        public Builder<K, V> journal(RedisStreamJournal journal) {
            this.journal = journal;
            return this;
        }

        /**
         * UPDATE invalidation mode for this transport's cache: journal rows
         * carry the value payload (up to {@code payloadCapBytes}; larger
         * values fall back to INVALIDATE semantics).
         */
        public Builder<K, V> invalidationMode(io.tiercache.InvalidationMode mode, long payloadCapBytes) {
            this.invalidationMode = mode;
            this.payloadCapBytes = payloadCapBytes;
            return this;
        }

        public LettuceRemoteCache<K, V> build() {
            if (keySerializer == null) {
                keySerializer = new JdkCacheSerializer<>();
            }
            if (valueSerializer == null) {
                valueSerializer = new JdkCacheSerializer<>();
            }
            return new LettuceRemoteCache<>(this);
        }
    }
}
