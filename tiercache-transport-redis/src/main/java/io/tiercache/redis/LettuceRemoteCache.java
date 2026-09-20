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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
 *   <li>stale-window writes use an extended frame (new tag, 8-byte
 *       big-endian write timestamp after the version) and a physical expiry
 *       of {@code ttl + staleTtl}, so one GET yields payload and age;</li>
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
 * <p><b>Internal — not part of the supported API.</b> Wire through
 * {@code TierCacheFactory.Builder.remoteCache}/{@code remoteCacheFactory} or
 * the Spring Boot starter.
 *
 * @param <K> key type
 * @param <V> value type
 * @since 0.1.0
 */
public final class LettuceRemoteCache<K, V> implements RemoteCache<K, V>, LockProviderSource, AutoCloseable {

    /**
     * Default connect timeout applied when this transport owns the client
     * (100 ms).
     *
     * @since 0.1.0
     */
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofMillis(100);

    /**
     * Default per-command timeout applied when this transport owns the client
     * (250 ms).
     *
     * @since 0.1.0
     */
    public static final Duration DEFAULT_COMMAND_TIMEOUT = Duration.ofMillis(250);

    /** Tombstone lifetime after evict: bounds the resurrect-protection window. */
    private static final long TOMBSTONE_TTL_MILLIS = 60_000;

    private final RedisClient client;
    private final boolean ownsClient;
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final RedisCommands<byte[], byte[]> commands;
    private final String cacheName;
    private final String journalName;
    private final byte[] keyPrefix;
    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;
    private final RedisStreamJournal journal; // null = unversioned mode
    private io.tiercache.InvalidationMode invalidationMode = io.tiercache.InvalidationMode.INVALIDATE;
    private long payloadCapBytes = 64 * 1024;

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
        this.journalName = builder.journalName != null ? builder.journalName : builder.cacheName;
        this.keyPrefix = (builder.cacheName + ":").getBytes(StandardCharsets.UTF_8);
        this.keySerializer = builder.keySerializer;
        this.valueSerializer = builder.valueSerializer;
        this.journal = builder.journal;
        this.invalidationMode = builder.invalidationMode;
        this.payloadCapBytes = builder.payloadCapBytes;
    }

    /**
     * Starts building a transport that creates and owns its Redis client from
     * {@code redisUri} (e.g. {@code redis://localhost:6379}).
     *
     * @param <K>      key type
     * @param <V>      value type
     * @param redisUri the Redis/Valkey connection URI
     * @return a new builder
     * @since 0.1.0
     */
    public static <K, V> Builder<K, V> builder(String redisUri) {
        return new Builder<>(redisUri);
    }

    @Override
    public StoredEntry<V> get(K key) {
        byte[] bytes = commands.get(namespaced(key));
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return ValueFrame.decode(bytes, valueSerializer);
    }

    @Override
    public void put(K key, StoredEntry<V> entry, Duration ttl) {
        putInternal(key, entry, ttl, null);
    }

    /**
     * Stale-window write: the physical Redis expiry becomes
     * {@code ttl + staleTtl} and the frame carries the write timestamp
     * (extended frame), so a single GET yields both payload and age.
     */
    @Override
    public void put(K key, StoredEntry<V> entry, Duration ttl, Duration staleTtl) {
        putInternal(key, entry, ttl, staleTtl);
    }

    private void putInternal(K key, StoredEntry<V> entry, Duration ttl, Duration staleTtl) {
        if (journal != null && entry.version() != null) {
            // Versioned entries always go through the conditional Lua write
            // so the journal row and the data write stay atomic.
            putIfNewerInternal(key, entry, ttl, staleTtl);
            return;
        }
        commands.set(namespaced(key), encode(entry, staleWindowActive(staleTtl)),
                SetArgs.Builder.px(physicalTtl(ttl, staleTtl)));
    }

    private static boolean staleWindowActive(Duration staleTtl) {
        return staleTtl != null && staleTtl.toMillis() > 0;
    }

    private static Duration physicalTtl(Duration ttl, Duration staleTtl) {
        return staleWindowActive(staleTtl) ? ttl.plus(staleTtl) : ttl;
    }

    /**
     * Version-conditional write with atomic journal append (strict
     * last-write-wins). Requires an attached journal; falls back to plain
     * put in unversioned mode.
     */
    @Override
    public boolean putIfNewer(K key, StoredEntry<V> entry, Duration ttl) {
        return putIfNewerInternal(key, entry, ttl, null);
    }

    private boolean putIfNewerInternal(K key, StoredEntry<V> entry, Duration ttl, Duration staleTtl) {
        if (journal == null || entry.version() == null) {
            putInternal(key, entry, ttl, staleTtl);
            return true;
        }
        Long result = commands.eval(Lua.CONDITIONAL_WRITE, io.lettuce.core.ScriptOutputType.INTEGER,
                new byte[][]{namespaced(key), RedisStreamJournal.streamKeyBytes(journalName)},
                entry.version().toWire().getBytes(StandardCharsets.UTF_8),
                encode(entry, staleWindowActive(staleTtl)),
                String.valueOf(physicalTtl(ttl, staleTtl).toMillis()).getBytes(StandardCharsets.UTF_8),
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
                new byte[][]{namespaced, RedisStreamJournal.streamKeyBytes(journalName)},
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

    /**
     * Namespace-scoped clear plus a journal EVICT_ALL row (not transactional:
     * SCAN can't be).
     *
     * @param version the version stamped on the journal row, or {@code null}
     *                for an unversioned clear
     * @since 0.1.0
     */
    /**
     * Versioned clear: the namespace-scoped clear plus an EVICT_ALL journal
     * row, so reconnect replay heals receivers that missed the live
     * notification.
     *
     * @param version the version stamped on the journal row, or {@code null}
     *                for an unversioned clear
     * @since 1.2.1
     */
    @Override
    public void clear(Version version) {
        clear();
        if (journal != null && version != null) {
            journal.append(journalName, new InvalidationMessage(journalName, null, version,
                    version.instanceId(), InvalidationMessage.Type.EVICT_ALL));
        }
    }

    /**
     * Alias kept for source compatibility; delegates to
     * {@link #clear(Version)}.
     *
     * @param version the version stamped on the journal row, or {@code null}
     *                for an unversioned clear
     * @since 0.1.0
     */
    public void clearWithJournal(Version version) {
        clear(version);
    }

    @Override
    public boolean setIfAbsent(K key, StoredEntry<V> entry, Duration ttl) {
        if (journal == null || entry.version() == null) {
            String result = commands.set(namespaced(key), encode(entry, false), SetArgs.Builder.px(ttl).nx());
            return "OK".equals(result);
        }
        Long result = commands.eval(Lua.SET_IF_ABSENT, io.lettuce.core.ScriptOutputType.INTEGER,
                new byte[][]{namespaced(key), RedisStreamJournal.streamKeyBytes(journalName)},
                encode(entry, false),
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

    private byte[] encode(StoredEntry<V> entry, boolean withWriteTimestamp) {
        return ValueFrame.encode(entry, valueSerializer, withWriteTimestamp);
    }

    // --- Tag registry: tiercache:tags:<cache>:<tag> sets + reverse index
    // tiercache:tagkeys:<cache>:<key>. Redis sets have no per-member TTL,
    // so boundedness comes from two mechanisms (Lua.TAG_ADD/TAG_LIVE_MEMBERS):
    // each tag set carries an extend-only TTL (a shorter-lived entry never
    // shrinks the index, so a set outlives its longest member by a bounded
    // margin and then expires — including tags never touched again), and
    // dead members are reclaimed by janitor sampling on writes and by
    // read-time pruning, each an atomic check-and-remove. The per-key
    // reverse index keeps the data entry's exact TTL. ---

    /** Janitor sample size K per tag write (see the invalidation spec's bound). */
    private static final int TAG_JANITOR_SAMPLE = 8;

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
            long ttlMillis = Math.max(1, ttl.toMillis());
            commands.del(keyTags);
            commands.sadd(keyTags, java.util.Arrays.stream(tags)
                    .map(t -> t.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new));
            commands.pexpire(keyTags, ttlMillis);
            for (String tag : tags) {
                commands.eval(Lua.TAG_ADD, io.lettuce.core.ScriptOutputType.INTEGER,
                        new byte[][]{tagSetKey(tag)},
                        namespaced,
                        String.valueOf(ttlMillis).getBytes(StandardCharsets.UTF_8),
                        String.valueOf(TAG_JANITOR_SAMPLE).getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Override
    public java.util.List<K> keysByTag(String tag) {
        java.util.List<byte[]> live = commands.eval(Lua.TAG_LIVE_MEMBERS,
                io.lettuce.core.ScriptOutputType.MULTI,
                new byte[][]{tagSetKey(tag)});
        java.util.List<K> out = new java.util.ArrayList<>(live.size());
        for (byte[] namespaced : live) {
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
                        + "if tag ~= 2 and tag ~= 3 and tag ~= 4"
                        + " and tag ~= 5 and tag ~= 6 then return nil end "
                        + "local l = string.byte(bytes,2)*16777216 + string.byte(bytes,3)*65536"
                        + " + string.byte(bytes,4)*256 + string.byte(bytes,5) "
                        + "if l == 0 then return nil end "
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

        /**
         * Tag write: add the member, then lift the set TTL extend-only
         * (PTTL -1/-2 both fall below any real TTL, so the comparison
         * covers them; a shorter-lived entry never shrinks the index —
         * the 6.2-compatible emulation of PEXPIRE GT). The janitor runs
         * in the same call: a random member sample, each SREM conditional
         * on the data key being absent — atomically, so a concurrently
         * rewritten key keeps its membership.
         */
        static final String TAG_ADD =
                "redis.call('sadd', KEYS[1], ARGV[1]) "
                        + "local ttl = tonumber(ARGV[2]) "
                        + "if redis.call('pttl', KEYS[1]) < ttl then "
                        + "redis.call('pexpire', KEYS[1], ttl) end "
                        + "local candidates = redis.call('srandmember', KEYS[1], tonumber(ARGV[3])) "
                        + "for _, m in ipairs(candidates) do "
                        + "if redis.call('exists', m) == 0 then redis.call('srem', KEYS[1], m) end "
                        + "end "
                        + "return 1";

        /**
         * Tag read: returns the live members only; dead members are
         * removed inside the same atomic existence check (no
         * EXISTS→SREM race against a concurrent rewrite).
         */
        static final String TAG_LIVE_MEMBERS =
                "local members = redis.call('smembers', KEYS[1]) "
                        + "local live = {} "
                        + "for _, m in ipairs(members) do "
                        + "if redis.call('exists', m) == 1 then live[#live + 1] = m "
                        + "else redis.call('srem', KEYS[1], m) end "
                        + "end "
                        + "return live";
    }

    /**
     * Builder for {@link LettuceRemoteCache}; obtain via
     * {@link LettuceRemoteCache#builder(String)}.
     *
     * @param <K> key type
     * @param <V> value type
     * @since 0.1.0
     */
    public static final class Builder<K, V> {

        private final String redisUri;
        private RedisClient sharedClient;
        private String cacheName = "default";
        private String journalName;
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
         * the client lifecycle, including its timeout options.
         *
         * @param sharedClient the client to reuse
         * @return this builder
         * @since 0.1.0
         */
        @SuppressWarnings("unchecked")
        public Builder<K, V> client(RedisClient sharedClient) {
            this.sharedClient = Objects.requireNonNull(sharedClient, "sharedClient");
            return this;
        }

        /**
         * Namespace for keys (used as {@code <cacheName>:} prefix). Required
         * when several caches share one server.
         *
         * <p>Identity rule: {@code cacheName} controls the L2 data-key layout
         * only. The invalidation journal follows the logical cache name —
         * see {@link #journalName(String)}. Framework integrations that
         * namespace data keys (e.g. {@code spring:users}) must keep the
         * journal name at the logical name ({@code users}) so the write and
         * replay paths address the same journal stream.
         *
         * @param cacheName the cache name
         * @return this builder
         * @since 0.1.0
         */
        public Builder<K, V> cacheName(String cacheName) {
            this.cacheName = Objects.requireNonNull(cacheName, "cacheName");
            return this;
        }

        /**
         * Name of the invalidation journal stream this transport appends to;
         * defaults to {@link #cacheName(String)}. Set this to the logical
         * cache name when {@code cacheName} carries a framework namespace
         * prefix: the data keys and the journal are identified independently,
         * and the recovery path replays the journal by logical name.
         *
         * @param journalName the logical cache name owning the journal stream
         * @return this builder
         * @since 1.2.0
         */
        public Builder<K, V> journalName(String journalName) {
            this.journalName = Objects.requireNonNull(journalName, "journalName");
            return this;
        }

        /**
         * Serializer for keys; defaults to {@link JdkCacheSerializer}.
         *
         * @param keySerializer the key serializer
         * @return this builder
         * @since 0.1.0
         */
        @SuppressWarnings("unchecked")
        public Builder<K, V> keySerializer(CacheSerializer<?> keySerializer) {
            this.keySerializer = (CacheSerializer<K>) Objects.requireNonNull(keySerializer);
            return this;
        }

        /**
         * Serializer for values; defaults to {@link JdkCacheSerializer}.
         *
         * @param valueSerializer the value serializer
         * @return this builder
         * @since 0.1.0
         */
        @SuppressWarnings("unchecked")
        public Builder<K, V> valueSerializer(CacheSerializer<?> valueSerializer) {
            this.valueSerializer = (CacheSerializer<V>) Objects.requireNonNull(valueSerializer);
            return this;
        }

        /**
         * Connect timeout for a client owned by this transport; defaults to
         * {@link #DEFAULT_CONNECT_TIMEOUT}. Ignored when {@link #client} is
         * used.
         *
         * @param connectTimeout the connect timeout
         * @return this builder
         * @since 0.1.0
         */
        public Builder<K, V> connectTimeout(Duration connectTimeout) {
            this.connectTimeout = Objects.requireNonNull(connectTimeout, "connectTimeout");
            return this;
        }

        /**
         * Per-command timeout for a client owned by this transport; defaults
         * to {@link #DEFAULT_COMMAND_TIMEOUT}. Must stay below the business
         * timeout so L2 outages trip the circuit breaker instead of hanging
         * requests. Ignored when {@link #client} is used.
         *
         * @param commandTimeout the per-command timeout
         * @return this builder
         * @since 0.1.0
         */
        public Builder<K, V> commandTimeout(Duration commandTimeout) {
            this.commandTimeout = Objects.requireNonNull(commandTimeout, "commandTimeout");
            return this;
        }

        /**
         * Attaches the invalidation journal: writes become version-conditional
         * with atomic journal rows, and evicts leave versioned tombstones
         * (strict last-write-wins convergence).
         *
         * @param journal the journal to append invalidation rows to
         * @return this builder
         * @since 0.1.0
         */
        public Builder<K, V> journal(RedisStreamJournal journal) {
            this.journal = journal;
            return this;
        }

        /**
         * UPDATE invalidation mode for this transport's cache: journal rows
         * carry the value payload (up to {@code payloadCapBytes}; larger
         * values fall back to INVALIDATE semantics).
         *
         * @param mode            the invalidation mode
         * @param payloadCapBytes maximum serialized UPDATE payload size in
         *                        bytes
         * @return this builder
         * @since 0.1.0
         */
        public Builder<K, V> invalidationMode(io.tiercache.InvalidationMode mode, long payloadCapBytes) {
            this.invalidationMode = mode;
            this.payloadCapBytes = payloadCapBytes;
            return this;
        }

        /**
         * Builds the transport, opening its connection immediately.
         *
         * @return the configured transport
         * @since 0.1.0
         */
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
