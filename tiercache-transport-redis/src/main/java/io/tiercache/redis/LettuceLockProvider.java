package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.tiercache.spi.DistributedLock;
import io.tiercache.spi.DistributedLockProvider;

import java.time.Duration;
import java.util.UUID;

/**
 * {@link DistributedLockProvider} over Redis/Valkey via Lettuce, used for
 * cluster-wide rebuild coordination.
 *
 * <p>Acquire: {@code SET name token PX lease NX}. Release: Lua
 * compare-and-delete on the ownership token, so a stale holder can never
 * release another's lock. Extend: Lua token-checked {@code PEXPIRE ... XX}.
 * Locks live in the {@code tiercache:rebuild:*} keyspace, separate from data
 * entries.
 *
 * <p><b>Incubating:</b> 0.x API, may change before the public API freeze.
 */
public final class LettuceLockProvider implements DistributedLockProvider {

    /** Keyspace prefix for rebuild locks (never collides with data keys). */
    public static final String LOCK_KEYSPACE = "tiercache:rebuild:";

    private static final String RELEASE_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1])"
                    + " else return 0 end";

    // Token check implies the key exists, so plain PEXPIRE suffices
    // (PEXPIRE ... XX requires Redis 7.0; our baseline is 6.2).
    private static final String EXTEND_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1],"
                    + " ARGV[2]) else return 0 end";

    private final RedisCommands<String, String> commands;

    public LettuceLockProvider(RedisClient client) {
        this(client.connect());
    }

    public LettuceLockProvider(StatefulRedisConnection<String, String> connection) {
        this.commands = connection.sync();
    }

    @Override
    public DistributedLock tryLock(String name, Duration lease) {
        String token = UUID.randomUUID().toString();
        String result = commands.set(LOCK_KEYSPACE + name, token,
                SetArgs.Builder.px(lease).nx());
        return "OK".equals(result) ? new LettuceLock(LOCK_KEYSPACE + name, token) : null;
    }

    private final class LettuceLock implements DistributedLock {
        private final String fullName;
        private final String token;

        LettuceLock(String fullName, String token) {
            this.fullName = fullName;
            this.token = token;
        }

        @Override
        public boolean extend(Duration lease) {
            Long extended = commands.eval(EXTEND_SCRIPT, ScriptOutputType.INTEGER,
                    new String[]{fullName}, token, String.valueOf(lease.toMillis()));
            return extended != null && extended == 1L;
        }

        @Override
        public void release() {
            commands.eval(RELEASE_SCRIPT, ScriptOutputType.INTEGER,
                    new String[]{fullName}, token);
        }
    }
}
