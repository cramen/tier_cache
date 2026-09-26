package io.tiercache.tck;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;
import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.testkit.CountingLocalCache;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;
class ConvergenceDiagnosticTest extends AbstractInvalidationChaosTest {
    @Test void successfulRedisWriteMustNotLeaveOldLocalValue() throws Exception {
        try (var server = startServer(DockerImageName.parse("redis:6.2-alpine"))) {
            String uri = uri(server); var side = new Side(io.lettuce.core.RedisClient.create(uri),uri,1000);
            try {
                var hook = new java.util.concurrent.atomic.AtomicReference<Runnable>();
                RemoteCache<String,String> remote = new RemoteCache<>() {
                    public StoredEntry<String> get(String key) { return side.l2.get(key); }
                    public void put(String k, StoredEntry<String> v, Duration ttl) { side.l2.put(k,v,ttl); }
                    public boolean putIfNewer(String k, StoredEntry<String> v, Duration ttl) {
                        boolean accepted = side.l2.putIfNewer(k,v,ttl);
                        var after = hook.getAndSet(null); if (after != null) after.run();
                        return accepted;
                    }
                    public boolean setIfAbsent(String k, StoredEntry<String> v, Duration ttl) { return side.l2.setIfAbsent(k,v,ttl); }
                    public void evict(String key) { side.l2.evict(key); }
                    public void clear() { side.l2.clear(); }
                };
                var local = new io.tiercache.internal.CaffeineLocalCache<String,String>(CacheSettings.defaults());
                var cache = new DefaultTierCache<String,String>(CACHE, local, remote, CacheSettings.defaults(),
                        true, null, null, new VersionGenerator(), null);
                cache.put("k","old"); cache.put("other","old");
                hook.set(() -> cache.put("other","new")); cache.put("k","new");
                assertEquals("new",side.l2.get("k").value()); assertEquals("new",cache.get("k"));
            } finally { side.close(); }
        }
    }
}
