package io.tiercache;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.*;
import io.tiercache.testkit.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;
class CommittedWriteCoherenceTest {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void unrelatedBarrierChangeCannotLeaveOldValueAfterSuccessfulPut(boolean remove) {
        var data = new InMemoryRemoteCache<String,String>();
        var afterCommit = new java.util.concurrent.atomic.AtomicReference<Runnable>();
        RemoteCache<String,String> remote = new RemoteCache<>() {
            public StoredEntry<String> get(String k) { return data.get(k); }
            public void put(String k, StoredEntry<String> v, Duration ttl) {
                data.put(k,v,ttl);
                var hook = afterCommit.getAndSet(null); if (hook != null) hook.run();
            }
            public void evict(String k) { data.evict(k); }
            public void clear() { data.clear(); }
            public boolean setIfAbsent(String k, StoredEntry<String> v, Duration ttl) { return data.setIfAbsent(k,v,ttl); }
        };
        var local = new CountingLocalCache<String,String>();
        var cache = new DefaultTierCache<String,String>("c", local, remote, CacheSettings.defaults(),
                true, null, null, new VersionGenerator(), null);
        cache.put("k", "old"); cache.put("other", "old");
        afterCommit.set(() -> {
            if (!remove) { cache.put("other", "new"); return; }
            try {
                var field = DefaultTierCache.class.getDeclaredField("l1Metas"); field.setAccessible(true);
                var map = field.get(cache); var invalidate = map.getClass().getDeclaredMethod("invalidate", Object.class);
                invalidate.setAccessible(true); invalidate.invoke(map, "other");
            } catch (ReflectiveOperationException e) { throw new AssertionError(e); }
        });
        cache.put("k", "new");
        assertEquals("new", data.get("k").value());
        assertEquals("new", cache.get("k"));
    }
}
