package io.tiercache;

import io.tiercache.internal.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class AcknowledgedWriteTest {
    static final Duration TTL = Duration.ofMinutes(1);
    static void forget(DefaultTierCache<?,?> cache) {
        try {
            var f = DefaultTierCache.class.getDeclaredField("l1Metas"); f.setAccessible(true);
            var map=f.get(cache); var m=map.getClass().getDeclaredMethod("invalidate",Object.class);
            m.setAccessible(true); m.invoke(map,"other");
        } catch (ReflectiveOperationException e) { throw new AssertionError(e); }
    }
    static final class Remote implements RemoteCache<String,String> {
        final InMemoryRemoteCache<String,String> data = new InMemoryRemoteCache<>();
        final AtomicReference<Runnable> afterWrite = new AtomicReference<>();
        int writes, reads; boolean unavailable, reject;
        void completed() { var action=afterWrite.getAndSet(null); if(action!=null) action.run(); }
        public StoredEntry<String> get(String k) { reads++; return data.get(k); }
        public void put(String k,StoredEntry<String> e,Duration ttl) {
            writes++; if(unavailable) { completed(); throw L2UnavailableException.OPEN; }
            data.put(k,e,ttl); completed();
        }
        public boolean putIfNewer(String k,StoredEntry<String> e,Duration ttl) {
            if(reject) { writes++; completed(); return false; } put(k,e,ttl); return true;
        }
        public boolean putIfNewer(String k,StoredEntry<String> e,Duration ttl,Duration stale) { return putIfNewer(k,e,ttl.plus(stale)); }
        public void put(String k,StoredEntry<String> e,Duration ttl,Duration stale) { put(k,e,ttl.plus(stale)); }
        public boolean setIfAbsent(String k,StoredEntry<String> e,Duration ttl) {
            writes++; boolean won=data.setIfAbsent(k,e,ttl); completed(); return won;
        }
        public boolean supportsTaggedWriteOutcomes() { return true; }
        public TaggedWriteOutcome putTaggedIfNewer(String k,StoredEntry<String> e,Duration ttl,String[] tags) {
            if(reject) { writes++; completed(); return TaggedWriteOutcome.LOST; }
            put(k,e,ttl); return TaggedWriteOutcome.WON;
        }
        public void evict(String k) { data.evict(k); }
        public void clear() { data.clear(); }
    }
    static final class Rig {
        final Remote remote=new Remote(); final LocalCache<String,String> local;
        final AtomicInteger publications=new AtomicInteger(); final Queue<Runnable> refreshes=new ArrayDeque<>();
        final DefaultTierCache<String,String> cache;
        Rig(boolean stock,boolean retention,boolean versioned,boolean refresh) {
            var settings=new CacheSettings(100,TTL,null,TTL,0,NullPolicy.allow(TTL),InvalidationMode.INVALIDATE,65536,
                    refresh?Duration.ofMinutes(2):Duration.ZERO,false,Duration.ofSeconds(1),retention?TTL:Duration.ZERO);
            local=stock?new CaffeineLocalCache<>(settings):new CountingLocalCache<>();
            InvalidationHandler handler=new InvalidationHandler() {
                public void onLocalWrite(String c,Object k,Version v,InvalidationMessage.Type t) { publications.incrementAndGet(); }
                public void registerTarget(String c,InvalidationTarget t) { }
                public void onL2Recovery() { }
                public void close() { }
            };
            cache=new DefaultTierCache<>("c",local,remote,settings,true,null,null,
                    versioned?new VersionGenerator():null,handler,null,CacheMetricsListener.NOOP,refreshes::add);
            cache.put("k","old"); cache.put("other","old"); remote.writes=remote.reads=0; publications.set(0);
        }
    }
    @ParameterizedTest @ValueSource(strings={"put","marker","insert","tagged","load","refresh"})
    void everyAcceptedStoreDiscardsOlderLocalValueAfterTrueBarrierLoss(String operation) {
        for(boolean stock:new boolean[]{false,true}) for(boolean retention:new boolean[]{false,true}) {
            var h=new Rig(stock,retention,true,operation.equals("refresh"));
            var old=h.local.get("k");
            if(operation.equals("insert") || operation.equals("load")) h.remote.data.evict("k");
            if(operation.equals("load") || operation.equals("refresh")) h.local.evict("k");
            if(operation.equals("refresh")) h.remote.data.put("k",StoredEntry.ofValue("old",old.version(),System.currentTimeMillis()-70_000),Duration.ofHours(1));
            h.remote.afterWrite.set(() -> {
                if(operation.equals("load") || operation.equals("refresh")) h.local.put("k",old,TTL);
                forget(h.cache);
            });
            switch(operation) {
                case "put" -> h.cache.put("k","new");
                case "marker" -> h.cache.putNull("k");
                case "insert" -> assertTrue(h.cache.putIfAbsent("k","new"));
                case "tagged" -> h.cache.put("k","new","tag");
                case "load" -> assertEquals("new",h.cache.getOrCompute("k",k->"new"));
                case "refresh" -> { assertEquals("old",h.cache.getOrCompute("k",k->"new")); assertEquals(1,h.refreshes.size()); h.refreshes.remove().run(); }
            }
            assertEquals(1,h.remote.writes); assertEquals(1,h.publications.get());
            assertNull(h.local.get("k"),operation+": refused fill must remove old value");
            h.cache.applyUpdateL1("k","obsolete",new Version(old.version().sequence()-1,old.version().instanceId()));
            assertNull(h.local.get("k"), "cleanup must retain the surviving barrier");
            assertEquals(operation.equals("marker")?null:"new",h.cache.get("k"));
            assertEquals(operation.equals("marker"),h.remote.data.get("k").isNullMarker());
        }
    }
    @ParameterizedTest @ValueSource(strings={"newer","equal","clear","absent","unversioned"})
    void cleanupPreservesConcurrentStateAndDoesNotReinstallAfterClear(String state) throws Exception {
        var h=new Rig(true,true,true,false); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        h.remote.afterWrite.set(() -> { entered.countDown(); try { assertTrue(release.await(5,TimeUnit.SECONDS)); } catch(InterruptedException e) { throw new AssertionError(e); } });
        var pool=Executors.newSingleThreadExecutor();
        try {
            var put=pool.submit(() -> h.cache.put("k","new")); assertTrue(entered.await(5,TimeUnit.SECONDS));
            StoredEntry<String> preserved=null;
            switch(state) {
                case "newer" -> { h.cache.put("k","newest"); preserved=h.local.get("k"); }
                case "equal" -> { var accepted=h.remote.data.get("k"); h.cache.applyUpdateL1("k",accepted.value(),accepted.version()); preserved=h.local.get("k"); }
                case "clear" -> h.cache.evictAllL1();
                case "absent" -> h.local.evict("k");
                case "unversioned" -> h.local.put("k",StoredEntry.ofValue("legacy"),TTL);
            }
            forget(h.cache); release.countDown(); put.get(5,TimeUnit.SECONDS);
            if(preserved!=null) { assertSame(preserved,h.local.get("k")); assertNotNull(preserved.localFreshness()); }
            else assertNull(h.local.get("k"));
        } finally { release.countDown(); pool.shutdownNow(); }
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void versionlessCleanupUsesPreOperationIdentity(boolean replace) {
        var h=new Rig(false,false,false,false);
        var original=h.local.get("k"); var newer=StoredEntry.ofValue("concurrent");
        // Versionless caches have no barriers: use a clear epoch while retaining/replacing a value afterward.
        h.remote.afterWrite.set(() -> { h.cache.evictAllL1(); h.local.put("k",replace?newer:original,TTL); });
        h.cache.put("k","new");
        if(replace) assertSame(newer,h.local.get("k")); else assertNull(h.local.get("k"));
        assertEquals("new",h.remote.data.get("k").value()); assertEquals(1,h.remote.writes);
    }
    @Test void versionedWriteDoesNotReadL1BeforeRemoteExecution() {
        var h=new Rig(false,false,true,false); var local=(CountingLocalCache<String,String>)h.local; local.gets.set(0);
        h.remote.afterWrite.set(() -> assertEquals(0,local.gets.get())); h.cache.put("k","new");
    }
    @Test void failedRemoteAttemptDoesNotAcquireAcknowledgedCleanupPolicy() {
        var h=new Rig(false,true,true,false); var old=h.local.get("k");
        h.remote.unavailable=true; h.remote.afterWrite.set(() -> forget(h.cache)); h.cache.put("k","new");
        assertSame(old,h.local.get("k")); assertEquals(0,h.publications.get()); assertEquals("old",h.remote.data.get("k").value());
    }
}
