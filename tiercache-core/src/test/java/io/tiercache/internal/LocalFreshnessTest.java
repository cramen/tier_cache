package io.tiercache.internal;

import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class LocalFreshnessTest {
    static CacheSettings settings(long ttl, Long access, long window, NullPolicy policy) {
        return new CacheSettings(100, Duration.ofMinutes(ttl), access == null ? null : Duration.ofMinutes(access),
                Duration.ofHours(4), 0, policy, InvalidationMode.INVALIDATE, 65536,
                Duration.ZERO, false, Duration.ofSeconds(1), Duration.ofMinutes(window));
    }
    static final class Rig {
        final AtomicLong now = new AtomicLong(1);
        final AtomicInteger stale = new AtomicInteger();
        final CaffeineLocalCache<String,String> l1;
        final InMemoryRemoteCache<String,String> l2 = new InMemoryRemoteCache<>();
        final CircuitBreaker breaker = new CircuitBreaker(new CircuitBreaker.Config(1,1,1,Duration.ofDays(1),1),
                new CircuitBreaker.Listener(){public void onOpen(){} public void onClose(){}});
        final DefaultTierCache<String,String> cache;
        Rig(CacheSettings settings) {
            l1 = new CaffeineLocalCache<>(settings, now::get);
            cache = new DefaultTierCache<>("c",l1,new CircuitBreakerRemoteCache<>(l2,breaker),settings,true,
                    null,null,new VersionGenerator(),null,breaker,new CacheMetricsListener(){
                        public void onRequest(String c,Outcome outcome){if(outcome==Outcome.STALE_DEGRADED)stale.incrementAndGet();}
                    },null,new TtlJitter(),()->true,now::get);
        }
        void minute(long value){now.set(1+Duration.ofMinutes(value).toNanos());}
        @SuppressWarnings("unchecked") L1BarrierMap<String> fences() throws Exception {
            var f=DefaultTierCache.class.getDeclaredField("l1Metas");f.setAccessible(true);return (L1BarrierMap<String>)f.get(cache);
        }
    }
    static String noLoad(String key){throw new AssertionError("unexpected source call: "+key);}

    @Test void thirtyMinuteValueRemainsFreshAfterIndependentFenceExpires() throws Exception {
        var r=new Rig(settings(30,null,30,NullPolicy.deny()));r.cache.put("x","value");r.breaker.onFailure();
        r.minute(11);assertNull(r.fences().get("x"));
        r.minute(29);assertEquals("value",r.cache.getOrCompute("x",LocalFreshnessTest::noLoad));assertEquals(0,r.stale.get());
        r.minute(30);assertEquals("value",r.cache.getOrCompute("x",LocalFreshnessTest::noLoad));assertEquals(1,r.stale.get());
        r.minute(60);assertEquals("new",r.cache.getOrCompute("x",k->"new"));
    }
    @Test void freshAccessSlidesBeyondOldHorizonWhileHotStaleReadsNeverSlide() {
        var r=new Rig(settings(20,20L,30,NullPolicy.deny()));r.cache.put("x","value");r.breaker.onFailure();
        r.minute(15);assertEquals("value",r.cache.get("x"));
        r.minute(30);assertEquals("value",r.cache.get("x"));
        var snapshot=r.l1.get("x").localFreshness();
        assertEquals(1+Duration.ofMinutes(50).toNanos(),snapshot.logicalDeadlineNanos());
        for(int minute=50;minute<80;minute++) {r.minute(minute);assertEquals("value",r.cache.getOrCompute("x",LocalFreshnessTest::noLoad));assertSame(snapshot,r.l1.get("x").localFreshness());}
        r.minute(80);assertNull(r.cache.get("x"));assertEquals(30,r.stale.get());
    }
    @Test void shortAccessDoesNotShortenStoreFloorOrExtendStaleCutoff() {
        var r=new Rig(settings(30,2L,10,NullPolicy.deny()));r.cache.put("x","value");r.breaker.onFailure();
        r.minute(1);assertEquals("value",r.cache.get("x"));
        var state=r.l1.get("x").localFreshness();
        assertEquals(1+Duration.ofMinutes(40).toNanos(),state.retentionUntilNanos());
        r.minute(12);assertEquals("value",r.cache.get("x"));
        r.minute(13);assertNotNull(r.l1.get("x"));assertNull(r.cache.get("x"));
        r.minute(39);assertNotNull(r.l1.get("x"));r.minute(40);assertNull(r.l1.get("x"));
    }
    @Test void fencesStayBoundedWithoutErasingFreshnessOrCurrentVersion() throws Exception {
        var r=new Rig(settings(30,null,10,NullPolicy.deny()));r.cache.put("x","value");
        var old=r.l1.get("x");
        var field=DefaultTierCache.class.getDeclaredField("l1Generation");field.setAccessible(true);var generation=(AtomicLong)field.get(r.cache);
        var fences=new L1BarrierMap<String>(2,Duration.ofMinutes(10),generation::incrementAndGet,r.now::get);
        field=DefaultTierCache.class.getDeclaredField("l1Metas");field.setAccessible(true);field.set(r.cache,fences);
        long before=generation.get();
        UUID origin=UUID.randomUUID();
        for(int i=0;i<1000;i++) r.cache.evictL1IfNewer("absent-"+i,new Version(i+1,origin));
        var rawField=L1BarrierMap.class.getDeclaredField("barriers");rawField.setAccessible(true);
        // PIT may load the shaded core artifact; use its declared cache interface.
        var raw=rawField.get(fences);var cacheType=rawField.getType();
        cacheType.getMethod("cleanUp").invoke(raw);
        assertTrue((Long)cacheType.getMethod("estimatedSize").invoke(raw)<=2);assertTrue(generation.get()>before);
        r.breaker.onFailure();assertEquals("value",r.cache.getOrCompute("x",LocalFreshnessTest::noLoad));
        var commit=DefaultTierCache.class.getDeclaredMethod("commitL1",Object.class,StoredEntry.class,Duration.class,long.class);commit.setAccessible(true);
        assertEquals(false,commit.invoke(r.cache,"x",StoredEntry.ofValue("obsolete",new Version(1,origin)),Duration.ofMinutes(30),generation.get()));
        assertEquals(false,commit.invoke(r.cache,"absent-old",old,Duration.ofMinutes(30),before));
        assertSame(old,r.l1.get("x"));
    }
    @Test void markerOwnsItsTtlAndReplacementNeverInheritsPreviousDeadlines() {
        var r=new Rig(settings(30,null,10,NullPolicy.allow(Duration.ofMinutes(2))));r.cache.put("x","value");
        var value=r.l1.get("x");r.minute(1);r.cache.putNull("x");var marker=r.l1.get("x");
        assertNotSame(value.localFreshness(),marker.localFreshness());assertTrue(marker.isNullMarker());
        r.breaker.onFailure();r.minute(4);assertNull(r.cache.getOrCompute("x",LocalFreshnessTest::noLoad));
        r.minute(13);assertEquals("new",r.cache.getOrCompute("x",k->"new"));
        r.cache.evictAll();assertNull(r.l1.get("x"));assertEquals("after-clear",r.cache.getOrCompute("x",k->"after-clear"));
        r.l1.evict("x");assertNull(r.cache.get("x"));
    }
    @Test void losingPutIfAbsentAndDuplicateUpdateDoNotExtendDeadline() {
        var r=new Rig(settings(30,null,10,NullPolicy.deny()));r.cache.put("x","value");var entry=r.l1.get("x");
        r.minute(5);assertFalse(r.cache.putIfAbsent("x","loser"));assertSame(entry,r.l1.get("x"));
        r.cache.applyUpdateL1("x","duplicate",entry.version());assertSame(entry,r.l1.get("x"));
        var next=new Version(entry.version().sequence()+1,entry.version().instanceId());
        r.cache.applyUpdateL1("x","update",next);assertEquals("update",r.l1.get("x").value());assertNotSame(entry.localFreshness(),r.l1.get("x").localFreshness());
    }
    @Test void sharedMarkersAndRemoteEntriesReceiveIndependentLocalCopies() {
        var a=new Rig(settings(30,null,10,NullPolicy.allow(Duration.ofMinutes(2))));
        var b=new Rig(settings(30,null,20,NullPolicy.allow(Duration.ofMinutes(3))));
        var shared=StoredEntry.<String>nullMarker();a.l2.put("x",shared,Duration.ofHours(1));b.l2.put("x",shared,Duration.ofHours(1));
        a.cache.get("x");b.cache.get("x");
        assertNull(shared.localFreshness());assertNotSame(shared,a.l1.get("x"));assertNotSame(a.l1.get("x"),b.l1.get("x"));
        assertNotEquals(a.l1.get("x").localFreshness(),b.l1.get("x").localFreshness());
        assertNotEquals(StoredEntry.ofValue("same"),StoredEntry.ofValue("same"));
    }
    @Test void defaultOffKeepsOpaqueEntryAndLegacyAccessExpiry() {
        var r=new Rig(settings(30,20L,0,NullPolicy.deny()));r.cache.put("x","value");r.breaker.onFailure();
        assertNull(r.l1.get("x").localFreshness());r.minute(15);assertEquals("value",r.cache.get("x"));
        r.minute(30);assertEquals("value",r.cache.get("x"));r.minute(51);assertNull(r.cache.get("x"));assertEquals(0,r.stale.get());
    }
    @Test void failedAtomicReplacementDoesNotRefreshAnotherValue() {
        var now=new AtomicLong(1);var config=settings(30,20L,10,NullPolicy.deny());
        var l1=new CaffeineLocalCache<String,String>(config,now::get);
        var old=StoredEntry.ofValue("old");var current=StoredEntry.ofValue("current");
        l1.put("x",current,Duration.ofSeconds(5));now.addAndGet(Duration.ofSeconds(4).toNanos());
        assertFalse(l1.replaceIfSame("x",old,StoredEntry.ofValue("candidate"),Duration.ofMinutes(30)));
        assertSame(current,l1.get("x"));now.addAndGet(Duration.ofSeconds(1).toNanos());assertNull(l1.get("x"));
        assertFalse(l1.replaceIfSame("x",current,old,Duration.ofMinutes(30)));assertNull(l1.get("x"));
    }
    @Test void customOpaqueProviderWorksExceptUnsupportedSlidingCombination() {
        var retained=new CountingLocalCache<String,String>();
        LocalCache<String,String> legacy=new LocalCache<>() {
            public StoredEntry<String> get(String k){return retained.get(k);}
            public void put(String k,StoredEntry<String> v,Duration ttl){retained.put(k,v,ttl);}
            public boolean setIfAbsent(String k,StoredEntry<String> v,Duration ttl){return retained.setIfAbsent(k,v,ttl);}
            public void evict(String k){retained.evict(k);}
            public void clear(){retained.clear();}
        };
        assertFalse(legacy.supportsAtomicReplace());
        assertThrows(UnsupportedOperationException.class,()->legacy.replaceIfSame("x",StoredEntry.nullMarker(),StoredEntry.nullMarker(),Duration.ofSeconds(1)));
        var error=assertThrows(IllegalArgumentException.class,()->new DefaultTierCache<>("custom-l1",legacy,
                new InMemoryRemoteCache<String,String>(),settings(30,20L,10,NullPolicy.deny()),true,null,null,null,null));
        assertTrue(error.getMessage().contains("custom-l1"));assertTrue(error.getMessage().contains("atomic replacement"));
        var cache=new DefaultTierCache<>("custom-l1",legacy,new InMemoryRemoteCache<String,String>(),settings(30,null,10,NullPolicy.deny()),true,null,null,null,null);
        cache.put("x","value");assertNotNull(retained.get("x").localFreshness());assertEquals("value",cache.get("x"));
        assertDoesNotThrow(()->new DefaultTierCache<>("off",legacy,new InMemoryRemoteCache<String,String>(),settings(30,20L,0,NullPolicy.deny()),true,null,null,null,null));
    }
    @Test void oldRemoteTimestampDoesNotShortenNewLocalFreshness() {
        var r=new Rig(settings(30,null,10,NullPolicy.deny()));
        r.l2.put("x",StoredEntry.ofValue("remote",new Version(1,UUID.randomUUID()),0),Duration.ofHours(1));
        r.minute(30);assertEquals("remote",r.cache.get("x"));r.breaker.onFailure();
        r.minute(59);assertEquals("remote",r.cache.getOrCompute("x",LocalFreshnessTest::noLoad));assertEquals(0,r.stale.get());
        r.minute(60);assertEquals("remote",r.cache.getOrCompute("x",LocalFreshnessTest::noLoad));assertEquals(1,r.stale.get());
    }

}
