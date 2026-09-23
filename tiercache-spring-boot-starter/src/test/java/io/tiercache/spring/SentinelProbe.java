package io.tiercache.spring;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.tiercache.*;
import io.tiercache.redis.*;
import io.tiercache.spi.*;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** Invoked in the isolated Docker network by compatibility/run-sentinel.py. */
public final class SentinelProbe {
    static final Path CONTROL = Path.of("/control");
    static final class Metrics implements CacheMetricsListener {
        final AtomicInteger replayed = new AtomicInteger(), l1Hits = new AtomicInteger();
        public void onRequest(String cache, Outcome outcome) { if (outcome == Outcome.L1_HIT) l1Hits.incrementAndGet(); }
        public void onInvalidation(String cache, Direction direction) {
            if (direction == Direction.REPLAYED) replayed.incrementAndGet();
        }
    }
    static AnnotationConfigApplicationContext application(String id, Metrics metrics) {
        var uri = RedisURI.Builder.sentinel("sentinel0",26379,"mymaster")
                .withSentinel("sentinel1",26379).withSentinel("sentinel2",26379).build();
        var context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("fixture", Map.of(
            "tiercache.enabled", "true", "tiercache.redis-uri", uri.toURI().toString(),
            "tiercache.defaults.l1-expire-after-write", "10m", "tiercache.defaults.l2-ttl", "20m")));
        context.registerBean("fixtureMetrics", CacheMetricsListener.class, () -> metrics);
        context.register(TiercacheAutoConfiguration.class);
        context.refresh();
        System.out.println("APPLICATION_READY " + id);
        return context;
    }
    static void signal(String phase) throws Exception {
        Files.writeString(CONTROL.resolve(phase), "ready");
        System.out.println("PHASE " + phase);
    }
    static void await(String phase) throws Exception {
        long deadline = System.nanoTime()+TimeUnit.SECONDS.toNanos(90);
        while (!Files.exists(CONTROL.resolve(phase))) {
            if (System.nanoTime()>deadline) throw new AssertionError("Timeout waiting for " + phase);
            Thread.sleep(50);
        }
    }
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        System.out.println("RUNTIME jdk="+System.getProperty("java.runtime.version")
            +" lettuce="+RedisClient.class.getPackage().getImplementationVersion()
            +" boot="+org.springframework.boot.SpringBootVersion.getVersion());
        Metrics ma = new Metrics(), mb = new Metrics();
        try (var a = application("a", ma); var b = application("b", mb)) {
            var fa = a.getBean(TierCacheFactory.class); var fb = b.getBean(TierCacheFactory.class);
            TierCache<String,String> ca = fa.getCache("sentinel"), cb = fb.getCache("sentinel");
            ca.put("changed", "v1"); ca.put("unaffected", "stable");
            assertEquals("v1", cb.get("changed")); assertEquals("stable", cb.get("unaffected"));
            if (!mode.equals("outage")) {
                // Persist a versioned update and journal row without publishing it: deterministic
                // missed delivery, recovered through the actual starter's reconnect callback.
                try (var writer = LettuceRemoteCache.<String,String>builder("redis://unused")
                        .client(a.getBean(RedisClient.class)).cacheName("spring:sentinel")
                        .journalName("sentinel").journal(a.getBean(RedisStreamJournal.class)).build()) {
                    writer.put("changed", StoredEntry.ofValue("v2", new Version(
                        System.currentTimeMillis()*1000+1_000_000, UUID.randomUUID())), Duration.ofMinutes(20));
                }
                assertEquals("v1", cb.get("changed"), "L1 deliberately missed this update");
            }
            var journal = a.getBean(RedisStreamJournal.class);
            long changedRowsBefore = journal.readRange("sentinel", "0-0").stream()
                    .filter(row -> "changed".equals(row.message().key())).count();
            var source = new java.util.concurrent.atomic.AtomicReference<>("v1");
            signal("ready");
            var traffic = Executors.newSingleThreadExecutor();
            try {
                var work = traffic.submit(() -> {
                    int count=0; long max=0;
                    while (!Files.exists(CONTROL.resolve("topology-ready"))) {
                        long start=System.nanoTime();
                        String value=ca.getOrCompute("traffic-"+count, k -> "source-v1");
                        assertEquals("source-v1", value);
                        max=Math.max(max,System.nanoTime()-start); count++;
                        assertTrue(max<TimeUnit.SECONDS.toNanos(5), "protected call exceeded five seconds");
                        Thread.sleep(50);
                    }
                    System.out.println("TRAFFIC operations="+count+" maxMillis="+max/1_000_000);
                    assertTrue(count>0);
                    return count;
                });
                if (mode.equals("outage")) {
                    await("offline");
                    // Source has advanced to v2; this eviction cannot reach any Redis server.
                    source.set("v2");
                    ca.evict("changed");
                    System.out.println("OFFLINE sourceVersion="+source.get()+" observedByB="+cb.get("changed"));
                    signal("offline-written");
                }
                work.get(100,TimeUnit.SECONDS);
            } finally { traffic.shutdownNow(); assertTrue(traffic.awaitTermination(5,TimeUnit.SECONDS)); }
            if (mode.equals("outage")) {
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
                do {
                    ca.getOrCompute("recover-a-"+System.nanoTime(), k -> "probe");
                    cb.getOrCompute("recover-b-"+System.nanoTime(), k -> "probe");
                    if (fa.breakerState() == BreakerState.CLOSED && fb.breakerState() == BreakerState.CLOSED) break;
                    Thread.sleep(100);
                } while (System.nanoTime()<deadline);
                assertEquals(BreakerState.CLOSED, fa.breakerState()); assertEquals(BreakerState.CLOSED, fb.breakerState());
                assertEquals(changedRowsBefore, journal.readRange("sentinel", "0-0").stream()
                    .filter(row -> "changed".equals(row.message().key())).count(), "offline eviction was not journaled");
                String observed=cb.get("changed");
                System.out.println("RECOVERED sourceVersion="+source.get()+" observedByB="+observed
                    +" changedJournalRows="+changedRowsBefore);
                assertNotEquals("v2", observed, "unstored source update cannot be reconstructed from the journal");
            } else {
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
                String observed;
                do {
                    observed=cb.get("changed");
                    // Both breakers must complete recovery before asserting a new distributed write.
                    ca.getOrCompute("recovery-probe-"+System.nanoTime(), k -> "probe");
                    if ("v2".equals(observed) && mb.replayed.get()>0 && fb.breakerState() == BreakerState.CLOSED && fa.breakerState() == BreakerState.CLOSED) break;
                    Thread.sleep(100);
                } while(System.nanoTime()<deadline);
                assertEquals("v2",observed);
                assertTrue(mb.replayed.get()>0,"retained history must actually replay");
                int hits = mb.l1Hits.get();
                assertEquals("stable",cb.get("unaffected"));
                assertEquals(hits+1, mb.l1Hits.get(), "verified-history recovery preserves unaffected L1 entries");
                assertEquals(BreakerState.CLOSED, fb.breakerState()); assertEquals(BreakerState.CLOSED, fa.breakerState());
                // New writes, locks (getOrCompute), journal and cross-instance delivery after promotion.
                ca.put("after", "v2");
                assertEquals("v2", cb.get("after"));
                ca.put("after", "v3");
                deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
                while(!"v3".equals(cb.get("after")) && System.nanoTime()<deadline) Thread.sleep(50);
                assertEquals("v3",cb.get("after"));
                assertTrue(a.getBean(RedisStreamJournal.class).size("sentinel")>0);
                assertEquals("computed",ca.getOrCompute("rebuild", k -> "computed"));
                var lockA = a.getBean(LettuceLockProvider.class).tryLock("sentinel-proof", Duration.ofSeconds(10));
                assertNotNull(lockA, "first starter lock connection recovered");
                try {
                    var lockB = b.getBean(LettuceLockProvider.class).tryLock("sentinel-proof", Duration.ofSeconds(10));
                    try { assertNull(lockB, "both lock connections must coordinate on the elected primary"); }
                    finally { if (lockB != null) lockB.release(); }
                } finally { lockA.release(); }
                System.out.println("RECOVERED observedVersion="+observed+" replayed="+mb.replayed.get());
            }
            signal("passed");
        }
    }
}
