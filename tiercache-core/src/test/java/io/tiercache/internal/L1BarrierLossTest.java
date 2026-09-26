package io.tiercache.internal;
import io.tiercache.Version;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
class L1BarrierLossTest {
    static final UUID WRITER = UUID.randomUUID();
    static Version version(int n) { return new Version(n, WRITER); }
    static void clean(L1BarrierMap<?> map) throws Exception {
        var f = L1BarrierMap.class.getDeclaredField("barriers"); f.setAccessible(true);
        Object cache = f.get(map); var m = cache.getClass().getMethod("cleanUp"); m.setAccessible(true); m.invoke(cache);
    }
    @Test void replacementsNeverForgetProtection() throws Exception {
        var lost = new AtomicInteger(); var map = new L1BarrierMap<String>(10, Duration.ofMinutes(1), lost::incrementAndGet);
        map.put("k",version(1)); map.put("k",version(2)); map.put("k",version(2)); map.put("k",version(1)); clean(map);
        assertEquals(version(2), map.get("k").highestSeen()); assertEquals(0,lost.get());
        map.invalidate("k"); assertEquals(1,lost.get());
        map.put("a",version(1)); map.put("b",version(1)); map.invalidateAll(); clean(map);
        assertEquals(3,lost.get());
    }
    @Test void expiryAndSizeRemovalStillForgetProtection() throws Exception {
        var clock = new AtomicLong(); var expired = new AtomicInteger();
        var map = new L1BarrierMap<String>(10,Duration.ofNanos(10),expired::incrementAndGet,clock::get);
        map.put("k",version(1)); clock.set(11); assertNull(map.get("k")); clean(map); assertEquals(1,expired.get());
        var evicted = new AtomicInteger(); var bounded = new L1BarrierMap<String>(1,Duration.ofMinutes(1),evicted::incrementAndGet);
        bounded.put("a",version(1)); bounded.put("b",version(2)); clean(bounded);
        assertTrue(evicted.get() >= 1); assertTrue(bounded.get("a") == null || bounded.get("b") == null);
    }
}
