package io.tiercache.invalidation;

import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class RecoveryMonitorTest {
    @Test void parkedJournalDoesNotHoldCacheMonitorOrCallbackThread() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var returned = new CountDownLatch(1); var held = new AtomicBoolean();
        var owner = new AtomicReference<InvalidationService>();
        InvalidationJournal journal = new InvalidationJournal() {
            public String append(String c, InvalidationMessage m) { return "0"; }
            public List<JournalRow> readRange(String c, String cursor) { return List.of(); }
            public String endCursor(String c) { return "0"; }
            public boolean isTrimmed(String c, String cursor) { return false; }
            public CheckedRange checkedRead(String c, String cursor, int count) {
                try {
                    java.lang.reflect.Field field;
                    try { field = InvalidationService.class.getDeclaredField("states"); }
                    catch (NoSuchFieldException e) { field = InvalidationService.class.getDeclaredField("cacheLocks"); }
                    field.setAccessible(true);
                    Object gate = ((Map<?, ?>) field.get(owner.get())).get(c);
                    held.set(Thread.holdsLock(gate)); entered.countDown();
                    if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("journal gate timeout");
                    return new CheckedRange(true, List.of());
                } catch (ReflectiveOperationException | InterruptedException e) { throw new AssertionError(e); }
            }
        };
        var transport = new InMemoryInvalidationTransport(new InMemoryInvalidationTransport.Hub());
        var service = new InvalidationService(transport, journal, UUID.randomUUID(), InvalidationListener.NOOP);
        owner.set(service);
        service.registerTarget("c", new InvalidationTarget() {
            public Version versionOfL1Entry(Object key) { return null; }
            public void evictL1IfNewer(Object key, Version version) { }
            public void evictAllL1() { }
        });
        Thread callback = new Thread(() -> { service.onL2Recovery(); returned.countDown(); }, "transport-callback");
        callback.start();
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertFalse(held.get(), "journal I/O must not own the cache state monitor");
            assertTrue(returned.await(1, TimeUnit.SECONDS), "callback must return before journal I/O completes");
        } finally { release.countDown(); callback.join(5000); service.close(); }
    }
}
