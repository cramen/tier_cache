package io.tiercache.internal;

import io.tiercache.*;
import io.tiercache.spi.StoredEntry;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

/** Exact-clock contracts shared by the previous engine path and the atomic provider. */
class AtomicFreshnessContractTest {
    @Test void staleAndExpiredReadsNeverMoveThePhysicalFloor() {
        var r = new LocalFreshnessTest.Rig(LocalFreshnessTest.settings(30, 2L, 10, NullPolicy.deny()));
        r.cache.put("x", "value"); r.minute(1); assertEquals("value", r.cache.get("x"));
        var lifetime = r.l1.get("x").localFreshness();
        r.breaker.onFailure();
        for (int minute = 3; minute < 40; minute++) {
            r.minute(minute);
            assertEquals(minute < 13 ? "value" : null, r.cache.get("x"));
            assertSame(lifetime, r.l1.get("x").localFreshness());
        }
        r.minute(40); assertNull(r.l1.get("x"));
    }

    @Test void unknownMetadataDoesNotAcquireAnotherPhysicalLifetime() {
        var r = new LocalFreshnessTest.Rig(LocalFreshnessTest.settings(30, 2L, 10, NullPolicy.deny()));
        r.l1.put("x", StoredEntry.ofValue("unknown"), Duration.ofMinutes(50));
        r.breaker.onFailure();
        for (int minute = 1; minute < 50; minute++) {
            r.minute(minute); assertNull(r.cache.get("x")); assertNotNull(r.l1.get("x"));
        }
        r.minute(50); assertNull(r.l1.get("x"));
    }

    @Test void shortMarkerReplacesTheValuesFloorAndStaleReadsNeverSlide() {
        var r = new LocalFreshnessTest.Rig(LocalFreshnessTest.settings(30, 2L, 10,
                NullPolicy.allow(Duration.ofMinutes(2))));
        r.cache.put("x", "old"); r.minute(1); r.cache.putNull("x");
        r.minute(2); assertEquals(LookupResult.cachedNull(), r.cache.lookup("x"));
        var marker = r.l1.get("x");
        assertEquals(1 + Duration.ofMinutes(14).toNanos(), marker.localFreshness().retentionUntilNanos());
        r.breaker.onFailure();
        for (int minute = 4; minute < 14; minute++) {
            r.minute(minute); assertEquals(LookupResult.cachedNull(), r.cache.lookup("x"));
            assertSame(marker, r.l1.get("x"));
        }
        r.minute(14); assertNull(r.l1.get("x")); assertEquals(LookupResult.miss(), r.cache.lookup("x"));
    }

    @Test void negativeControlDetectsAnExtendedStaleHorizon() {
        var r = new LocalFreshnessTest.Rig(LocalFreshnessTest.settings(30, 2L, 10, NullPolicy.deny()));
        r.cache.put("x", "value"); r.minute(1); r.cache.get("x");
        var entry = r.l1.get("x"); var old = entry.localFreshness();
        r.minute(4);
        // Deliberately broken refresh: retain an unchanged logical lifetime for too long.
        r.l1.put("x", entry.withLocalFreshness(new StoredEntry.LocalFreshness(
                old.logicalDeadlineNanos(), old.staleServeUntilNanos(), old.storeRetentionFloorNanos(),
                1 + Duration.ofMinutes(60).toNanos(), old.highestSeen())), Duration.ofMinutes(56));
        r.minute(40);
        assertThrows(AssertionError.class, () -> assertNull(r.l1.get("x"), "physical horizon moved"));
    }

    @Test void negativeControlDetectsBlindPutResurrection() {
        var r = new LocalFreshnessTest.Rig(LocalFreshnessTest.settings(30, 2L, 10, NullPolicy.deny()));
        r.cache.put("x", "value"); var staleCallerCopy = r.l1.get("x");
        r.l1.evict("x");
        // A freshness operation implemented as unconditional put would recreate the mapping.
        r.l1.put("x", staleCallerCopy, Duration.ofMinutes(40));
        assertThrows(AssertionError.class, () -> assertNull(r.l1.get("x"), "removed entry was resurrected"));
    }
}
