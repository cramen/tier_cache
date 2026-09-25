package io.tiercache;

import io.tiercache.testkit.FreshHitRemoteCache;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FreshHitAttributionTest {
    @Test void rejectsAnAccidentalCascadeEvenWhenTheValueIsCorrect() {
        FreshHitRemoteCache<String, String> remote = new FreshHitRemoteCache<>();
        try (TierCacheFactory factory = TierCacheFactory.builder().remoteCache(remote).build()) {
            TierCache<String, String> cache = factory.getCache("bench");
            cache.put("key", "value");
            remote.resetAttribution();
            assertEquals("value", cache.get("key"));
            remote.assertNoCalls();
            // Deliberately invalidate the supposedly pure L1 workload.
            assertNotNull(remote.get("key"));
            assertThrows(AssertionError.class, remote::assertNoCalls);
            remote.resetAttribution();
            remote.recordLoad();
            assertThrows(AssertionError.class, remote::assertNoCalls);
        }
    }
}
