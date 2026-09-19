package io.tiercache;

import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cross-instance causality of versions through real factories: a write that
 * happens later in real time MUST carry a newer version than the writes of a
 * long-running instance, so last-write-wins cannot reject it. Regression for
 * the reviewer-reported ordering bug (per-instance counters starting at 1).
 */
class VersionOrderingTest {

    @Test
    void laterWriteFromFreshInstanceCarriesNewerVersion() throws Exception {
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        TierCacheFactory factoryA = TierCacheFactory.builder().remoteCache(l2).build();
        TierCache<String, String> a = factoryA.getCache("c");
        for (int i = 0; i < 100; i++) {
            a.put("k", "a-" + i);
        }
        Version lastFromA = l2.get("k").version();

        // Strictly later in real time: the clock moves past A's burst.
        Thread.sleep(5);
        TierCacheFactory factoryB = TierCacheFactory.builder().remoteCache(l2).build();
        TierCache<String, String> b = factoryB.getCache("c");
        b.put("k", "b");

        Version fromB = l2.get("k").version();
        assertTrue(fromB.compareTo(lastFromA) > 0,
                "fresh instance's later write must carry a newer version");
        assertEquals("b", l2.get("k").value(), "the later write is the L2 truth");
        factoryB.close();
        factoryA.close();
    }
}
