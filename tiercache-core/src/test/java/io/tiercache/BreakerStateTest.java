package io.tiercache;

import io.tiercache.internal.CircuitBreaker;
import io.tiercache.testkit.FailingRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Factory-exposed breaker state: closed -> open -> half-open -> closed. */
class BreakerStateTest {

    /** Breaker that opens after 2 failures and probes after 50 ms. */
    private static final CircuitBreaker.Config FAST =
            new CircuitBreaker.Config(10, 0.5, 2, Duration.ofMillis(50), 1);

    @Test
    void transitionsThroughOpenAndHalfOpen() throws Exception {
        FailingRemoteCache<String, String> l2 = new FailingRemoteCache<>();
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(l2)
                .circuitBreakerConfig(FAST)
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        assertEquals(BreakerState.CLOSED, factory.breakerState());

        l2.fail();
        cache.get("probe-1"); // failure 1
        cache.get("probe-2"); // failure 2 -> open
        assertEquals(BreakerState.OPEN, factory.breakerState());

        Thread.sleep(100); // past halfOpenAfter
        assertEquals(BreakerState.HALF_OPEN, factory.breakerState(),
                "wait elapsed: half-open even before the next probe call");

        l2.heal();
        cache.getOrCompute("k", key -> "v"); // successful probe -> close
        assertEquals(BreakerState.CLOSED, factory.breakerState());
        factory.close();
    }

    @Test
    void disabledBreakerReportsClosed() {
        FailingRemoteCache<String, String> l2 = new FailingRemoteCache<>();
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(l2)
                .disableCircuitBreaker()
                .build();
        assertEquals(BreakerState.CLOSED, factory.breakerState(),
                "no breaker: calls are never rejected");
        factory.close();
    }
}
