package io.tiercache.tck;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T-01 (seed): cache stampede. With singleflight enabled, N concurrent
 * readers of one missing key trigger exactly 1 loader execution per
 * instance. The same harness must detect the stampede when the protection
 * is explicitly disabled — proving it is sensitive, not vacuous.
 */
class StampedeTest {

    private static final int THREADS = 64;

    @Test
    void stampedeWithProtectionLoadsOnce() throws Exception {
        int loaderCalls = new StampedeHarness(THREADS, Duration.ofMinutes(1)).run(true);
        assertEquals(1, loaderCalls,
                "F-20: concurrent misses of one key must share one loader execution");
    }

    @Test
    void stampedeWithoutProtectionIsDetected() throws Exception {
        int loaderCalls = new StampedeHarness(THREADS, Duration.ofMinutes(1)).run(false);
        assertTrue(loaderCalls > 1,
                "harness must detect the stampede when singleflight is disabled; got "
                        + loaderCalls + " loader calls");
    }
}
