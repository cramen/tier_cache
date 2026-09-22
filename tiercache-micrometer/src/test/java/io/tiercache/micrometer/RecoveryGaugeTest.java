package io.tiercache.micrometer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

class RecoveryGaugeTest {
    @Test void pendingTracksSourcesAndCloseDoesNotRemoveAnotherOwner() throws Exception {
        var registry = new SimpleMeterRegistry();
        try {
            var metrics = new MicrometerCacheMetrics(registry);
            var first = new AtomicBoolean(); var second = new AtomicBoolean(true);
            var one = metrics.registerRecovery("c", first::get);
            var two = metrics.registerRecovery("c", second::get);
            var gauge = registry.get("tiercache.invalidation.recovery.pending").tag("cache", "c").gauge();
            assertEquals(1, gauge.value()); second.set(false); assertEquals(0, gauge.value());
            first.set(true); assertEquals(1, gauge.value()); one.close(); assertEquals(0, gauge.value());
            two.close(); assertNull(registry.find("tiercache.invalidation.recovery.pending").gauge());
            var replacement = metrics.registerRecovery("c", first::get);
            one.close(); two.close();
            assertEquals(1, registry.get("tiercache.invalidation.recovery.pending").gauge().value());
            replacement.close(); assertNull(registry.find("tiercache.invalidation.recovery.pending").gauge());
        } finally { registry.close(); }
    }
}
