package io.tiercache.micrometer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.RecoveryResult;
import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class DispatchMetricsTest {
    @Test void gaugesAggregateOwnersAndRetireWithoutRemovingReplacement() throws Exception {
        var registry = new SimpleMeterRegistry();
        try {
            var metrics = new MicrometerCacheMetrics(registry);
            var count = new AtomicLong(2); var bytes = new AtomicLong(42);
            var first = metrics.registerDispatch(count::get, bytes::get);
            var second = metrics.registerDispatch(() -> 3, () -> 64);
            assertEquals(5, registry.get("tiercache.invalidation.dispatch.retained.messages").gauge().value());
            assertEquals(106, registry.get("tiercache.invalidation.dispatch.retained.bytes").gauge().value());
            first.close(); first.close();
            assertEquals(3, registry.get("tiercache.invalidation.dispatch.retained.messages").gauge().value());
            second.close(); assertNull(registry.find("tiercache.invalidation.dispatch.retained.messages").gauge());
            var pending = metrics.registerDispatchPending("cache", () -> true);
            assertEquals(1, registry.get("tiercache.invalidation.dispatch.repair.pending").gauge().value());
            pending.close(); assertNull(registry.find("tiercache.invalidation.dispatch.repair.pending").gauge());
            metrics.onDispatchRejected(CacheMetricsListener.DispatchReason.BYTES, 4);
            assertEquals(4, registry.get("tiercache.invalidation.dispatch.rejected").tag("reason", "bytes").counter().count());
            metrics.onDispatchRepair("cache", RecoveryResult.Status.FAILED);
            assertEquals(1, registry.get("tiercache.invalidation.dispatch.repair").tag("result", "failed").counter().count());
        } finally { registry.close(); }
    }
}
