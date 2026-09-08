package io.tiercache.micrometer;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The reference dashboard must be valid JSON covering every exported metric,
 * and the alert rules file must define the three required alerts.
 */
class DashboardCoverageTest {

    private static final Path DOCS = Path.of("..", "docs", "grafana");

    @Test
    void dashboardCoversEveryMetric() throws Exception {
        String json = Files.readString(DOCS.resolve("tiercache-dashboard.json"));
        // Valid JSON.
        assertTrue(json.strip().startsWith("{") && json.strip().endsWith("}"));
        // Every exported metric appears (Prometheus naming: dots -> underscores,
        // counters gain _total).
        for (String metric : new String[]{"tiercache_requests_total", "tiercache_latency",
                "tiercache_invalidation_total", "tiercache_degraded", "tiercache_breaker_state",
                "tiercache_journal_size", "tiercache_entry_age_max", "tiercache_null_entries_total"}) {
            assertTrue(json.contains(metric), "dashboard misses " + metric);
        }
    }

    @Test
    void alertRulesDefined() throws Exception {
        String yaml = Files.readString(DOCS.resolve("alerts.yml"));
        assertTrue(yaml.contains("TiercacheMissGrowth"), "miss growth alert");
        assertTrue(yaml.contains("TiercacheDroppedInvalidations"), "dropped invalidations alert");
        assertTrue(yaml.contains("TiercacheDegraded"), "degraded alert");
        assertTrue(yaml.contains("tiercache_requests_total"));
        assertTrue(yaml.contains("tiercache_invalidation_total"));
        assertTrue(yaml.contains("tiercache_degraded"));
    }
}
