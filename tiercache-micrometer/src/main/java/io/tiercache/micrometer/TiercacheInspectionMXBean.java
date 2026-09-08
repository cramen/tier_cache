package io.tiercache.micrometer;

import io.tiercache.TierCacheFactory;
import io.tiercache.spi.InvalidationJournal;

import java.util.List;

/**
 * JMX view of cache state. Deliberately no top-N keys: per-key counting
 * taxes the hot path, so key-level inspection is refused by design.
 */
public interface TiercacheInspectionMXBean {

    String[] getCacheNames();

    double getL1HitRatio(String cache);

    double getL2HitRatio(String cache);

    /** "closed" or "open" (open = degraded L1-only mode). */
    String getBreakerState();

    long getJournalSize(String cache);
}
