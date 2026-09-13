package io.tiercache.spi;

/**
 * Signals invalidation-journal events that operators must see. Metrics are
 * layered on top of this callback by the observability module.
 */
public interface InvalidationListener {

    InvalidationListener NOOP = cache -> {
    };

    /**
     * The journal window was exceeded during a disconnect; the receiver
     * flushed its entire L1 for the cache.
     */
    void onJournalOverflow(String cache);
}
