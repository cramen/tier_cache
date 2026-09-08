package io.tiercache.spi;

/**
 * Signals invalidation-journal events that operators must see. Metrics are
 * layered on top of this callback by the observability module.
 *
 * <p><b>Incubating:</b> 0.x API, may change before the public API freeze.
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
