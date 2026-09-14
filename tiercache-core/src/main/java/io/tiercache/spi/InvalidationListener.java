package io.tiercache.spi;

/**
 * Signals invalidation-journal events that operators must see. Metrics are
 * layered on top of this callback by the observability module.
 *
 * <p><b>Internal — not part of the supported API.</b> Extension point for
 * the observability module.
 *
 * @since 0.1.0
 */
public interface InvalidationListener {

    /**
     * A listener that ignores every event.
     *
     * @since 0.1.0
     */
    InvalidationListener NOOP = cache -> {
    };

    /**
     * The journal window was exceeded during a disconnect; the receiver
     * flushed its entire L1 for the cache.
     *
     * @param cache the cache whose L1 was flushed
     * @since 0.1.0
     */
    void onJournalOverflow(String cache);
}
