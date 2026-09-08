package io.tiercache.spi;

import io.tiercache.InvalidationMessage;

import java.util.List;

/**
 * SPI for the bounded invalidation journal at L2. Writers append every
 * invalidation message in the same atomic unit as the data write; receivers
 * replay missed ranges after a reconnect. If the missed range has been
 * trimmed, receivers flush L1 (signaled via {@link InvalidationListener}).
 *
 * <p><b>Incubating:</b> 0.x API, may change before the public API freeze.
 */
public interface InvalidationJournal {

    /**
     * Appends a message, returning its cursor (monotonically ordered per
     * cache).
     */
    String append(String cache, InvalidationMessage message);

    /**
     * Returns messages after {@code cursorExclusive}, oldest first.
     */
    List<InvalidationMessage> readRange(String cache, String cursorExclusive);

    /**
     * The cursor at the journal's current end (for first-subscribe: do not
     * replay history).
     */
    String endCursor(String cache);

    /**
     * True if {@code cursor} has been trimmed away (the missed range is
     * unrecoverable — receiver must flush L1).
     */
    boolean isTrimmed(String cache, String cursor);

    /**
     * Current number of journal entries for the cache (for the journal-size
     * gauge). -1 if unknown.
     */
    default long size(String cache) {
        return -1;
    }
}
