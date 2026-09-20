package io.tiercache.spi;

import io.tiercache.InvalidationMessage;

import java.util.List;

/**
 * SPI for the bounded invalidation journal at L2. Writers append every
 * invalidation message in the same atomic unit as the data write; receivers
 * replay missed ranges after a reconnect. If the missed range has been
 * trimmed, receivers flush L1 (signaled via {@link InvalidationListener}).
 *
 * <p><b>Internal — not part of the supported API.</b> Implemented by the
 * invalidation transport.
 *
 * @since 0.1.0
 */
public interface InvalidationJournal {

    /**
     * Appends a message, returning its cursor (monotonically ordered per
     * cache).
     *
     * @param cache   the cache the message belongs to
     * @param message the message to append
     * @return the cursor identifying the appended entry
     * @since 0.1.0
     */
    String append(String cache, InvalidationMessage message);

    /**
     * Returns rows after {@code cursorExclusive}, oldest first, with their
     * row cursors.
     *
     * @param cache           the cache whose journal is read
     * @param cursorExclusive the cursor to start after
     * @return the missed rows in append order
     * @since 0.1.0
     */
    List<JournalRow> readRange(String cache, String cursorExclusive);

    /**
     * One atomic checked read: the integrity proof for the read's start and
     * up to {@code maxRows} rows, both derived from the same response (see
     * {@link CheckedRange} for the per-cursor-kind semantics). All replay
     * paths (cadence tick, overflow catch-up, reconnect) use this method —
     * never a separate survival check followed by a range read, which a
     * concurrent trim could tear apart.
     *
     * @param cache   the cache whose journal is read
     * @param cursor  the confirmed cursor to read from
     * @param maxRows maximum rows to return
     * @return the checked range
     * @since 1.3.0
     */
    CheckedRange checkedRead(String cache, String cursor, int maxRows);

    /**
     * The cursor at the journal's current end (for first-subscribe: do not
     * replay history).
     *
     * @param cache the cache whose journal is queried
     * @return the end cursor
     * @since 0.1.0
     */
    String endCursor(String cache);

    /**
     * True if {@code cursor} has been trimmed away (the missed range is
     * unrecoverable — receiver must flush L1).
     *
     * @param cache  the cache whose journal is queried
     * @param cursor the cursor to check
     * @return {@code true} if the cursor points into trimmed history
     * @since 0.1.0
     */
    boolean isTrimmed(String cache, String cursor);

    /**
     * Current number of journal entries for the cache (for the journal-size
     * gauge). -1 if unknown.
     *
     * @param cache the cache whose journal is measured
     * @return the entry count, or {@code -1} if unknown
     * @since 0.1.0
     */
    default long size(String cache) {
        return -1;
    }
}
