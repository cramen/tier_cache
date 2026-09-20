package io.tiercache.spi;

import io.tiercache.InvalidationMessage;

/**
 * One journal entry: the message plus the cursor identifying its row.
 * Cursors are monotonically ordered per cache (Redis stream entry IDs for
 * the Redis transport).
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @param cursor  the row's cursor (never {@code null})
 * @param message the journaled message (never {@code null})
 * @since 1.3.0
 */
public record JournalRow(String cursor, InvalidationMessage message) {
}
