package io.tiercache.spi;

import java.util.List;

/**
 * The result of an atomic checked journal read: the integrity proof for
 * the read's start and the rows read, both from the same response — the
 * proof and the range can never diverge (a trim between a separate check
 * and read would invalidate the proof).
 *
 * <p>Semantics per cursor kind:
 * <ul>
 *   <li>Non-beginning cursor: {@code rows} starts AT the cursor row
 *       (inclusive read); {@code startIntact} is {@code true} iff the
 *       first returned row IS the cursor row. If it is not (the cursor
 *       row was trimmed), prefix integrity is unconfirmable and the
 *       receiver must take the flush path.</li>
 *   <li>Beginning cursor (an end cursor recorded against an empty
 *       journal): there is no cursor row; {@code rows} starts from the
 *       journal's first row and {@code startIntact} reflects the atomic
 *       trim counter ({@code false} = rows were trimmed = genuine loss
 *       for a beginning receiver).</li>
 * </ul>
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @param startIntact whether the read's start is confirmed intact
 * @param rows        the rows read (oldest first, at most the requested
 *                    limit)
 * @since 1.3.0
 */
public record CheckedRange(boolean startIntact, List<JournalRow> rows) {
}
