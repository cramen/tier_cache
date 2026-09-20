package io.tiercache.testkit;

import io.tiercache.InvalidationMessage;
import io.tiercache.spi.CheckedRange;
import io.tiercache.spi.InvalidationJournal;
import io.tiercache.spi.JournalRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link InvalidationJournal}: bounded ring per cache with
 * monotonically increasing per-cache long cursors. Mirrors the Redis
 * adapter's semantics: a beginning cursor is {@code "0"} (the end cursor
 * of an empty journal), trims are counted exactly, and
 * {@link #checkedRead} applies the same integrity rules (beginning cursor
 * checked against the trim count; a non-zero cursor intact iff its own
 * row survives).
 */
public final class InMemoryJournal implements InvalidationJournal {

    private final int capacity;
    private final Map<String, List<Entry>> journals = new ConcurrentHashMap<>();
    private final Map<String, Long> trimCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> sequences = new ConcurrentHashMap<>();

    public InMemoryJournal(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public synchronized String append(String cache, InvalidationMessage message) {
        List<Entry> journal = journals.computeIfAbsent(cache, c -> new ArrayList<>());
        long cursor = sequences.merge(cache, 1L, Long::sum);
        journal.add(new Entry(cursor, message));
        while (journal.size() > capacity) {
            journal.remove(0);
            trimCounts.merge(cache, 1L, Long::sum);
        }
        return Long.toString(cursor);
    }

    @Override
    public synchronized List<JournalRow> readRange(String cache, String cursorExclusive) {
        long after = Long.parseLong(cursorExclusive);
        List<JournalRow> out = new ArrayList<>();
        for (Entry e : journals.getOrDefault(cache, List.of())) {
            if (e.cursor > after) {
                out.add(new JournalRow(Long.toString(e.cursor), e.message));
            }
        }
        return out;
    }

    @Override
    public synchronized CheckedRange checkedRead(String cache, String cursor, int maxRows) {
        List<Entry> journal = journals.getOrDefault(cache, List.of());
        if ("0".equals(cursor)) {
            // Beginning cursor: intact iff nothing was ever trimmed.
            boolean intact = trimCounts.getOrDefault(cache, 0L) == 0L;
            List<JournalRow> rows = new ArrayList<>();
            for (Entry e : journal) {
                if (rows.size() >= maxRows) {
                    break;
                }
                rows.add(new JournalRow(Long.toString(e.cursor), e.message));
            }
            return new CheckedRange(intact, rows);
        }
        long from = Long.parseLong(cursor);
        List<JournalRow> rows = new ArrayList<>();
        for (Entry e : journal) {
            if (e.cursor >= from && rows.size() < maxRows) {
                rows.add(new JournalRow(Long.toString(e.cursor), e.message));
            }
        }
        boolean intact = !rows.isEmpty() && rows.get(0).cursor().equals(cursor);
        return new CheckedRange(intact, rows);
    }

    @Override
    public String endCursor(String cache) {
        return sequences.getOrDefault(cache, 0L).toString();
    }

    @Override
    public synchronized boolean isTrimmed(String cache, String cursor) {
        if ("0".equals(cursor)) {
            return trimCounts.getOrDefault(cache, 0L) > 0L;
        }
        List<Entry> journal = journals.getOrDefault(cache, List.of());
        if (journal.isEmpty()) {
            return false;
        }
        return Long.parseLong(cursor) < journal.get(0).cursor;
    }

    @Override
    public synchronized long size(String cache) {
        return journals.getOrDefault(cache, List.of()).size();
    }

    private record Entry(long cursor, InvalidationMessage message) {
    }
}
