package io.tiercache.testkit;

import io.tiercache.InvalidationMessage;
import io.tiercache.spi.InvalidationJournal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory {@link InvalidationJournal}: bounded ring per cache with
 * monotonically increasing long cursors.
 */
public final class InMemoryJournal implements InvalidationJournal {

    private final int capacity;
    private final Map<String, List<Entry>> journals = new ConcurrentHashMap<>();
    private final Map<String, Long> lastTrimmed = new ConcurrentHashMap<>();
    private final AtomicLong cursorSequence = new AtomicLong();

    public InMemoryJournal(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public synchronized String append(String cache, InvalidationMessage message) {
        List<Entry> journal = journals.computeIfAbsent(cache, c -> new ArrayList<>());
        long cursor = cursorSequence.incrementAndGet();
        journal.add(new Entry(cursor, message));
        while (journal.size() > capacity) {
            Entry removed = journal.remove(0);
            lastTrimmed.put(cache, removed.cursor);
        }
        return Long.toString(cursor);
    }

    @Override
    public synchronized List<InvalidationMessage> readRange(String cache, String cursorExclusive) {
        long after = Long.parseLong(cursorExclusive);
        List<InvalidationMessage> out = new ArrayList<>();
        for (Entry e : journals.getOrDefault(cache, List.of())) {
            if (e.cursor > after) {
                out.add(e.message);
            }
        }
        return out;
    }

    @Override
    public String endCursor(String cache) {
        return Long.toString(cursorSequence.get());
    }

    @Override
    public synchronized boolean isTrimmed(String cache, String cursor) {
        // The missed range is unrecoverable if entries newer than the cursor
        // have been trimmed for this cache.
        Long trimmed = lastTrimmed.get(cache);
        return trimmed != null && Long.parseLong(cursor) < trimmed;
    }

    @Override
    public synchronized long size(String cache) {
        return journals.getOrDefault(cache, List.of()).size();
    }

    private record Entry(long cursor, InvalidationMessage message) {
    }
}
