package io.tiercache.spi;

/** Sanitized journal integrity failure shared by transport decoding and recovery. */
public class JournalCorruptionException extends IllegalStateException {
    private final String cache;
    private final String rowId;
    public JournalCorruptionException(String cache, String rowId, String category) {
        super("Invalid invalidation row: cache=" + cache + ", row=" + rowId + ", reason=" + category);
        this.cache = cache; this.rowId = rowId;
    }
    public String cache() { return cache; }
    public String rowId() { return rowId; }
}
