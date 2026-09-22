package io.tiercache.redis;

/** Sanitized corruption metadata. Never retains serializer exceptions, keys or payload bytes. */
public final class StreamRowCorruptionException extends io.tiercache.spi.JournalCorruptionException {
    /** The structural/decoding failure category. */
    public enum Reason { MISSING_ROW, FIELDS, TYPE, VERSION, KEY, PAYLOAD, ROW_ID }
    private final Reason reason;
    StreamRowCorruptionException(String cache, String rowId, Reason reason) {
        super(cache, rowId, reason.name());
        this.reason = reason;
    }
    public Reason reason() { return reason; }
}
