package io.tiercache.redis;

import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import static io.tiercache.redis.StreamRowCorruptionException.Reason;

/** One validation policy for live Streams and journal replay. */
final class StreamRowDecoder {
    private StreamRowDecoder() { }
    static InvalidationMessage decode(String cache, String row, Map<byte[], byte[]> body,
            CacheSerializer<Object> keys, CacheSerializer<Object> values) {
        if (!validId(row)) throw bad(cache, row, Reason.ROW_ID);
        if (body == null || body.isEmpty()) throw bad(cache, row, Reason.MISSING_ROW);
        byte[] type = field(cache, row, body, "t", true);
        byte[] version = field(cache, row, body, "v", true);
        byte[] key = field(cache, row, body, "k", true);
        byte[] payload = field(cache, row, body, "p", false);
        if (type.length != 1 || Byte.toUnsignedInt(type[0]) >= InvalidationMessage.Type.values().length) throw bad(cache, row, Reason.TYPE);
        var kind = InvalidationMessage.Type.values()[Byte.toUnsignedInt(type[0])];
        Version parsed;
        try {
            String wire = new String(version, StandardCharsets.UTF_8);
            parsed = Version.fromWire(wire);
            if (!parsed.toWire().equalsIgnoreCase(wire)) throw new IllegalArgumentException();
        } catch (RuntimeException e) { throw bad(cache, row, Reason.VERSION); }
        if (kind == InvalidationMessage.Type.EVICT_ALL) {
            if (key.length != 0 || (payload != null && payload.length != 0)) throw bad(cache, row, Reason.FIELDS);
            return new InvalidationMessage(cache, null, parsed, parsed.instanceId(), kind);
        }
        Object decodedKey;
        try { decodedKey = keys.fromBytes(key); if (decodedKey == null) throw new IllegalArgumentException(); }
        catch (RuntimeException e) { throw bad(cache, row, Reason.KEY); }
        Object value = null;
        boolean update = kind == InvalidationMessage.Type.UPDATE || (payload != null && payload.length > 0);
        if (update) {
            if (payload == null) throw bad(cache, row, Reason.PAYLOAD);
            try { value = values.fromBytes(payload); if (value == null) throw new IllegalArgumentException(); }
            catch (RuntimeException e) { throw bad(cache, row, Reason.PAYLOAD); }
            kind = InvalidationMessage.Type.UPDATE; // legacy journal payload implies UPDATE
        }
        return new InvalidationMessage(cache, decodedKey, parsed, parsed.instanceId(), kind, value);
    }
    private static byte[] field(String cache, String row, Map<byte[], byte[]> body, String name, boolean required) {
        byte[] result = null; boolean found = false;
        byte[] wanted = name.getBytes(StandardCharsets.US_ASCII);
        for (var entry : body.entrySet()) if (Arrays.equals(wanted, entry.getKey())) {
            if (found || entry.getValue() == null) throw bad(cache, row, Reason.FIELDS);
            found = true; result = entry.getValue();
        }
        if (!found && required) throw bad(cache, row, Reason.FIELDS);
        return result;
    }
    static boolean validId(String id) {
        if (id == null || !id.matches("[0-9]+-[0-9]+")) return false;
        try { parts(id); return true; } catch (RuntimeException e) { return false; }
    }
    static int compareIds(String left, String right) {
        long[] a = parts(left), b = parts(right);
        int first = Long.compareUnsigned(a[0], b[0]);
        return first != 0 ? first : Long.compareUnsigned(a[1], b[1]);
    }
    private static long[] parts(String id) {
        int dash = id.indexOf('-');
        if (dash < 0) return new long[]{Long.parseUnsignedLong(id), 0};
        return new long[]{Long.parseUnsignedLong(id.substring(0, dash)), Long.parseUnsignedLong(id.substring(dash + 1))};
    }
    private static StreamRowCorruptionException bad(String cache, String row, Reason reason) {
        return new StreamRowCorruptionException(cache, row, reason);
    }
}
