package io.tiercache.invalidation;

import io.tiercache.InvalidationMessage;
import io.tiercache.Version;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Binary wire codec for invalidation messages (compact, no serialization
 * framework). Keys travel as raw bytes; (de)serialization is the transport's
 * concern.
 *
 * <pre>
 * [type:1][cacheLen:2][cache UTF-8][keyLen:4][key bytes][seq:8][instanceId:16][payloadLen:4][payload]
 * </pre>
 *
 * EVICT_ALL messages carry keyLen = -1 and no key bytes. The payload tail is
 * absent in v1 messages (written before UPDATE mode existed) — decodes as
 * payload=null, plain INVALIDATE semantics.
 */
public final class MessageCodec {

    private MessageCodec() {
    }

    public static byte[] encode(InvalidationMessage message, byte[] keyBytes) {
        byte[] cacheBytes = message.cache().getBytes(StandardCharsets.UTF_8);
        byte[] payload = (byte[]) message.payload();
        int payloadLen = payload != null ? payload.length : -1;
        int keyLen = keyBytes != null ? keyBytes.length : -1;
        int capacity = 1 + 2 + cacheBytes.length + 4 + (keyLen > 0 ? keyLen : 0) + 8 + 16
                + 4 + (payloadLen > 0 ? payloadLen : 0);
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put((byte) message.type().ordinal());
        buffer.putShort((short) cacheBytes.length);
        buffer.put(cacheBytes);
        buffer.putInt(keyLen);
        if (keyLen > 0) {
            buffer.put(keyBytes);
        }
        buffer.putLong(message.version().sequence());
        buffer.putLong(message.originInstanceId().getMostSignificantBits());
        buffer.putLong(message.originInstanceId().getLeastSignificantBits());
        buffer.putInt(payloadLen);
        if (payloadLen > 0) {
            buffer.put(payload);
        }
        return buffer.array();
    }

    /** Decoded message parts; the key remains raw bytes for the transport to deserialize. */
    public record Decoded(String cache, byte[] keyBytes, Version version, UUID originInstanceId,
            InvalidationMessage.Type type, byte[] payload) {
    }

    public static Decoded decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        InvalidationMessage.Type type = InvalidationMessage.Type.values()[buffer.get()];
        int cacheLen = buffer.getShort();
        byte[] cacheBytes = new byte[cacheLen];
        buffer.get(cacheBytes);
        int keyLen = buffer.getInt();
        byte[] keyBytes = null;
        if (keyLen >= 0) {
            keyBytes = new byte[keyLen];
            buffer.get(keyBytes);
        }
        long seq = buffer.getLong();
        UUID origin = new UUID(buffer.getLong(), buffer.getLong());
        byte[] payload = null;
        if (buffer.remaining() >= 4) { // v2 tail
            int payloadLen = buffer.getInt();
            if (payloadLen >= 0) {
                payload = new byte[payloadLen];
                buffer.get(payload);
            }
        }
        return new Decoded(new String(cacheBytes, StandardCharsets.UTF_8), keyBytes,
                new Version(seq, origin), origin, type, payload);
    }
}
