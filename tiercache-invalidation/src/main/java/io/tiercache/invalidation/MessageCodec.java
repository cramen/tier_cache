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
 *
 * <p><b>Internal — not part of the supported API.</b> Used by the invalidation
 * transport implementations; may change in any release without notice.
 *
 * @since 0.1.0
 */
public final class MessageCodec {

    private MessageCodec() {
    }

    /**
     * Encodes a message into its binary wire form.
     *
     * @param message  the invalidation message to encode
     * @param keyBytes the serialized key bytes, or {@code null} for
     *                 {@code EVICT_ALL} messages (which carry no key)
     * @return the encoded wire bytes
     * @since 0.1.0
     */
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

    /**
     * Decoded message parts; the key remains raw bytes for the transport to
     * deserialize.
     *
     * @param cache            the cache name
     * @param keyBytes         the raw serialized key bytes, or {@code null}
     *                         for {@code EVICT_ALL} messages
     * @param version          the write version
     * @param originInstanceId ID of the instance that produced the write
     * @param type             the message type
     * @param payload          the UPDATE payload, or {@code null} for plain
     *                         invalidations
     * @since 0.1.0
     */
    public record Decoded(String cache, byte[] keyBytes, Version version, UUID originInstanceId,
            InvalidationMessage.Type type, byte[] payload) {
    }

    /**
     * Decodes wire bytes produced by {@link #encode}. Messages written before
     * UPDATE mode existed (no payload tail) decode with a {@code null}
     * payload.
     *
     * @param bytes the wire bytes
     * @return the decoded message parts
     * @since 0.1.0
     */
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
