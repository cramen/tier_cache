package io.tiercache.invalidation;

import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Codec round-trip, including non-String (opaque binary) keys. */
class MessageCodecTest {

    @Test
    void roundTripInvalidate() {
        UUID origin = UUID.randomUUID();
        InvalidationMessage message = new InvalidationMessage("users", "ignored",
                new Version(42, origin), origin, InvalidationMessage.Type.INVALIDATE);
        byte[] keyBytes = new byte[]{0x00, (byte) 0xAC, (byte) 0xED, 0x7F}; // opaque binary key
        MessageCodec.Decoded decoded = MessageCodec.decode(MessageCodec.encode(message, keyBytes));
        assertEquals("users", decoded.cache());
        assertArrayEquals(keyBytes, decoded.keyBytes());
        assertEquals(new Version(42, origin), decoded.version());
        assertEquals(origin, decoded.originInstanceId());
        assertEquals(InvalidationMessage.Type.INVALIDATE, decoded.type());
    }

    @Test
    void roundTripEvictAll() {
        UUID origin = UUID.randomUUID();
        InvalidationMessage message = new InvalidationMessage("users", null,
                new Version(7, origin), origin, InvalidationMessage.Type.EVICT_ALL);
        MessageCodec.Decoded decoded = MessageCodec.decode(MessageCodec.encode(message, null));
        assertEquals(InvalidationMessage.Type.EVICT_ALL, decoded.type());
        assertNull(decoded.keyBytes());
    }
}
