package io.tiercache.redis;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class RedisKeyspaceTest {
    private static String text(byte[] bytes) { return new String(bytes, StandardCharsets.US_ASCII); }

    @Test void exactWireLayoutUsesCompleteTokensAndRawDataKeys() {
        byte[] key = {0, (byte) 255, ':'};
        assertEquals("dXNlcjpyb2xlcw", RedisKeyspace.token("user:roles"));
        assertEquals("", RedisKeyspace.token(""));
        assertEquals("tiercache:v2:data:dXNlcjpyb2xlcw:", text(RedisKeyspace.dataPrefix("user:roles")));
        byte[] data = RedisKeyspace.dataKey("user:roles", key);
        assertArrayEquals(key, java.util.Arrays.copyOfRange(data, data.length - key.length, data.length));
        assertEquals("tiercache:v2:tagkeys:dXNlcjpyb2xlcw:AP86", text(RedisKeyspace.reverseKey("user:roles", key)));
        assertEquals("tiercache:v2:tags:dXNlcjpyb2xlcw:QTpC", text(RedisKeyspace.tagKey("user:roles", "A:B")));
        assertEquals("tiercache:v2:journal:dXNlcnM", text(RedisKeyspace.journal("users")));
        assertEquals("tiercache:v2:journal-trims:dXNlcnM", text(RedisKeyspace.trims("users")));
        assertEquals("tiercache:v2:inv:dXNlcnM", text(RedisKeyspace.channel("users")));
        UUID id = UUID.fromString("00000000-0000-0000-0000-000000000001");
        assertEquals("tiercache:v2:cg:dXNlcnM:" + id, text(RedisKeyspace.group("users", id)));
        assertEquals("tiercache:v2:rebuild:dXNlcjpr", RedisKeyspace.lock("user:k"));
    }

    @Test void namesRemainDistinctWithoutNormalizationAndCannotInjectDelimiters() {
        String[] names = {"", "user", "user:roles", "a?", "a*", "a[12]", "a\\b", "é", "e\u0301", "Users", "users", "用户", "😀"};
        var tokens = new java.util.HashSet<String>();
        for (String name : names) {
            String token = RedisKeyspace.token(name);
            assertTrue(token.matches("[A-Za-z0-9_-]*"));
            assertTrue(tokens.add(token), name);
            assertEquals(name, new String(java.util.Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8));
        }
        assertThrows(IllegalArgumentException.class, () -> RedisKeyspace.token("\ud800"));
        assertThrows(IllegalArgumentException.class, () -> RedisKeyspace.token("x\udc00"));
    }

    @Test void redisLengthLimitUsesLongArithmeticWithoutAllocatingHugeKeys() {
        assertDoesNotThrow(() -> RedisKeyspace.checkLength(512L * 1024 * 1024));
        assertThrows(IllegalArgumentException.class, () -> RedisKeyspace.checkLength(512L * 1024 * 1024 + 1));
        assertThrows(IllegalArgumentException.class, () -> RedisKeyspace.checkLength(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> RedisKeyspace.checkLength(-1));
    }
}
