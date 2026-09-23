package io.tiercache.redis;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

/**
 * Canonical v2 Redis addresses. Internal transport API, not an application API.
 * Names are strict UTF-8 encoded as unpadded Base64URL tokens; application key
 * bytes remain opaque. No v1 fallback or cleanup is performed.
 */
public final class RedisKeyspace {
    static final String DATA = "tiercache:v2:data:";
    static final String TAGS = "tiercache:v2:tags:";
    static final String REVERSE = "tiercache:v2:tagkeys:";
    static final String JOURNAL = "tiercache:v2:journal:";
    static final String TRIMS = "tiercache:v2:journal-trims:";
    static final String CHANNEL = "tiercache:v2:inv:";
    static final String GROUP = "tiercache:v2:cg:";
    static final String LOCK = "tiercache:v2:rebuild:";
    private static final long MAX_KEY_BYTES = 512L * 1024 * 1024;

    private RedisKeyspace() { }

    /** Encodes a complete name without normalization, rejecting malformed UTF-16. */
    public static String token(String name) {
        Objects.requireNonNull(name, "name");
        try {
            ByteBuffer utf8 = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(name));
            byte[] bytes = new byte[utf8.remaining()];
            utf8.get(bytes);
            return byteToken(bytes);
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Redis namespace/name contains malformed Unicode", e);
        }
    }

    /** Encodes original bytes, without hashing or text conversion. */
    public static String byteToken(byte[] bytes) {
        checkLength(((long) bytes.length * 4 + 2) / 3);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /** Exact literal data prefix; only this prefix may be scanned by clear. */
    public static byte[] dataPrefix(String namespace) { return address(DATA + token(namespace) + ":"); }

    /** Data address, preserving serialized application key bytes. */
    public static byte[] dataKey(String namespace, byte[] key) { return join(dataPrefix(namespace), key); }

    /** Exact tag-set prefix of one physical namespace. */
    public static byte[] tagPrefix(String namespace) { return address(TAGS + token(namespace) + ":"); }

    /** Tag-set address; both namespace and tag are complete tokens. */
    public static byte[] tagKey(String namespace, String tag) {
        return join(tagPrefix(namespace), address(token(tag)));
    }

    /** Reverse-index address of an opaque serialized application key. */
    public static byte[] reverseKey(String namespace, byte[] key) {
        return address(REVERSE + token(namespace) + ":" + byteToken(key));
    }

    /** Journal address uses the logical cache identity, not its physical data namespace. */
    public static byte[] journal(String logical) { return address(JOURNAL + token(logical)); }

    /** Trim counter paired with the logical cache's journal. */
    public static byte[] trims(String logical) { return address(TRIMS + token(logical)); }

    /** Pub/Sub channel for a logical cache. */
    public static byte[] channel(String logical) { return address(CHANNEL + token(logical)); }

    /** Consumer group for a logical cache and stable receiver identity. */
    public static byte[] group(String logical, UUID instance) {
        return address(groupPrefix(logical) + Objects.requireNonNull(instance, "instance"));
    }

    static String groupPrefix(String logical) { return GROUP + token(logical) + ":"; }

    /** Built-in lock address; the provider's lock name is opaque. */
    public static String lock(String name) {
        String result = LOCK + token(name);
        checkLength(result.length());
        return result;
    }

    static byte[] join(byte[] prefix, byte[] suffix) {
        checkLength((long) prefix.length + suffix.length);
        byte[] result = java.util.Arrays.copyOf(prefix, prefix.length + suffix.length);
        System.arraycopy(suffix, 0, result, prefix.length, suffix.length);
        return result;
    }

    private static byte[] address(String ascii) {
        checkLength(ascii.length());
        return ascii.getBytes(StandardCharsets.US_ASCII);
    }

    static void checkLength(long bytes) {
        if (bytes < 0 || bytes > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("Redis key exceeds the 512 MiB key length limit");
        }
    }
}
