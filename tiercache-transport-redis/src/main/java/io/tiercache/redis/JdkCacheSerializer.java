package io.tiercache.redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;

/**
 * Default serializer based on JDK serialization.
 *
 * <p><b>Not recommended for production payloads:</b> JDK serialization is
 * brittle across class changes and unsafe for untrusted data. Provide your
 * own {@link CacheSerializer} (e.g. JSON-based) for real deployments.
 *
 * <p><b>Incubating:</b> 0.x API, may change before the public API freeze.
 */
public final class JdkCacheSerializer<T> implements CacheSerializer<T> {

    @Override
    public byte[] toBytes(T value) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(buffer)) {
            out.writeObject(value);
        } catch (IOException e) {
            throw new UncheckedIOException("JDK serialization failed", e);
        }
        return buffer.toByteArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T fromBytes(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (T) in.readObject();
        } catch (IOException e) {
            throw new UncheckedIOException("JDK deserialization failed", e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("JDK deserialization failed: class not found", e);
        }
    }
}
