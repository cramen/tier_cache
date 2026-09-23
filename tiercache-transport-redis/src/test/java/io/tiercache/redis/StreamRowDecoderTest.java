package io.tiercache.redis;
import io.tiercache.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.*;

class StreamRowDecoderTest {
    static final JdkCacheSerializer<Object> CODEC = new JdkCacheSerializer<>();
    static byte[] b(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    static Map<byte[],byte[]> row() {
        Map<byte[],byte[]> result = new LinkedHashMap<>();
        result.put(b("t"),new byte[]{0}); result.put(b("k"),CODEC.toBytes("k"));
        result.put(b("v"),b(new Version(1,UUID.randomUUID()).toWire())); return result;
    }
    static void replace(Map<byte[],byte[]> row, String field, byte[] value) {
        row.keySet().removeIf(k -> Arrays.equals(k,b(field))); row.put(b(field),value);
    }
    @Test void malformedRowsHaveTypedSanitizedFailures() {
        for (String kind : List.of("missing","type","version","key","payload","duplicate")) {
            Map<byte[],byte[]> row = row();
            switch(kind) {
                case "missing" -> row.clear();
                case "type" -> replace(row,"t",new byte[]{(byte)255});
                case "version" -> replace(row,"v",b("secret-invalid-version"));
                case "key" -> replace(row,"k",b("secret-invalid-key"));
                case "payload" -> replace(row,"p",b("secret-invalid-payload"));
                case "duplicate" -> row.put(b("t"),new byte[]{0});
            }
            var error = assertThrows(StreamRowCorruptionException.class, () -> StreamRowDecoder.decode("c","1-0",row,CODEC,CODEC));
            assertEquals("c",error.cache()); assertEquals("1-0",error.rowId()); assertNull(error.getCause());
            assertFalse(error.toString().contains("secret"));
        }
        assertThrows(StreamRowCorruptionException.class, () -> StreamRowDecoder.decode("c","1-0",null,CODEC,CODEC));
    }
    @Test void validPayloadAndControlShapesStayCompatible() {
        Map<byte[],byte[]> row = row(); row.put(b("p"),CODEC.toBytes("value"));
        var message = StreamRowDecoder.decode("c","1-0",row,CODEC,CODEC);
        assertEquals(InvalidationMessage.Type.UPDATE,message.type()); assertEquals("value",message.payload());
        replace(row,"t",new byte[]{1}); replace(row,"k",new byte[0]); replace(row,"p",new byte[0]);
        assertEquals(InvalidationMessage.Type.EVICT_ALL,StreamRowDecoder.decode("c","2-0",row,CODEC,CODEC).type());
        replace(row,"p",CODEC.toBytes("unexpected"));
        assertThrows(StreamRowCorruptionException.class,()->StreamRowDecoder.decode("c","2-0",row,CODEC,CODEC));
        var missingPayload=row(); replace(missingPayload,"t",new byte[]{2});
        assertThrows(StreamRowCorruptionException.class,()->StreamRowDecoder.decode("c","3-0",missingPayload,CODEC,CODEC));
    }
    @Test void serializerNullsAreNotMistakenForSuccessfulInvalidation() {
        CacheSerializer<Object> nulls=new CacheSerializer<>() { public byte[] toBytes(Object v){return new byte[0];} public Object fromBytes(byte[] b){return null;} };
        var row=row(); assertThrows(StreamRowCorruptionException.class,()->StreamRowDecoder.decode("c","1-0",row,nulls,CODEC));
        row.put(b("p"),CODEC.toBytes("value")); assertThrows(StreamRowCorruptionException.class,()->StreamRowDecoder.decode("c","1-0",row,CODEC,nulls));
    }
}
