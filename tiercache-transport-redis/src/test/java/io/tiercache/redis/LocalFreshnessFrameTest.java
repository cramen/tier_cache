package io.tiercache.redis;

import io.tiercache.Version;
import io.tiercache.spi.StoredEntry;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.nio.ByteBuffer;
import static org.junit.jupiter.api.Assertions.*;

class LocalFreshnessFrameTest {
    @Test void localStateNeverChangesLegacyVersionedOrTimestampedFrameLayout() {
        var version=new Version(123,UUID.randomUUID());var serializer=new JdkCacheSerializer<String>();
        for(var plain:List.of(StoredEntry.ofValue("v"),StoredEntry.<String>nullMarker(),
                StoredEntry.ofValue("v",version),StoredEntry.<String>nullMarker(version),
                StoredEntry.ofValue("v",version,321),StoredEntry.<String>nullMarker(version,321))) {
            var decorated=plain.withLocalFreshness(new StoredEntry.LocalFreshness(11,22,33,44,version));
            for(boolean timestamp:new boolean[]{false,true}) {
                byte[] expected=ValueFrame.encode(plain,serializer,timestamp),actual=ValueFrame.encode(decorated,serializer,timestamp);
                if(timestamp) {
                    int offset=5+ByteBuffer.wrap(expected,1,4).getInt();
                    Arrays.fill(expected,offset,offset+8,(byte)0);Arrays.fill(actual,offset,offset+8,(byte)0);
                }
                assertArrayEquals(expected,actual);
                var decoded=ValueFrame.decode(actual,serializer);assertNull(decoded.localFreshness());
                assertEquals(plain.version(),decoded.version());assertEquals(plain.isNullMarker(),decoded.isNullMarker());
            }
            assertNull(plain.localFreshness());assertNotSame(plain,decorated);
            assertEquals(plain.hasWriteTimestamp(),decorated.hasWriteTimestamp());
            if(plain.hasWriteTimestamp())assertEquals(plain.writeTimestampMillis(),decorated.writeTimestampMillis());
        }
    }
}
