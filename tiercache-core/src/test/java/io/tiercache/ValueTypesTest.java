package io.tiercache;

import io.tiercache.spi.StoredEntry;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Edge-case branches of the value types. */
class ValueTypesTest {

    @Test
    void storedEntryRejectsNullValue() {
        assertThrows(NullPointerException.class, () -> StoredEntry.ofValue(null));
    }

    @Test
    void markerHasNoValue() {
        assertThrows(IllegalStateException.class, () -> StoredEntry.nullMarker().value());
    }

    @Test
    void allowRejectsNonPositiveTtl() {
        assertThrows(IllegalArgumentException.class, () -> NullPolicy.allow(Duration.ZERO));
        assertThrows(NullPointerException.class, () -> NullPolicy.allow(null));
    }

    @Test
    void policyToStringIsInformative() {
        assertEquals("deny", NullPolicy.deny().toString());
        assertEquals("allow(PT1M)", NullPolicy.allow(Duration.ofMinutes(1)).toString());
    }
}
