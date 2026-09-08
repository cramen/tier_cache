package io.tiercache;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Spec: cache-configuration — validation branches of the settings model.
 */
class CacheSettingsTest {

    @Test
    void rejectsNonPositiveL1MaxSize() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(0, Duration.ofMinutes(1), null, Duration.ofHours(1), 0.0));
    }

    @Test
    void rejectsNonPositiveL1ExpireAfterWrite() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ZERO, null, Duration.ofHours(1), 0.0));
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(-1), null, Duration.ofHours(1), 0.0));
    }

    @Test
    void rejectsNonPositiveL2Ttl() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ZERO, 0.0));
    }

    @Test
    void rejectsNonPositiveExpireAfterAccess() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), Duration.ZERO, Duration.ofHours(1), 0.0));
    }

    @Test
    void rejectsJitterAmplitudeOutOfRange() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1), -0.1));
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1), 1.0));
    }

    @Test
    void acceptsBoundaryJitterAmplitude() {
        CacheSettings s = new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1), 0.0);
        assertEquals(0.0, s.jitterAmplitude());
    }

    @Test
    void rejectsNullMandatoryDurations() {
        assertThrows(NullPointerException.class, () ->
                new CacheSettings(10, null, null, Duration.ofHours(1), 0.0));
        assertThrows(NullPointerException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, null, 0.0));
    }

    @Test
    void overrideInheritsUnspecifiedFields() {
        CacheSettings defaults = CacheSettings.defaults();
        CacheSettings resolved = new CacheOverride().l2Ttl(Duration.ofMinutes(30)).resolve(defaults);
        assertEquals(Duration.ofMinutes(30), resolved.l2Ttl());
        assertEquals(defaults.l1MaxSize(), resolved.l1MaxSize());
        assertEquals(defaults.l1ExpireAfterWrite(), resolved.l1ExpireAfterWrite());
        assertEquals(defaults.l1ExpireAfterAccess(), resolved.l1ExpireAfterAccess());
        assertEquals(defaults.jitterAmplitude(), resolved.jitterAmplitude());
    }

    @Test
    void overrideReplacesEveryField() {
        CacheSettings resolved = new CacheOverride()
                .l1MaxSize(42)
                .l1ExpireAfterWrite(Duration.ofMinutes(2))
                .l1ExpireAfterAccess(Duration.ofMinutes(1))
                .l2Ttl(Duration.ofMinutes(30))
                .jitterAmplitude(0.05)
                .resolve(CacheSettings.defaults());
        assertEquals(42, resolved.l1MaxSize());
        assertEquals(Duration.ofMinutes(2), resolved.l1ExpireAfterWrite());
        assertEquals(Duration.ofMinutes(1), resolved.l1ExpireAfterAccess());
        assertEquals(Duration.ofMinutes(30), resolved.l2Ttl());
        assertEquals(0.05, resolved.jitterAmplitude());
    }

    @Test
    void defaultsHaveNullExpireAfterAccess() {
        assertNull(CacheSettings.defaults().l1ExpireAfterAccess());
    }
}
