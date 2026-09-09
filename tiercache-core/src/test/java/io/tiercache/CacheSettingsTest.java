package io.tiercache;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: cache-configuration — validation branches of the settings model.
 */
class CacheSettingsTest {

    @Test
    void rejectsNonPositiveL1MaxSize() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(0, Duration.ofMinutes(1), null, Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024));
    }

    @Test
    void rejectsNonPositiveL1ExpireAfterWrite() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ZERO, null, Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024));
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(-1), null, Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024));
    }

    @Test
    void rejectsNonPositiveL2Ttl() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ZERO, 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024));
    }

    @Test
    void rejectsNonPositiveExpireAfterAccess() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), Duration.ZERO, Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024));
    }

    @Test
    void rejectsJitterAmplitudeOutOfRange() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1), -0.1, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024));
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1), 1.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024));
    }

    @Test
    void acceptsBoundaryJitterAmplitude() {
        CacheSettings s = new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024);
        assertEquals(0.0, s.jitterAmplitude());
    }

    @Test
    void rejectsNullMandatoryDurations() {
        assertThrows(NullPointerException.class, () ->
                new CacheSettings(10, null, null, Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024));
        assertThrows(NullPointerException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, null, 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024));
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
    void overrideReplacesInvalidationModeAndCap() {
        CacheSettings resolved = new CacheOverride()
                .invalidationMode(InvalidationMode.UPDATE)
                .payloadCapBytes(128 * 1024)
                .resolve(CacheSettings.defaults());
        assertEquals(InvalidationMode.UPDATE, resolved.invalidationMode());
        assertEquals(128 * 1024, resolved.payloadCapBytes());
        // Inherits when unspecified.
        CacheSettings inherited = new CacheOverride().resolve(CacheSettings.defaults());
        assertEquals(InvalidationMode.INVALIDATE, inherited.invalidationMode());
    }

    @Test
    void rejectsTinyPayloadCap() {
        assertThrows(IllegalArgumentException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1), 0.0,
                        NullPolicy.deny(), InvalidationMode.INVALIDATE, 100));
        assertThrows(NullPointerException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1), 0.0,
                        NullPolicy.deny(), null, 64 * 1024));
    }

    @Test
    void defaultsHaveNullExpireAfterAccess() {
        assertNull(CacheSettings.defaults().l1ExpireAfterAccess());
    }

    @Test
    void defaultsKeepStaleServingOff() {
        CacheSettings defaults = CacheSettings.defaults();
        assertEquals(Duration.ZERO, defaults.staleTtl());
        assertFalse(defaults.xfetchEnabled());
        assertEquals(Duration.ofSeconds(1), defaults.xfetchBeta());
    }

    @Test
    void compactConstructorDisablesStaleServing() {
        CacheSettings s = new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1),
                0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024);
        assertEquals(Duration.ZERO, s.staleTtl());
        assertFalse(s.xfetchEnabled());
        assertEquals(Duration.ofSeconds(1), s.xfetchBeta());
    }

    @Test
    void rejectsNullStaleServingDurations() {
        assertThrows(NullPointerException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1), 0.0,
                        NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                        null, false, Duration.ofSeconds(1)));
        assertThrows(NullPointerException.class, () ->
                new CacheSettings(10, Duration.ofMinutes(1), null, Duration.ofHours(1), 0.0,
                        NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                        Duration.ZERO, false, null));
    }

    @Test
    void overrideInheritsStaleServingSettings() {
        CacheSettings resolved = new CacheOverride().resolve(CacheSettings.defaults());
        assertEquals(Duration.ZERO, resolved.staleTtl());
        assertFalse(resolved.xfetchEnabled());
        assertEquals(Duration.ofSeconds(1), resolved.xfetchBeta());
    }

    @Test
    void overrideReplacesStaleServingSettings() {
        CacheSettings resolved = new CacheOverride()
                .staleTtl(Duration.ofMinutes(5))
                .xfetchEnabled(true)
                .xfetchBeta(Duration.ofSeconds(2))
                .resolve(CacheSettings.defaults());
        assertEquals(Duration.ofMinutes(5), resolved.staleTtl());
        assertTrue(resolved.xfetchEnabled());
        assertEquals(Duration.ofSeconds(2), resolved.xfetchBeta());
    }
}
