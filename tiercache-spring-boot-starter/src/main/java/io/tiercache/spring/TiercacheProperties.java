package io.tiercache.spring;

import io.tiercache.CacheOverride;
import io.tiercache.CacheSettings;
import io.tiercache.NullPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuration properties under the {@code tiercache.*} prefix.
 * Maps 1:1 onto core's configuration model; validation is core's
 * (fail-fast at startup).
 */
@ConfigurationProperties("tiercache")
public class TiercacheProperties {

    /** Master switch: the starter contributes beans only when true. */
    private boolean enabled = false;

    /** Redis/Valkey URI for the L2 transport, e.g. redis://localhost:6379. */
    private String redisUri;

    /** Global defaults applied to every cache without explicit overrides. */
    private CacheProps defaults = new CacheProps();

    /** Per-cache overrides by cache name. */
    private Map<String, CacheProps> caches = new LinkedHashMap<>();

    /** Cross-instance invalidation settings. */
    private InvalidationProps invalidation = new InvalidationProps();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getRedisUri() {
        return redisUri;
    }

    public void setRedisUri(String redisUri) {
        this.redisUri = redisUri;
    }

    public CacheProps getDefaults() {
        return defaults;
    }

    public void setDefaults(CacheProps defaults) {
        this.defaults = defaults;
    }

    public Map<String, CacheProps> getCaches() {
        return caches;
    }

    public InvalidationProps getInvalidation() {
        return invalidation;
    }

    public void setInvalidation(InvalidationProps invalidation) {
        this.invalidation = invalidation;
    }

    /** Invalidation settings: Pub/Sub profile is on by default when the Redis transport is used. */
    public static class InvalidationProps {

        private boolean enabled = true;

        /** Invalidation transport profile: pubsub (default) or streams. */
        private String profile = "pubsub";

        /** Max journal entries kept per cache stream. */
        private int journalCapacity = 10_000;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getProfile() {
            return profile;
        }

        public void setProfile(String profile) {
            this.profile = profile;
        }

        public int getJournalCapacity() {
            return journalCapacity;
        }

        public void setJournalCapacity(int journalCapacity) {
            this.journalCapacity = journalCapacity;
        }
    }

    public void setCaches(Map<String, CacheProps> caches) {
        this.caches = caches;
    }

    /**
     * Per-cache settings; null fields inherit from the defaults level.
     */
    public static class CacheProps {

        private Long l1MaxSize;
        private Duration l1ExpireAfterWrite;
        private Duration l1ExpireAfterAccess;
        private Duration l2Ttl;
        private Double jitterAmplitude;
        private Kind nullPolicy;
        private Duration nullMarkerTtl;
        private io.tiercache.InvalidationMode invalidationMode;
        private Long payloadCapBytes;
        private Duration staleTtl;
        private Boolean xfetchEnabled;
        private Duration xfetchBeta;

        public enum Kind {
            DENY, ALLOW
        }

        public Long getL1MaxSize() {
            return l1MaxSize;
        }

        public void setL1MaxSize(Long l1MaxSize) {
            this.l1MaxSize = l1MaxSize;
        }

        public Duration getL1ExpireAfterWrite() {
            return l1ExpireAfterWrite;
        }

        public void setL1ExpireAfterWrite(Duration l1ExpireAfterWrite) {
            this.l1ExpireAfterWrite = l1ExpireAfterWrite;
        }

        public Duration getL1ExpireAfterAccess() {
            return l1ExpireAfterAccess;
        }

        public void setL1ExpireAfterAccess(Duration l1ExpireAfterAccess) {
            this.l1ExpireAfterAccess = l1ExpireAfterAccess;
        }

        public Duration getL2Ttl() {
            return l2Ttl;
        }

        public void setL2Ttl(Duration l2Ttl) {
            this.l2Ttl = l2Ttl;
        }

        public Double getJitterAmplitude() {
            return jitterAmplitude;
        }

        public void setJitterAmplitude(Double jitterAmplitude) {
            this.jitterAmplitude = jitterAmplitude;
        }

        public Kind getNullPolicy() {
            return nullPolicy;
        }

        public void setNullPolicy(Kind nullPolicy) {
            this.nullPolicy = nullPolicy;
        }

        public io.tiercache.InvalidationMode getInvalidationMode() {
            return invalidationMode;
        }

        public void setInvalidationMode(io.tiercache.InvalidationMode invalidationMode) {
            this.invalidationMode = invalidationMode;
        }

        public Long getPayloadCapBytes() {
            return payloadCapBytes;
        }

        public void setPayloadCapBytes(Long payloadCapBytes) {
            this.payloadCapBytes = payloadCapBytes;
        }

        public Duration getNullMarkerTtl() {
            return nullMarkerTtl;
        }

        public void setNullMarkerTtl(Duration nullMarkerTtl) {
            this.nullMarkerTtl = nullMarkerTtl;
        }

        public Duration getStaleTtl() {
            return staleTtl;
        }

        public void setStaleTtl(Duration staleTtl) {
            this.staleTtl = staleTtl;
        }

        public Boolean getXfetchEnabled() {
            return xfetchEnabled;
        }

        public void setXfetchEnabled(Boolean xfetchEnabled) {
            this.xfetchEnabled = xfetchEnabled;
        }

        public Duration getXfetchBeta() {
            return xfetchBeta;
        }

        public void setXfetchBeta(Duration xfetchBeta) {
            this.xfetchBeta = xfetchBeta;
        }

        NullPolicy toNullPolicy() {
            if (nullPolicy == null) {
                return null;
            }
            return switch (nullPolicy) {
                case DENY -> NullPolicy.deny();
                case ALLOW -> NullPolicy.allow(
                        nullMarkerTtl != null ? nullMarkerTtl : Duration.ofMinutes(1));
            };
        }

        /** Resolves into full settings against the given base. */
        CacheSettings toSettings(CacheSettings base) {
            return toOverride().resolve(base);
        }

        CacheOverride toOverride() {
            CacheOverride override = new CacheOverride();
            if (l1MaxSize != null) {
                override.l1MaxSize(l1MaxSize);
            }
            if (l1ExpireAfterWrite != null) {
                override.l1ExpireAfterWrite(l1ExpireAfterWrite);
            }
            if (l1ExpireAfterAccess != null) {
                override.l1ExpireAfterAccess(l1ExpireAfterAccess);
            }
            if (l2Ttl != null) {
                override.l2Ttl(l2Ttl);
            }
            if (jitterAmplitude != null) {
                override.jitterAmplitude(jitterAmplitude);
            }
            NullPolicy policy = toNullPolicy();
            if (policy != null) {
                override.nullPolicy(policy);
            }
            if (invalidationMode != null) {
                override.invalidationMode(invalidationMode);
            }
            if (payloadCapBytes != null) {
                override.payloadCapBytes(payloadCapBytes);
            }
            if (staleTtl != null) {
                override.staleTtl(staleTtl);
            }
            if (xfetchEnabled != null) {
                override.xfetchEnabled(xfetchEnabled);
            }
            if (xfetchBeta != null) {
                override.xfetchBeta(xfetchBeta);
            }
            return override;
        }
    }
}
