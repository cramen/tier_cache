package io.tiercache.spi;

/**
 * Result of a tagged store, distinct from transport failure or rejected L2 admission.
 * Internal SPI; not part of the supported application API.
 *
 * @since 1.5.0
 */
public enum TaggedWriteOutcome {
    /** The store was accepted under the transport's configured versioning contract. */
    WON,
    /** A newer entry or tombstone rejected the candidate without side effects. */
    LOST,
    /** This transport cannot report a versioned tagged-write outcome. */
    UNSUPPORTED
}
