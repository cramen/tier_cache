package io.tiercache.spi;

/** Terminal publication classification; acknowledgement is not receiver application. */
public enum PublicationOutcome {
    ACKNOWLEDGED, FAILED, UNCONFIRMED, NOT_REQUIRED
}
