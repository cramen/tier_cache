package io.tiercache.internal;

/** Internal lifecycle outcome: coordination is closed, not contended or failed remotely. */
public final class LockProviderClosedException extends IllegalStateException {
    public LockProviderClosedException() { super("Lock provider is closed"); }
}
