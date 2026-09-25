package io.tiercache.spi;

import java.util.concurrent.CompletionStage;

/** Internal handshake: covered rows require a committed, still-current per-cache reset proof. */
public interface InvalidationGapHandler {
    /** Request bounded baseline-before-clear recovery. Failed/closed results authorize no ACK. */
    CompletionStage<RecoveryResult> reset(String cache);
    /** Repair locally lost delivery; legacy implementations retain conservative reset behavior. */
    default CompletionStage<RecoveryResult> recoverLocalGap(String cache) { return reset(cache); }
    /** Local resume proof, separate from the Streams RESET_SAFE row-coverage proof. */
    default boolean isLocalRecoveryCurrent(String cache, RecoveryResult result) {
        return isCurrent(cache, result);
    }
    /** Baseline established while registering an empty/reset target, or null when unavailable. */
    RecoveryResult registrationBaseline(String cache);
    /** Side-effect-free validity check; rejects proofs from another cache or superseded lifecycle. */
    boolean isCurrent(String cache, RecoveryResult result);
}
