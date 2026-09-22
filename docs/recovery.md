# Triggered invalidation recovery

## Requests do not run replay

A detected reconnect, enough successful HALF_OPEN probes, or journal
tracking pressure schedules recovery. Live invalidations still apply under
the per-cache in-memory state gate; journal reads and observer callbacks run
outside cache-state, L1 and breaker monitors. A successful probe returns its
actual cache result without waiting for the journal backlog. These changes
do not make cache reads strongly consistent.

Each factory with an invalidation handler owns one scheduler with two daemon
workers. A directly constructed built-in invalidation service lazily owns
its own two workers unless a factory supplies them. Recovery never waits
for another job submitted to the same pool. There is at most one queued
token and one active pass per registered cache, without a per-key/event
backlog. A pass reads at most 16 batches of at most 256 rows, then yields so
another ready cache can progress. Version tracking remains bounded to 512
unconfirmed versions and 256 recently confirmed versions plus one read batch.
Live events received during a read remain tracked until that read or a later
read actually accounts for them.

After a failed pass, the cache retains one pending job and retries after
1, 2, 4, 8, 16, then at most 30 seconds. New triggers do not bypass that delay.
A safe reset also delays its follow-up read, so a persistently unreadable
journal cannot cause an immediate repeated-flush loop. Healthy caches are
not periodically polled. A silently missed event can remain stale until a
recovery trigger; having a journal alone does not detect every gap.

## Completion and fallback

With a recovery handler, successful probes leave the breaker HALF_OPEN
while coherence recovery is pending. Additional L2 attempts are rejected
and use the existing local/degradation paths. CLOSED and the recovered
notification occur only after that episode's replay or safe reset completes.
An old completion cannot close a newer outage. A failed attempt returns the
breaker to OPEN; another attempt respects both the probe wait and recovery
backoff, never retrying faster than once per second.

Verified retained history preserves unaffected L1 entries. If required
history is trimmed or cannot be verified, recovery may clear that cache's
L1. It captures the journal-end baseline **before** clearing, validates the
reserved generation, then commits the clear and cursor together. Rows after
the baseline remain eligible for subsequent replay. This is not a source
transaction, and a fleet-wide fallback can create a source-load burst.

If the baseline read fails, L1 is still cleared conservatively, the previous
confirmed cursor is retained, and recovery stays pending. Such a clear is
not successful recovery and cannot authorize skipping a poison row. The
per-cache reset result carries a safe baseline and generation only when
those were established. An engine explicitly configured without a journal
can clear registered L1 caches, but does not report journal replay/overflow.
A factory with no invalidation handler has no coherence-recovery hook.

Callbacks run after state commits and outside state monitors. An observer
exception is logged and cannot undo a committed row, cursor or clear. This
does not make arbitrary user callbacks fast or provide durable observer
delivery. There is no fixed global recovery deadline; total duration depends
on backlog, server latency and concurrent writes. Commands, per-pass work,
concurrency, retained state and retry frequency are bounded separately.

## Signals and shutdown

`tiercache.invalidation.recovery.pending{cache}` is 1 while triggered work is
unsettled, including failure retries, and 0 when settled. A safe reset may
allow the breaker to close while its follow-up read is still pending, so
use the gauge together with breaker state and logs. `tiercache.degraded`
still describes OPEN only; its value 0 during HALF_OPEN does not mean that
new L2 calls are admitted while recovery is pending.

Closing the factory cancels queued jobs, invalidates in-flight generations,
unregisters pending gauges and shuts down its workers. Unregistration does
not report successful recovery. The breaker detaches the coherence hook
without inventing a successful probe; previously obtained synchronous caches
can later probe an available caller-owned L2 without restarting closed
coherence services. They have no post-close cross-instance coherence promise.
This does not redesign watchdog/lock-provider shutdown behavior.

## Internal SPI migration

Existing handler signatures remain. `configureRecoveryExecutor` and
`recoverAsync` are additive defaults: legacy recovery hooks run on the owned
workers and must throw if they fail; a normal legacy return is treated as
completion. Built-in recovery composes asynchronous cache passes instead of
blocking a worker on queued work. Code that needs completion must observe
the returned stage, not assume that returning from `onL2Recovery` or a
transport reconnect callback means replay has finished.

Targets with concurrent local clears implement the additive recovery epoch
and conditional-application hooks. The built-in target checks those epochs
inside its existing per-key commit protocol. Legacy target defaults preserve
source compatibility but cannot invent atomic clear ordering for a custom
implementation. No stored value or invalidation wire format changes here.

## Verification

Deterministic gates cover parked reads versus delivery, clear, repeated
recovery and close; baseline-before-clear and failed-baseline outcomes;
throwing observers; one-token-per-cache scheduling, fairness and backoff;
and epoch-guarded breaker completion. The real Redis JFR case holds journal
recovery while a virtual-thread HTTP request and registered reconnect
callback must return. It then delays actual Redis reads and verifies no
library-attributed `jdk.VirtualThreadPinned` events on JDK 21. Repeat on a
newer JDK for functional ordering; lack of pinning on newer JVMs alone does
not prove absence of monitor serialization.

```sh
./gradlew :tiercache-tck:vtStressTest --tests '*RecoveryJfrTest'
./gradlew :tiercache-tck:vtStressTest --tests '*RecoveryJfrTest' -PtiercacheVtJdk=25
```

Recordings are retained in `tiercache-tck/build/reports/recovery-jdk*.jfr`.
