# L1/L2 convergence and Java 25 pinning investigation

Investigation date: 2026-09-26. Source baseline: `7e566e7`. Breaker optimization was paused before any production implementation changes. All temporary production/test instrumentation was restored after the experiments. The files in `reproducers/` are intentionally failing diagnostic tests outside the build's source sets, not fixes.

## L1/L2 implementation follow-up

The acknowledged-write correction and regression evidence are documented in [the implementation report](../implementation/acknowledged-write-coherence.md). The original findings and experiments below describe baseline `7e566e7`; promoted regressions cover both replacement and true barrier removal. The pinning investigation remains separate.

## Finding 1: a successful write can leave an older L1 value

This is a reproducible correctness defect, not just a noisy timeout in the original TCK. The following ordering fails deterministically:

1. The same cache already holds `k=old` and `other=old` in L1 and L2, with version barriers for both keys.
2. `put(k, new)` captures the L1 generation and successfully commits its new version to L2.
3. Before its L1 warm completes, another operation updates the barrier for `other`.
4. `L1BarrierMap` reports the barrier replacement through its removal listener. The listener currently advances the global L1 generation for **every** removal cause, including `REPLACED`.
5. `commitL1` rejects the first operation's captured generation. Its caller ignores the unsuccessful warm, leaving the previous `k=old` entry in L1.
6. `put(k, new)` returns normally, and the next `get(k)` returns `old` even though L2 contains `new`.

The local write does not require a lost Pub/Sub event to fail. In a normal invalidation configuration, its own publication is ignored by its sender; that publication is not a repair mechanism for the sender's stale L1. An older retained entry may continue to be served until another invalidation or expiration.

### Evidence

| Experiment | Outcome |
|---|---|
| Controlled core write interleaving, unchanged code | L2=`new`, L1=`old`; assertion fails |
| Same interleaving with a temporary filter excluding `RemovalCause.REPLACED` | Both levels contain `new`; assertion passes |
| Same filter, but an actual barrier removal on the other key | L2=`new`, L1=`old`; assertion still fails |
| Controlled interleaving using stock Caffeine L1 and actual Redis 6.2 versioned Lua writes | L2=`new`, L1=`old`; assertion fails |
| Original randomized Redis race, 20 repetitions in one JVM | All passed; the random scheduling does not reliably exercise this ordering |

The stock-Caffeine/Redis reproducer recorded the same writer UUID on both versions: L2 sequence `1790425836229183`, L1 sequence `1790425836215350`. The cache retained an older write by that writer, not a conflicting UUID tie-break result.

The controlled ordering uses an SPI wrapper that runs the second operation after the remote write returns and before the first local commit. It adds no delay and models a legal concurrent interleaving. The actual-removal variant invokes the existing internal barrier invalidation operation directly to isolate generation loss from timing and retention thresholds.

This establishes a mechanism capable of explaining the earlier TCK convergence failure. The earlier failure did not retain per-key values and versions, so this investigation does **not** prove it was the only mechanism behind every historical timeout.

### What needs fixing

Two related corrections are needed; the first alone is insufficient:

1. In `L1BarrierMap`, replacing a barrier with an equal or higher protective version must not be classified as forgetting protection. Preserve generation advancement for actual loss, including expiry, size eviction and explicit invalidation. Add tests for each removal cause rather than ignoring all callbacks.
2. Audit successful remote write paths in `DefaultTierCache`. A rejected local warm must not leave an older local entry after an acknowledged newer write. Distinguish a write-side local commit from an optional read-side warm, and act on the local commit outcome. A safe fallback can evict an older retained entry under the same key stripe, preserving a newer concurrent entry and the protective barrier. Do not call an unversioned cleanup that also forgets the barrier, and do not reinstall the candidate after its generation was rejected.

Cover `put`, `putNull`, successful `putIfAbsent`, tagged writes, loader stores and refresh stores. Their return values still describe the remote write outcome; rejection of an L1 warm must neither fabricate failure of an acknowledged L2 write nor retain stale local state.

Do **not** simply remove the generation check. It prevents stale in-flight work from repopulating L1 after protection is forgotten or a full clear changes the generation. Also do not remove the per-key version check or clear newer local values indiscriminately.

Required regressions include: an unrelated barrier replacement; a real unrelated barrier removal; a concurrent full clear; a newer same-key local write; a rejected write against a newer remote value or tombstone; and a degraded L1-only write. Verify final values and versions, not just eventual absence of exceptions. Then repeat the original Redis/Valkey race suite and the multi-instance convergence tests.

### Reproduction

The test copies below are outside Gradle source sets so the diagnostic does not silently make the normal checkout fail. To reproduce against this baseline, copy them into the matching module test package and run the indicated test. Both should fail on baseline `7e566e7` and pass after the acknowledged-write correction; restore/remove the copied diagnostic classes afterward.

```sh
cp docs/diagnostics/reproducers/PutInterleavingDiagnosticTest.java \
  tiercache-core/src/test/java/io/tiercache/
./gradlew :tiercache-core:test --tests '*PutInterleavingDiagnosticTest' --offline

cp docs/diagnostics/reproducers/ConvergenceDiagnosticTest.java \
  tiercache-tck/src/test/java/io/tiercache/tck/
./gradlew :tiercache-tck:test --tests '*ConvergenceDiagnosticTest' --offline
```

The second test needs Docker. The first has two cases: barrier replacement and explicit barrier removal. No service network or Pub/Sub loss is simulated.

## Finding 2: the observed Java 25 pinning is startup-sensitive

The existing VT stress test records from the first execution of several library read/load paths. Its initial puts warm data for hot keys but do not initialize every read, loader, metric and Caffeine code path. It fails on any recorded `jdk.VirtualThreadPinned` event whose stack includes `io.tiercache.*`.

A preserved cold recording contained 66 pinning events, including explicit reasons for waiting on initialization of `DefaultTierCache.L1Freshness`, `CacheMetricsListener.Outcome`, `CacheMetricsListener.Level` and shaded Caffeine `RemovalCause`. Other stacks showed `BuiltinClassLoader`, `ZipFile` and JAR resource loading. Some events report the less specific reason `Freeze or preempt failed (2)`; a library frame alone does not establish that a TierCache state monitor caused the pin.

### Matched controls

The control warms code using a **different cache**, including the same read/loader operations on virtual threads. The measured cache retains its original hot-key setup and cold loader keys. Measurement still launches 100,000 virtual threads with 20 operations each. The three paired controls also retain every submitted Future and call `get()` on it, so worker exceptions cannot silently turn a failed workload into a successful gate.

| Fresh Java 25 JVM | Total pinning events | Events with product-code frames | Maximum event duration |
|---|---:|---:|---:|
| Cold 2 | 48 | 45 | 6.733 ms |
| Cold 3 | 44 | 43 | 2.540 ms |
| Cold 4 | 26 | 26 | 9.304 ms |
| Code-warm 2 | 0 | 0 | — |
| Code-warm 3 | 0 | 0 | — |
| Code-warm 4 | 0 | 0 | — |

An additional preliminary warm run also had zero events. This is evidence for startup-sensitive pinning in this fixture, not a guarantee of zero pinning in every production workload. It is not an explanation for the independent L1/L2 correctness defect and does not justify removing `synchronized` from the cache.

The runtime was the installed GraalVM JDK 25. No generalization to every JDK 25 distribution or patch level is implied. See [recording summaries](java25-pinning-results.json) for all eight recordings and their event reasons. Aggregate durations there sum waits across threads and must not be read as request latency.

### What to change in the gate

- Separate cold-start observation from a steady-state zero-pinning gate. Keep cold-start events visible, retain their reasons and durations, and decide an explicit acceptance policy for them; do not silently discard them as irrelevant.
- Warm the exercised code paths on a separate cache before steady-state recording. Preserve the measured workload's cold loader keys, failure paths and concurrency. Merely warming every measured key would change the workload into L1 hits and conceal the paths under test.
- Retain the JFR recording on failure. The current `finally` deletes it, leaving only one stack in the assertion message, which makes attribution unnecessarily difficult.
- Await every submitted Future and validate results. Executor shutdown waits for completion but does not propagate exceptions stored in ignored Future instances.
- Attribute product frames separately from `io.tiercache.tck` and `io.tiercache.testkit` frames. Record `blockingOperation`, `pinnedReason`, duration and stack, rather than assuming any library frame is the blocking cause.
- Retain the separate recovery and lock-lifecycle JFR gates and rerun the supported JVM matrix after changing the test protocol.

The temporary warmup was a diagnostic control, not an accepted change to the project's zero-pinning contract. No production prewarming API or special-case class initialization workaround was added.

## Recommended order

First fix the successful-write/L1-commit defect with deterministic regressions, including actual barrier loss. Then update the VT validation protocol explicitly with separate startup and steady-state evidence. Resume CLOSED breaker admission optimization only afterward; neither confirmed stale reads nor a redefined pinning gate should be hidden inside that performance change.
