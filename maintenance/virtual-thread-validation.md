# Virtual-thread validation: startup and steady state

The general VT gate now has two acceptance policies. Cold-start pinning is mandatory diagnostic evidence; its event count alone does not fail validation. Steady-state product-attributed pinning remains strictly zero. Both phases fail on wrong results, worker failures/cancellation, incomplete work, deadline or cleanup failure, invalid runtime selection, or unusable JFR evidence.

## Running the complete gate

```sh
./gradlew :tiercache-tck:vtStressTest -PtiercacheVtJdk=21
./gradlew :tiercache-tck:vtStressTest -PtiercacheVtJdk=25
python3 scripts/verify-vt-gates.py --output build/vt-validation
```

The script performs three complete invocations per runtime, checks unique phase JVMs, operation/loader counts, test results and artifacts, and retains every result without retry-to-green. Use a new output directory for each acceptance series.

`vtStressTest` includes `vtColdStartTest`, `vtSteadyStateTest`, real-journal recovery, lock-lifecycle and phase-failure tests. Each phase has one test class in a separate fresh test process; it cannot inherit warmup from the other phase. Running just a phase is useful for diagnosis but is not full-suite acceptance. Explicit gate invocations do not reuse up-to-date or build-cache results.

Java 17 compiles/runs the shared harness unit tests, without virtual-thread APIs:

```sh
./gradlew :tiercache-tck:test --tests '*VtEvidenceTest' --tests '*ObservedWorkersTest'
```

The VT launcher defaults to Java 21 for backward-compatible local invocation. Explicit `tiercacheVtJdk` selection requires that runtime; unavailability is a failure, not a pass or fallback to another installed JVM. With no explicit selection and no default VT toolchain, the existing loud local skip remains a skip. CI explicitly selects each 21/25 leg and uploads available artifacts even after failure.

## Workload and phase boundaries

Cold recording begins before cache construction/data setup, without explicit library warmup. It measures first use of the library inside a fresh test JVM, not complete OS process startup.

Steady-state warmup runs 1,000 VTs with 20 operations each on a disposable cache/factory and separate local/remote storage. Warmup workers terminate and that factory closes before measured state is created. It is fixed work, not an adaptive loop that repeats until pinning vanishes.

Each measured phase launches 100,000 VTs with 20 operations each: half hot reads and half loader-backed requests across 1,000 hot and 1,000 loader keys. Only hot keys are seeded. The harness checks loader-key absence in both L1 and L2 before workers start, validates every returned value, and requires actual loader executions during measurement. Loader keys become cached naturally; two million operations do not mean two million misses. Setup, warmup and measured totals are separate.

One overall worker deadline covers submission and result collection. Defaults are 30 seconds for warmup, 120 seconds for measured workers and 10 seconds for cancellation/termination. Test-only overrides `-Dtiercache.vt.warmupSeconds`, `workSeconds` and `cleanupSeconds` must be positive and no greater than 3600; they are reported. A five-minute Gradle task timeout is an independent outer containment bound, including when larger individual overrides are supplied. A killed/incomplete run is not a pass. The worker runner avoids unbounded executor.close, observes future failures including Errors, cancels remaining work and bounds termination while preserving the original failure.

## Evidence and attribution

By default artifacts are under `tiercache-tck/build/reports/vt/jdk-<actual-runtime>/<phase>/<run-id>/`. Override the root with `-PtiercacheVtOutput=<path>`. Each cold/steady directory keeps `recording.jfr` and `summary.json`, on success and failure. Independent recovery/lock recordings occupy separate run directories under `independent/`. No fixed user-home or temporary-platform path is required.

The summary records phase status, runtime/vendor, PID, requested JDK, source revision/dirty status, JVM settings, workload/deadlines, observed work and pinning evidence. Cold success is `DIAGNOSTIC_COMPLETE`; steady-state success is `PASS`. Startup/workload errors are `FAILED`; the initial `INCOMPLETE` summary survives an unexpected process termination. Failure before recording does not manufacture a JFR file.

The requested pinning event must be available, enabled at threshold zero and stack-bearing. A valid zero-event recording differs from missing or corrupt data. Every recording carries a matching run-identity marker and is decoded before acceptance. Captured events without classifiable stacks fail evidence validation. Product attribution includes `io.tiercache.*` and shaded Caffeine, excluding `io.tiercache.tck.*` and `io.tiercache.testkit.*`. Other events remain in totals and in the full recording. Optional `pinnedReason` and `blockingOperation` fields are retained where the JVM supplies them; older runtimes report unavailable fields explicitly.

A product frame is conservative attribution, not proof that frame is the blocking cause. No reason or duration whitelist exempts a steady-state product event. Summary examples are bounded; the full JFR retains all events. Aggregate wait duration sums across threads and is not request latency. Zero pinning in this fixture is not a throughput claim or a universal guarantee for arbitrary user callbacks, network I/O, JVM distributions or workloads.

## Harness checks

Java 17 tests cover attribution, positive event decoding, optional fields, zero-event evidence, missing/corrupt/mismatched recordings, missing stacks, disabled/nonzero-threshold settings, JSON escaping, exact worker totals, Errors, cancellation and deadline cleanup. VT phase-failure tests verify original worker failures retain recordings and a JVM mismatch cannot produce a pass or fake recording. Existing recovery and lock-lifecycle policies remain unchanged.
