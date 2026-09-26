# Lock-provider shutdown investigation

## Finding

The previous assertion confused requesting executor shutdown with observing worker termination, and counted every provider's compensation threads in the JVM. Neither the acquisition thread's join nor `shutdownNow()` establishes that compensation workers have exited. A controlled test now reproduces the same positive live-worker observation after close, identifies the exact owned executor, and proves it terminates after its already-admitted command returns without a retry. A second-provider control shows why a global zero-thread assertion is invalid. Running the original test source from `8b4cb12` unchanged alongside an independent active provider deterministically failed in round 0 (`expected 0, observed 2`), although that independent provider was correctly still active. The reproducer then closed it and verified termination.

No production lifecycle change is justified by this investigation so far. Only tests and their diagnostic helpers have changed; the production `LettuceLockProvider` remains byte-for-byte unchanged from baseline `8b4cb12`. This is not a promise of synchronous worker termination on return from close, or evidence that arbitrary uninterruptible user code will terminate.

The historical Java 25 failure did not preserve a provider identity or stack, so its exact thread cannot retrospectively be attributed. The baseline focused test and full compensation/ownership classes both passed in this run. Those passes are not the explanation: the gated reproduction establishes that the old assertion can reject a correct shutdown, while negative controls require the replacement assertions to detect actual lifecycle defects.

## Lifecycle and causal evidence

| Phase | Ownership / ordering | Check |
| --- | --- | --- |
| First ambiguous acquire | Under `lifecycle`, check `closed`, publish scheduler, register compensation, schedule | Close-first creates no scheduler; publication-first exposes exactly the scheduler close must dispose |
| Retirement | Under `lifecycle`, set closed, capture scheduler and owned connection, retire initialization and clear pending tasks | No new initialization or compensation admission; pending count zero |
| Disposal | Outside lifecycle lock, fail initialization waiters, call shutdownNow, close owned connection | Exact scheduler is shutdown and eventually terminated; borrowed connection remains owned by caller |
| Running compensation | EVAL outside lock, then recheck retirement before retry | Controlled EVAL can survive the return from close; releasing it produces termination and exactly one EVAL |
| Concurrent close | One caller wins retirement; others may return earlier than its disposal | Join all close callers before asserting exactly-once scheduler disposal |
| Late callback | Retired bookkeeping prevents work through the old callback | No EVAL and no schedule attempt after invoking the captured retired callback |

`LockProviderShutdownTest` covers both publication orders in 100 gated iterations per JVM, interruptible and temporarily non-cooperative commands, an independent live provider, and concurrent close. `LockShutdownProbe` reads the specific scheduler by reflection only in tests; its only write installs a counting executor before use for the retired-callback control. There is no new runtime test seam or public API. The default executor is used in all other scenarios.

The old real-Redis race test still exercises uncontrolled scheduling, but now records the exact acquisition outcome, asserts the acquisition thread has exited, checks the actual provider's scheduler, and cleans up in `finally`. The related close-during-acquire assertion checks that this provider never constructed a scheduler rather than inspecting global thread names.

Tests assert retirement separately from termination. The five-second termination observation ceiling is a test safety bound, not a production shutdown SLA; it was fixed before candidate runs. Failures include executor identity/state, pending count, a monotonic timestamp, and supplemental worker stacks. Thread names are not used to infer ownership. Controlled blocking work is always released in teardown.

## Sensitivity controls

Disposable copies of the production class were compiled outside the repository and prepended to the test classpath:

- Removing scheduler shutdown makes the new identity-based shutdown assertion fail with the exact executor still Running.
- Removing retirement guards allows a captured old callback to execute Redis I/O and attempt scheduling; the callback test fails.

Both mutants were rejected (exit 1), while the unchanged class passes. The temporary classes never replaced repository build outputs. The first mutant compilation attempt failed because the host's default javac encoding was US-ASCII; explicit UTF-8 corrected compilation before any mutant test was run. This was a harness compilation error, not a library test failure.

## Validation

All planned checks completed successfully on macOS arm64 with Docker 20.10.21. The direct-launch matrix used Amazon Corretto 17.0.5, OpenJDK 21+35, and Oracle GraalVM 25+37. Exact versions, commands, elapsed times, and environment details are in the evidence.

| Check | Result |
| --- | --- |
| Focused lifecycle matrix | 20 fresh JVMs on each of Java 17/21/25; 60 forks, 6 tests per fork, 6,000 gated iterations; no failures |
| Full compensation/ownership classes | Three fresh JVMs per JDK, 18 tests each; all nine forks passed |
| Final cleanup review | Another three full-class forks per JDK after making connection cleanup unconditional even on join failure; all nine passed; focused test code unchanged |
| Redis 6.2.24 / 7.4.11 / 8.10.2 / Valkey 9.1.2 | 169 transport tests per pinned profile; zero failures/errors/skips |
| Normal build, then final build | Both successful; 200 transport tests in the final build, with unchanged tasks legitimately up-to-date |
| Complete VT gate, Java 21 and 25 | Cold-start diagnostics complete, steady-state PASS, four independent tests per JDK pass, including lock lifecycle |
| Sensitivity | Both faulty runtime mutants rejected; original assertion deterministically rejects an unrelated live provider |

No library/test failure was retried to green. The additional full-class runs follow an explicit cleanup-only test change; earlier results are retained. The test observation deadline stayed at five seconds.

Portable evidence is in [lock-provider-shutdown-raw.json.gz](lock-provider-shutdown-raw.json.gz): a gzip-compressed JSON object containing a summary and a `files` map of relative paths to original text. It includes the matrix plans/results, launcher and diagnostic sources, logs, per-profile JUnit XML, JVM metadata, and VT summaries. Binary JFR recordings are retained with the local workspace evidence under `outputs/lock-provider-shutdown`, rather than embedded in the text bundle.

## Reproduction

The compressed JSON evidence bundle stores the JUnit launcher source as `files["RunTests.java"]`, all matrix commands/results, original-assertion reproducer source, mutant source, and logs. Export the current test classpath first:

```sh
./gradlew -I scripts/pubsub-runtime.gradle :tiercache-transport-redis:exportPubSubRuntime \
  -PpubsubClasspathFile=/tmp/lock-shutdown-cp.txt --offline --console=plain
```

Extract `RunTests.java` to a temporary directory, compile it with Java 17 (`javac -encoding UTF-8 -cp <exported-classpath> RunTests.java`), and run the selected JDK's `java -cp <launcher-directory>:<exported-classpath> RunTests` with these class arguments:

- Focused: `io.tiercache.redis.LockProviderShutdownTest` (20 fresh JVMs per JDK; each includes 100 gated iterations).
- Full classes: `io.tiercache.redis.LettuceLockProviderCompensationTest io.tiercache.redis.LockOwnershipTest` (three fresh JVMs per JDK).

Use the installed JDK paths rather than assuming the launcher JVM and the Gradle toolchain are the same. The evidence records the actual Java 17/21/25 vendors and versions. The integration commands use `:tiercache-transport-redis:serverContractTest -PserverImage=<pinned-image>` and `:tiercache-tck:vtStressTest -PtiercacheVtJdk=21` / `25`; their complete argument lists are retained in the evidence. All runs use a fixed plan, fail on unexpected test failures, and preserve output from every fork.

## Limits

This investigation targets compensation shutdown and existing resource ownership. It does not validate maximum cache throughput, Sentinel failover, or new lifecycle guarantees. A provider using a borrowed connection cannot forcibly close that resource to terminate arbitrary blocking code. Actual Redis commands remain bounded by their configured timeout; lock-token checks and the lease-expiry residual remain unchanged. Nothing here weakens the existing resource-disposal requirement.
