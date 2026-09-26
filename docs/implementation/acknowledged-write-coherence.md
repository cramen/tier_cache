# Local coherence after an acknowledged remote write

The fix separates an optional local read fill from local handling of a confirmed remote write. A generation change still rejects installation. The acknowledged-write path then removes an older retained L1 value under the same key stripe, keeping equal/newer values and protective metadata. No remote repair I/O is performed and the successful remote outcome is unchanged.

Monotonic replacement in L1BarrierMap no longer invokes the forgotten-protection callback. Expiration, capacity eviction, explicit invalidation and bulk removal still advance protection generations. Disabling replacement callbacks alone is insufficient: true barrier loss must also be handled by write-side cleanup.

## Call-site and outcome inventory

| Engine path | Confirmed remote result | Rejected, skipped or unavailable result | Publication / returned result |
|---|---|---|---|
| `put` | `l2ConditionalPut == true` uses `commitAcknowledgedWrite` | false converges through the existing remote read; null remains local-only | Publish only accepted store; void return remains unchanged |
| `putIfAbsent` | true uses acknowledged-write policy | false remains a loser; null uses existing local atomic commit | Accepted winner stays true even if its local fill was refused |
| Tagged put | `TaggedWriteOutcome.WON` uses acknowledged-write policy | LOST converges through the existing read; unavailable remains local-only; unsupported is still an error | Tags and publication follow the actual remote outcome |
| `putNull` | accepted `storeVersioned` uses acknowledged-write policy with marker TTL | Existing deny, losing-version and local-only behavior | Marker/version semantics remain distinct from absence |
| Foreground loader / coordinated load | accepted `storeVersioned` uses acknowledged-write policy | Preserve effective winner result and bounded losing-load reload | Return actual obtained result; no false miss from refused fill |
| Background revalidation / refresh | accepted store reaches the same `loadAndStore` / `storeVersioned` path | Existing lock/generation skips remain skipped, not fabricated stores | Existing revalidation and foreground-waiter outcomes remain unchanged |
| `get`, `lookup`, L2-hit compute, `readThrough`, `awaitValue`, age classification | Optional `warmL1` retains ordinary generation/version checks | Refused read fill does not invoke acknowledged-write cleanup | Return the obtained read result within existing retry budgets |
| All unavailable/degraded store branches | No remote acknowledgement is invented | Existing `warmL1` or local atomic commit is used | No successful publication is invented |

Versioned stores have no additional pre-I/O L1 read. Versionless compatibility captures the pre-remote entry identity; if local installation is refused, cleanup only removes that same object and does not infer order for a replacement. This does not create a new distributed version-ordering guarantee for legacy transports/providers.

The cleanup deliberately does not call `evictLocal`, which also forgets barriers. It never retries installation using a newly captured generation, clears the entire cache, extends a retention horizon, or publishes while holding a stripe. SPI/provider operations already owned by that stripe remain inside it; remote I/O, loaders and observer/publication callbacks remain outside.

## Regression evidence

Before the fix, both promoted core cases failed (replacement and actual removal); the promoted Redis and Valkey cases also failed. The replacement-only control passed replacement but still failed actual removal. Baseline XML and logs were retained during implementation.

- `CommittedWriteCoherenceTest`: the original deterministic interleaving.
- `L1BarrierLossTest`: monotonic replacement, expiry, capacity eviction and explicit/bulk removal using the real barrier provider.
- `AcknowledgedWriteTest`: accepted put/marker/conditional/tagged/loader/refresh stores, stock/custom L1, degradation retention, preserved barriers, concurrent newer/equal entries, full clear, absent/unversioned entries, versionless identity and unavailable remote calls.
- `CommittedWriteConvergenceTest`: stock Caffeine plus real Redis/Valkey, both barrier replacement and actual removal.
- `InvalidationRaceTest`: the existing randomized concurrent writes/evictions, with failure diagnostics.

Reproduce repeated integration acceptance with:

```sh
python3 scripts/verify-write-convergence.py --output build/write-convergence
```

The runner requires at least five invocations. Each launches a fresh TCK test JVM and runs both server profiles, including four controlled cases and two random races. It preserves every XML/log and stops on failure rather than retrying to green. It does not extend convergence timeouts or perform a global L1 clear to manufacture success.

## Limits and rollout

The fix prevents this local stale-value mechanism; it is not cross-instance linearizability and does not extend Redis tombstone or local fencing horizons. Existing stale L1 entries are not retroactively located and repaired by loading new code. Normal process replacement starts with empty L1; nodes still running old code retain the defect during a rolling deployment. Any remaining convergence failures must be investigated rather than attributed away to randomness.

Java 25 startup-pinning policy is handled by the separate `separate-vt-startup-and-steady-state-gates` change; this fix does not weaken that gate or alter runtime synchronization policy.

## Validation result (2026-09-26)

The full Java 17 `./gradlew build --offline` passed, including demo Spring, all adapters, 471 core, 80 invalidation, 195 transport and 60 TCK tests. Core branch coverage is 826/914 (90.37%); dependency audit passed. The five fresh integration JVMs passed all 30 controlled/randomized Redis/Valkey cases with no retries-to-green.

Java 21 passed 471 core, 80 invalidation and 195 transport tests. Java 25 passed all 471 core and 80 invalidation tests; transport passed 194/195 in the first run. The unrelated `LettuceLockProviderCompensationTest.closeRacingSchedulerPublicationLeavesNoPool` observed one live compensation thread immediately after close in round 29. Its isolated Java 25 rerun passed. The test directly exercises the unchanged lock provider without constructing the cache engine or barrier map. `close()` calls shutdownNow without awaiting termination, while the test immediately counts all matching live threads; this suggests a timing-sensitive assertion but is not a complete root-cause diagnosis. The first failure is retained, and the full Java 25 run is not presented as green.

The change's write-coherence regressions passed on all three JVMs. General Java 25 VT startup pinning is not changed or reclassified by this implementation. See [machine-readable results](acknowledged-write-coherence-results.json) for module counts, every convergence run and the separate retry outcome.
