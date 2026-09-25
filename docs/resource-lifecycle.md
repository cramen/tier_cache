# Resource ownership and shutdown

Close the factory when its owning application component stops. Each factory owns
its async, refresh, watchdog and recovery workers. Closing it is idempotent and
stops admission to those auxiliary services before disposing their resources.

## Who closes what

| Resource | Owner |
| --- | --- |
| RedisClient supplied by the application | Application |
| Existing connection passed to LettuceLockProvider | Application |
| Dedicated connection opened by LettuceLockProvider(RedisClient) | Provider |
| Provider derived from LettuceRemoteCache by a factory | That factory |
| Provider explicitly passed to builder.lockProvider(...) | Caller |
| Provider created as a Spring or Micronaut bean | Framework bean lifecycle |
| L2 supplied to a factory | Caller or its framework lifecycle |

Derived lock providers are lazy. Two factories sharing one L2 have independent
provider connections; closing one does not close the shared client or the other
provider. Closing an unused provider opens no connection. A connection attempt
already in progress can finish after close, but its owned connection is disposed
instead of published. Connect, Redis commands and resource cleanup run outside
intrinsic lifecycle monitors.

## Using a cache after factory close

An already-obtained synchronous cache can still read L1/L2, write, and invoke its
loader with local singleflight **while the supplied L2 remains usable**. It no
longer starts distributed rebuild coordination, lease renewal, background refresh,
invalidation publication or recovery. This is not continued cluster coherence:
other nodes may retain stale values after a post-close write. In a framework
shutdown the L2 itself may also be disposed; the surviving-view guarantee does
not keep it open.

Discarded queued refreshes release their claims, allowing later foreground reads
to load. Running application loaders retain the existing interruption limits;
shutdown does not undo their side effects. Existing async views keep their
cancellation/rejection contract, and completed stages keep their results. They
do not switch to successful synchronous execution after close.

A pending coherence recovery is retired with its breaker epoch. Later real L2
probes may restore data-path availability without restarting recovery workers or
emitting a coherence-recovered notification. Closing alone is not a successful
probe and does not bypass the wait of an OPEN breaker.

## Locks during shutdown

Direct acquisition on a closed built-in provider now fails immediately with an
internal lifecycle exception. The engine treats that outcome as unavailable
coordination and loads locally; it does not wait for a supposed lock holder or
count closure as Redis failure/success.

If shutdown rejects watchdog scheduling after acquisition, the token is released
once before the fallback loader runs. A live scheduler's unrelated rejection
remains an error. Cleanup cannot replace a successful loader result or its original
exception. Renewal admitted before shutdown may stop, so exclusivity beyond the
surviving lease is not guaranteed.

Existing handles and compensation tasks retain the commands used for acquisition;
they never reopen a provider. Release remains token-checked and cannot delete a
new owner's lock. Shutdown retires compensation bookkeeping and stops rescheduling.
An already-dispatched acquire can still have executed remotely. Cleanup is best
effort; if connection shutdown prevents it, the orphan expires within its lease.
The compensation cap, retry window and lease settings are unchanged.

## Async followers and retained work

An async read joining an existing load releases its API worker and waits through
a private completion attachment. Synchronous reads, async reads and background
refresh still use one engine claim. An async owner keeps its worker until the
load finishes, including the wait for an `AsyncLoader` stage. This is not native
asynchronous source execution or a concurrency limit across all source callers.
Even L1 hits enter through the API executor; no caller-thread fast path is used.

Every async view of a factory shares one admission budget: configured API workers
plus 10,000. Queued tasks, running tasks and detached attachments consume credits.
Cancelling or manually completing the returned future does not release a credit
while its task or attachment remains retained. A never-finishing shared load can
therefore saturate admission even with an empty executor queue. Further requests
fail with `RejectedExecutionException`, without executing cache work. Closed views
instead fail with `CancellationException`, regardless of budget occupancy.

Close cancels pending caller stages and disposes queued tasks. Running owners and
attached followers keep their credits until their actual work or notification
ends; close does not cancel an application-supplied loader stage. Already-completed
results survive. A completed load is removed from singleflight before notifying
followers, so a blocked user continuation cannot make a later miss join an old
result. It can still delay notification of other participants in that old round.
Avoid blocking completion callbacks: they may run on the thread completing the
shared owner, and keep the associated credit until they return.

Spring retrieval, Reactor, Kotlin and Micronaut inherit this factory admission
bound. Adapter cancellation rules remain unchanged; in particular, Kotlin loaders
retain their originating coroutine scope ownership. No adapter adds another
singleflight map or an unmanaged executor.

## Registration readiness

A factory publishes a new cache only after invalidation registration succeeds.
Waiting for that readiness does not hold the factory lifecycle or cache-map monitor;
other cache creation and factory close can proceed. A registration failure is visible
rather than exposing a cache whose required baseline, reset or subscription is incomplete.

The internal `InvalidationHandler.registerTargetAsync` SPI reports readiness through
a `CompletionStage`. Its compatibility default invokes a legacy handler's registration.
The stock service waits for queued-delivery quiescence asynchronously, without occupying
recovery or transport workers. Replacing a registration or closing the service settles
its pending readiness; obsolete completions cannot clear or publish the replacement.

The synchronous `registerTarget` entry point remains available off internal workers.
Registration from a delivery/recovery callback must use the asynchronous operation;
a synchronous attempt is rejected with an actionable error instead of waiting on itself.
Calling `getCache` to create a new cache from such a callback is rejected for the same
reason. An already-created cache remains accessible.
