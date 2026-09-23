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
