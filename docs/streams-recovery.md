# Streams pending and gap recovery

## One group per receiver

The Streams profile consumes the same bounded journal used by replay. Its
retention window is the actual journal window; choosing Streams does not
create longer history or exactly-once delivery. Each receiver has its own
v2 consumer group so invalidations are broadcast rather than distributed
between application instances.

The default transport constructor generates a random identity for a new
volatile L1. Its own group is destroyed best-effort on graceful close.
Crashes or an unavailable server can leave that group behind. The constructor
with an explicit UUID represents a **stable identity** and retains its group
on close. The operator must ensure exactly one live owner of that identity;
concurrent owners are unsupported. A resumed owner can claim an older consumer
only inside its own group. It never claims or destroys another receiver's group.

Service registration establishes the journal baseline and clears the target
before Streams starts dispatch. Historical pending UPDATE rows covered by
that baseline are acknowledged without installing old payloads into the new
L1. Replacing a Streams target retires the old subscription and establishes
a fresh baseline; callbacks from its old registration cannot apply to the
replacement. An ordinary readable reconnect of the same registered target
can drain pending without an unconditional L1 clear.

## The processing boundary

The reader drains its own pending IDs before reading `>`. It then resumes
older consumers inside the same exclusively owned group. Missing pending
payloads are detected before a claim could remove their PEL entries, including
on Redis 6.2 without a deleted-ID field in XAUTOCLAIM replies.

Each cache retains at most one batch of 50 rows plus one current row/reset
stage. A row has separate decode, application and ACK outcomes. Failure does
not abandon the delivered batch's remainder. Application failures have at
most three attempts, spaced by 1 and 2 seconds, before requesting recovery.
If application succeeded but its ACK failed, only settlement is retried;
an unknown ACK outcome is not proof that Redis failed to acknowledge it.
Cross-process redelivery is still possible and relies on version/idempotency
rules, not an exactly-once claim.

## No ACK based on an unsafe reset

Corrupt or missing rows are unconfirmable history. The reader requests the
service's asynchronous gap handler and waits on its own dedicated reader
thread, outside state monitors and outside business/Lettuce I/O threads.
Other caches can continue. It does not repeatedly decode the known poison
while waiting for recovery.

The service captures the journal-end baseline **before** clearing L1. Only
a completed clear with a still-current baseline/generation authorizes ACK
of rows at or below that baseline. A row appended after it is processed
normally. There is no blind XGROUP SETID to the latest tail. Failed baseline
reads, failed clears and superseded/closed reset completions authorize no
poison ACK. A failed baseline still clears L1 conservatively, retains the old
cursor and remains pending; a failed clear is a failed attempt with backoff,
not a reason to spin immediately on another reset.

The live reader and journal replay use the same required-field, version,
type and serializer-result validation. Corruption fails a checked range as
a whole, before its decoded prefix can advance recovery. After a successful
reset, a corrupt tail can be the confirmed anchor: checked reads validate
its raw ID atomically, omit its already-accounted payload and decode only
later rows. Missing anchors still report unconfirmable history.

Failed connection, ACK and resync attempts back off through 1, 2, 4, 8, 16 and
30 seconds; coordinator retries retain their own bounded schedule. A cache
may remain visibly pending when safe recovery is unavailable. A standalone
Streams transport without a capable gap handler does not fabricate a clear
or silently ACK poison. If a missing pending ID is beyond the available
reset baseline, it remains pending until a safe baseline covers it.

Closing a reader prevents new dispatch/ACK admission, invalidates its reset
wait and unregisters its pending gauge source. A command already admitted
before close can still have executed remotely; close is not a rollback of
an in-flight ACK. Late reset callbacks cannot initiate ACK through a closed
reader generation.

## Diagnostics and operator checks

`tiercache.invalidation.stream{cache,result}` counts `decode_failed`,
`apply_failed`, `ack_failed` and `resync_failed`. Decoding failures encountered
by journal replay also use `decode_failed`. Labels contain no row ID, key,
payload or exception text. Cache/row/failure-class diagnostics are rate-limited
to one warning per cache per 30 seconds. Serializer exceptions are replaced
by sanitized typed corruption metadata without retaining their messages/causes.

`tiercache.invalidation.recovery.pending{cache}` includes reader settlement/resync retries as well as coordinator work. A failed-baseline clear is not
reported as recovery success. A committed safe reset also emits the existing
flush signal. These metrics require the metrics listener/binder to be wired;
without one, diagnostics remain available through logs.

Redis PEL/group memory is not bounded by the in-process batch limit. Inspect
backlog and retention explicitly, using the exact logical cache token and
receiver UUID from [keyspace v2](redis-keyspace-v2.md). For logical `users`:

```text
XPENDING tiercache:v2:journal:dXNlcnM tiercache:v2:cg:dXNlcnM:<receiver-uuid>
XINFO GROUPS tiercache:v2:journal:dXNlcnM
XINFO STREAM tiercache:v2:journal:dXNlcnM
```

Pending count zero alone does not prove there is no unread history. Before
retiring a group, prove the owning instance is stopped and will not resume,
verify the exact cache/group ownership, and then explicitly destroy only
that retired group. Do not infer ownership by scanning other groups or their
consumer counts. Provision journal retention for actual traffic/outages.

## Compatibility

Value frames and row fields are unchanged. The v2 coordinated cold cutover
still applies; no v1 cursor translation or bridge is added. Existing void
transport methods remain. Additive gap/metrics hooks are ignored by legacy
transports. Custom gap handlers must provide a committed reset result and a
side-effect-free current-generation check; merely returning a successful
future is insufficient authorization.

The `CheckedRange` record shape is unchanged, but an intact non-beginning
range may omit its raw-validated anchor. Custom journals may still return a
valid typed anchor; the service skips it by ID. Consumers must use
`startIntact` and must not assume that `rows[0]` always equals the cursor.
