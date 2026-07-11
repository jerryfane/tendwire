# Goal 04B: Stabilize Attention Lifecycles and Notification Episodes

## Objective

Ensure one continuous human-attention condition produces one durable lifecycle
episode and at most one initial connector notification, even when Herdr
observation is intermittent or the worker's severity/status changes.

## Confirmed Defect

The live system repeatedly delivered the same attention condition. One stored
item accumulated 1,190 observations, while other conditions reopened and were
delivered 8-9 times within nine minutes.

The immediate mitigation added `ATTENTION_RESOLVE_GRACE_SECONDS = 120` and now
requires `last_seen_at` to be older than the grace cutoff before a missing item
is resolved. This correctly suppresses a short present -> missing -> present
flap, but it is not a complete lifecycle contract:

1. `last_seen_at` records the last positive observation, not when continuous
   absence began. The first missing snapshot after a polling pause longer than
   120 seconds can therefore resolve immediately; no grace interval was
   actually observed.
2. Attention IDs include severity, status, and reason. A single worker changing
   between `blocked` and `failed` creates different IDs. The grace keeps the old
   row open while the new row can enqueue another `attention_created` or
   `attention_escalated` job.
3. Delivery keys include transition timestamps. Every false reopen therefore
   has a new key and bypasses connector/outbox deduplication by design.

Keep the 120-second mitigation until this goal is accepted. Do not treat its
two focused regressions as proof of the complete state machine.

## Required Lifecycle Model

1. Separate the immutable public signal ID/fingerprint from a durable lifecycle
   family. The family identifies the same human condition across volatile
   severity, status, reason, and presentation changes. For worker-derived
   signals, use stable public ownership such as host, source, and kind; do not
   use pane, terminal, session, backend-target, connector, or Telegram values.
2. Persist an explicit episode/generation for each lifecycle family. A new
   generation begins only after the previous episode was conclusively resolved.
3. Track absence independently from `last_seen_at`, including at minimum the
   first accepted missing observation and enough ordered-observation state to
   reject duplicates and out-of-order snapshots.
4. The first authoritative healthy missing observation starts a pending-clear
   state but leaves the item open. Resolve only after both:
   - a documented minimum number of distinct authoritative missing
     observations; and
   - a documented grace interval measured from the first accepted missing
     observation.
5. A positive observation before confirmation clears pending absence and
   continues the same episode without another `attention_created` job.
6. Degraded, unavailable, malformed, partial, duplicate, or out-of-order
   observations must not start or advance pending absence.
7. A severity/status/reason change within one family updates the same episode.
   A severity increase may emit exactly one `attention_escalated` transition
   for that new level. A repeat, presentation-only change, or downgrade must not
   emit another initial notification.
8. If multiple signal rows represent mutually exclusive states for one family,
   mark the replaced row as superseded without waiting for absence hysteresis.
   Public reads must expose one current condition for that family, not both old
   and new states.
9. After confirmed resolution, a later genuine recurrence opens the next
   generation and emits exactly one new initial notification.
10. Resolution remains snapshot/event driven; do not add a timer thread solely
    to expire attention. A later authoritative observation may complete a
    pending clear.

## Delivery and Outbox Contract

1. Derive lifecycle delivery keys from the stable family, persisted generation,
   and transition type/stage. Do not use a fresh wall-clock timestamp as the
   only uniqueness component.
2. State mutation and outbox insertion must remain in one SQLite transaction.
   A transaction retry or daemon restart cannot create another job for the same
   episode transition.
3. Keep the connector payload neutral. Tendwire owns attention lifecycle and
   outbox identity; Herdres owns Telegram delivery and presentation. Do not add
   Telegram-specific dedupe or state to Tendwire.
4. Repeated connector polls, lease expiry, fail/defer retries, and acknowledgments
   must preserve the same delivery key and existing exactly-once state.
5. Provide a bounded migration for existing flap damage. Preserve delivered and
   terminal audit history, but prevent obsolete queued/retry/deferred duplicate
   jobs for the same historical episode from flooding a connector after
   upgrade. Define safe handling for an already leased duplicate rather than
   deleting it blindly.
6. `signal_count` may continue counting observations, but it must not determine
   notification uniqueness and must not be confused with a delivery count.

## Observation-Time Rules

- Do not use malformed timestamps as `now` or silently substitute wall-clock
  time inside lifecycle comparisons.
- Clamp or reject regressing observation order explicitly. A delayed snapshot
  cannot reopen, resolve, or advance absence for a newer episode.
- Use injected deterministic timestamps/sequence values in tests. Do not use
  sleeps to prove hysteresis.
- Restart must recover pending absence and episode generation from SQLite; no
  process-local cache may be required for correctness.

## Schema and Migration

Add only the durable fields needed to represent lifecycle family, episode,
pending absence, and ordered progress. The implementation must state whether it
extends `attention_items` or introduces a narrowly scoped companion table.

The migration must:

1. be idempotent and transactional where SQLite permits;
2. backfill a deterministic family key without private identity material;
3. deterministically select one current row when legacy rows collide within a
   family, preserving the newest authoritative state and audit history;
4. preserve resolved history and delivered connector records;
5. safely terminalize only provably obsolete undelivered duplicates;
6. cooperate with Goal 06's later migration registry and bounded-maintenance
   work rather than adding another permanent migration mechanism.

## Public and Cross-Repository Contract

- Do not change the public attention signal ID/fingerprint schema solely to
  implement private lifecycle grouping.
- Public attention JSON must remain free of private bindings and connector or
  Telegram identifiers.
- Do not expose internal family keys, episode counters, absence counters, or
  outbox bookkeeping unless a separate public contract justifies them.
- Herdres source mode remains a consumer of neutral connector jobs and must
  retain `direct_herdr_calls=0`.
- No direct Herdr pane call, Herdr restart, deployment, or Telegram-side patch
  is part of this implementation goal.

## Implementation Quality Constraints

- Implement one explicit transition function/state machine used by snapshot
  and event-backed persistence paths. Do not scatter grace checks among SQL
  call sites.
- Keep lifecycle family, signal content, and delivery identity as separate
  concepts with names that make their roles clear.
- Prefer a small number of conditional SQL updates guarded by generation/state
  over loading all attention history into Python.
- Keep constants internal unless operators have a demonstrated need to tune
  them. Do not add configuration merely to avoid choosing defensible defaults.
- Remove the temporary `last_seen_at`-only resolution rule when the complete
  transition model replaces it; do not leave two competing hysteresis paths.

## Required Tests

- `present -> one missing -> present` remains one open episode and one initial
  outbox job, even when the missing snapshot arrives long after `last_seen_at`.
- Resolution requires distinct authoritative misses and elapsed grace from the
  first miss, not from the last positive observation.
- Duplicate, reordered, degraded, malformed, and partial snapshots do not
  advance pending absence.
- Restart between first miss and confirmation preserves the pending-clear state.
- `blocked -> failed -> failed -> blocked` remains one episode; only the defined
  escalation transition is emitted and one current row is publicly visible.
- Confirmed resolution followed by a later recurrence increments the generation
  once and emits one new initial notification.
- Concurrent identical saves and transaction retries produce one transition
  job.
- Connector poll/fail/defer/lease-expiry/ack cycles retain one delivery key.
- A generated legacy flap fixture with hundreds of observations and duplicate
  undelivered jobs migrates without replaying a notification storm.
- Multi-host and multi-worker families cannot suppress or resolve each other.
- Public JSON and outbox payload scans contain no private identifiers or raw
  backend values.

## Acceptance Evidence

- Focused attention lifecycle, store, connector-outbox, event, and daemon tests
  pass.
- Full Tendwire suite passes.
- A deterministic 30-minute generated flap simulation produces one initial
  delivery, no false resolution, and bounded rows/jobs.
- A genuine clear and later recurrence produces exactly two initial deliveries
  across two generations.
- SQLite integrity check passes after the legacy-damage migration fixture.
- Herdres source smoke remains `ok=true` with `direct_herdr_calls=0`.

## Non-Goals

- Do not tune Telegram formatting, topic behavior, or notification wording.
- Do not make Herdres infer attention episodes.
- Do not suppress genuine recurrence forever with a permanent content hash.
- Do not add a general workflow engine, distributed queue, or new runtime
  dependency.
- Do not deploy, merge, or restart Tendwire, Herdres, or Herdr.
