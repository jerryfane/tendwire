# Goal 10: Retain Final Turns Until Delivery Is Durable

## Objective

Replace the hard six-turn source-history cap with bounded, delivery-aware
retention so connector outages cannot permanently erase unseen responses.

## Confirmed Defect

`src/tendwire/store/sqlite.py` sets `_SOURCE_TURN_HISTORY_LIMIT = 6` and
`_prune_source_turn_history` deletes older source turns solely by row order. If
Herdres is unavailable while a worker completes more than six turns, an older
undelivered final can disappear before the connector observes it.

This is data loss. Increasing six to another guessed number only changes how
long it takes to recur.

## Required Retention Model

1. Give each complete turn revision a deterministic neutral delivery identity.
   The identity must be stable across daemon/connector restarts and contain no
   private source IDs or Telegram values.
2. Materialize deliverable final-turn items into Tendwire's neutral connector
   outbox (or an equivalently durable connector-neutral feed ledger) when the
   final becomes authoritative. Do not wait for Herdres to be online before the
   item exists.
3. A complete final and any Goal 05 content segments are prune-protected while
   their outbox/feed state is queued, leased, deferred, retryable, or otherwise
   not durably acknowledged.
4. Connector acknowledgment is the delivery boundary. Telegram-specific
   message/topic IDs remain private Herdres state and must not be stored in
   Tendwire public payloads or acknowledgment responses.
5. Use deterministic delivery keys and the existing lease/ack/fail/defer
   semantics so retries are at-least-once transport with idempotent effective
   delivery. A stale lease/ref cannot acknowledge a newer attempt.
6. Do not delete a final merely because retry attempts reached a dead-letter
   state. Dead-letter is unresolved delivery and must remain inspectable/retryable
   through explicit operator action.
7. After durable acknowledgment, retain finals for a documented age/count
   history window. Working-only revisions may use a much shorter bounded
   retention because they are replaceable progress.
8. Apply both age and storage bounds, but never satisfy a bound by silently
   deleting unresolved deliverables. Surface pressure/degraded health and require
   explicit operator resolution when unresolved data prevents cleanup.
9. Preserve command-linked pending turns until they resolve to a final or an
   explicit terminal failure. Do not confuse command receipt completion with
   response delivery.
10. Migration must classify existing finals conservatively. If delivery cannot
    be proven, treat the row as undelivered or use a one-time connector cursor
    migration that prevents both loss and mass historical reposting.
11. Two forced source syncs after acknowledgment must produce zero sends. A
    connector outage/restart must resume only unresolved items in order.
12. Keep the model connector-neutral. Herdres owns Telegram formatting, topic
    choice, rate limits, and message IDs.

The implementation should remove `_SOURCE_TURN_HISTORY_LIMIT` as the correctness
mechanism. A count can remain as an acknowledged-history storage policy, never
as an undelivered-final deletion rule.

## Implementation Quality Constraints

- Extend the existing neutral outbox/lease state machine. Do not add a second
  delivery ledger, connector cursor system, or Telegram-specific acknowledgment
  table.
- Define delivery identity/revision generation in one helper used by ingestion,
  outbox creation, retention, and tests.
- Keep eligibility for deletion as one auditable query/predicate. Avoid cleanup
  conditions scattered across snapshot and turn-write paths.
- Separate unresolved delivery protection from acknowledged-history retention;
  do not encode both in one status or magic count.
- Bound tombstones, attempts, and acknowledged history explicitly so the safety
  fix does not create another unbounded table.
- Migration must be a narrow versioned transition, not permanent dual-read of
  old and new retention models.

## Required Tests

- Keep Herdres/connector offline while one worker completes at least 20 turns;
  every final remains available and is delivered once after recovery.
- Acknowledging the first N items and failing later items allows retention to
  remove only eligible acknowledged history.
- Lease expiry, defer, transient fail, process restart, and dead-letter do not
  lose a final.
- Stale acknowledgment refs cannot mark a newer lease delivered.
- Multipart finals protect all parts until each required part is acknowledged.
- Migration from six-row history does not repost arbitrary old turns and does
  not mark unknown delivery as proven.
- Two forced syncs after all acknowledgments produce no duplicate delivery.
- Content revision behavior does not send both stale and authoritative finals.
- Storage-pressure health is visible and public-safe.

Add a generated long-outage fixture. Do not use live Telegram or Herdr in the
hermetic tests.

## Acceptance Evidence

- Focused turn-retention and connector-outbox tests pass.
- Full Tendwire and relevant Herdres hermetic suites pass.
- The 20-turn outage smoke delivers all 20 exactly once after recovery.
- No old-turn reposting occurs in the migration/restart smoke.

## Non-Goals

- Do not put Telegram delivery state into Tendwire.
- Do not silently discard unresolved dead letters to meet a size target.
- Do not deploy, merge, or restart services.
