# Goal 02: Deduplicate Real Herdr Events Without Losing Transitions

## Objective

Make event deduplication match Herdr's actual event envelope and preserve valid
repeated state transitions such as `working -> idle -> working`.

## Confirmed Defect

`src/tendwire/backends/herdr_events.py::normalize_event` prefers synthetic
`server_id`/`sequence` or event-ID fields. When none exist it hashes event name
and payload, then `HerdrEventBackend::_seen_duplicate` stores that hash in a
long-lived LRU.

The reviewed Herdr `EventEnvelope` contains only `event` and `data`. Therefore
ordinary real events take the fallback path. Two legitimate identical
`working` events separated by an `idle` event have the same content hash; the
second `working` is discarded. A deterministic probe queued all three events as
`True, True, False` and left the worker incorrectly idle.

Existing tests that add `server_id` and `sequence` validate an envelope Herdr
does not currently emit.

## Required Behavior

1. Treat only authoritative producer identity as durable dedupe identity. A
   documented event ID or producer sequence may use the bounded LRU.
2. Do not retain a content hash for idless envelopes across unrelated events or
   batches.
3. For idless envelopes, either process every event or collapse only adjacent
   byte/semantic duplicates within the same drain batch. Any collapse must not
   cross an intervening state transition.
4. Keep state application idempotent so processing a repeated event does not
   create duplicate workers, bindings, events, or snapshots.
5. Represent the distinction explicitly. For example, make durable dedupe keys
   optional and keep batch-local comparison separate; do not label a content
   hash as though it were a producer sequence.
6. Preserve debounce and batch ordering. A later event must observe all earlier
   events in arrival order.
7. Reconnects must not suppress the first valid event merely because a matching
   payload appeared before disconnect.
8. Update comments and fixtures to state which envelope fields are confirmed by
   Herdr and which are forward-compatible optional fields.

## Implementation Quality Constraints

- Represent durable producer identity and batch-local duplicate detection as
  separate concepts. Do not accumulate more string prefixes in one catch-all
  `dedupe_key` convention.
- Keep one ordered queue and one application path. Do not introduce a second
  event buffer, replay subsystem, or generic event-bus abstraction.
- Make idless-event behavior obvious from the type/API; avoid boolean switches
  whose combinations must be inferred by callers.
- Preserve idempotency in focused state-application helpers instead of hiding
  duplicate side effects with broad exception handling.
- Remove the long-lived fallback-content-hash behavior and obsolete assertions;
  do not leave it reachable as an undocumented compatibility path.
- Use exact real Herdr envelopes in tests. Never add synthetic protocol fields
  solely to make deduplication easier.

## Required Tests

- Use exact `{"event": ..., "data": ...}` envelopes for
  `working -> idle -> working`; all transitions are accepted and final state is
  working.
- Repeat the same test with one flush per event and with all events in one
  debounced batch.
- Adjacent duplicate idless events do not produce duplicate persisted effects,
  whether both are processed idempotently or one is batch-collapsed.
- An identical idless event after a reconnect is not lost.
- Real authoritative event IDs/sequences, when present, are deduplicated across
  retries and the LRU remains bounded.
- Event ordering survives concurrent queueing under the backend lock.
- Move, close, exited, attention, and degraded-state event regressions remain
  green.

Do not retain tests whose only proof depends on invented Herdr fields. Optional
forward-compatibility tests are acceptable only when clearly labeled.

## Acceptance Evidence

- The original three-event probe ends in working state.
- Focused event tests and the full Tendwire suite pass.
- Offline Herdr fixture smoke passes with the real envelope shape.
- No additional snapshot/outbox duplication is observed in a repeated-event
  fixture.

## Non-Goals

- Do not change Herdr's event protocol.
- Do not poll direct pane APIs as a workaround.
- Do not deploy, merge, or restart any service.
