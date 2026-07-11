# Goal 09: Make Pending State Independently Correct

## Objective

Ensure `pending.list` discovers, updates, and clears backend decisions correctly
even when no client has called `turn.list`.

## Confirmed Defect

`src/tendwire/daemon.py::get_pending` overlays rows from `backend_pending`, but it
does not refresh the source. Those rows are populated/pruned as a side effect of
`refresh_structured_turn_content`, currently called by `get_turns`. A consumer
that polls only `pending.list` can therefore miss a new prompt forever or retain
stale state until some unrelated turn request occurs.

Recent code added worker-disappearance pruning and recomputes the wrapper
fingerprint after overlay. This goal must preserve those fixes and prove them in
the independent path rather than duplicate ad hoc refresh calls.

## Required Behavior

1. Use Goal 07's background ingestion as the authoritative owner of both turn
   content and backend-pending observation. `pending.list` is a fast cache/store
   read and does not parse source files synchronously.
2. A successful source read with an open prompt upserts the normalized prompt.
   A successful source read with no prompt clears that worker's prior row.
3. A successful authoritative binding reconciliation that removes a worker
   reaps that worker's pending row.
4. A transient source read failure is not evidence that a prompt was dismissed.
   Retain the last known row with explicit stale/degraded metadata for a bounded
   grace period, then expire it according to a documented policy.
5. Distinguish `read_failed`, `read_succeeded_no_prompt`, and
   `worker_authoritatively_absent` in private ingestion results. Do not encode
   all three as `None`.
6. Do not let one malformed pending prompt suppress the snapshot-derived
   fallback for that worker. Overlay remains atomic per worker.
7. Recompute `content_fingerprint` after every overlay, prune, stale-state
   change, and normalization. The fingerprint must represent exactly the public
   list and health fields returned.
8. Pending IDs/choice handles must be opaque and deterministic for one observed
   decision revision. Raw tool-use IDs and source identifiers remain private.
9. A command answering a pending choice must bind to the observed revision and
   fail safely if the prompt changed or disappeared; do not answer a stale
   backend decision.
10. Public pending text follows Goal 03's sanitizer. Choice labels remain useful,
    but internal values never enter JSON or connector payloads.
11. Health/doctor exposes pending freshness and ingestion degradation without
    source paths, pane IDs, or tool IDs.

## Implementation Quality Constraints

- Introduce one explicit read outcome that distinguishes open prompt, successful
  absence, transient failure, and authoritative worker removal. Do not overload
  `None` or infer meaning from exception presence.
- Keep pending transitions in one store/service helper shared by background
  ingestion. `pending.list` must remain side-effect free.
- Reuse Goal 07 scheduling and adapter reads. Do not add a pending-only poller,
  parser, executor, or retry loop.
- Compute public fingerprinting once after the final normalized list is built;
  do not patch fingerprints at each overlay branch.
- Keep stale/grace policy named and centralized with an injected clock for tests.
- Model the transition table directly in tests rather than reproducing internal
  branching logic in fixtures.

## Required Tests

- Start with an empty store, expose a backend prompt, call only `pending.list`,
  and observe it after the background ingestion cycle.
- Dismiss the prompt and continue polling only `pending.list`; it clears.
- Remove the worker in a successful authoritative reconcile; its pending row is
  reaped.
- Simulate a transient read failure; the prompt is retained/stale during grace,
  not immediately deleted, then expires under the documented policy.
- A malformed backend prompt leaves the valid synthetic fallback visible.
- Overlay, prune, and stale transitions each change the wrapper fingerprint;
  unchanged polls preserve it.
- A stale choice revision cannot route a command to a changed/new prompt.
- Forbidden sentinels in prompt text, choices, metadata, and tool IDs are absent
  from all public surfaces.
- Repeated polling creates no duplicate pending rows or connector jobs.

Tests must not call `turn.list` as setup unless the individual test is explicitly
verifying cross-endpoint consistency.

## Acceptance Evidence

- Focused pending/backend-pending/daemon tests pass.
- Full Tendwire suite passes.
- The independent-poll regression proves discovery and clearing with zero turn
  endpoint calls.
- Cached pending-list latency remains within Goal 07's budget.

## Non-Goals

- Do not create a second pending-only source parser.
- Do not make Herdres read panes or source session files.
- Do not deploy, merge, or restart services.
