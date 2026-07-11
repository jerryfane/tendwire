# Goal 07: Move Turn Ingestion Off the Request Path

## Objective

Make the long-lived daemon the single owner of structured-turn refresh so reads
and commands remain fast, concurrent refreshes cannot stale-overwrite state, and
large response sets are paged safely.

## Confirmed Defect

`src/tendwire/daemon.py::get_turns` synchronously calls
`refresh_structured_turn_content`. The daemon API serves connections
sequentially, while CLI non-command calls use a 0.35-second timeout. On timeout,
`src/tendwire/cli.py::cmd_turns` falls back and runs another refresh. One slow
adapter read can therefore block commands and trigger overlapping work.

`src/tendwire/store/sqlite.py::merge_turn_content` selects and decodes rows
before `BEGIN IMMEDIATE`; two refreshes can choose stale bases and overwrite a
newer final. The API client also has a fixed 1 MiB response ceiling, while turn
lists are unpaged.

## Required Architecture

1. The running daemon owns one bounded background turn-ingestion scheduler per
   host/store. API handlers read cached durable projections only.
2. Refresh on a documented short cadence and on useful Herdr change signals,
   with coalescing so a burst creates one refresh rather than N concurrent jobs.
3. Permit at most one refresh for the same worker/session at a time. A slow
   worker must not block reads from other workers or `command.submit`.
4. Use a bounded worker pool and per-adapter timeout. Expose degraded/stale state
   rather than accumulating unbounded threads/tasks.
5. `turn.list`, `pending.list`, health, and snapshot reads must not synchronously
   parse source session files when the daemon is available.
6. If the daemon is unavailable, the CLI may perform one documented bounded
   direct refresh. It must not start a second refresh merely because a first
   daemon request timed out ambiguously.
7. Acquire the write transaction before reading the merge base, or use an
   explicit optimistic revision check/retry. A stale refresh may not replace a
   newer final/complete state with older working content.
8. Make completion monotonic unless a new authoritative source-turn ID begins.
   Late stream data for a completed turn cannot reopen it.
9. Add cursor/since pagination to turn APIs and coordinate with Goal 05 for
   content pages. Never raise the frame cap as the sole fix.
10. Use a concurrent or dispatch architecture that lets command submission and
    health requests progress during slow read requests. Bound request count,
    frame size, and work per connection.
11. On shutdown, stop scheduling, allow a bounded in-flight flush, then close
    cleanly. On restart, resume from durable state without replaying old finals.
12. Report private operational metrics: refresh duration, queue depth, workers
    refreshed/failed, last successful refresh, stale age, and coalesced count.
    Public health must not expose source paths or IDs.

Keep the implementation simple enough to reason about. A single scheduler plus
bounded executor and serialized store transactions is preferable to a new
general job framework.

## Implementation Quality Constraints

- Implement one daemon-owned scheduler with one bounded executor/queue. Do not
  introduce a generic task framework, second daemon, or parallel refresh path.
- Make ownership and locking explicit in a small state object. Avoid module-level
  mutable dictionaries and lock acquisition spread across adapters.
- Share the same refresh operation between scheduled/event-triggered work and
  bounded CLI fallback; do not copy orchestration into each caller.
- Keep API handlers as projection reads plus pagination. They must not contain
  hidden refresh heuristics or timeout-driven retries.
- Prefer transaction/revision invariants over large critical sections. Document
  lock ordering next to the code that enforces it.
- Concurrency tests must use barriers/events and deterministic clocks, not
  generous sleeps that can hide races.

## Required Tests

- A source adapter blocked for several seconds does not delay
  `command.submit`, health, or cached `turn.list` beyond the stated latency
  budget.
- Repeated API calls during a refresh do not invoke another source read.
- A timed-out daemon attempt does not cause the CLI to duplicate an in-flight
  refresh.
- Two concurrent updates to one turn cannot stale-overwrite a newer final.
- Late working content cannot reopen a completed source turn.
- Different workers refresh concurrently up to the configured bound.
- Burst events coalesce and a later event schedules another refresh when needed.
- Pagination returns a stable, non-overlapping view under concurrent inserts,
  with invalid/expired cursors failing clearly.
- Responses larger than 1 MiB are retrieved through pages without frame errors.
- Shutdown and restart leave no duplicate outbox delivery or abandoned lock.

Add deterministic clocks/barriers rather than timing-sensitive sleeps wherever
possible.

## Acceptance Evidence

- Focused daemon, CLI, store-race, and turn tests pass.
- Full Tendwire suite passes.
- A generated slow-adapter benchmark records p95 cached read/health latency and
  command latency inside the documented budget.
- No request handler performs source-session parsing in normal daemon mode.

## Non-Goals

- Do not add a distributed queue or external database.
- Do not make Herdres poll Herdr directly.
- Do not deploy, merge, or restart services.
