# Goal 06: Bound Store Growth and Make Migrations Cheap

## Objective

Stop unchanged snapshots and repeated schema work from growing disk/latency
without bound, and provide a safe maintenance path for existing large stores.

## Confirmed Defect

The reviewed live database was approximately 556 MiB and contained 48,237
snapshot rows. `src/tendwire/store/sqlite.py::save_snapshot` inserts every
snapshot even when its content fingerprint is unchanged. Snapshot retention is
not automatic.

`_ensure_schema` also runs migration/backfill/index work at many ordinary store
call sites. Several helpers can scan/update tables on every operation even when
`PRAGMA user_version` is current. Connection setup repeats persistent database
configuration such as WAL negotiation.

## Required Design

1. Do not insert a new historical snapshot when the newest snapshot for that
   host has the same canonical content. Projections/health timestamps may still
   be updated where semantically required.
2. Retain the latest valid snapshot per host unconditionally.
3. Add configurable age and count retention for changed historical snapshots.
   Defaults must be documented and justified with measured daily-use volume;
   both limits should be bounded and conservative.
4. Run small bounded maintenance automatically on a coarse cadence, not on
   every read or every poll. Record the last maintenance time in private store
   metadata.
5. Make `PRAGMA user_version` the schema fast path. Full DDL, column checks,
   dedupe, and backfills run only during initialization or an actual versioned
   migration.
6. Migrations must be ordered, transactional where SQLite permits, idempotent,
   and resumable after interruption. Never advance `user_version` before the
   migration is complete.
7. Configure WAL and connection pragmas deliberately. Persistent mode changes
   should occur during initialization; per-connection safety pragmas may still
   be applied when cheap and necessary.
8. Add indexes supporting newest-snapshot lookup, host-scoped retention, and
   maintenance without full-table scans.
9. Automatic maintenance must not run blocking `VACUUM` in the request path.
   Provide an explicit offline/controlled compaction command with preflight
   space checks, backup guidance, integrity checks, and rollback.
10. Preserve commands, receipts, private bindings, turns, pending state,
    attention, projections, backend health, connector outbox, and the latest
    snapshot. Retention must not cascade into unrelated durable state.
11. Apply Goal 04 permissions to DB, WAL/SHM, backup, and replacement files.
12. Expose aggregate maintenance metrics/doctor state without publishing DB
    paths or private row content.

Do not solve this by retaining only one snapshot. A small changed-history window
is useful for diagnostics and rollback; it just needs explicit bounds.

## Implementation Quality Constraints

- Use an explicit ordered schema-version migration registry. Current-schema
  connections must take a visibly short path with no hidden backfill work.
- Keep retention policy, bounded deletion, WAL checkpointing, and offline
  compaction as distinct operations with clear ownership.
- Prefer a few measured SQL statements and supporting indexes over loading rows
  into Python or introducing an ORM/maintenance framework.
- Centralize maintenance cadence/state. Do not place time checks and cleanup
  calls throughout unrelated store functions.
- Migration helpers should be version-scoped and removable after their support
  horizon; do not turn `_ensure_schema` into an ever-growing procedural script.
- Every new index and retention default needs query-plan or benchmark evidence,
  not intuition alone.

## Existing-Store Migration

Provide a dry-run report containing counts and estimated reclaimable bytes, not
payloads. The actual cleanup must:

1. verify ownership, permissions, and free disk headroom;
2. create or require a secure backup before destructive compaction;
3. run `PRAGMA quick_check` or `integrity_check` before and after;
4. retain the latest row and configured changed history for every host;
5. checkpoint WAL safely;
6. compact only in an explicit maintenance window;
7. leave the original usable if replacement fails.

## Required Tests and Benchmarks

- Saving 10,000 identical snapshots creates one historical content row (or one
  documented minimal equivalent), while latest projections remain correct.
- Alternating changed snapshots are pruned to configured age/count bounds and
  always retain the latest per host.
- Multi-host retention cannot delete another host's latest snapshot.
- A current-schema open performs no backfill/update DML. Instrument SQL or use
  explicit migration spies to prove the fast path.
- Every migration version can rerun safely and recover from a simulated
  interruption before `user_version` advances.
- Ordinary read/write latency stays bounded as a fixture grows from hundreds to
  tens of thousands of snapshots.
- Maintenance does bounded work per run and resumes later.
- Compaction interruption leaves a valid original DB.
- Permissions remain private throughout.

Record before/after DB size, snapshot count, p50/p95 save latency, and schema
open latency on a generated fixture. Do not copy the live DB into the repo.

## Acceptance Evidence

- Focused store/migration tests and full Tendwire suite pass.
- Generated large-store benchmark demonstrates bounded row growth.
- Integrity checks pass before/after the migration fixture.
- No user-visible turn/outbox loss occurs in a migration smoke.

## Non-Goals

- Do not purge conversation finals under snapshot retention.
- Do not auto-vacuum in the daemon request loop.
- Do not touch the live DB, deploy, merge, or restart services.
