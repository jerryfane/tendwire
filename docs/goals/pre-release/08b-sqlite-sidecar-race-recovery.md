# Goal 08B: Make SQLite Sidecar Handling Race-Safe

## Objective

Prevent the running Tendwire daemon and connector clients from entering a tight
failure loop when SQLite creates or removes transient WAL, SHM, or rollback
journal files while local-state permission validation is in progress.

## Confirmed Defect

On 2026-07-12, `tendwired.service` and `herdres.service` were both reported by
systemd as active, but Tendwire was repeatedly failing with:

```text
tendwire.local_state.LocalStateError: required local-state entry is missing
```

The failure originates in `prepare_sqlite_family_at`. It first inspects the
whole SQLite family and later repairs every member that was present. SQLite may
legitimately unlink `-wal`, `-shm`, or `-journal` between those operations. The
later repair interprets that valid lifecycle transition as missing required
state. Herdres then repeatedly invokes Tendwire against the unhealthy daemon.

Observed operational evidence included roughly 12 CPU-hours consumed by each
service over a 19-hour interval despite the source connector being effectively
unavailable. Restarting alone is not a correction because the race remains.

## Required Behavior

1. Treat WAL, SHM, and rollback-journal entries as optional transient SQLite
   sidecars. Their disappearance during inspect/validate/repair is a valid
   absent result, not `MISSING_ENTRY`.
2. Keep the main database mandatory after it has been selected or opened. Main
   database disappearance, substitution, wrong ownership, wrong type, unsafe
   mode, symlink traversal, or inode ambiguity must continue to fail closed.
3. Never create an absent WAL, SHM, or rollback journal merely to satisfy
   permission inspection. SQLite exclusively owns sidecar creation and removal.
4. A sidecar that appears or is replaced during validation must not bypass the
   local-state boundary. Validate its current type, owner, mode, and identity at
   the operation that consumes it; do not trust an earlier pathname snapshot.
5. If a sidecar disappears after a descriptor or identity has been captured,
   close retained descriptors and report the current member as absent. Do not
   chmod, unlink, or otherwise act on an unrelated replacement.
6. Preserve narrow-only repair: an existing owned regular sidecar with broad
   mode may be narrowed at an explicit preparation boundary, while stricter
   modes remain unchanged.
7. Ordinary read paths remain validation-only. Do not hide mode repair or file
   creation inside cached reads to make the regression pass.
8. Repeated transient sidecar churn must not spin, recursively retry without a
   bound, or cause Tendwire/Herdres retry loops to consume sustained CPU.
9. Errors remain typed and path-free. Public or connector output must not expose
   database paths, sidecar names, inode data, UIDs/GIDs, or raw `OSError` text.
10. Preserve schema, migration, transaction, backup, compaction, WAL,
    permissions, and source-mode behavior accepted in Goals 04, 06, and 07.

## Required Design

- Keep one authoritative SQLite-family transition in `local_state.py`. Do not
  add store-local copies of the permission or race policy.
- Model an optional sidecar's terminal inspection result explicitly as present,
  absent, or invalid. Do not use a broad `except LocalStateError` that converts
  ownership, type, symlink, permission, or identity failures into absence.
- Prefer descriptor-relative, no-follow operations and existing identity checks.
  If a helper is added, its name and contract must make optional-entry semantics
  explicit; required private files must not accidentally inherit that behavior.
- Keep retries, if any, small and deterministic. Correctness must not depend on
  eventually winning a continuously changing pathname race.
- Do not weaken SQLite connection-lifetime locks or Goal 06 compaction
  serialization.
- Do not change public schemas, add a daemon, add a dependency, or introduce a
  generic filesystem framework.

## Required Tests

Use barriers or injected descriptor-relative operations rather than timing
sleeps for the core race regressions.

1. WAL disappears after family inspection and before repair: preparation
   succeeds and reports it absent.
2. SHM disappears at the same boundary: preparation succeeds and reports it
   absent.
3. Rollback journal disappears at the same boundary: preparation succeeds and
   reports it absent.
4. Main database disappears at the equivalent boundary: preparation fails
   closed with the expected typed error.
5. A sidecar is replaced by a symlink, directory, wrong-owner entry, or different
   inode during the race: every case fails closed without modifying the target.
6. An owned broad-mode sidecar that remains present is narrowed; an existing
   stricter mode remains unchanged.
7. An absent sidecar is not created by inspection, repair, doctor, or a read-only
   store operation.
8. Repeated real SQLite WAL open/checkpoint/close churn can run concurrently
   with bounded family preparation without an unhandled missing-entry failure.
9. Connection and descriptor accounting proves no leaks after present, absent,
   replacement, and failure outcomes.
10. Path-free doctor/CLI error regressions and the existing local-state,
    SQLite-store, backup, VACUUM, compaction, and migration suites remain green.

## Operational Acceptance Evidence

Against isolated temporary state, not the live database:

- Run a deterministic churn harness for a documented iteration count and wall
  time.
- Record operations, optional disappearances, failures, descriptor counts, and
  CPU/wall time. There must be zero unhandled missing-sidecar failures and no
  growth in descriptors, processes, or threads.
- Start an isolated Tendwire daemon from the candidate artifact, repeatedly
  create and retire real WAL state, and prove cached snapshot/turn/health
  requests continue to respond.
- Run an isolated Herdres source smoke with `direct_herdr_calls=0` and prove two
  subsequent no-op syncs do not spin or duplicate work.
- Run the focused permission/store/daemon tests, full Tendwire suite,
  compilation, and `git diff --check`.

## Deployment Boundary

Implementation and review must use a dedicated worktree and isolated state. Do
not access or migrate the live database, restart Tendwire/Herdres, or restart
Herdr while implementing this goal.

After independent acceptance and integration, an owner-authorized deployment
may restart `tendwired.service`, `herdres.service`, and
`herdres-gateway.service` to restore the live connector. Never restart
`herdr-server.service`.

The final Goal 12 deployment must also stop importing Tendwire from a mutable
development checkout. Services must run an installed, versioned candidate
artifact so uncommitted work cannot silently change live behavior.

## Completion Report

Report the reproduced race, exact files changed, focused and full test results,
churn/service-smoke evidence, descriptor/process/thread accounting, public
privacy checks, and confirmation that no live state or services were touched.
Do not mark the goal complete if the main-database fail-closed cases or any
sidecar replacement adversary remain unproven.

## Non-Goals

- Do not redesign SQLite storage or disable WAL mode.
- Do not weaken local-state ownership, type, symlink, or permission checks.
- Do not deploy, migrate live state, or restart services during implementation.
- Do not restart Herdr.
