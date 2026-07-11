# Goal 04: Secure Tendwire Local State by Default

## Objective

Make private Tendwire state inaccessible to other local users on creation and
repair insecure permissions on existing installations without exposing data in
logs.

## Confirmed Defect

The reviewed live installation had a `0755` data directory, `0644` SQLite
database, and `0775` daemon socket. The database contains private worker
bindings and conversation state. Current creation paths in
`src/tendwire/store/sqlite.py` and `src/tendwire/daemon_api.py` rely on ambient
umask and do not enforce a private mode.

## Required Behavior

1. Tendwire's private state directory must be `0700` by default.
2. The database, `-wal`, `-shm`, local identity key, and other private files must
   be `0600`.
3. The Unix socket must be `0600` by default. A group-sharing mode may exist only
   as explicit opt-in configuration with documented threat model and group
   validation.
4. Use secure creation semantics, not a later chmod as the only protection.
   Apply restrictive mode/umask before the path becomes visible.
5. On startup/doctor/install, inspect existing paths with `lstat`; reject or
   safely repair symlinks, unexpected owners, non-regular database/key files,
   and sockets owned by another user.
6. Existing overly broad modes should be narrowed idempotently. Never widen a
   stricter existing mode.
7. Migration logs may name the kind of path repaired, but must not print socket
   values, private targets, raw bindings, secrets, or database content.
8. Service examples/installers must set `UMask=0077` as defense in depth while
   application code remains secure when run outside systemd.
9. Atomic replacement, WAL creation, backup, and compaction paths must preserve
   the required mode.
10. Doctor output should report pass/fail and an actionable generic remediation,
    without publishing private path values in public JSON.

Centralize mode enforcement in a small internal helper used by store, daemon,
installer, and maintenance code. Avoid duplicated chmod sequences with
different behavior.

## Implementation Quality Constraints

- Keep secure path opening, ownership/type validation, and mode enforcement in
  one small POSIX-focused module. Callers should express intent, not duplicate
  `lstat`/`open`/`chmod` sequences.
- Use dir-fd and no-follow operations where they close a real race. Do not build
  a general filesystem security framework unrelated to Tendwire state.
- Return a typed, actionable failure. Never swallow permission errors and
  continue with a less secure path.
- Separate first creation, validation, and existing-state repair so tests can
  prove each transition without monkeypatching internal assignments.
- Keep platform-specific branches isolated and explicit; Linux production
  behavior must remain readable without following many wrappers.
- Do not spread permission repair across ordinary read/query functions.

## Required Tests

- Under a deliberately permissive process umask, newly created data directory,
  DB, WAL/SHM, identity key, and socket have the required modes.
- Startup narrows `0755`/`0644`/`0775` fixtures idempotently.
- Startup refuses a symlinked DB/key/socket target and a wrong-owner fixture
  where the platform permits ownership testing.
- An explicit group-sharing configuration is off by default and validates its
  group before changing mode.
- Backup/replace/compaction retains private modes.
- Doctor/public JSON contains no raw private path or state value.
- Tests clean up sockets and do not depend on the real user's home directory.

Include POSIX platform guards where necessary, but do not silently skip the
Linux behavior used in production.

## Acceptance Evidence

- Focused permission tests pass under Python 3.10+.
- Full Tendwire suite passes.
- A temporary-install smoke reports `0700` state directory, `0600` DB family,
  and `0600` socket.
- No source-mode public contract changes and no direct Herdr calls are added.

## Non-Goals

- Do not require root.
- Do not print or upload local paths to prove permissions.
- Do not restart Herdr or deploy/merge this branch.
