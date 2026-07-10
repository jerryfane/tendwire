# Release checklist (Tendwire source-only RC)

Build release artifacts from a **clean git checkout only**. Never zip the working
directory directly — it can contain `__pycache__/`, `*.pyc`, `.pytest_cache/`,
local `*.db` state, `installation.key`, `installation.key.sha256`, or
`installation.key.initialized`, none of which may ship. `.gitignore` excludes
these local-state filenames, so building from tracked content is what
guarantees a clean artifact.

## 1. Preconditions

```sh
git status --porcelain            # must be empty
python -m py_compile $(find src tests -name '*.py')
python -m pytest -q               # all green
```

## 2. Build a clean artifact

Source zip/tar (tracked files only, respects `.gitignore`):

```sh
git archive --format=zip -o dist/tendwire-$(git describe --always).zip HEAD
```

Or a Python sdist/wheel (hatchling; packages `src/tendwire` + declared includes):

```sh
python -m build
```

## 3. Verify the artifact is clean

The following must print nothing:

```sh
git archive --format=tar HEAD | tar -t | grep -E '__pycache__|\.pyc$|\.pytest_cache|\.db$|(^|/)installation\.key(\.sha256|\.initialized)?$'
```

For sdist/wheel:

```sh
tar -tf dist/*.tar.gz | grep -E '__pycache__|\.pyc$|\.pytest_cache|\.db$|(^|/)installation\.key(\.sha256|\.initialized)?$' || echo clean
```

## 4. Coherent backup and continuity verification

Before an ordinary Tendwire/Herdres upgrade:

1. Stop Herdres, Tendwire, and every other identity consumer. Capture one
   access-restricted recovery set containing the active Tendwire database,
   `data_dir/installation.key`, `data_dir/installation.key.sha256`,
   `data_dir/installation.key.initialized`, and complete Herdres persistent
   state. The three identity artifacts and all dependent state must come from
   the same stopped-service checkpoint.
2. Confirm the Tendwire data directory is mode `0700`, all three identity files
   are mode `0600`, and the files are owned by the Tendwire service account.
   Confirm that `installation.key.initialized` is the exact nonsecret one-byte
   value `1` and that the release artifact contains none of the three
   filenames.
3. Retain all three identity artifacts through the upgrade. Start Tendwire
   before Herdres and confirm a known same-workspace worker has the same
   exact-format `meta.stable_key` and integer `stable_key_version: 1` as before.
   Then start Herdres and confirm its existing binding/topic remains singular.
4. Verify a same-workspace tab move preserves the handle and a controlled
   cross-workspace move changes it. Also verify a fixture restore preserves the
   handle while terminal/session identifiers change; those volatile identifiers
   are not continuity inputs.

Ordinary load validates and reuses initialized state and never rotates it. With
`installation.key.initialized` present, loss of the key, digest, or both fails
closed; stop every identity consumer and restore the complete coherent recovery
set rather than repairing individual artifacts. The sentinel is created only
after Tendwire has validated and published the key and digest.

Deliberate offline rotation is not release continuity verification. With
Tendwire and every identity consumer stopped, invoke
`tendwire.worker_identity.reset_installation_key(Path(data_dir),
acknowledge_continuity_break=True)` through a controlled operator Python
environment; never delete identity files manually. The next eligible load
bootstraps a new three-artifact identity and changes every `wsk1_` handle.
Herdres state, bindings, and topics require explicit migration and review;
stale bindings are quarantined and old topics are not silently rebound or
automatically reused.

## 5. Local hygiene (optional, before building from a dirty tree)

```sh
find . -type d -name __pycache__ -not -path './.git/*' -exec rm -rf {} +
rm -rf .pytest_cache
find . -name '*.pyc' -not -path './.git/*' -delete
```

## Notes

- `HANDOFF.md`, `*.db`, `installation.key`, `installation.key.sha256`, and
  `installation.key.initialized` are git-ignored and never appear in
  `git archive`.
- The public contract shipped is `command.submit` (`tendwire command --json`);
  see the README "Send transport" section. No `pane_id`/`send_keys` is exposed.
- `tests/test_release_readiness.py` guards the public JSON contract (zero
  forbidden keys, no pseudo pane ids).
