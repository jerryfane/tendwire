# Install

Tendwire can be run from a checkout or installed as a Python package. Python
3.10 or newer is required.

## From A Checkout

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
tendwire doctor --json
```

For direct module execution from a source tree without installing:

```bash
PYTHONPATH=src python3 -m tendwire.cli doctor --json
```

## Source-Mode Service Setup

For daily source-mode use with Herdres, run Tendwire as the Herdr observation and
command-routing daemon:

```ini
[Unit]
Description=Tendwire daemon

[Service]
Type=simple
UMask=0077
Environment=TENDWIRE_HERDR_BACKEND=socket
Environment=TENDWIRE_DB_PATH=%h/.local/share/tendwire/tendwire.db
ExecStart=%h/.local/bin/tendwire daemon --db-path %h/.local/share/tendwire/tendwire.db
Restart=always
RestartSec=5s

[Install]
WantedBy=default.target
```

Install it as `~/.config/systemd/user/tendwired.service`, then run:

```bash
systemctl --user daemon-reload
systemctl --user enable --now tendwired.service
systemctl --user is-active tendwired.service
```

## Continuity State, Backup, Upgrade, Loss, and Rotation

Tendwire keeps its optional worker-continuity identity in the configured
`data_dir` (default `~/.local/share/tendwire`, controlled by
`TENDWIRE_DATA_DIR`), independently of an overridden `TENDWIRE_DB_PATH`. On
first use with a valid worker identity, Tendwire creates a 32-byte private
`installation.key`, publishes its nonsecret digest marker
`installation.key.sha256`, validates the pair, and only then creates
`installation.key.initialized`. The sentinel contains the exact nonsecret
one-byte value `1`. The data directory is restricted to mode `0700`; all three
files are restricted to mode `0600`. The service `UMask=0077` above protects
newly created local state as well. Once initialized, ordinary loads only
validate and reuse this state; they never rotate it.

Treat Tendwire and Herdres continuity data as one recovery unit:

1. Identify the active Tendwire database path, Tendwire `data_dir`, and the
   deployment-specific Herdres persistent state path. Stop Herdres consumers
   first, then stop `tendwired.service`, and confirm both are stopped before
   copying anything.
2. Into one access-restricted backup, copy the Tendwire database,
   `data_dir/installation.key`, `data_dir/installation.key.sha256`,
   `data_dir/installation.key.initialized`, and the complete Herdres persistent
   state. Preserve ownership and modes. The three identity artifacts must come
   from the same stopped-service checkpoint and must be backed up and restored
   together. Do not publish the backup as an issue attachment or build
   artifact.
3. For an ordinary upgrade, update only the installed software or checkout.
   Retain the database, all three identity artifacts, and Herdres state
   unchanged. Restart Tendwire first, verify that a known same-workspace worker
   retains its `meta.stable_key`, then restart Herdres and verify the existing
   binding/topic remains singular.
4. For a restore, keep both services stopped and restore the database, all
   three Tendwire identity artifacts, and Herdres state from the same recovery
   checkpoint. Restore service-user ownership, set the Tendwire data directory
   to mode `0700`, and set `installation.key`, `installation.key.sha256`, and
   `installation.key.initialized` to mode `0600` before starting Tendwire.
   Confirm that the sentinel is exactly one byte containing `1`. Start Tendwire
   and validate continuity before starting Herdres.
5. Once `installation.key.initialized` exists, a missing key or digest marker
   fails closed, including loss of both files while the sentinel remains. A
   replaced key, key/digest mismatch, malformed sentinel, unsafe mode, or wrong
   ownership also fails closed instead of rotating or accepting source
   identity. An absent sentinel is only initial-bootstrap or legacy-migration
   state, never a rotation request: Tendwire must validate and publish the key
   and digest before publishing the sentinel. If continuity state is damaged,
   stop Tendwire and every identity consumer and restore the complete recovery
   set; never repair or copy individual artifacts.

Deliberate rotation is a separate, destructive identity operation, not an
ordinary load, upgrade, or recovery shortcut:

1. Stop Tendwire and every identity consumer, including Herdres, and make a
   fresh coherent backup as described above.
2. From a controlled operator Python environment, invoke
   `tendwire.worker_identity.reset_installation_key(Path(data_dir),
   acknowledge_continuity_break=True)`. This acknowledged reset validates and
   removes the existing three-artifact identity while consumers are offline.
   Do not delete, replace, or edit any identity artifact by hand.
3. On the next eligible Tendwire load, bootstrap a new key, digest, and
   one-byte initialized sentinel. Keep all consumers stopped until Tendwire has
   completed a valid observation. Confirm that every observed
   `meta.stable_key` changed; rotation intentionally provides no old-to-new
   equivalence.
4. Review and explicitly migrate or retire old Herdres state, bindings, and
   topics before enabling normal reconciliation. Herdres quarantines stale
   bindings and creates distinct topics for the new handles; it does not
   silently rebind workers or automatically reuse topics after rotation.

Restore the coherent recovery set instead whenever continuity is meant to
survive.

## Verification

```bash
tendwire doctor --json
tendwire snapshot --json --store --db-path ~/.local/share/tendwire/tendwire.db
tendwire store status --db-path ~/.local/share/tendwire/tendwire.db
python3 - <<'PY'
import sqlite3
from pathlib import Path
db = Path.home() / ".local/share/tendwire/tendwire.db"
with sqlite3.connect(db) as conn:
    print(conn.execute("PRAGMA integrity_check").fetchone()[0])
PY
```

Healthy release-candidate output has healthy backend health, an `ok` store
status, and SQLite integrity `ok`.

## RC Checklist

Before tagging a Tendwire/Herdres source-mode pair:

```bash
# Tendwire source checkout
git status -sb
python3 -m py_compile $(git ls-files '*.py')
python3 -m pytest -q
tendwire doctor --json
tendwire snapshot --json --store --db-path ~/.local/share/tendwire/tendwire.db
tendwire turns --json
tendwire pending --json
tendwire attention --json
tendwire store status --db-path ~/.local/share/tendwire/tendwire.db

# Local store integrity
python3 - <<'PY'
import sqlite3
from pathlib import Path
db = Path.home() / ".local/share/tendwire/tendwire.db"
with sqlite3.connect(db) as conn:
    print(conn.execute("PRAGMA integrity_check").fetchone()[0])
PY
```

Pair this with the Herdres source-mode RC checklist. Herdres source smoke must
report `direct_herdr_calls=0`, two forced source syncs must not repost completed
turn text, and `herdr-server.service` should be checked with status-only
commands unless the operator explicitly asks for an external restart.

## Rollback

Tendwire does not own Telegram delivery state. To roll back a Herdres source-mode
deployment, switch Herdres to `HERDRES_TENDWIRE_MODE=enrich` or
`HERDRES_TENDWIRE_MODE=off`, restart Herdres services, and leave Tendwire running
or stop `tendwired.service` after clients have stopped using it.
