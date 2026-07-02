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

## Rollback

Tendwire does not own Telegram delivery state. To roll back a Herdres source-mode
deployment, switch Herdres to `HERDRES_TENDWIRE_MODE=enrich` or
`HERDRES_TENDWIRE_MODE=off`, restart Herdres services, and leave Tendwire running
or stop `tendwired.service` after clients have stopped using it.
