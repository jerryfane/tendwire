"""Minimal local-first persistence using stdlib sqlite3.

The CLI snapshot path works without requiring a live store. This module is
provided for optional persistence and is kept intentionally simple.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from ..core.models import Snapshot


SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
"""


def _ensure_dir(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def init_store(db_path: Path) -> None:
    """Initialize the snapshots table if it does not exist."""
    _ensure_dir(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(SCHEMA)


def save_snapshot(db_path: Path, snapshot: Snapshot) -> None:
    """Persist a snapshot as JSON in the sqlite store."""
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "INSERT INTO snapshots (host_id, created_at, payload) VALUES (?, ?, ?)",
            (snapshot.host_id, snapshot.updated_at, snapshot.to_json()),
        )


def latest_snapshot(db_path: Path) -> Snapshot | None:
    """Return the most recently persisted snapshot, or None."""
    if not db_path.exists():
        return None
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT payload FROM snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if row is None:
        return None
    return Snapshot.from_json(row[0])


def list_hosts(db_path: Path) -> list[str]:
    """Return distinct host_ids seen in the store."""
    if not db_path.exists():
        return []
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT DISTINCT host_id FROM snapshots ORDER BY host_id"
        ).fetchall()
    return [r[0] for r in rows]
