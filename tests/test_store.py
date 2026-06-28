"""Tests for the sqlite store contract."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from tendwire.config import Config
from tendwire.core.projector import project_empty, project_from_raw
from tendwire.store.sqlite import (
    get_command_receipt,
    init_store,
    latest_snapshot,
    list_hosts,
    save_command_receipt,
    save_snapshot,
)


def _user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _indexed_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    columns: set[str] = set()
    for row in conn.execute(f"PRAGMA index_list({table})").fetchall():
        index_name = row[1]
        for index_row in conn.execute(f"PRAGMA index_info({index_name})").fetchall():
            columns.add(index_row[2])
    return columns


def test_store_initializes_v2_schema_with_content_fingerprint_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(snapshots)")}
        assert _user_version(conn) == 2
        assert {"host_id", "created_at", "payload", "content_fingerprint"} <= columns
        indexed = _indexed_columns(conn, "snapshots")
        assert "host_id" in indexed
        assert "content_fingerprint" in indexed


def test_store_migrates_v1_schema_and_persists_content_fingerprint(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            PRAGMA user_version = 1;
            """
        )

    init_store(db_path)
    config = Config(host_id="storehost", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Agent One", "status": "blocked"}],
    )
    save_snapshot(db_path, snapshot)

    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == 2
        row = conn.execute(
            "SELECT host_id, content_fingerprint, payload FROM snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert row[0] == "storehost"
    assert row[1] == snapshot.content_fingerprint
    assert json.loads(row[2]) == json.loads(snapshot.to_json())
    restored = latest_snapshot(db_path)
    assert restored is not None
    assert restored.host_id == "storehost"
    assert restored.content_fingerprint == snapshot.content_fingerprint


def test_store_save_latest_and_list_hosts(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"
    config = Config(host_id="host-a", db_path=db_path)

    init_store(db_path)
    assert latest_snapshot(db_path) is None

    snapshot = project_empty(config)
    save_snapshot(db_path, snapshot)
    restored = latest_snapshot(db_path)

    assert restored is not None
    assert restored.host_id == "host-a"
    assert restored.content_fingerprint == snapshot.content_fingerprint
    assert list_hosts(db_path) == ["host-a"]


def test_store_command_receipts_track_idempotency(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"
    init_store(db_path)

    assert get_command_receipt(db_path, "host-a", "req-1", "send_instruction") is None

    save_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-1",
        action="send_instruction",
        payload_fingerprint="fp-1",
        status="backend_unsupported",
        result_json='{"ok": false}',
    )

    receipt = get_command_receipt(db_path, "host-a", "req-1", "send_instruction")
    assert receipt is not None
    assert receipt["payload_fingerprint"] == "fp-1"
    assert receipt["status"] == "backend_unsupported"
    assert receipt["uncertain"] is False
    assert receipt["completed_at"] is not None

    save_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-2",
        action="send_instruction",
        payload_fingerprint="fp-2",
        status="request_state_uncertain",
        result_json='{"ok": false}',
        uncertain=True,
    )

    uncertain = get_command_receipt(db_path, "host-a", "req-2", "send_instruction")
    assert uncertain is not None
    assert uncertain["uncertain"] is True
    assert uncertain["completed_at"] is None
