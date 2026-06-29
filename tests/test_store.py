"""Tests for the sqlite store contract."""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from tendwire.core.commands import STATUS_ACCEPTED
from tendwire.config import Config
from tendwire.core.models import WorkerBinding
from tendwire.core.projector import project_empty, project_from_raw
from tendwire.store.sqlite import (
    expire_stale_worker_bindings,
    expire_worker_bindings,
    get_command_receipt,
    init_store,
    latest_snapshot,
    list_hosts,
    list_worker_bindings,
    reserve_command_receipt,
    resolve_worker_binding,
    save_command_receipt,
    save_snapshot,
    upsert_worker_bindings,
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


def _unique_index_columns(conn: sqlite3.Connection, table: str) -> dict[str, tuple[str, ...]]:
    indexes: dict[str, tuple[str, ...]] = {}
    for row in conn.execute(f"PRAGMA index_list({table})").fetchall():
        if int(row[2]) != 1:
            continue
        index_name = str(row[1])
        columns = tuple(
            str(index_row[2])
            for index_row in conn.execute(f"PRAGMA index_info({index_name})").fetchall()
        )
        indexes[index_name] = columns
    return indexes


def test_store_initializes_v2_schema_with_content_fingerprint_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(snapshots)")}
        assert _user_version(conn) == 3
        assert {"host_id", "created_at", "payload", "content_fingerprint"} <= columns
        indexed = _indexed_columns(conn, "snapshots")
        assert "host_id" in indexed
        assert "content_fingerprint" in indexed
        binding_columns = {row[1] for row in conn.execute("PRAGMA table_info(worker_bindings)")}
        assert {
            "host_id",
            "worker_id",
            "worker_fingerprint",
            "backend",
            "target_kind",
            "target_value",
            "turn_target_kind",
            "turn_target_value",
            "sendable",
            "reason",
            "observed_at",
            "expires_at",
            "private_fingerprint",
        } <= binding_columns
        binding_indexed = _indexed_columns(conn, "worker_bindings")
        assert {
            "worker_id",
            "worker_fingerprint",
            "private_fingerprint",
            "target_kind",
            "target_value",
            "expires_at",
        } <= binding_indexed


def test_store_command_receipts_have_unique_logical_key_index(tmp_path: Path) -> None:
    db_path = tmp_path / "receipts.db"

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        indexes = _unique_index_columns(conn, "command_receipts")

    assert indexes["ux_command_receipts_host_request_action"] == (
        "host_id",
        "request_id",
        "action",
    )


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
        assert _user_version(conn) == 3
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


def _worker_binding(
    *,
    worker_id: str = "worker-1",
    worker_fingerprint: str = "fp-1",
    target_kind: str = "pane_id",
    target_value: str = "pane-1",
    private_fingerprint: str = "priv-1",
    sendable: bool = True,
    reason: str | None = None,
    observed_at: str = "2026-01-01T00:00:00+00:00",
    expires_at: str = "2026-01-02T00:00:00+00:00",
) -> WorkerBinding:
    return WorkerBinding(
        host_id="host-a",
        worker_id=worker_id,
        worker_fingerprint=worker_fingerprint,
        backend="herdr",
        target_kind=target_kind,
        target_value=target_value,
        turn_target_kind=None,
        turn_target_value=None,
        sendable=sendable,
        reason=reason,
        observed_at=observed_at,
        expires_at=expires_at,
        private_fingerprint=private_fingerprint,
    )


def test_store_worker_binding_upsert_list_resolve_and_expire(tmp_path: Path) -> None:
    db_path = tmp_path / "bindings.db"
    first = _worker_binding()
    moved = _worker_binding(
        target_value="pane-2",
        observed_at="2026-01-01T00:10:00+00:00",
    )

    init_store(db_path)
    assert upsert_worker_bindings(db_path, [first]) == 1
    assert upsert_worker_bindings(db_path, [moved]) == 1

    current = list_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    )
    assert len(current) == 1
    assert current[0].target_value == "pane-2"
    assert current[0].worker_id == "worker-1"
    resolved = resolve_worker_binding(
        db_path,
        "host-a",
        "worker-1",
        worker_fingerprint="fp-1",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    )
    assert resolved is not None
    assert resolved.target_value == "pane-2"

    expired_count = expire_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        private_fingerprints=["priv-1"],
        now="2026-01-01T00:45:00+00:00",
        reason="stale_target",
    )
    assert expired_count == 1
    assert list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:46:00+00:00") == []
    history = list_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        include_expired=True,
        now="2026-01-01T00:46:00+00:00",
    )
    assert len(history) == 1
    assert history[0].sendable is False
    assert history[0].reason == "stale_target"
    assert resolve_worker_binding(
        db_path,
        "host-a",
        "worker-1",
        backend="herdr",
        now="2026-01-01T00:46:00+00:00",
    ) is None


def test_store_worker_bindings_allow_duplicate_targets_and_expire_stale(tmp_path: Path) -> None:
    db_path = tmp_path / "duplicate-bindings.db"
    binding_a = _worker_binding(
        worker_id="worker-a",
        worker_fingerprint="fp-a",
        private_fingerprint="priv-a",
        target_value="same-pane",
        sendable=False,
        reason="duplicate_backend_target",
    )
    binding_b = _worker_binding(
        worker_id="worker-b",
        worker_fingerprint="fp-b",
        private_fingerprint="priv-b",
        target_value="same-pane",
        sendable=False,
        reason="duplicate_backend_target",
    )
    upsert_worker_bindings(db_path, [binding_a, binding_b])

    current = list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:30:00+00:00")
    assert len(current) == 2
    assert {binding.target_value for binding in current} == {"same-pane"}
    assert {binding.reason for binding in current} == {"duplicate_backend_target"}
    assert resolve_worker_binding(
        db_path,
        "host-a",
        "worker-a",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    ) is None

    expired_count = expire_stale_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        current_private_fingerprints=["priv-a"],
        now="2026-01-01T00:40:00+00:00",
        reason="stale_observation",
    )
    assert expired_count == 1
    remaining = list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:41:00+00:00")
    assert [binding.private_fingerprint for binding in remaining] == ["priv-a"]


def test_store_snapshot_payload_does_not_contain_private_worker_bindings(tmp_path: Path) -> None:
    db_path = tmp_path / "payload-clean.db"
    config = Config(host_id="host-a", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    binding = _worker_binding(target_value="pane-secret", private_fingerprint="priv-secret")

    save_snapshot(db_path, snapshot)
    upsert_worker_bindings(db_path, [binding])

    with sqlite3.connect(str(db_path)) as conn:
        payload = conn.execute("SELECT payload FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()[0]
        target_value = conn.execute("SELECT target_value FROM worker_bindings LIMIT 1").fetchone()[0]

    assert target_value == "pane-secret"
    assert "pane-secret" not in payload
    assert "priv-secret" not in payload
    assert "target_kind" not in payload


def test_store_save_latest_host_scope_and_list_hosts(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"
    config_a = Config(host_id="host-a", db_path=db_path)
    config_b = Config(host_id="host-b", db_path=db_path)

    init_store(db_path)
    assert latest_snapshot(db_path) is None

    snapshot_a_old = project_from_raw(
        config_a,
        workers=[{"id": "worker-a-old", "name": "Host A Old", "status": "active"}],
    )
    snapshot_b = project_from_raw(
        config_b,
        workers=[{"id": "worker-b", "name": "Host B", "status": "idle"}],
    )
    snapshot_a_new = project_from_raw(
        config_a,
        workers=[{"id": "worker-a-new", "name": "Host A New", "status": "waiting"}],
    )

    save_snapshot(db_path, snapshot_a_old)
    save_snapshot(db_path, snapshot_b)
    save_snapshot(db_path, snapshot_a_new)

    global_restored = latest_snapshot(db_path)
    assert global_restored is not None
    assert global_restored.host_id == "host-a"
    assert global_restored.content_fingerprint == snapshot_a_new.content_fingerprint

    restored_a = latest_snapshot(db_path, "host-a")
    assert restored_a is not None
    assert restored_a.host_id == "host-a"
    assert restored_a.content_fingerprint == snapshot_a_new.content_fingerprint
    assert restored_a.workers[0].id == "worker-a-new"

    restored_b = latest_snapshot(db_path, "host-b")
    assert restored_b is not None
    assert restored_b.host_id == "host-b"
    assert restored_b.content_fingerprint == snapshot_b.content_fingerprint
    assert restored_b.workers[0].id == "worker-b"

    assert latest_snapshot(db_path, "missing-host") is None
    assert list_hosts(db_path) == ["host-a", "host-b"]


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


def test_store_migrates_legacy_duplicate_command_receipts_by_latest_row(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy-receipts.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                content_fingerprint TEXT NOT NULL DEFAULT '',
                payload TEXT NOT NULL
            );
            CREATE TABLE command_receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                action TEXT NOT NULL,
                payload_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                uncertain INTEGER NOT NULL DEFAULT 0
            );
            INSERT INTO command_receipts (
                host_id, request_id, action, payload_fingerprint, status,
                result_json, created_at, completed_at, uncertain
            ) VALUES
                ('host-a', 'req-1', 'send_instruction', 'fp-old', 'backend_failed',
                 '{"status":"backend_failed"}', '2026-01-01T00:00:00+00:00',
                 '2026-01-01T00:00:01+00:00', 0),
                ('host-a', 'req-1', 'send_instruction', 'fp-new', 'accepted',
                 '{"status":"accepted"}', '2026-01-01T00:00:02+00:00',
                 '2026-01-01T00:00:03+00:00', 0);
            PRAGMA user_version = 2;
            """
        )

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]
        indexes = _unique_index_columns(conn, "command_receipts")

    receipt = get_command_receipt(db_path, "host-a", "req-1", "send_instruction")
    assert count == 1
    assert receipt is not None
    assert receipt["payload_fingerprint"] == "fp-new"
    assert receipt["status"] == STATUS_ACCEPTED
    assert indexes["ux_command_receipts_host_request_action"] == (
        "host_id",
        "request_id",
        "action",
    )


def test_store_completion_updates_reserved_receipt_row(tmp_path: Path) -> None:
    db_path = tmp_path / "completion.db"
    init_store(db_path)

    reservation = reserve_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-update",
        action="send_instruction",
        payload_fingerprint="fp-update",
        pending_result_json='{"ok": false, "status": "request_state_uncertain"}',
    )
    assert reservation["reserved"] is True

    save_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-update",
        action="send_instruction",
        payload_fingerprint="fp-update",
        status=STATUS_ACCEPTED,
        result_json='{"ok": true, "status": "accepted"}',
    )

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]

    receipt = get_command_receipt(db_path, "host-a", "req-update", "send_instruction")
    assert count == 1
    assert receipt is not None
    assert receipt["status"] == STATUS_ACCEPTED
    assert receipt["uncertain"] is False
    assert receipt["completed_at"] is not None


def test_store_command_receipt_reservation_allows_one_concurrent_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / "race.db"
    init_store(db_path)
    barrier = threading.Barrier(2)
    mutations: list[str] = []
    results: list[dict[str, object]] = []
    lock = threading.Lock()

    def attempt(label: str) -> None:
        barrier.wait(timeout=5)
        reservation = reserve_command_receipt(
            db_path,
            host_id="host-a",
            request_id="req-race",
            action="send_instruction",
            payload_fingerprint="same-fp",
            pending_result_json='{"ok": false, "status": "request_state_uncertain"}',
        )
        with lock:
            results.append(reservation)
            if reservation["reserved"]:
                mutations.append(label)
        if reservation["reserved"]:
            save_command_receipt(
                db_path,
                host_id="host-a",
                request_id="req-race",
                action="send_instruction",
                payload_fingerprint="same-fp",
                status=STATUS_ACCEPTED,
                result_json='{"ok": true, "status": "accepted"}',
            )

    threads = [threading.Thread(target=attempt, args=(label,)) for label in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert not any(thread.is_alive() for thread in threads)
    assert len(results) == 2
    assert sum(1 for result in results if result["reserved"]) == 1
    assert len(mutations) == 1
    receipt = get_command_receipt(db_path, "host-a", "req-race", "send_instruction")
    assert receipt is not None
    assert receipt["status"] == STATUS_ACCEPTED
    assert receipt["uncertain"] is False
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]
    assert count == 1
