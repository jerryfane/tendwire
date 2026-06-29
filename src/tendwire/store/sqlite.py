"""Local-first sqlite persistence for canonical Tendwire snapshots.

The CLI snapshot path works without requiring a live store. This module is
provided for optional persistence and is kept intentionally stdlib-only.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from ..core.commands import CommandEnvelope
from ..core.models import (
    FINGERPRINT_HEX_CHARS,
    SCHEMA_VERSION,
    Snapshot,
    WorkerBinding,
    separate_duplicate_worker_bindings,
    stable_fingerprint,
    stable_json_dumps,
    utc_timestamp,
)


FINGERPRINT_HEX_LENGTH = FINGERPRINT_HEX_CHARS
STORE_SCHEMA_VERSION = 3

CREATE_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL
);
"""

CREATE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_snapshots_host_id ON snapshots(host_id)",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON snapshots(created_at)",
    (
        "CREATE INDEX IF NOT EXISTS idx_snapshots_content_fingerprint "
        "ON snapshots(content_fingerprint)"
    ),
)

CREATE_COMMAND_RECEIPTS_TABLE = """
CREATE TABLE IF NOT EXISTS command_receipts (
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
"""

CREATE_COMMAND_RECEIPT_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_command_receipts_host_request_action "
    "ON command_receipts(host_id, request_id, action)",
)
CREATE_COMMAND_RECEIPT_UNIQUE_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_command_receipts_host_request_action "
    "ON command_receipts(host_id, request_id, action)"
)

CREATE_WORKER_BINDINGS_TABLE = """
CREATE TABLE IF NOT EXISTS worker_bindings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT NOT NULL,
    backend TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target_value TEXT NOT NULL,
    turn_target_kind TEXT,
    turn_target_value TEXT,
    sendable INTEGER NOT NULL DEFAULT 0,
    reason TEXT,
    observed_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    private_fingerprint TEXT NOT NULL
);
"""

CREATE_WORKER_BINDING_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_worker_id "
    "ON worker_bindings(host_id, worker_id)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_worker_fingerprint "
    "ON worker_bindings(host_id, worker_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_private_fingerprint "
    "ON worker_bindings(host_id, backend, private_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_backend_target "
    "ON worker_bindings(host_id, backend, target_kind, target_value)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_expires_at "
    "ON worker_bindings(host_id, expires_at)",
)
CREATE_WORKER_BINDING_UNIQUE_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_worker_bindings_host_backend_private "
    "ON worker_bindings(host_id, backend, private_fingerprint)"
)


def _ensure_dir(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _canonical_json(data: Any) -> str:
    return stable_json_dumps(data)


def _snapshot_dict(snapshot: Snapshot) -> dict[str, Any]:
    if hasattr(snapshot, "to_dict"):
        data = snapshot.to_dict()
    else:
        data = json.loads(snapshot.to_json())
    return dict(data)


def _sort_observations(value: Any) -> Any:
    if not isinstance(value, list):
        return value
    if not all(isinstance(item, Mapping) for item in value):
        return value
    return sorted(
        (dict(item) for item in value),
        key=lambda item: (
            str(item.get("id") or item.get("fingerprint") or ""),
            str(item.get("fingerprint") or ""),
            _canonical_json(item),
        ),
    )


def _strip_content_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_content_volatile(item)
            for key, item in value.items()
            if str(key).lower() not in {"updated_at", "observed_at", "content_fingerprint"}
        }
    if isinstance(value, list | tuple):
        return [_strip_content_volatile(item) for item in value]
    return value


def _fingerprint_input(data: Mapping[str, Any]) -> dict[str, Any]:
    fingerprint_data = dict(_strip_content_volatile(data))
    for collection in ("spaces", "workers", "attention"):
        if collection in fingerprint_data:
            fingerprint_data[collection] = _sort_observations(
                fingerprint_data[collection]
            )
    return fingerprint_data


def _content_fingerprint(data: Mapping[str, Any]) -> str:
    raw = data.get("content_fingerprint")
    if isinstance(raw, str) and raw:
        return raw
    return stable_fingerprint(
        _fingerprint_input(data),
        length=FINGERPRINT_HEX_LENGTH,
    )


def _command_receipt_from_row(row: Any) -> dict[str, Any]:
    return {
        "host_id": row[0],
        "request_id": row[1],
        "action": row[2],
        "payload_fingerprint": row[3],
        "status": row[4],
        "result_json": row[5],
        "created_at": row[6],
        "completed_at": row[7],
        "uncertain": bool(row[8]),
    }


def _worker_binding_from_row(row: Any) -> WorkerBinding:
    return WorkerBinding(
        host_id=row[0],
        worker_id=row[1],
        worker_fingerprint=row[2],
        backend=row[3],
        target_kind=row[4],
        target_value=row[5],
        turn_target_kind=row[6],
        turn_target_value=row[7],
        sendable=bool(row[8]),
        reason=row[9],
        observed_at=row[10],
        expires_at=row[11],
        private_fingerprint=row[12],
    )


def _dedupe_command_receipts(conn: sqlite3.Connection) -> None:
    """Keep the latest legacy receipt per logical command key before uniquing."""
    rows = conn.execute(
        """
        SELECT
            id,
            host_id,
            request_id,
            action,
            created_at,
            completed_at
        FROM command_receipts
        ORDER BY id
        """
    ).fetchall()
    keep_by_key: dict[tuple[str, str, str], tuple[str, str, int]] = {}
    for row in rows:
        row_id = int(row[0])
        key = (str(row[1]), str(row[2]), str(row[3]))
        created_at = str(row[4] or "")
        completed_at = str(row[5] or "")
        sort_key = (completed_at or created_at, created_at, row_id)
        if key not in keep_by_key or sort_key > keep_by_key[key]:
            keep_by_key[key] = sort_key

    keep_ids = {item[2] for item in keep_by_key.values()}
    delete_ids = [int(row[0]) for row in rows if int(row[0]) not in keep_ids]
    if not delete_ids:
        return
    placeholders = ",".join("?" for _ in delete_ids)
    conn.execute(
        f"DELETE FROM command_receipts WHERE id IN ({placeholders})",
        delete_ids,
    )


def _ensure_command_receipt_unique_index(conn: sqlite3.Connection) -> None:
    for row in conn.execute("PRAGMA index_list(command_receipts)").fetchall():
        index_name = str(row[1])
        is_unique = int(row[2]) == 1
        if index_name == "ux_command_receipts_host_request_action" and not is_unique:
            conn.execute("DROP INDEX ux_command_receipts_host_request_action")
            break
    conn.execute(CREATE_COMMAND_RECEIPT_UNIQUE_INDEX)


def _latest_command_receipt_row(
    conn: sqlite3.Connection,
    host_id: str,
    request_id: str,
    action: str,
) -> Any:
    return conn.execute(
        """
        SELECT
            host_id,
            request_id,
            action,
            payload_fingerprint,
            status,
            result_json,
            created_at,
            completed_at,
            uncertain
        FROM command_receipts
        WHERE host_id = ? AND request_id = ? AND action = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (str(host_id), str(request_id), str(action)),
    ).fetchone()


def _snapshot_payload(data: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    payload_data = dict(data)
    payload_data.setdefault("schema_version", SCHEMA_VERSION)
    fingerprint = _content_fingerprint(payload_data)
    raw = payload_data.get("content_fingerprint")
    if not isinstance(raw, str) or not raw:
        payload_data["content_fingerprint"] = fingerprint
    return payload_data, fingerprint


def _table_columns(conn: sqlite3.Connection, table: str = "snapshots") -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]) for row in rows}


def _backfill_content_fingerprints(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT id, payload
        FROM snapshots
        WHERE content_fingerprint IS NULL OR content_fingerprint = ''
        """
    ).fetchall()
    for row_id, payload in rows:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            fingerprint = _content_fingerprint({"payload": payload})
            conn.execute(
                "UPDATE snapshots SET content_fingerprint = ? WHERE id = ?",
                (fingerprint, row_id),
            )
            continue
        if not isinstance(data, Mapping):
            fingerprint = _content_fingerprint({"payload": data})
            conn.execute(
                "UPDATE snapshots SET content_fingerprint = ? WHERE id = ?",
                (fingerprint, row_id),
            )
            continue
        payload_data, fingerprint = _snapshot_payload(
            Snapshot.from_dict(data).to_dict()
        )
        conn.execute(
            """
            UPDATE snapshots
            SET content_fingerprint = ?, payload = ?
            WHERE id = ?
            """,
            (fingerprint, _canonical_json(payload_data), row_id),
        )


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_SNAPSHOTS_TABLE)
    columns = _table_columns(conn)
    if "content_fingerprint" not in columns:
        conn.execute(
            "ALTER TABLE snapshots ADD COLUMN "
            "content_fingerprint TEXT NOT NULL DEFAULT ''"
        )
    _backfill_content_fingerprints(conn)
    for statement in CREATE_INDEXES:
        conn.execute(statement)
    conn.execute(CREATE_COMMAND_RECEIPTS_TABLE)
    _dedupe_command_receipts(conn)
    for statement in CREATE_COMMAND_RECEIPT_INDEXES:
        conn.execute(statement)
    _ensure_command_receipt_unique_index(conn)
    conn.execute(CREATE_WORKER_BINDINGS_TABLE)
    for statement in CREATE_WORKER_BINDING_INDEXES:
        conn.execute(statement)
    conn.execute(CREATE_WORKER_BINDING_UNIQUE_INDEX)
    conn.execute(f"PRAGMA user_version = {STORE_SCHEMA_VERSION}")


def init_store(db_path: Path) -> None:
    """Initialize or migrate the sqlite store to the current schema."""
    _ensure_dir(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)


def save_snapshot(db_path: Path, snapshot: Snapshot) -> None:
    """Persist a canonical snapshot JSON blob in the sqlite store."""
    data, fingerprint = _snapshot_payload(_snapshot_dict(snapshot))
    payload = _canonical_json(data)
    _ensure_dir(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO snapshots (host_id, created_at, content_fingerprint, payload)
            VALUES (?, ?, ?, ?)
            """,
            (snapshot.host_id, snapshot.updated_at, fingerprint, payload),
        )


def latest_snapshot(db_path: Path, host_id: str | None = None) -> Snapshot | None:
    """Return the latest snapshot globally, or scoped to host_id when provided."""
    if not db_path.exists():
        return None
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        if host_id is None:
            row = conn.execute(
                "SELECT payload FROM snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT payload
                FROM snapshots
                WHERE host_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (host_id,),
            ).fetchone()
    if row is None:
        return None
    return Snapshot.from_json(row[0])


def list_hosts(db_path: Path) -> list[str]:
    """Return distinct host_ids seen in the store."""
    if not db_path.exists():
        return []
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            "SELECT DISTINCT host_id FROM snapshots ORDER BY host_id"
        ).fetchall()
    return [r[0] for r in rows]


def upsert_worker_bindings(db_path: Path, bindings: Iterable[WorkerBinding]) -> int:
    """Persist observed private worker bindings by private identity.

    The upsert key is host/backend/private_fingerprint so a moved pane or
    changed backend target updates the private routing record while preserving
    the public worker identity associated with that private Herdr identity.
    """
    binding_list = separate_duplicate_worker_bindings(
        binding if isinstance(binding, WorkerBinding) else WorkerBinding(**binding)
        for binding in bindings
    )
    if not binding_list:
        return 0
    _ensure_dir(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        conn.executemany(
            """
            INSERT INTO worker_bindings (
                host_id,
                worker_id,
                worker_fingerprint,
                backend,
                target_kind,
                target_value,
                turn_target_kind,
                turn_target_value,
                sendable,
                reason,
                observed_at,
                expires_at,
                private_fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, backend, private_fingerprint) DO UPDATE SET
                worker_id = excluded.worker_id,
                worker_fingerprint = excluded.worker_fingerprint,
                target_kind = excluded.target_kind,
                target_value = excluded.target_value,
                turn_target_kind = excluded.turn_target_kind,
                turn_target_value = excluded.turn_target_value,
                sendable = excluded.sendable,
                reason = excluded.reason,
                observed_at = excluded.observed_at,
                expires_at = excluded.expires_at
            """,
            [
                (
                    binding.host_id,
                    binding.worker_id,
                    binding.worker_fingerprint,
                    binding.backend,
                    binding.target_kind,
                    binding.target_value,
                    binding.turn_target_kind,
                    binding.turn_target_value,
                    int(binding.sendable),
                    binding.reason,
                    binding.observed_at,
                    binding.expires_at,
                    binding.private_fingerprint,
                )
                for binding in binding_list
            ],
        )
    return len(binding_list)


def list_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str | None = None,
    include_expired: bool = False,
    now: str | None = None,
) -> list[WorkerBinding]:
    """Return private worker bindings for a host, current/unexpired by default."""
    if not db_path.exists():
        return []
    current_time = now or utc_timestamp()
    clauses = ["host_id = ?"]
    params: list[Any] = [str(host_id)]
    if backend is not None:
        clauses.append("backend = ?")
        params.append(str(backend))
    if not include_expired:
        clauses.append("expires_at > ?")
        params.append(current_time)
    where = " AND ".join(clauses)
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            f"""
            SELECT
                host_id,
                worker_id,
                worker_fingerprint,
                backend,
                target_kind,
                target_value,
                turn_target_kind,
                turn_target_value,
                sendable,
                reason,
                observed_at,
                expires_at,
                private_fingerprint
            FROM worker_bindings
            WHERE {where}
            ORDER BY observed_at DESC, id DESC
            """,
            params,
        ).fetchall()
    return [_worker_binding_from_row(row) for row in rows]


def resolve_worker_binding(
    db_path: Path,
    host_id: str,
    worker_id: str,
    *,
    worker_fingerprint: str | None = None,
    backend: str | None = None,
    now: str | None = None,
) -> WorkerBinding | None:
    """Resolve a single current, sendable private binding for a public worker."""
    if not db_path.exists():
        return None
    current_time = now or utc_timestamp()
    clauses = ["host_id = ?", "worker_id = ?", "sendable = 1", "expires_at > ?"]
    params: list[Any] = [str(host_id), str(worker_id), current_time]
    if worker_fingerprint:
        clauses.append("worker_fingerprint = ?")
        params.append(str(worker_fingerprint))
    if backend is not None:
        clauses.append("backend = ?")
        params.append(str(backend))
    where = " AND ".join(clauses)
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            f"""
            SELECT
                host_id,
                worker_id,
                worker_fingerprint,
                backend,
                target_kind,
                target_value,
                turn_target_kind,
                turn_target_value,
                sendable,
                reason,
                observed_at,
                expires_at,
                private_fingerprint
            FROM worker_bindings
            WHERE {where}
            ORDER BY observed_at DESC, id DESC
            LIMIT 2
            """,
            params,
        ).fetchall()
    if len(rows) != 1:
        return None
    return _worker_binding_from_row(rows[0])


def expire_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str | None = None,
    worker_id: str | None = None,
    private_fingerprints: Iterable[str] | None = None,
    now: str | None = None,
    reason: str = "expired",
) -> int:
    """Mark matching private bindings expired and unsendable without deleting rows."""
    current_time = now or utc_timestamp()
    fingerprints = [str(value) for value in (private_fingerprints or [])]
    clauses = ["host_id = ?", "expires_at > ?"]
    params: list[Any] = [str(host_id), current_time]
    if backend is not None:
        clauses.append("backend = ?")
        params.append(str(backend))
    if worker_id is not None:
        clauses.append("worker_id = ?")
        params.append(str(worker_id))
    if fingerprints:
        placeholders = ",".join("?" for _ in fingerprints)
        clauses.append(f"private_fingerprint IN ({placeholders})")
        params.extend(fingerprints)
    where = " AND ".join(clauses)
    _ensure_dir(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        cursor = conn.execute(
            f"""
            UPDATE worker_bindings
            SET sendable = 0,
                reason = ?,
                expires_at = ?
            WHERE {where}
            """,
            [str(reason), current_time, *params],
        )
        return int(cursor.rowcount or 0)


def expire_stale_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str,
    current_private_fingerprints: Iterable[str],
    now: str | None = None,
    reason: str = "stale_observation",
) -> int:
    """Expire host/backend bindings absent from a fresh successful observation."""
    current_time = now or utc_timestamp()
    current = {str(value) for value in current_private_fingerprints}
    _ensure_dir(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        if current:
            placeholders = ",".join("?" for _ in current)
            cursor = conn.execute(
                f"""
                UPDATE worker_bindings
                SET sendable = 0,
                    reason = ?,
                    expires_at = ?
                WHERE host_id = ?
                  AND backend = ?
                  AND expires_at > ?
                  AND private_fingerprint NOT IN ({placeholders})
                """,
                [
                    str(reason),
                    current_time,
                    str(host_id),
                    str(backend),
                    current_time,
                    *sorted(current),
                ],
            )
        else:
            cursor = conn.execute(
                """
                UPDATE worker_bindings
                SET sendable = 0,
                    reason = ?,
                    expires_at = ?
                WHERE host_id = ?
                  AND backend = ?
                  AND expires_at > ?
                """,
                [
                    str(reason),
                    current_time,
                    str(host_id),
                    str(backend),
                    current_time,
                ],
            )
        return int(cursor.rowcount or 0)


def get_command_receipt(
    db_path: Path,
    host_id: str,
    request_id: str,
    action: str,
) -> dict[str, Any] | None:
    """Return the latest command receipt for a host/request/action key, or None."""
    if not db_path.exists():
        return None
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        row = _latest_command_receipt_row(conn, host_id, request_id, action)
    if row is None:
        return None
    return _command_receipt_from_row(row)


def reserve_command_receipt(
    db_path: Path,
    host_id: str,
    request_id: str,
    action: str,
    payload_fingerprint: str,
    pending_result_json: str,
    *,
    status: str = "pending",
) -> dict[str, Any]:
    """Atomically reserve a mutating command receipt key if it is unused.

    Returns {"reserved": True, "receipt": None} when this caller owns the
    mutation attempt. If another receipt already exists for the same key, the
    existing latest receipt is returned and no new row is inserted.
    """
    _ensure_dir(db_path)
    conn = sqlite3.connect(str(db_path), timeout=30.0, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_schema(conn)
        row = _latest_command_receipt_row(conn, host_id, request_id, action)
        if row is not None:
            conn.execute("COMMIT")
            return {"reserved": False, "receipt": _command_receipt_from_row(row)}
        now = utc_timestamp()
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id,
                request_id,
                action,
                payload_fingerprint,
                status,
                result_json,
                created_at,
                completed_at,
                uncertain
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(host_id),
                str(request_id),
                str(action),
                str(payload_fingerprint),
                str(status),
                str(pending_result_json),
                now,
                None,
                1,
            ),
        )
        conn.execute("COMMIT")
        return {"reserved": True, "receipt": None}
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def save_command_receipt(
    db_path: Path,
    host_id: str,
    request_id: str,
    action: str,
    payload_fingerprint: str,
    status: str,
    result_json: str,
    *,
    uncertain: bool = False,
) -> None:
    """Persist a neutral command receipt for idempotency tracking.

    Dry-runs must not call this function. The receipt records the final or
    pending state of a mutating command so repeated requests can be detected
    and rejected instead of retried blindly.
    """
    _ensure_dir(db_path)
    now = utc_timestamp()
    conn = sqlite3.connect(str(db_path), timeout=30.0, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT id, payload_fingerprint
            FROM command_receipts
            WHERE host_id = ? AND request_id = ? AND action = ?
            LIMIT 1
            """,
            (str(host_id), str(request_id), str(action)),
        ).fetchone()
        completed_at = None if uncertain else now
        if row is not None:
            if str(row[1]) != str(payload_fingerprint):
                raise ValueError("receipt payload fingerprint mismatch")
            conn.execute(
                """
                UPDATE command_receipts
                SET
                    status = ?,
                    result_json = ?,
                    completed_at = ?,
                    uncertain = ?
                WHERE id = ? AND payload_fingerprint = ?
                """,
                (
                    str(status),
                    str(result_json),
                    completed_at,
                    int(uncertain),
                    int(row[0]),
                    str(payload_fingerprint),
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO command_receipts (
                    host_id,
                    request_id,
                    action,
                    payload_fingerprint,
                    status,
                    result_json,
                    created_at,
                    completed_at,
                    uncertain
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(host_id),
                    str(request_id),
                    str(action),
                    str(payload_fingerprint),
                    str(status),
                    str(result_json),
                    now,
                    completed_at,
                    int(uncertain),
                ),
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def envelope_to_receipt_json(envelope: CommandEnvelope) -> str:
    """Serialize a command envelope for storage in a receipt."""
    return stable_json_dumps(envelope.to_dict())
