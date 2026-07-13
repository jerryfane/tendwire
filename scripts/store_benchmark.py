#!/usr/bin/env python3
"""Deterministic, synthetic SQLite store lifecycle benchmark.

Run from a source checkout with ``PYTHONPATH=src``. The benchmark creates only
private temporary fixtures and prints one aggregate JSON document to stdout.
Timings are evidence, not pass/fail thresholds.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import sqlite3
import stat
import sys
import tempfile
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter_ns
from typing import Any

from tendwire.core.models import Snapshot, Worker
from tendwire.store import sqlite as store

REPORT_SCHEMA_VERSION = 1
FIXTURE_END = datetime(2026, 6, 1, tzinfo=timezone.utc)
OBSERVATION_CADENCE_SECONDS = 300
FIXTURE_HOST = "synthetic-benchmark-host"
SENTINEL_HOST = "synthetic-durable-host"
FIXTURE_DB_NAME = "store.db"
MUTATING_PREFIXES = ("INSERT ", "UPDATE ", "DELETE ", "REPLACE ")
DDL_PREFIXES = ("CREATE ", "ALTER ", "DROP ")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _snapshot(index: int, observed_at: datetime) -> Snapshot:
    marker = f"{index:06d}"
    return Snapshot(
        host_id=FIXTURE_HOST,
        updated_at=_timestamp(observed_at),
        workers=[
            Worker(
                id=f"worker-{marker}",
                name=f"Worker {marker}",
                status="active",
            )
        ],
    )


def _fixture_rows(
    snapshot_rows: int,
    *,
    batch_size: int,
) -> Iterable[list[tuple[str, str, str, str]]]:
    first = FIXTURE_END - timedelta(
        seconds=OBSERVATION_CADENCE_SECONDS * (snapshot_rows - 1)
    )
    batch: list[tuple[str, str, str, str]] = []
    for index in range(snapshot_rows):
        snapshot = _snapshot(
            index,
            first + timedelta(seconds=OBSERVATION_CADENCE_SECONDS * index),
        )
        payload = _canonical_json(snapshot.to_dict())
        batch.append(
            (
                snapshot.host_id,
                snapshot.updated_at,
                snapshot.content_fingerprint,
                payload,
            )
        )
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _insert_fixture(
    db_path: Path,
    *,
    snapshot_rows: int,
    batch_size: int,
) -> dict[str, int]:
    payload_min: int | None = None
    payload_max = 0
    sql_batches = 0
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("BEGIN IMMEDIATE")
        try:
            for batch in _fixture_rows(snapshot_rows, batch_size=batch_size):
                sizes = [len(row[3].encode("utf-8")) for row in batch]
                payload_min = min(min(sizes), payload_min if payload_min is not None else min(sizes))
                payload_max = max(payload_max, max(sizes))
                conn.executemany(
                    """
                    INSERT INTO snapshots (
                        host_id, created_at, content_fingerprint, payload
                    ) VALUES (?, ?, ?, ?)
                    """,
                    batch,
                )
                sql_batches += 1
            _insert_durable_sentinels(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "sql_batches": sql_batches,
        "payload_bytes_min": int(payload_min or 0),
        "payload_bytes_max": payload_max,
    }


def _insert_durable_sentinels(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO turns (
            host_id, turn_id, worker_id, worker_fingerprint, space_id,
            status, kind, updated_at, fingerprint,
            snapshot_content_fingerprint, observed_at, payload_json,
            list_sequence
        ) VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
        (
            SENTINEL_HOST,
            "durable-turn",
            "durable-worker",
            "completed",
            "command",
            "2026-01-01T00:00:00+00:00",
            "turn-sentinel",
            "snapshot-sentinel",
            "2026-01-01T00:00:00+00:00",
            _canonical_json({"state": "durable"}),
        ),
    )
    conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id, connector, delivery_key, status, payload_json,
            private_state_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            SENTINEL_HOST,
            "synthetic",
            "durable-outbox",
            "queued",
            _canonical_json({"state": "durable"}),
            _canonical_json({"state": "durable-private"}),
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00+00:00",
        ),
    )


def _sentinels_intact(db_path: Path) -> tuple[bool, bool]:
    with sqlite3.connect(str(db_path)) as conn:
        turn = conn.execute(
            """
            SELECT status, kind, payload_json
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (SENTINEL_HOST, "durable-turn"),
        ).fetchall()
        outbox = conn.execute(
            """
            SELECT status, payload_json, private_state_json
            FROM connector_outbox
            WHERE host_id = ? AND delivery_key = ?
            """,
            (SENTINEL_HOST, "durable-outbox"),
        ).fetchall()
    return (
        turn
        == [
            (
                "completed",
                "command",
                _canonical_json({"state": "durable"}),
            )
        ],
        outbox
        == [
            (
                "queued",
                _canonical_json({"state": "durable"}),
                _canonical_json({"state": "durable-private"}),
            )
        ],
    )


def _downgrade_to_v7(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("BEGIN IMMEDIATE")
        try:
            for index_name in (
                "ux_turns_host_list_sequence",
                "idx_turns_host_worker_list_sequence",
            ):
                conn.execute(f"DROP INDEX IF EXISTS {index_name}")
            conn.execute("DROP TABLE turn_list_state")
            conn.execute("ALTER TABLE turns DROP COLUMN list_sequence")
            conn.execute("DROP TABLE store_maintenance_state")
            conn.execute("DROP INDEX IF EXISTS idx_snapshots_host_newest")
            conn.execute("DROP INDEX IF EXISTS idx_snapshots_created_host_id")
            for statement in store.CREATE_LEGACY_SNAPSHOT_INDEXES:
                conn.execute(statement)
            conn.execute("PRAGMA user_version = 7")
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _checks(db_path: Path) -> dict[str, Any]:
    with sqlite3.connect(str(db_path)) as conn:
        integrity_rows = conn.execute("PRAGMA integrity_check").fetchall()
        foreign_key_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    return {
        "integrity_ok": integrity_rows == [("ok",)],
        "foreign_key_violations": len(foreign_key_rows),
        "schema_version": version,
    }


def _checkpoint(db_path: Path) -> dict[str, int]:
    with sqlite3.connect(str(db_path), isolation_level=None) as conn:
        row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if row is None:
        raise RuntimeError("checkpoint_result_missing")
    return {
        "busy": int(row[0]),
        "log_frames": int(row[1]),
        "checkpointed_frames": int(row[2]),
    }


def _family_bytes(db_path: Path) -> int:
    return sum(
        candidate.stat().st_size
        for candidate in (
            db_path,
            Path(f"{db_path}-wal"),
            Path(f"{db_path}-shm"),
        )
        if candidate.exists()
    )


def _snapshot_count(db_path: Path) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0])


def _distinct_snapshot_content_count(db_path: Path) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        return int(
            conn.execute(
                "SELECT COUNT(DISTINCT content_fingerprint) FROM snapshots"
            ).fetchone()[0]
        )


def _page_metrics(db_path: Path) -> dict[str, int]:
    with sqlite3.connect(str(db_path)) as conn:
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    return {
        "page_size_bytes": page_size,
        "page_count": page_count,
        "freelist_pages": freelist_count,
        "logical_live_bytes": (page_count - freelist_count) * page_size,
        "logical_reclaimable_bytes": freelist_count * page_size,
    }


def _nearest_rank(samples: list[int], percentile: float) -> int:
    if not samples:
        raise ValueError("samples_required")
    ordered = sorted(samples)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def _metric(samples: list[int], *, warmup: int) -> dict[str, int]:
    return {
        "samples": len(samples),
        "warmup": warmup,
        "min_ns": min(samples),
        "p50_ns": _nearest_rank(samples, 0.50),
        "p95_ns": _nearest_rank(samples, 0.95),
        "max_ns": max(samples),
    }


def _measure(
    operation: Callable[[int], Any],
    *,
    warmup: int,
    samples: int,
) -> dict[str, int]:
    for index in range(warmup):
        operation(index)
    timings: list[int] = []
    for index in range(samples):
        started = perf_counter_ns()
        operation(warmup + index)
        timings.append(perf_counter_ns() - started)
    return _metric(timings, warmup=warmup)


def _remove_family(db_path: Path) -> None:
    for candidate in (
        Path(f"{db_path}-shm"),
        Path(f"{db_path}-wal"),
        db_path,
    ):
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass


def _measure_current_init(
    root: Path,
    *,
    warmup: int,
    samples: int,
) -> dict[str, int]:
    def initialize(index: int) -> None:
        db_path = root / f"init-{index:04d}.db"
        store.init_store(db_path)
        with sqlite3.connect(str(db_path)) as conn:
            version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version != store.STORE_SCHEMA_VERSION:
            raise RuntimeError("current_init_version_mismatch")
        _remove_family(db_path)

    return _measure(initialize, warmup=warmup, samples=samples)


def _current_open_statement_evidence(db_path: Path) -> dict[str, int]:
    statements: list[str] = []
    original = store._apply_connection_pragmas

    def traced(conn: sqlite3.Connection, path: Path | str) -> None:
        conn.set_trace_callback(statements.append)
        original(conn, path)

    store._apply_connection_pragmas = traced
    try:
        store.init_store(db_path)
    finally:
        store._apply_connection_pragmas = original
    normalized = [" ".join(statement.upper().split()) for statement in statements]
    return {
        "statements": len(normalized),
        "pragma_statements": sum(item.startswith("PRAGMA ") for item in normalized),
        "user_version_statements": sum(item == "PRAGMA USER_VERSION" for item in normalized),
        "mutating_statements": sum(item.startswith(MUTATING_PREFIXES) for item in normalized),
        "ddl_statements": sum(item.startswith(DDL_PREFIXES) for item in normalized),
        "journal_mode_statements": sum(item.startswith("PRAGMA JOURNAL_MODE") for item in normalized),
        "vacuum_statements": sum(item.startswith("VACUUM") for item in normalized),
    }


def _plan_details(
    conn: sqlite3.Connection,
    sql: str,
    params: dict[str, Any] | tuple[Any, ...],
) -> list[str]:
    return [
        str(row[3]).upper()
        for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
    ]


def _plan_summary(details: list[str], expected_indexes: tuple[str, ...]) -> dict[str, Any]:
    return {
        "details": details,
        "plan_nodes": len(details),
        "expected_indexes_used": {
            name: any(name.upper() in detail for detail in details)
            for name in expected_indexes
        },
        "unindexed_scan_nodes": sum(
            "SCAN " in detail
            and "USING INDEX" not in detail
            and "USING COVERING INDEX" not in detail
            for detail in details
        ),
        "temporary_btree_nodes": sum("TEMP B-TREE" in detail for detail in details),
    }


def _query_plan_evidence(db_path: Path, retention_count: int) -> dict[str, Any]:
    latest_sql = """
        SELECT payload FROM snapshots
        WHERE host_id = ? ORDER BY id DESC LIMIT 1
    """
    with sqlite3.connect(str(db_path)) as conn:
        latest = _plan_details(conn, latest_sql, (FIXTURE_HOST,))
        age = _plan_details(
            conn,
            store._SNAPSHOT_AGE_CANDIDATE_SQL,
            {
                "cutoff_at": "2026-05-18T00:00:00+00:00",
                "candidate_limit": 101,
            },
        )
        count = _plan_details(
            conn,
            store._SNAPSHOT_COUNT_CANDIDATE_SQL,
            {
                "retention_offset": retention_count - 1,
                "candidate_limit": 101,
            },
        )
    return {
        "latest_read": _plan_summary(latest, ("idx_snapshots_host_newest",)),
        "retention_age": _plan_summary(
            age,
            ("idx_snapshots_created_host_id", "idx_snapshots_host_newest"),
        ),
        "retention_count": _plan_summary(
            count,
            ("idx_snapshots_host_newest",),
        ),
    }


def _with_updated_at(snapshot: Snapshot, observed_at: datetime) -> Snapshot:
    return Snapshot(
        host_id=snapshot.host_id,
        updated_at=_timestamp(observed_at),
        spaces=list(snapshot.spaces),
        workers=list(snapshot.workers),
        attention=list(snapshot.attention),
        backend_health=list(snapshot.backend_health),
    )


def _changed_snapshot(sequence: int) -> Snapshot:
    marker = f"{sequence:06d}"
    return Snapshot(
        host_id=FIXTURE_HOST,
        updated_at=_timestamp(FIXTURE_END + timedelta(seconds=sequence + 1)),
        workers=[
            Worker(
                id=f"changed-{marker}",
                name=f"Changed {marker}",
                status="active",
            )
        ],
    )


def _retention_policy_state(
    db_path: Path,
    *,
    retention_days: int,
    retention_count: int,
) -> dict[str, Any]:
    cutoff_at = _timestamp(FIXTURE_END - timedelta(days=retention_days))
    with sqlite3.connect(str(db_path)) as conn:
        snapshot_rows = int(
            conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        )
        host_rows = [
            (str(row[0]), int(row[1]))
            for row in conn.execute(
                """
                SELECT host_id, COUNT(*)
                FROM snapshots
                GROUP BY host_id
                ORDER BY host_id
                """
            ).fetchall()
        ]
        rows_older_than_cutoff_excluding_latest = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM snapshots AS candidate
                WHERE candidate.created_at < ?
                  AND candidate.id <> (
                      SELECT newest.id
                      FROM snapshots AS newest
                      WHERE newest.host_id = candidate.host_id
                      ORDER BY newest.id DESC
                      LIMIT 1
                  )
                """,
                (cutoff_at,),
            ).fetchone()[0]
        )
    max_rows_per_host = max((row_count for _, row_count in host_rows), default=0)
    return {
        "cutoff_at": cutoff_at,
        "snapshot_rows": snapshot_rows,
        "host_row_counts": [
            {"host_id": host_id, "rows": row_count}
            for host_id, row_count in host_rows
        ],
        "max_rows_per_host": max_rows_per_host,
        "rows_older_than_cutoff_excluding_latest": (
            rows_older_than_cutoff_excluding_latest
        ),
        "count_policy_satisfied": max_rows_per_host <= retention_count,
        "age_policy_satisfied": rows_older_than_cutoff_excluding_latest == 0,
    }


def _run_retention(
    db_path: Path,
    *,
    retention_days: int,
    retention_count: int,
    batch_size: int,
    warmup: int,
) -> tuple[dict[str, Any], list[int]]:
    now = _timestamp(FIXTURE_END)
    for _ in range(warmup):
        warmed = store.cleanup_snapshot_retention(
            db_path,
            retention_days=retention_days,
            retention_count=retention_count,
            batch_size=batch_size,
            now=now,
            dry_run=True,
        )
        if not warmed.get("ok") or int(warmed.get("deleted", -1)) != 0:
            raise RuntimeError("retention_warmup_failed")
    timings: list[int] = []
    calls = 0
    deleted = 0
    max_examined = 0
    max_deleted = 0
    while True:
        started = perf_counter_ns()
        result = store.cleanup_snapshot_retention(
            db_path,
            retention_days=retention_days,
            retention_count=retention_count,
            batch_size=batch_size,
            now=now,
            dry_run=False,
        )
        timings.append(perf_counter_ns() - started)
        calls += 1
        if not result.get("ok"):
            raise RuntimeError("retention_failed")
        examined = int(result.get("examined", 0))
        deleted_now = int(result.get("deleted", 0))
        if examined > batch_size or deleted_now > batch_size:
            raise RuntimeError("retention_batch_unbounded")
        deleted += deleted_now
        max_examined = max(max_examined, examined)
        max_deleted = max(max_deleted, deleted_now)
        if not result.get("remaining_candidates"):
            break
    return (
        {
            "calls": calls,
            "deleted_rows": deleted,
            "max_examined_per_call": max_examined,
            "max_deleted_per_call": max_deleted,
            "batch_bound_honored": max_examined <= batch_size and max_deleted <= batch_size,
            "terminal_remaining_candidates": bool(
                result.get("remaining_candidates")
            ),
        },
        timings,
    )


def _benchmark(args: argparse.Namespace) -> dict[str, Any]:
    temporary_path: Path | None = None
    with tempfile.TemporaryDirectory(prefix="tendwire-store-benchmark-") as raw_root:
        root = Path(raw_root)
        temporary_path = root
        private_directory = stat.S_IMODE(root.stat().st_mode) == 0o700
        db_path = root / FIXTURE_DB_NAME

        init_metric = _measure_current_init(
            root,
            warmup=args.warmup,
            samples=args.samples,
        )
        store.init_store(db_path)
        fixture = _insert_fixture(
            db_path,
            snapshot_rows=args.snapshot_rows,
            batch_size=args.fixture_batch_size,
        )
        fixture_distinct_content = _distinct_snapshot_content_count(db_path)
        if fixture["payload_bytes_min"] != fixture["payload_bytes_max"]:
            raise RuntimeError("fixture_payload_size_not_fixed")
        if _snapshot_count(db_path) != args.snapshot_rows:
            raise RuntimeError("fixture_row_count_mismatch")
        if fixture_distinct_content != args.snapshot_rows:
            raise RuntimeError("fixture_content_not_distinct")

        _downgrade_to_v7(db_path)
        checkpoint_before = _checkpoint(db_path)
        before_checks = _checks(db_path)
        before_turn, before_outbox = _sentinels_intact(db_path)
        family_bytes_before = _family_bytes(db_path)
        rows_before = _snapshot_count(db_path)
        pages_before = _page_metrics(db_path)

        started = perf_counter_ns()
        store.init_store(db_path)
        migration_ns = perf_counter_ns() - started
        checkpoint_after_migration = _checkpoint(db_path)
        migration_checks = _checks(db_path)
        after_migration_turn, after_migration_outbox = _sentinels_intact(db_path)
        family_bytes_after_migration = _family_bytes(db_path)
        rows_after_migration = _snapshot_count(db_path)

        statement_evidence = _current_open_statement_evidence(db_path)
        open_metric = _measure(
            lambda _index: store.init_store(db_path),
            warmup=args.warmup,
            samples=args.samples,
        )
        latest_metric = _measure(
            lambda _index: store.latest_snapshot(db_path, FIXTURE_HOST),
            warmup=args.warmup,
            samples=args.samples,
        )
        latest = store.latest_snapshot(db_path, FIXTURE_HOST)
        if latest is None:
            raise RuntimeError("latest_snapshot_missing")

        rows_before_unchanged = _snapshot_count(db_path)
        unchanged_snapshots = [
            _with_updated_at(latest, FIXTURE_END + timedelta(seconds=index + 1))
            for index in range(args.warmup + args.samples)
        ]
        unchanged_metric = _measure(
            lambda index: store.save_snapshot(db_path, unchanged_snapshots[index]),
            warmup=args.warmup,
            samples=args.samples,
        )
        rows_after_unchanged = _snapshot_count(db_path)

        changed_metric = _measure(
            lambda index: store.save_snapshot(db_path, _changed_snapshot(index)),
            warmup=args.warmup,
            samples=args.samples,
        )
        rows_before_retention = _snapshot_count(db_path)
        expected_changed_growth = args.warmup + args.samples
        changed_growth = rows_before_retention - rows_after_unchanged
        if changed_growth != expected_changed_growth:
            raise RuntimeError("changed_save_growth_mismatch")

        query_plans = _query_plan_evidence(db_path, args.retention_count)
        checkpoint_before_retention = _checkpoint(db_path)
        family_bytes_before_retention = _family_bytes(db_path)
        retention, retention_timings = _run_retention(
            db_path,
            retention_days=args.retention_days,
            retention_count=args.retention_count,
            batch_size=args.maintenance_batch_size,
            warmup=args.warmup,
        )
        checkpoint_after_retention = _checkpoint(db_path)
        family_bytes_after_retention = _family_bytes(db_path)
        rows_after_retention = _snapshot_count(db_path)
        pages_after = _page_metrics(db_path)
        after_checks = _checks(db_path)
        final_turn, final_outbox = _sentinels_intact(db_path)
        final_latest = store.latest_snapshot(db_path, FIXTURE_HOST)
        retention_policy_state = _retention_policy_state(
            db_path,
            retention_days=args.retention_days,
            retention_count=args.retention_count,
        )

        db_private = stat.S_IMODE(db_path.stat().st_mode) & 0o077 == 0
        query_indexes_used = all(
            all(plan["expected_indexes_used"].values())
            for plan in query_plans.values()
        )
        invariant_checks = {
            "fixture_row_count_generated": rows_before == args.snapshot_rows,
            "fixture_content_distinct": (
                fixture_distinct_content == args.snapshot_rows
            ),
            "private_temporary_directory": private_directory,
            "private_database_mode": db_private,
            "migration_row_count_preserved": rows_before == rows_after_migration,
            "schema_migrated_to_current": (
                migration_checks["schema_version"] == store.STORE_SCHEMA_VERSION
            ),
            "integrity_before_after": (
                before_checks["integrity_ok"]
                and migration_checks["integrity_ok"]
                and after_checks["integrity_ok"]
            ),
            "foreign_keys_before_after": (
                before_checks["foreign_key_violations"]
                == migration_checks["foreign_key_violations"]
                == after_checks["foreign_key_violations"]
                == 0
            ),
            "durable_turn_survived": (
                before_turn and after_migration_turn and final_turn
            ),
            "durable_outbox_survived": (
                before_outbox and after_migration_outbox and final_outbox
            ),
            "latest_snapshot_survived": final_latest is not None,
            "unchanged_save_deduplicated": (
                rows_before_unchanged == rows_after_unchanged
            ),
            "changed_save_appended": changed_growth == expected_changed_growth,
            "retention_batch_bound_honored": retention["batch_bound_honored"],
            "retention_terminated": (
                not retention["terminal_remaining_candidates"]
            ),
            "retention_count_policy_satisfied": retention_policy_state[
                "count_policy_satisfied"
            ],
            "retention_age_policy_satisfied": retention_policy_state[
                "age_policy_satisfied"
            ],
            "query_indexes_used": query_indexes_used,
            "current_schema_open_nonmutating": (
                statement_evidence["mutating_statements"] == 0
                and statement_evidence["ddl_statements"] == 0
                and statement_evidence["vacuum_statements"] == 0
            ),
        }
        report: dict[str, Any] = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "ok": False,
            "status": "validating",
            "command": (
                "PYTHONPATH=src python3 scripts/store_benchmark.py "
                f"--snapshot-rows {args.snapshot_rows}"
                + (" --json" if args.json else "")
            ),
            "parameters": {
                "snapshot_rows": args.snapshot_rows,
                "observation_cadence_seconds": OBSERVATION_CADENCE_SECONDS,
                "fixture_sql_batch_size": args.fixture_batch_size,
                "retention_days": args.retention_days,
                "retention_count_including_latest": args.retention_count,
                "maintenance_batch_size": args.maintenance_batch_size,
                "measurement_samples": args.samples,
                "warmup_operations": args.warmup,
            },
            "environment": {
                "python_version": platform.python_version(),
                "sqlite_version": sqlite3.sqlite_version,
                "operating_system": platform.system(),
                "platform_release": platform.release(),
                "platform": platform.platform(),
                "architecture": platform.machine(),
                "timer": "perf_counter_ns",
                "percentiles": "nearest_rank",
                "source_checkout_pythonpath": "src",
            },
            "fixture": {
                **fixture,
                "snapshot_rows": args.snapshot_rows,
                "distinct_content_fingerprints": fixture_distinct_content,
                "payload_bytes_fixed": fixture["payload_bytes_min"]
                == fixture["payload_bytes_max"],
                "private_temporary_directory": private_directory,
                "private_database_mode": db_private,
                "durable_turn_rows": 1,
                "durable_outbox_rows": 1,
            },
            "migration_v7_to_v9": {
                "source_schema_version": before_checks["schema_version"],
                "target_schema_version": migration_checks["schema_version"],
                "elapsed_ns": migration_ns,
                "rows_before": rows_before,
                "rows_after": rows_after_migration,
                "row_count_preserved": rows_before == rows_after_migration,
                "family_bytes_before": family_bytes_before,
                "family_bytes_after": family_bytes_after_migration,
                "checkpoint_before": checkpoint_before,
                "checkpoint_after": checkpoint_after_migration,
            },
            "latency_ns": {
                "current_schema_init": init_metric,
                "current_schema_open": open_metric,
                "latest_read": latest_metric,
                "unchanged_save": unchanged_metric,
                "changed_save": changed_metric,
                "retention_batch": _metric(retention_timings, warmup=args.warmup),
            },
            "row_counts": {
                "fixture_before_migration": rows_before,
                "after_migration": rows_after_migration,
                "before_unchanged_save": rows_before_unchanged,
                "after_unchanged_save": rows_after_unchanged,
                "unchanged_save_growth": rows_after_unchanged
                - rows_before_unchanged,
                "changed_save_growth": changed_growth,
                "before_retention": rows_before_retention,
                "after_retention": rows_after_retention,
            },
            "retention": retention,
            "retention_policy_state": retention_policy_state,
            "storage": {
                "family_bytes_before_migration": family_bytes_before,
                "family_bytes_after_migration": family_bytes_after_migration,
                "family_bytes_before_retention": family_bytes_before_retention,
                "family_bytes_after_retention": family_bytes_after_retention,
                "pages_before": pages_before,
                "pages_after": pages_after,
                "checkpoint_before_retention": checkpoint_before_retention,
                "checkpoint_after_retention": checkpoint_after_retention,
            },
            "work_evidence": {
                "current_schema_open_statements": statement_evidence,
                "query_plans": query_plans,
            },
            "checks": {
                "before": before_checks,
                "after_migration": migration_checks,
                "after_retention": after_checks,
                **invariant_checks,
            },
        }
    report["checks"]["temporary_artifacts_removed"] = bool(
        temporary_path is not None and not temporary_path.exists()
    )
    failed_invariants = sorted(
        name
        for name, passed in report["checks"].items()
        if isinstance(passed, bool) and not passed
    )
    if failed_invariants:
        raise RuntimeError(
            "benchmark_invariants_failed:" + ",".join(failed_invariants)
        )
    report["ok"] = True
    report["status"] = "completed"
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a deterministic synthetic Tendwire SQLite lifecycle benchmark."
    )
    parser.add_argument(
        "--snapshot-rows", type=int, choices=(500, 50_000), default=500
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the aggregate report as one compact JSON object.",
    )
    parser.add_argument("--samples", type=int, default=21)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--fixture-batch-size", type=int, default=1_000)
    parser.add_argument("--retention-days", type=int, default=14)
    parser.add_argument("--retention-count", type=int, default=4_096)
    parser.add_argument("--maintenance-batch-size", type=int, default=100)
    return parser


def main() -> int:
    benchmark_started = perf_counter_ns()
    args = _parser().parse_args()
    if (
        args.samples <= 0
        or args.warmup < 0
        or args.fixture_batch_size <= 0
        or args.retention_days <= 0
        or args.retention_count <= 0
        or args.maintenance_batch_size <= 0
    ):
        print(
            _canonical_json(
                {
                    "schema_version": REPORT_SCHEMA_VERSION,
                    "ok": False,
                    "status": "invalid_arguments",
                }
            )
        )
        return 2
    try:
        report = _benchmark(args)
        report["wall_time_ns"] = perf_counter_ns() - benchmark_started
    except Exception as exc:
        print(
            _canonical_json(
                {
                    "schema_version": REPORT_SCHEMA_VERSION,
                    "ok": False,
                    "status": "benchmark_failed",
                    "error_type": type(exc).__name__,
                }
            )
        )
        return 1
    print(_canonical_json(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
