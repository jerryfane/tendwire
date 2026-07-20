from __future__ import annotations

import sqlite3
from pathlib import Path
from time import perf_counter

from tendwire.core import models
from tendwire.core.models import Snapshot, Worker
from tendwire.store import sqlite as store_sqlite


HOST_ID = "snapshot-performance-host"


def _worker(index: int, *, status: str = "active") -> Worker:
    return Worker(
        id=f"worker-{index}",
        name=f"Worker {index}",
        status=status,
        meta={
            "stable_key": f"wsk1_{index:064x}",
            "stable_key_version": 1,
        },
    )


def _snapshot(second: int, workers: list[Worker]) -> Snapshot:
    return Snapshot(
        host_id=HOST_ID,
        updated_at=f"2026-07-20T00:00:{second:02d}+00:00",
        workers=workers,
    )


def _install_turn_update_counter(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE turn_update_counter (count INTEGER NOT NULL);
            INSERT INTO turn_update_counter VALUES (0);
            CREATE TRIGGER count_turn_updates
            AFTER UPDATE ON turns
            BEGIN
                UPDATE turn_update_counter SET count = count + 1;
            END;
            """
        )


def _turn_update_count(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        return int(conn.execute("SELECT count FROM turn_update_counter").fetchone()[0])


def test_unchanged_snapshot_decodes_and_sanitizes_no_retained_turns(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "unchanged.db"
    workers = [_worker(1), _worker(2)]
    store_sqlite.init_store(db_path)
    store_sqlite.save_snapshot(db_path, _snapshot(1, workers))
    _install_turn_update_counter(db_path)

    decoded_counts: list[int] = []
    sanitize_calls = 0
    original_decode = store_sqlite._decode_turn_content_rows
    original_sanitize = store_sqlite.sanitize_public_value

    def recording_decode(rows):
        materialized = list(rows)
        decoded_counts.append(len(materialized))
        return original_decode(materialized)

    def recording_sanitize(value, **kwargs):
        nonlocal sanitize_calls
        sanitize_calls += 1
        return original_sanitize(value, **kwargs)

    monkeypatch.setattr(store_sqlite, "_decode_turn_content_rows", recording_decode)
    monkeypatch.setattr(store_sqlite, "sanitize_public_value", recording_sanitize)
    store_sqlite.save_snapshot(db_path, _snapshot(2, workers))

    assert decoded_counts == []
    assert sanitize_calls == 0
    assert _turn_update_count(db_path) == 0


def test_changed_snapshot_decodes_only_the_changed_owned_turn(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "one-changed.db"
    workers = [_worker(1), _worker(2)]
    store_sqlite.init_store(db_path)
    store_sqlite.save_snapshot(db_path, _snapshot(1, workers))
    _install_turn_update_counter(db_path)

    decoded_counts: list[int] = []
    sanitize_calls = 0
    original_decode = store_sqlite._decode_turn_content_rows
    original_sanitize = store_sqlite.sanitize_public_value

    def recording_decode(rows):
        materialized = list(rows)
        decoded_counts.append(len(materialized))
        return original_decode(materialized)

    def recording_sanitize(value, **kwargs):
        nonlocal sanitize_calls
        sanitize_calls += 1
        return original_sanitize(value, **kwargs)

    monkeypatch.setattr(store_sqlite, "_decode_turn_content_rows", recording_decode)
    monkeypatch.setattr(store_sqlite, "sanitize_public_value", recording_sanitize)
    changed = [_worker(1), _worker(2, status="waiting")]
    store_sqlite.save_snapshot(db_path, _snapshot(2, changed))

    assert decoded_counts == [1]
    assert sanitize_calls == 1
    assert _turn_update_count(db_path) == 1


def test_public_text_sanitize_cache_hits_and_separates_configurations(monkeypatch) -> None:
    value = "Repeated public text " + ("a1b2c3d4" * 2048)
    models._clear_public_sanitize_cache()
    calls = 0
    original = models._redact_and_truncate_public_text

    def recording_redact(text: str, max_chars: int | None) -> str:
        nonlocal calls
        calls += 1
        return original(text, max_chars)

    monkeypatch.setattr(models, "_redact_and_truncate_public_text", recording_redact)
    first = models.sanitize_public_text(value)
    second = models.sanitize_public_text(value)
    truncated = models.sanitize_public_text(value, max_chars=128)

    assert second == first
    assert len(truncated) <= 128
    assert calls == 2


def test_forbidden_phrase_scan_scales_near_linearly_for_token_dense_text() -> None:
    def elapsed(size: int) -> float:
        value = ("a1b2c3d4_" * ((size + 8) // 9))[:size]
        started = perf_counter()
        for _ in range(200):
            assert not models._is_forbidden_public_text_phrase(value)
        return perf_counter() - started

    small = min(elapsed(5_000) for _ in range(3))
    large = min(elapsed(50_000) for _ in range(3))

    assert large <= small * 15 + 0.01
