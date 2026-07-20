"""Stage 1 coverage for the observation-authoritative turn model."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    TURN_SUBMISSION_STATE_TRANSITIONS,
    init_store,
    is_valid_turn_submission_state_transition,
)


_LEDGER_TABLES = ("turn_submissions", "turn_supersessions")


def _ledger_schema(conn: sqlite3.Connection) -> tuple[tuple[object, ...], ...]:
    placeholders = ", ".join("?" for _ in _LEDGER_TABLES)
    return tuple(
        conn.execute(
            f"""
            SELECT type, name, tbl_name, sql
            FROM sqlite_master
            WHERE tbl_name IN ({placeholders})
            ORDER BY type, name
            """,
            _LEDGER_TABLES,
        ).fetchall()
    )


def _assert_empty_v19_ledgers(conn: sqlite3.Connection) -> None:
    assert conn.execute("PRAGMA user_version").fetchone() == (19,)
    assert conn.execute("SELECT COUNT(*) FROM turn_submissions").fetchone() == (0,)
    assert conn.execute("SELECT COUNT(*) FROM turn_supersessions").fetchone() == (0,)

    submission_columns = tuple(
        row[1] for row in conn.execute("PRAGMA table_info(turn_submissions)")
    )
    assert submission_columns == (
        "host_id",
        "submission_id",
        "request_id",
        "owner_key",
        "owner_key_version",
        "instruction_fingerprint",
        "state",
        "linked_turn_id",
        "link_not_before",
        "link_expires_at",
        "hard_expires_at",
        "linked_at",
        "terminal_at",
        "submitted_at",
        "send_started_at",
        "updated_at",
    )
    supersession_columns = tuple(
        row[1] for row in conn.execute("PRAGMA table_info(turn_supersessions)")
    )
    assert supersession_columns == (
        "host_id",
        "superseded_turn_id",
        "canonical_turn_id",
        "reason",
        "created_at",
    )

    submission_indexes = {
        str(row[1]): tuple(
            str(column[2])
            for column in conn.execute(f"PRAGMA index_info({row[1]})").fetchall()
        )
        for row in conn.execute("PRAGMA index_list(turn_submissions)").fetchall()
    }
    assert submission_indexes["idx_turn_submissions_link_candidates"] == (
        "host_id",
        "owner_key",
        "instruction_fingerprint",
        "state",
    )
    assert submission_indexes["ux_turn_submissions_linked_turn"] == (
        "host_id",
        "linked_turn_id",
    )
    linked_turn_index = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name = 'ux_turn_submissions_linked_turn'"
    ).fetchone()[0]
    assert "WHERE linked_turn_id IS NOT NULL" in linked_turn_index
    assert conn.execute(
        """
        SELECT partial FROM pragma_index_list('turn_submissions')
        WHERE name = 'ux_turn_submissions_linked_turn'
        """
    ).fetchone() == (1,)
    assert submission_indexes["sqlite_autoindex_turn_submissions_1"] == (
        "host_id",
        "submission_id",
    )
    assert submission_indexes["sqlite_autoindex_turn_submissions_2"] == (
        "host_id",
        "request_id",
    )

    supersession_indexes = {
        str(row[1]): tuple(
            str(column[2])
            for column in conn.execute(f"PRAGMA index_info({row[1]})").fetchall()
        )
        for row in conn.execute("PRAGMA index_list(turn_supersessions)").fetchall()
    }
    assert supersession_indexes == {
        "idx_turn_supersessions_canonical": ("host_id", "canonical_turn_id"),
        "sqlite_autoindex_turn_supersessions_1": (
            "host_id",
            "superseded_turn_id",
        ),
    }


def test_fresh_v19_store_creates_empty_turn_ledgers_and_all_indexes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fresh-v19.db"
    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _assert_empty_v19_ledgers(conn)


def test_v18_to_v19_migration_matches_fresh_schema(tmp_path: Path) -> None:
    fresh_path = tmp_path / "fresh.db"
    upgrade_path = tmp_path / "upgrade.db"
    init_store(fresh_path)
    with sqlite3.connect(str(fresh_path)) as fresh:
        fresh_schema = _ledger_schema(fresh)

    with sqlite3.connect(str(upgrade_path)) as upgrade:
        store_sqlite._run_migrations(upgrade, target_version=18)
        assert not set(_LEDGER_TABLES) & {
            str(row[0])
            for row in upgrade.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        store_sqlite._run_migrations(upgrade, target_version=19)
        _assert_empty_v19_ledgers(upgrade)
        assert _ledger_schema(upgrade) == fresh_schema


@pytest.mark.parametrize("source_version", range(store_sqlite.STORE_SCHEMA_VERSION))
def test_every_prior_schema_upgrades_to_identical_empty_v19_ledgers(
    tmp_path: Path,
    source_version: int,
) -> None:
    fresh_path = tmp_path / f"fresh-{source_version}.db"
    upgrade_path = tmp_path / f"upgrade-{source_version}.db"
    init_store(fresh_path)
    with sqlite3.connect(str(fresh_path)) as fresh:
        fresh_schema = _ledger_schema(fresh)

    with sqlite3.connect(str(upgrade_path)) as upgrade:
        store_sqlite._run_migrations(upgrade, target_version=source_version)
        store_sqlite._run_migrations(upgrade)
        _assert_empty_v19_ledgers(upgrade)
        assert _ledger_schema(upgrade) == fresh_schema


def test_turn_submission_state_transition_table() -> None:
    assert TURN_SUBMISSION_STATE_TRANSITIONS == {
        "send_started": frozenset(
            {"submitted", "uncertain", "expired", "cancelled"}
        ),
        "submitted": frozenset({"linked", "ambiguous", "expired", "cancelled"}),
        "uncertain": frozenset({"linked", "ambiguous", "expired", "cancelled"}),
        "linked": frozenset(),
        "ambiguous": frozenset(),
        "expired": frozenset(),
        "cancelled": frozenset(),
    }

    for current_state, allowed_states in TURN_SUBMISSION_STATE_TRANSITIONS.items():
        for next_state in TURN_SUBMISSION_STATE_TRANSITIONS:
            assert is_valid_turn_submission_state_transition(
                current_state,
                next_state,
            ) is (next_state in allowed_states)

    assert not is_valid_turn_submission_state_transition("unknown", "submitted")
    assert not is_valid_turn_submission_state_transition("submitted", "unknown")
    assert not is_valid_turn_submission_state_transition(None, "submitted")
