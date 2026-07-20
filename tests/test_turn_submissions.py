"""Stage 1 coverage for the observation-authoritative turn model."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tendwire.core.commands import (
    instruction_fingerprint,
    normalize_instruction_text,
    turn_submission_id,
)
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    TURN_SUBMISSION_STATE_TRANSITIONS,
    cancel_turn_submission,
    init_store,
    is_valid_turn_submission_state_transition,
    sweep_expired_turn_submissions,
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


def test_instruction_fingerprint_is_normalized_deterministic_and_opaque() -> None:
    variants = (
        "  deploy   the build\nthen verify  ",
        "deploy the build\nthen   verify",
        "deploy\tthe build\nthen verify",
    )

    assert {normalize_instruction_text(value) for value in variants} == {
        "deploy the build\nthen verify"
    }
    fingerprints = {instruction_fingerprint(value) for value in variants}
    assert len(fingerprints) == 1
    fingerprint = fingerprints.pop()
    assert fingerprint.startswith("twins1.")
    assert "deploy" not in fingerprint
    assert instruction_fingerprint("deploy the other build") != fingerprint
    assert normalize_instruction_text(" \n\t ") == ""
    assert instruction_fingerprint(" \n\t ") != instruction_fingerprint("\n")

    first = turn_submission_id("host-a", "request-1")
    assert first == turn_submission_id("host-a", "request-1")
    assert first.startswith("twsub1.")
    assert turn_submission_id("host-b", "request-1") != first
    assert turn_submission_id("host-a", "request-2") != first


def _insert_submission(
    conn: sqlite3.Connection,
    *,
    request_id: str,
    state: str,
    hard_expires_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO turn_submissions (
            host_id, submission_id, request_id, owner_key,
            owner_key_version, instruction_fingerprint, state,
            linked_turn_id, link_not_before, link_expires_at,
            hard_expires_at, linked_at, terminal_at, submitted_at,
            send_started_at, updated_at
        ) VALUES (
            'host-a', ?, ?, 'owner-a', 1, ?, ?, NULL,
            '2026-01-01T00:00:00+00:00',
            '2026-01-01T00:02:00+00:00', ?, NULL, NULL,
            '2026-01-01T00:01:00+00:00',
            '2026-01-01T00:00:00+00:00',
            '2026-01-01T00:01:00+00:00'
        )
        """,
        (
            turn_submission_id("host-a", request_id),
            request_id,
            instruction_fingerprint("hello"),
            state,
            hard_expires_at,
        ),
    )


def test_submission_expiry_sweeper_expires_only_old_unlinked_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "expiry.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_submission(
            conn,
            request_id="old",
            state="submitted",
            hard_expires_at="2026-01-02T00:00:00+00:00",
        )
        _insert_submission(
            conn,
            request_id="future",
            state="uncertain",
            hard_expires_at="2026-03-01T00:00:00+00:00",
        )

    assert sweep_expired_turn_submissions(
        db_path,
        host_id="host-a",
        now="2026-02-01T00:00:00+00:00",
    ) == 1

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT request_id, state, terminal_at
            FROM turn_submissions ORDER BY request_id
            """
        ).fetchall()
    assert rows == [
        ("future", "uncertain", None),
        ("old", "expired", "2026-02-01T00:00:00+00:00"),
    ]


def test_submission_cancellation_is_terminal_and_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "cancel.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_submission(
            conn,
            request_id="cancel-me",
            state="send_started",
            hard_expires_at="2026-03-01T00:00:00+00:00",
        )

    assert cancel_turn_submission(
        db_path,
        host_id="host-a",
        request_id="cancel-me",
        now="2026-02-01T00:00:00+00:00",
    )
    assert not cancel_turn_submission(
        db_path,
        host_id="host-a",
        request_id="cancel-me",
        now="2026-02-01T00:00:01+00:00",
    )

    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT state, terminal_at FROM turn_submissions
            WHERE host_id = 'host-a' AND request_id = 'cancel-me'
            """
        ).fetchone() == ("cancelled", "2026-02-01T00:00:00+00:00")
