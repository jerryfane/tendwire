"""Stage 1 coverage for the observation-authoritative turn model."""

from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pytest

from tendwire.core.commands import (
    instruction_fingerprint,
    normalize_instruction_text,
    turn_submission_id,
)
from tendwire.config import Config
from tendwire.core.projector import project_from_raw
from tendwire.core.turns import Turn
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    TURN_SUBMISSION_STATE_TRANSITIONS,
    cancel_turn_submission,
    init_store,
    is_valid_turn_submission_state_transition,
    sweep_expired_turn_submissions,
    turn_delta_payload_from_store,
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


def _assert_empty_v20_ledgers(conn: sqlite3.Connection) -> None:
    assert conn.execute("PRAGMA user_version").fetchone() == (20,)
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


def test_fresh_v20_store_creates_empty_turn_ledgers_and_all_indexes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fresh-v20.db"
    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _assert_empty_v20_ledgers(conn)


def test_v18_to_v20_migration_matches_fresh_schema(tmp_path: Path) -> None:
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
        store_sqlite._run_migrations(upgrade, target_version=20)
        _assert_empty_v20_ledgers(upgrade)
        assert _ledger_schema(upgrade) == fresh_schema


@pytest.mark.parametrize("source_version", range(store_sqlite.STORE_SCHEMA_VERSION))
def test_every_prior_schema_upgrades_to_identical_empty_v20_ledgers(
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
        _assert_empty_v20_ledgers(upgrade)
        assert _ledger_schema(upgrade) == fresh_schema


def _insert_historical_send_receipt(
    conn: sqlite3.Connection,
    *,
    request_id: str,
    state: str,
    status: str,
    instruction_text: str,
) -> None:
    active = state in {"reserved", "send_started"}
    sent = state != "reserved"
    terminal = state in {"accepted", "rejected", "uncertain"}
    canonical = json.dumps(
        {
            "canonical_version": 1,
            "action": "send_instruction",
            "target": {"worker_id": "worker-a"},
            "instruction": {"text": instruction_text},
            "options": {},
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    conn.execute(
        """
        INSERT INTO command_receipts (
            host_id, request_id, action, canonical_version,
            canonical_fingerprint, canonical_request_json, public_worker_id,
            state, status, result_json, owner_token_hash, owner_expires_at,
            binding_fingerprint, created_at, reserved_at, send_started_at,
            terminal_at, updated_at, legacy_collision,
            legacy_collision_count
        ) VALUES (
            'host-a', ?, 'send_instruction', 1, ?, ?, 'worker-a', ?, ?, '{}',
            ?, ?, NULL, '2026-01-01T00:00:00+00:00',
            '2026-01-01T00:00:01+00:00', ?, ?, ?, 0, 0
        )
        """,
        (
            request_id,
            f"canonical:{request_id}",
            canonical,
            state,
            status,
            "owner-token-hash" if active else "",
            "2026-01-01T00:01:00+00:00" if active else None,
            "2026-01-01T00:00:02+00:00" if sent else None,
            "2026-01-01T00:00:03+00:00" if terminal else None,
            (
                "2026-01-01T00:00:03+00:00"
                if terminal
                else "2026-01-01T00:00:02+00:00"
            ),
        ),
    )


@pytest.mark.parametrize(
    ("receipt_state", "receipt_status", "expected"),
    (
        (None, None, None),
        ({"state": "accepted"}, [], None),
        ("unknown", "accepted", None),
        ("reserved", "pending", None),
        ("send_started", "pending", "send_started"),
        ("accepted", "accepted", "submitted"),
        ("uncertain", "request_state_uncertain", "uncertain"),
        ("rejected", "cancelled", "cancelled"),
        ("accepted", "purged", None),
    ),
)
def test_backfill_submission_state_fails_closed_for_malformed_receipt_values(
    receipt_state: object,
    receipt_status: object,
    expected: str | None,
) -> None:
    assert (
        store_sqlite._backfill_submission_state(receipt_state, receipt_status)
        == expected
    )


@pytest.mark.parametrize(
    "canonical_request_json",
    (
        None,
        "not-json",
        json.dumps([]),
        json.dumps({"action": "observe", "instruction": {"text": "hello"}}),
        json.dumps({"action": "send_instruction"}),
        json.dumps({"action": "send_instruction", "instruction": []}),
        json.dumps({"action": "send_instruction", "instruction": {}}),
        json.dumps(
            {"action": "send_instruction", "instruction": {"text": 42}}
        ),
    ),
)
def test_receipt_instruction_text_rejects_malformed_receipt_shapes(
    canonical_request_json: object,
) -> None:
    assert store_sqlite._receipt_instruction_text(canonical_request_json) is None


def test_receipt_instruction_text_accepts_valid_send_instruction() -> None:
    canonical_request_json = json.dumps(
        {
            "action": "send_instruction",
            "instruction": {"text": "ship the release"},
        }
    )

    assert (
        store_sqlite._receipt_instruction_text(canonical_request_json)
        == "ship the release"
    )


def test_v20_backfill_does_not_alias_live_adopted_command_turn(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "live-adopted-command.db"
    owner_key = _seed_link_worker(db_path)
    adopted = store_sqlite.upsert_command_pending_turn(
        db_path,
        "host-a",
        {
            "id": "worker-a",
            "meta": {"stable_key": owner_key, "stable_key_version": 1},
        },
        request_id="adopted-live",
        instruction_text="adopt this prompt",
        observed_at="2099-01-01T00:00:00+00:00",
    )
    assert adopted is not None
    adopted_turn_id = str(adopted["id"])
    assert store_sqlite.merge_turn_content(
        db_path,
        "host-a",
        "worker-a",
        {
            "source_turn_id": "source-observed-after-command",
            "user_text": "adopt this prompt",
            "assistant_final_text": "adopted answer",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2099-01-01T00:00:01+00:00",
        turn_model="legacy",
    ) == 1

    with sqlite3.connect(str(db_path)) as conn:
        live_payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
                ("host-a", adopted_turn_id),
            ).fetchone()[0]
        )
        assert live_payload["source"] == "command"
        assert str(live_payload.get("source_turn_id") or "").strip()
        assert str(Turn.from_dict(live_payload).id) != adopted_turn_id
        assert live_payload.get("superseded_by_turn_id") is None

        conn.execute("DELETE FROM turn_supersessions")
        conn.execute("PRAGMA user_version = 19")
        conn.commit()
        store_sqlite._run_migrations(conn)

        assert conn.execute(
            """
            SELECT 1 FROM turn_supersessions
            WHERE host_id = 'host-a' AND superseded_turn_id = ?
            """,
            (adopted_turn_id,),
        ).fetchone() is None
        assert conn.execute(
            """
            SELECT supersession.host_id, supersession.canonical_turn_id
            FROM turn_supersessions AS supersession
            LEFT JOIN turns AS canonical
              ON canonical.host_id = supersession.host_id
             AND canonical.turn_id = supersession.canonical_turn_id
            WHERE canonical.turn_id IS NULL
            """
        ).fetchall() == []


@pytest.mark.parametrize("source_version", (18, 19))
def test_v20_backfill_production_shape_is_lossless_idempotent_and_fail_closed(
    tmp_path: Path,
    source_version: int,
) -> None:
    db_path = tmp_path / f"phase2-backfill-v{source_version}.db"
    owner_key = _seed_link_worker(db_path)
    assert store_sqlite.merge_turn_content(
        db_path,
        "host-a",
        "worker-a",
        {
            "source_turn_id": "observed-source",
            "user_text": "duplicate prompt",
            "assistant_final_text": "observed answer",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2099-01-01T00:00:00+00:00",
        turn_model="dual",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        observed_turn_id = str(
            conn.execute(
                """
                SELECT turn_id FROM turns
                WHERE host_id = 'host-a'
                  AND json_extract(payload_json, '$.source_turn_id') IS NOT NULL
                  AND json_extract(payload_json, '$.source') != 'command'
                """
            ).fetchone()[0]
        )

    duplicate = store_sqlite.upsert_command_pending_turn(
        db_path,
        "host-a",
        {"id": "worker-a", "meta": {"stable_key": owner_key, "stable_key_version": 1}},
        request_id="duplicate-claim",
        instruction_text="temporary duplicate prompt",
        observed_at="2099-01-01T00:00:01+00:00",
    )
    indeterminate = store_sqlite.upsert_command_pending_turn(
        db_path,
        "host-a",
        {"id": "worker-a", "meta": {"stable_key": owner_key, "stable_key_version": 1}},
        request_id="indeterminate-claim",
        instruction_text="ambiguous prompt",
        observed_at="2099-01-01T00:00:02+00:00",
    )
    adopted = store_sqlite.upsert_command_pending_turn(
        db_path,
        "host-a",
        {"id": "worker-a", "meta": {"stable_key": owner_key, "stable_key_version": 1}},
        request_id="adopted-claim",
        instruction_text="adopted prompt",
        observed_at="2099-01-01T00:00:03+00:00",
    )
    assert duplicate is not None and indeterminate is not None and adopted is not None
    assert store_sqlite.merge_turn_content(
        db_path,
        "host-a",
        "worker-a",
        {
            "source_turn_id": "adopted-source",
            "user_text": "adopted prompt",
            "assistant_final_text": "adopted answer",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2099-01-01T00:00:04+00:00",
        turn_model="legacy",
    ) == 1

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET user_text = 'duplicate prompt'
            WHERE host_id = 'host-a' AND turn_id = ? AND is_current = 1
            """,
            (str(duplicate["id"]),),
        )
        assert store_sqlite._tombstone_turn_conn(
            conn,
            "host-a",
            str(duplicate["id"]),
            superseded_by_turn_id=observed_turn_id,
            superseded_at="2099-01-01T00:00:05+00:00",
        )
        assert store_sqlite._tombstone_turn_conn(
            conn,
            "host-a",
            str(indeterminate["id"]),
            superseded_by_turn_id=None,
            superseded_at="2099-01-01T00:00:06+00:00",
        )
        revision = str(
            conn.execute(
                """
                SELECT content_revision FROM turn_content_revisions
                WHERE host_id = 'host-a' AND turn_id = ? AND is_current = 1
                """,
                (observed_turn_id,),
            ).fetchone()[0]
        )
        outbox_id = int(
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, delivery_kind, turn_id,
                    content_revision, ordering_key, status, payload_json,
                    private_state_json, created_at, updated_at
                ) VALUES (
                    'host-a', 'migration-test', 'preserved-outbox', 'generic',
                    ?, ?, 'worker-a', 'dead_letter', '{}', '{}',
                    '2099-01-01T00:00:07+00:00',
                    '2099-01-01T00:00:07+00:00'
                ) RETURNING id
                """,
                (observed_turn_id, revision),
            ).fetchone()[0]
        )
        conn.execute(
            """
            INSERT INTO turn_presentation_plans (
                host_id, name, plan_token, turn_id, content_revision,
                source_outbox_id, presentation_version, generation,
                part_count, state, created_at
            ) VALUES (
                'host-a', 'migration-test', 'preserved-plan', ?, ?, ?,
                'v1', 1, 1, 'failed', '2099-01-01T00:00:07+00:00'
            )
            """,
            (observed_turn_id, revision, outbox_id),
        )
        for request_id, state, status, text in (
            ("reserved", "reserved", "pending", "reserved prompt"),
            ("send-started", "send_started", "pending", "send prompt"),
            ("accepted", "accepted", "accepted", "  accepted   prompt  "),
            ("uncertain", "uncertain", "request_state_uncertain", "uncertain prompt"),
            ("rejected", "rejected", "cancelled", "rejected prompt"),
            ("purged", "rejected", "purged", "purged prompt"),
            ("accepted-live", "accepted", "accepted", "live prompt"),
        ):
            _insert_historical_send_receipt(
                conn,
                request_id=request_id,
                state=state,
                status=status,
                instruction_text=text,
            )
        conn.execute("DELETE FROM turn_submissions")
        conn.execute("DELETE FROM turn_supersessions")
        if source_version == 19:
            conn.execute(
                """
                INSERT INTO turn_submissions (
                    host_id, submission_id, request_id, owner_key,
                    owner_key_version, instruction_fingerprint, state,
                    linked_turn_id, link_not_before, link_expires_at,
                    hard_expires_at, linked_at, terminal_at, submitted_at,
                    send_started_at, updated_at
                ) VALUES (
                    'host-a', ?, 'accepted-live', ?, 1, ?, 'linked', ?,
                    '2025-12-31T23:59:02+00:00',
                    '2026-01-01T00:01:02+00:00',
                    '2026-01-02T00:00:02+00:00',
                    '2026-01-01T00:00:04+00:00',
                    '2026-01-01T00:00:04+00:00',
                    '2026-01-01T00:00:03+00:00',
                    '2026-01-01T00:00:02+00:00',
                    '2026-01-01T00:00:04+00:00'
                )
                """,
                (
                    turn_submission_id("host-a", "accepted-live"),
                    owner_key,
                    instruction_fingerprint("live prompt"),
                    observed_turn_id,
                ),
            )
        else:
            conn.execute("DROP TABLE turn_submissions")
            conn.execute("DROP TABLE turn_supersessions")
        conn.execute(f"PRAGMA user_version = {source_version}")
        preserved_tables = (
            "turns",
            "turn_content_revisions",
            "connector_outbox",
            "turn_presentation_plans",
        )
        before = {
            table: conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            for table in preserved_tables
        }
        conn.commit()

        store_sqlite._run_migrations(conn)

        after = {
            table: conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            for table in preserved_tables
        }
        assert after == before
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        rows = conn.execute(
            """
            SELECT request_id, owner_key, owner_key_version,
                   instruction_fingerprint, state, linked_turn_id,
                   link_not_before, link_expires_at, hard_expires_at,
                   terminal_at, submitted_at, send_started_at, updated_at
            FROM turn_submissions ORDER BY request_id
            """
        ).fetchall()
        by_request = {str(row[0]): row for row in rows}
        assert set(by_request) == {
            "accepted",
            "accepted-live",
            "rejected",
            "send-started",
            "uncertain",
        }
        assert by_request["accepted"][1:6] == (
            "legacy-worker:worker-a",
            0,
            instruction_fingerprint("accepted prompt"),
            "submitted",
            None,
        )
        assert by_request["rejected"][4:6] == ("cancelled", None)
        assert by_request["uncertain"][4:6] == ("uncertain", None)
        assert by_request["send-started"][4:6] == ("send_started", None)
        assert by_request["accepted"][6:] == (
            "2025-12-31T23:59:02+00:00",
            "2026-01-01T00:01:02+00:00",
            "2026-01-02T00:00:02+00:00",
            "2026-01-01T00:00:03+00:00",
            "2026-01-01T00:00:03+00:00",
            "2026-01-01T00:00:02+00:00",
            "2026-01-01T00:00:03+00:00",
        )
        if source_version == 19:
            assert by_request["accepted-live"][1:6] == (
                owner_key,
                1,
                instruction_fingerprint("live prompt"),
                "linked",
                observed_turn_id,
            )
        else:
            assert by_request["accepted-live"][4:6] == ("submitted", None)

        aliases = conn.execute(
            """
            SELECT superseded_turn_id, canonical_turn_id
            FROM turn_supersessions ORDER BY superseded_turn_id
            """
        ).fetchall()
        assert aliases == [(str(duplicate["id"]), observed_turn_id)]
        assert str(adopted["id"]) not in {row[0] for row in aliases}
        assert str(indeterminate["id"]) not in {row[0] for row in aliases}
        assert conn.execute(
            """
            SELECT supersession.host_id, supersession.canonical_turn_id
            FROM turn_supersessions AS supersession
            LEFT JOIN turns AS canonical
              ON canonical.host_id = supersession.host_id
             AND canonical.turn_id = supersession.canonical_turn_id
            WHERE canonical.turn_id IS NULL
            """
        ).fetchall() == []

        ledger_before_rerun = (
            conn.execute("SELECT * FROM turn_submissions ORDER BY request_id").fetchall(),
            conn.execute(
                "SELECT * FROM turn_supersessions ORDER BY superseded_turn_id"
            ).fetchall(),
        )
        conn.execute("PRAGMA user_version = 19")
        store_sqlite._run_migrations(conn)
        ledger_after_rerun = (
            conn.execute("SELECT * FROM turn_submissions ORDER BY request_id").fetchall(),
            conn.execute(
                "SELECT * FROM turn_supersessions ORDER BY superseded_turn_id"
            ).fetchall(),
        )
        assert ledger_after_rerun == ledger_before_rerun
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_turn_submission_state_transition_table() -> None:
    assert TURN_SUBMISSION_STATE_TRANSITIONS == {
        "send_started": frozenset(
            {
                "submitted",
                "uncertain",
                "linked",
                "ambiguous",
                "expired",
                "cancelled",
            }
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


def _seed_link_worker(
    db_path: Path,
    *,
    host_id: str = "host-a",
    worker_id: str = "worker-a",
    owner_char: str = "a",
) -> str:
    owner_key = "wsk1_" + (owner_char * 64)
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": worker_id,
                "name": worker_id,
                "status": "active",
                "meta": {
                    "stable_key": owner_key,
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    store_sqlite.save_snapshot(db_path, snapshot)
    return owner_key


def _insert_link_submission(
    db_path: Path,
    *,
    request_id: str,
    owner_key: str,
    instruction_text: str = "hello",
    host_id: str = "host-a",
    state: str = "submitted",
    link_not_before: str = "2026-02-01T11:59:00+00:00",
    link_expires_at: str = "2026-02-01T12:00:00+00:00",
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO turn_submissions (
                host_id, submission_id, request_id, owner_key,
                owner_key_version, instruction_fingerprint, state,
                linked_turn_id, link_not_before, link_expires_at,
                hard_expires_at, linked_at, terminal_at, submitted_at,
                send_started_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, 1, ?, ?, NULL, ?, ?,
                '2026-02-02T12:00:00+00:00', NULL, NULL,
                '2026-02-01T12:00:00+00:00',
                '2026-02-01T12:00:00+00:00',
                '2026-02-01T12:00:00+00:00'
            )
            """,
            (
                host_id,
                turn_submission_id(host_id, request_id),
                request_id,
                owner_key,
                instruction_fingerprint(instruction_text),
                state,
                link_not_before,
                link_expires_at,
            ),
        )


def _set_link_worker_prod_shape(
    db_path: Path,
    *,
    worker_id: str = "worker-a",
    host_id: str = "host-a",
    explicit_null: bool = False,
) -> None:
    version_update = (
        "json_set(payload_json, '$.meta.stable_key_version', NULL)"
        if explicit_null
        else "json_remove(payload_json, '$.meta.stable_key_version')"
    )
    with sqlite3.connect(str(db_path)) as conn:
        updated = conn.execute(
            f"""
            UPDATE workers
            SET payload_json = {version_update}
            WHERE host_id = ? AND worker_id = ?
            """,
            (host_id, worker_id),
        )
        assert updated.rowcount == 1


def _observe_link_turn(
    db_path: Path,
    *,
    source_turn_id: str,
    worker_id: str = "worker-a",
    host_id: str = "host-a",
    instruction_text: str = "hello",
    observed_at: str = "2026-02-01T12:00:00+00:00",
    turn_model: str = "dual",
) -> str:
    result = store_sqlite.apply_turn_refresh(
        db_path,
        host_id,
        worker_id,
        {
            "source_turn_id": source_turn_id,
            "user_text": instruction_text,
            "assistant_final_text": f"answer for {source_turn_id}",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at=observed_at,
        turn_model=turn_model,
    )
    assert result.updated in {0, 1}
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = ?
              AND COALESCE(json_extract(payload_json, '$.source_turn_id'), '') != ''
            ORDER BY turn_id
            """,
            (host_id,),
        ).fetchall()
    matching = [
        (str(turn_id), store_sqlite._json_object(payload_json))
        for turn_id, payload_json in rows
        if store_sqlite._json_object(payload_json).get("source_turn_id")
        in store_sqlite.turn_source_id_candidates(
            source_turn_id,
            meta=store_sqlite._json_object(payload_json).get("meta") or {},
            source=store_sqlite._json_object(payload_json).get("source"),
            kind=store_sqlite._json_object(payload_json).get("kind"),
        )
    ]
    assert len(matching) == 1
    turn_id, payload = matching[0]
    assert Turn.from_dict(payload).id == turn_id
    return turn_id


def _submission_rows(db_path: Path) -> list[tuple[str, str, str | None]]:
    with sqlite3.connect(str(db_path)) as conn:
        return [
            (str(row[0]), str(row[1]), None if row[2] is None else str(row[2]))
            for row in conn.execute(
                """
                SELECT request_id, state, linked_turn_id
                FROM turn_submissions ORDER BY request_id
                """
            ).fetchall()
        ]


def test_shadow_linker_handles_both_race_directions_without_changing_turn_id(
    tmp_path: Path,
) -> None:
    submission_first = tmp_path / "submission-first.db"
    owner_key = _seed_link_worker(submission_first)
    _insert_link_submission(
        submission_first,
        request_id="submission-first",
        owner_key=owner_key,
    )
    first_turn_id = _observe_link_turn(
        submission_first,
        source_turn_id="submission-first-source",
    )
    assert _submission_rows(submission_first) == [
        ("submission-first", "linked", first_turn_id)
    ]

    observation_first = tmp_path / "observation-first.db"
    owner_key = _seed_link_worker(observation_first)
    observed_turn_id = _observe_link_turn(
        observation_first,
        source_turn_id="observation-first-source",
    )
    _insert_link_submission(
        observation_first,
        request_id="observation-first",
        owner_key=owner_key,
    )
    # Through Stage 5, observation-first settlement is opportunistic: the
    # submission stays open until this worker produces another refresh. Stage 6
    # must add a lazy or periodic sweep before linked_turn_id gains authority.
    assert _submission_rows(observation_first) == [
        ("observation-first", "submitted", None)
    ]
    refreshed_turn_id = _observe_link_turn(
        observation_first,
        source_turn_id="observation-first-source",
        observed_at="2026-02-01T12:00:01+00:00",
    )
    assert refreshed_turn_id == observed_turn_id
    assert _submission_rows(observation_first) == [
        ("observation-first", "linked", observed_turn_id)
    ]


@pytest.mark.parametrize(
    ("order", "explicit_null"),
    (("submission-first", True), ("observation-first", False)),
)
def test_observed_linker_accepts_prod_shape_turn_owner_version(
    tmp_path: Path,
    order: str,
    explicit_null: bool,
) -> None:
    db_path = tmp_path / f"prod-shape-{order}.db"
    owner_key = _seed_link_worker(db_path)
    assert store_sqlite._turn_submission_owner_identity(
        {
            "id": "worker-a",
            "meta": {"stable_key": owner_key, "stable_key_version": 1},
        }
    ) == (owner_key, 1)
    _set_link_worker_prod_shape(db_path, explicit_null=explicit_null)

    if order == "submission-first":
        _insert_link_submission(
            db_path,
            request_id=order,
            owner_key=owner_key,
        )
    observed_turn_id = _observe_link_turn(
        db_path,
        source_turn_id=f"prod-shape-{order}-source",
        turn_model="observed",
    )
    if order == "observation-first":
        _insert_link_submission(
            db_path,
            request_id=order,
            owner_key=owner_key,
        )

    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:01+00:00",
    )

    assert _submission_rows(db_path) == [(order, "linked", observed_turn_id)]
    with sqlite3.connect(str(db_path)) as conn:
        turn_shape = conn.execute(
            """
            SELECT json_extract(turns.payload_json, '$.meta.stable_key'),
                   json_extract(turns.payload_json, '$.meta.stable_key_version'),
                   json_extract(turns.payload_json, '$.stable_key_version'),
                   json_extract(turns.payload_json, '$.user_text'),
                   revisions.user_text
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = 'host-a' AND turns.turn_id = ?
            """,
            (observed_turn_id,),
        ).fetchone()
        turn_payload = store_sqlite._json_object(
            conn.execute(
                """
                SELECT payload_json FROM turns
                WHERE host_id = 'host-a' AND turn_id = ?
                """,
                (observed_turn_id,),
            ).fetchone()[0]
        )
    assert turn_shape == (owner_key, None, None, None, "hello")
    turn_worker = {"id": "worker-a", "meta": turn_payload.get("meta")}
    assert store_sqlite._turn_submission_owner_identity(turn_worker) == (
        "legacy-worker:worker-a",
        0,
    )
    assert store_sqlite._turn_link_candidate_owner_identity(turn_worker) == (
        owner_key,
        1,
    )


@pytest.mark.parametrize(
    ("submission_count", "observation_count"),
    ((2, 1), (1, 2), (2, 2)),
)
def test_observed_linker_prod_shape_turns_still_fail_closed_on_ambiguity(
    tmp_path: Path,
    submission_count: int,
    observation_count: int,
) -> None:
    db_path = tmp_path / (
        f"prod-shape-ambiguous-{submission_count}-{observation_count}.db"
    )
    owner_key = _seed_link_worker(db_path)
    _set_link_worker_prod_shape(db_path)
    for index in range(submission_count):
        _insert_link_submission(
            db_path,
            request_id=f"prod-shape-{index}",
            owner_key=owner_key,
        )

    for index in range(observation_count):
        _observe_link_turn(
            db_path,
            source_turn_id=f"prod-shape-ambiguous-source-{index}",
            turn_model="observed",
        )
    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:01+00:00",
    )

    rows = _submission_rows(db_path)
    assert [state for _request, state, _turn in rows] == [
        "ambiguous"
    ] * submission_count
    assert all(linked_turn_id is None for _request, _state, linked_turn_id in rows)


def test_observed_linker_prod_shape_turns_keep_owner_hash_isolated(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prod-shape-owner-isolation.db"
    first_owner = _seed_link_worker(db_path)
    second_owner = "wsk1_" + ("b" * 64)
    snapshot = project_from_raw(
        Config(host_id="host-a", db_path=db_path),
        workers=[
            {
                "id": "worker-a",
                "name": "worker-a",
                "status": "active",
                "meta": {
                    "stable_key": first_owner,
                    "stable_key_version": 1,
                },
            },
            {
                "id": "worker-b",
                "name": "worker-b",
                "status": "active",
                "meta": {
                    "stable_key": second_owner,
                    "stable_key_version": 1,
                },
            },
        ],
    )
    store_sqlite.save_snapshot(db_path, snapshot)
    _set_link_worker_prod_shape(db_path, worker_id="worker-a")
    _set_link_worker_prod_shape(db_path, worker_id="worker-b", explicit_null=True)
    _insert_link_submission(
        db_path,
        request_id="first-owner",
        owner_key=first_owner,
    )

    _observe_link_turn(
        db_path,
        worker_id="worker-b",
        source_turn_id="wrong-owner-source",
        turn_model="observed",
    )
    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:01+00:00",
    )
    assert _submission_rows(db_path) == [("first-owner", "submitted", None)]

    matching_turn_id = _observe_link_turn(
        db_path,
        worker_id="worker-a",
        source_turn_id="first-owner-source",
        turn_model="observed",
    )
    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:01+00:00",
    )
    assert _submission_rows(db_path) == [
        ("first-owner", "linked", matching_turn_id)
    ]


def test_shadow_linker_failure_keeps_observation_and_rolls_back_link_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "settle-failure.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="settle-failure",
        owner_key=owner_key,
    )

    def fail_after_partial_settlement(
        conn: sqlite3.Connection,
        *_args: object,
        **_kwargs: object,
    ) -> int:
        conn.execute(
            """
            UPDATE turn_submissions
            SET state = 'ambiguous'
            WHERE host_id = 'host-a' AND request_id = 'settle-failure'
            """
        )
        raise RuntimeError("injected settlement failure")

    monkeypatch.setattr(
        store_sqlite,
        "settle_submission_links_conn",
        fail_after_partial_settlement,
    )
    turn_id = _observe_link_turn(
        db_path,
        source_turn_id="settle-failure-source",
    )

    assert _submission_rows(db_path) == [
        ("settle-failure", "submitted", None)
    ]
    with sqlite3.connect(str(db_path)) as conn:
        observed = conn.execute(
            """
            SELECT json_extract(turns.payload_json, '$.source_turn_id'),
                   revisions.user_text, revisions.assistant_final_text
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = 'host-a' AND turns.turn_id = ?
            """,
            (turn_id,),
        ).fetchone()
        assert observed is not None
        assert str(observed[0])
        assert tuple(observed[1:]) == (
            "hello",
            "answer for settle-failure-source",
        )


def test_shadow_linker_settles_send_started_to_linked_and_ambiguous(
    tmp_path: Path,
) -> None:
    linked_path = tmp_path / "send-started-linked.db"
    owner_key = _seed_link_worker(linked_path)
    _insert_link_submission(
        linked_path,
        request_id="send-started-linked",
        owner_key=owner_key,
        state="send_started",
    )
    turn_id = _observe_link_turn(
        linked_path,
        source_turn_id="send-started-linked-source",
    )
    assert _submission_rows(linked_path) == [
        ("send-started-linked", "linked", turn_id)
    ]

    ambiguous_path = tmp_path / "send-started-ambiguous.db"
    owner_key = _seed_link_worker(ambiguous_path)
    for index in range(2):
        _insert_link_submission(
            ambiguous_path,
            request_id=f"send-started-ambiguous-{index}",
            owner_key=owner_key,
            state="send_started",
        )
    _observe_link_turn(
        ambiguous_path,
        source_turn_id="send-started-ambiguous-source",
    )
    assert _submission_rows(ambiguous_path) == [
        ("send-started-ambiguous-0", "ambiguous", None),
        ("send-started-ambiguous-1", "ambiguous", None),
    ]


def test_shadow_linker_waits_for_window_close_before_failing_closed(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "delayed.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="delayed",
        owner_key=owner_key,
        link_expires_at="2026-02-01T12:01:00+00:00",
    )
    _observe_link_turn(db_path, source_turn_id="delayed-source-a")
    assert _submission_rows(db_path) == [("delayed", "submitted", None)]
    _observe_link_turn(
        db_path,
        source_turn_id="delayed-source-b",
        observed_at="2026-02-01T12:00:30+00:00",
    )
    assert _submission_rows(db_path) == [("delayed", "submitted", None)]
    _observe_link_turn(
        db_path,
        source_turn_id="delayed-source-a",
        observed_at="2026-02-01T12:01:00+00:00",
    )
    assert _submission_rows(db_path) == [("delayed", "ambiguous", None)]


@pytest.mark.parametrize(
    ("submission_count", "observation_count"),
    ((2, 1), (1, 2), (2, 2)),
)
def test_shadow_linker_marks_larger_identical_components_ambiguous(
    tmp_path: Path,
    submission_count: int,
    observation_count: int,
) -> None:
    db_path = tmp_path / f"ambiguous-{submission_count}-{observation_count}.db"
    owner_key = _seed_link_worker(db_path)
    for index in range(observation_count):
        _observe_link_turn(
            db_path,
            source_turn_id=f"ambiguous-source-{index}",
        )
    for index in range(submission_count):
        _insert_link_submission(
            db_path,
            request_id=f"ambiguous-request-{index}",
            owner_key=owner_key,
        )
    _observe_link_turn(
        db_path,
        source_turn_id="ambiguous-source-0",
        observed_at="2026-02-01T12:00:01+00:00",
    )
    rows = _submission_rows(db_path)
    assert [state for _request, state, _turn in rows] == [
        "ambiguous"
    ] * submission_count
    assert all(linked_turn_id is None for _request, _state, linked_turn_id in rows)


def test_shadow_linker_isolates_owners_and_legacy_mode_is_a_noop(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "owners.db"
    first_owner = _seed_link_worker(first_path)
    second_owner = "wsk1_" + ("b" * 64)
    snapshot = project_from_raw(
        Config(host_id="host-a", db_path=first_path),
        workers=[
            {
                "id": "worker-a",
                "name": "worker-a",
                "status": "active",
                "meta": {"stable_key": first_owner, "stable_key_version": 1},
            },
            {
                "id": "worker-b",
                "name": "worker-b",
                "status": "active",
                "meta": {"stable_key": second_owner, "stable_key_version": 1},
            },
        ],
    )
    store_sqlite.save_snapshot(first_path, snapshot)
    _insert_link_submission(first_path, request_id="owner-a", owner_key=first_owner)
    _insert_link_submission(first_path, request_id="owner-b", owner_key=second_owner)
    first_turn_id = _observe_link_turn(
        first_path,
        source_turn_id="owner-a-source",
    )
    second_turn_id = _observe_link_turn(
        first_path,
        worker_id="worker-b",
        source_turn_id="owner-b-source",
    )
    assert _submission_rows(first_path) == [
        ("owner-a", "linked", first_turn_id),
        ("owner-b", "linked", second_turn_id),
    ]

    legacy_path = tmp_path / "legacy.db"
    owner_key = _seed_link_worker(legacy_path)
    _insert_link_submission(legacy_path, request_id="legacy", owner_key=owner_key)
    _observe_link_turn(
        legacy_path,
        source_turn_id="legacy-source",
        turn_model="legacy",
    )
    assert _submission_rows(legacy_path) == [("legacy", "submitted", None)]


def test_goal13_delta_is_unperturbed_when_observed_turn_links_later(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "delta.db"
    owner_key = _seed_link_worker(db_path)
    bootstrap = turn_delta_payload_from_store(db_path, "host-a")
    assert bootstrap["has_more"] is False
    checkpoint = str(bootstrap["checkpoint"])

    observed_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="delta-source",
    )
    observed_delta = turn_delta_payload_from_store(
        db_path,
        "host-a",
        watermark=checkpoint,
    )
    observed_changes = [
        change
        for change in observed_delta["changes"]
        if change["turn_id"] == observed_turn_id
    ]
    assert [(change["op"], change["turn_id"]) for change in observed_changes] == [
        ("upsert", observed_turn_id)
    ]

    _insert_link_submission(db_path, request_id="delta-request", owner_key=owner_key)
    assert _observe_link_turn(
        db_path,
        source_turn_id="delta-source",
        observed_at="2026-02-01T12:00:01+00:00",
    ) == observed_turn_id
    linked_delta = turn_delta_payload_from_store(
        db_path,
        "host-a",
        watermark=str(observed_delta["checkpoint"]),
    )
    assert not any(
        change["turn_id"] == observed_turn_id
        for change in linked_delta["changes"]
    )


def test_two_racing_refreshes_cannot_double_link_one_observation(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "race.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(db_path, request_id="race", owner_key=owner_key)

    def refresh() -> str:
        return _observe_link_turn(
            db_path,
            source_turn_id="race-source",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        turn_ids = list(pool.map(lambda _index: refresh(), range(2)))
    assert len(set(turn_ids)) == 1
    assert _submission_rows(db_path) == [("race", "linked", turn_ids[0])]
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*) FROM turn_submissions
            WHERE linked_turn_id = ?
            """,
            (turn_ids[0],),
        ).fetchone() == (1,)


def test_idle_observation_first_submission_links_from_lazy_turn_read(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "idle-observation-first.db"
    owner_key = _seed_link_worker(db_path)
    observed_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="idle-observation-source",
    )
    _insert_link_submission(
        db_path,
        request_id="idle-observation-first",
        owner_key=owner_key,
    )

    payload = store_sqlite.turns_payload_from_store(
        db_path,
        "host-a",
        now=datetime.fromisoformat(
            "2026-02-01T12:00:01+00:00"
        ).timestamp(),
    )

    assert payload["turns"]
    assert _submission_rows(db_path) == [
        ("idle-observation-first", "linked", observed_turn_id)
    ]


def test_turn_alias_resolves_public_content_and_final_root(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "alias-lookup.db"
    _seed_link_worker(db_path)
    canonical_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="alias-source",
    )
    legacy_turn_id = "turn-" + ("1" * 24)
    with sqlite3.connect(str(db_path)) as conn:
        revision = str(
            conn.execute(
                """
                SELECT content_revision FROM turn_content_revisions
                WHERE host_id = 'host-a' AND turn_id = ? AND is_current = 1
                """,
                (canonical_turn_id,),
            ).fetchone()[0]
        )
        conn.execute(
            """
            INSERT INTO turn_supersessions (
                host_id, superseded_turn_id, canonical_turn_id,
                reason, created_at
            ) VALUES ('host-a', ?, ?, 'phase1_migration', ?)
            """,
            (
                legacy_turn_id,
                canonical_turn_id,
                "2026-02-01T12:00:01+00:00",
            ),
        )

    legacy_page = store_sqlite.get_turn_content(
        db_path,
        "host-a",
        turn_id=legacy_turn_id,
        content_revision=revision,
        field="assistant_final_text",
    )
    assert legacy_page["status"] == "content_revision_not_found"

    page = store_sqlite.get_turn_content(
        db_path,
        "host-a",
        turn_id=legacy_turn_id,
        content_revision=revision,
        field="assistant_final_text",
        turn_model="observed",
    )
    assert page["turn_id"] == canonical_turn_id
    assert page["text"] == "answer for alias-source"

    leased = store_sqlite.poll_connector_outbox(
        db_path,
        "host-a",
        "turn-final",
        now="2026-02-01T12:00:02+00:00",
    )["items"][0]
    begun = store_sqlite.prepare_connector_plan_begin(
        db_path,
        "host-a",
        name="turn-final",
        turn_id=legacy_turn_id,
        content_revision=revision,
        presentation_version="alias-aware-v1",
        part_count=1,
        source_ref=leased["ref"],
        turn_model="observed",
        now="2026-02-01T12:00:03+00:00",
    )
    assert begun["ok"] is True
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT turn_id FROM turn_presentation_plans
            WHERE plan_token = ?
            """,
            (begun["plan_token"],),
        ).fetchone() == (canonical_turn_id,)
