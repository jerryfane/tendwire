"""Behavioral coverage for the schema-v11 final-delivery migration."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from tendwire.connectors import ConnectorOutboxAPI
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import init_store


_HOST_ID = "host-migration"
_CREATED_AT = "2026-01-01T00:00:00+00:00"
_RAW_USER_MARKER = "private-user-chat_id-telegram"
_RAW_FINAL_MARKER = "private-final-bot_token-herdres"
_PRIVATE_ROUTE_MARKER = "private-route-topic_id"
_V11_OUTBOX_COLUMNS = {"delivery_kind", "turn_id", "content_revision"}
_STABLE_KEY = "wsk1_" + ("c" * 64)


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _assert_v10_shape(conn: sqlite3.Connection) -> None:
    assert int(conn.execute("PRAGMA user_version").fetchone()[0]) == 10
    assert _V11_OUTBOX_COLUMNS.isdisjoint(_columns(conn, "connector_outbox"))
    assert "source_outbox_id" not in _columns(conn, "turn_presentation_plans")


def _create_v10_store(db_path: Path) -> None:
    """Create the historical schema through migrations, then restore v10 table shapes.

    The migration registry creates every earlier schema programmatically. Current
    CREATE constants contain additive v11 columns, so the two affected empty
    tables are put back into their exact pre-v11 shape before fixture rows are
    inserted.
    """

    with store_sqlite._connect(
        db_path,
        prepare=True,
        isolation_level=None,
    ) as conn:
        store_sqlite._run_migrations(conn, target_version=10)

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("DROP TABLE turn_presentation_jobs")
        conn.execute("DROP TABLE turn_presentation_plans")
        for column in sorted(_V11_OUTBOX_COLUMNS):
            conn.execute(f"ALTER TABLE connector_outbox DROP COLUMN {column}")
        conn.executescript(
            """
            CREATE TABLE turn_presentation_plans (
                id INTEGER PRIMARY KEY,
                host_id TEXT NOT NULL,
                name TEXT NOT NULL,
                plan_token TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                content_revision TEXT NOT NULL,
                presentation_version TEXT NOT NULL,
                generation INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
                part_count INTEGER NOT NULL CHECK (part_count > 0),
                state TEXT NOT NULL CHECK (state IN (
                    'preparing',
                    'waiting_predecessor',
                    'active',
                    'completed',
                    'superseded',
                    'failed'
                )),
                replaces_plan_token TEXT,
                recovers_plan_token TEXT,
                created_at TEXT NOT NULL,
                activated_at TEXT,
                completed_at TEXT,
                UNIQUE (host_id, name, plan_token),
                UNIQUE (
                    host_id,
                    name,
                    turn_id,
                    content_revision,
                    presentation_version,
                    generation
                ),
                FOREIGN KEY (host_id, turn_id, content_revision)
                    REFERENCES turn_content_revisions(
                        host_id,
                        turn_id,
                        content_revision
                    ) ON DELETE RESTRICT
            );
            CREATE TABLE turn_presentation_jobs (
                id INTEGER PRIMARY KEY,
                plan_id INTEGER NOT NULL,
                sequence_index INTEGER NOT NULL CHECK (sequence_index >= 0),
                operation TEXT NOT NULL CHECK (operation IN ('upsert', 'retire')),
                part_ordinal INTEGER NOT NULL CHECK (part_ordinal >= 0),
                spans_json TEXT NOT NULL,
                outbox_id INTEGER UNIQUE,
                created_at TEXT NOT NULL,
                UNIQUE (plan_id, sequence_index),
                UNIQUE (plan_id, operation, part_ordinal),
                FOREIGN KEY (plan_id)
                    REFERENCES turn_presentation_plans(id) ON DELETE CASCADE,
                FOREIGN KEY (outbox_id)
                    REFERENCES connector_outbox(id) ON DELETE RESTRICT
            );
            CREATE INDEX idx_turn_presentation_jobs_plan_sequence
                ON turn_presentation_jobs(plan_id, sequence_index);
            CREATE INDEX idx_turn_presentation_jobs_outbox
                ON turn_presentation_jobs(outbox_id);
            PRAGMA user_version = 10;
            """
        )
        conn.execute("PRAGMA foreign_keys = ON")
        _assert_v10_shape(conn)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def _insert_current_final(
    conn: sqlite3.Connection,
    *,
    turn_id: str,
    user_text: str,
    final_text: str,
) -> str:
    revision = store_sqlite.content_revision(
        turn_id,
        user_text,
        final_text,
        "complete",
        "complete",
    )
    user_segments = store_sqlite.segment_canonical_text(user_text)
    final_segments = store_sqlite.segment_canonical_text(final_text)
    conn.execute(
        """
        INSERT INTO turns (
            host_id,
            turn_id,
            worker_id,
            worker_fingerprint,
            space_id,
            status,
            kind,
            updated_at,
            fingerprint,
            snapshot_content_fingerprint,
            observed_at,
            payload_json,
            list_sequence
        ) VALUES (
            ?, ?, ?, NULL, NULL, 'complete', 'turn', ?, ?, ?, ?, ?,
            (SELECT COALESCE(MAX(list_sequence), 0) + 1 FROM turns WHERE host_id = ?)
        )
        """,
        (
            _HOST_ID,
            turn_id,
            f"worker-{turn_id}",
            _CREATED_AT,
            f"fingerprint-{turn_id}",
            f"snapshot-{turn_id}",
            _CREATED_AT,
            json.dumps(
                {
                    "source_turn_id": f"source-{turn_id}",
                    "complete": True,
                    "meta": {
                        "stable_key": _STABLE_KEY,
                        "stable_key_version": 1,
                    },
                    "chat_id": _PRIVATE_ROUTE_MARKER,
                },
                sort_keys=True,
            ),
            _HOST_ID,
        ),
    )
    conn.execute(
        """
        INSERT INTO turn_content_revisions (
            host_id,
            turn_id,
            content_revision,
            user_text,
            assistant_final_text,
            user_state,
            final_state,
            user_char_length,
            user_byte_length,
            final_char_length,
            final_byte_length,
            user_page_count,
            final_page_count,
            is_current,
            created_at,
            superseded_at
        ) VALUES (?, ?, ?, ?, ?, 'complete', 'complete', ?, ?, ?, ?, ?, ?, 1, ?, NULL)
        """,
        (
            _HOST_ID,
            turn_id,
            revision,
            user_text,
            final_text,
            len(user_text),
            len(user_text.encode("utf-8")),
            len(final_text),
            len(final_text.encode("utf-8")),
            len(user_segments),
            len(final_segments),
            _CREATED_AT,
        ),
    )
    for field, segments in (
        ("user_text", user_segments),
        ("assistant_final_text", final_segments),
    ):
        conn.executemany(
            """
            INSERT INTO turn_content_page_boundaries (
                host_id,
                turn_id,
                content_revision,
                field,
                page_index,
                start_char,
                start_byte
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    _HOST_ID,
                    turn_id,
                    revision,
                    field,
                    int(segment.index),
                    int(segment.start_char),
                    int(segment.start_byte),
                )
                for segment in segments
            ),
        )
    return revision


def _seed_v10_finals(db_path: Path) -> dict[str, tuple[str, str]]:
    finals: dict[str, tuple[str, str]] = {}
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        for ordinal, label in enumerate(("delivered", "hold-a", "hold-b"), start=1):
            turn_id = f"turn-{ordinal:02d}-{label}"
            revision = _insert_current_final(
                conn,
                turn_id=turn_id,
                user_text=f"{_RAW_USER_MARKER}-{label}",
                final_text=f"{_RAW_FINAL_MARKER}-{label}",
            )
            finals[label] = (turn_id, revision)

        delivered_turn, delivered_revision = finals["delivered"]
        part_payload = {
            "schema_version": 1,
            "operation": "upsert",
            "sequence_index": 0,
            "spans": [
                {
                    "field": "user_text",
                    "start_char": 0,
                    "end_char": len(f"{_RAW_USER_MARKER}-delivered"),
                },
                {
                    "field": "assistant_final_text",
                    "start_char": 0,
                    "end_char": len(f"{_RAW_FINAL_MARKER}-delivered"),
                },
            ],
        }
        part_cursor = conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id,
                connector,
                delivery_key,
                status,
                payload_json,
                private_state_json,
                created_at,
                updated_at,
                next_attempt_at
            ) VALUES (?, 'turn-final', ?, 'delivered', ?, ?, ?, ?, NULL)
            """,
            (
                _HOST_ID,
                "turn-final:legacy-completed:000000",
                json.dumps(part_payload, sort_keys=True),
                json.dumps({"route": _PRIVATE_ROUTE_MARKER}, sort_keys=True),
                _CREATED_AT,
                _CREATED_AT,
            ),
        )
        part_outbox_id = int(part_cursor.lastrowid)
        plan_cursor = conn.execute(
            """
            INSERT INTO turn_presentation_plans (
                host_id,
                name,
                plan_token,
                turn_id,
                content_revision,
                presentation_version,
                generation,
                part_count,
                state,
                replaces_plan_token,
                recovers_plan_token,
                created_at,
                activated_at,
                completed_at
            ) VALUES (
                ?, 'turn-final', 'twplan1.legacy-completed', ?, ?,
                'turn-present-v10', 1, 1, 'completed', NULL, NULL, ?, ?, ?
            )
            """,
            (
                _HOST_ID,
                delivered_turn,
                delivered_revision,
                _CREATED_AT,
                _CREATED_AT,
                _CREATED_AT,
            ),
        )
        plan_id = int(plan_cursor.lastrowid)
        conn.execute(
            """
            INSERT INTO turn_presentation_jobs (
                plan_id,
                sequence_index,
                operation,
                part_ordinal,
                spans_json,
                outbox_id,
                created_at
            ) VALUES (?, 0, 'upsert', 0, ?, ?, ?)
            """,
            (
                plan_id,
                json.dumps(part_payload["spans"], sort_keys=True),
                part_outbox_id,
                _CREATED_AT,
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id,
                host_id,
                connector,
                delivery_key,
                attempt,
                status,
                response_json,
                private_state_json,
                created_at,
                delivered_at
            ) VALUES (?, ?, 'turn-final', ?, 1, 'delivered', '{}', '{}', ?, ?)
            """,
            (
                part_outbox_id,
                _HOST_ID,
                "turn-final:legacy-completed:000000",
                _CREATED_AT,
                _CREATED_AT,
            ),
        )
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    return finals


def _final_key(turn_id: str, revision: str) -> str:
    identity = store_sqlite.turn_final_delivery_identity(
        _HOST_ID,
        turn_id,
        revision,
    )
    return f"turn-final:revision:{identity}"


def _assert_no_private_or_raw(value: Any) -> None:
    encoded = json.dumps(value, sort_keys=True).lower()
    for forbidden in (
        _RAW_USER_MARKER,
        _RAW_FINAL_MARKER,
        _PRIVATE_ROUTE_MARKER,
        "chat_id",
        "topic_id",
        "bot_token",
        "telegram",
        "herdres",
        "private_state_json",
    ):
        assert forbidden.lower() not in encoded


def _anchor_rows(db_path: Path) -> list[tuple[Any, ...]]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT
                id,
                delivery_key,
                delivery_kind,
                turn_id,
                content_revision,
                status,
                payload_json,
                private_state_json,
                created_at,
                updated_at,
                next_attempt_at
            FROM connector_outbox
            WHERE connector = 'turn-final'
              AND delivery_kind IN ('final_ready', 'final_migration_hold')
            ORDER BY id
            """
        ).fetchall()


def _database_dump(db_path: Path) -> tuple[int, str]:
    with sqlite3.connect(str(db_path)) as conn:
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        return version, "\n".join(conn.iterdump())


def test_v10_to_v11_migration_retains_finals_without_reposting_or_leaking(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "retention-v10.db"
    _create_v10_store(db_path)
    finals = _seed_v10_finals(db_path)

    init_store(db_path)
    assert store_sqlite.STORE_SCHEMA_VERSION == 11

    delivered_key = _final_key(*finals["delivered"])
    hold_keys = {_final_key(*finals[label]) for label in ("hold-a", "hold-b")}
    rows_after_first_migration = _anchor_rows(db_path)
    by_key = {str(row[1]): row for row in rows_after_first_migration}
    assert set(by_key) == {delivered_key, *hold_keys}
    assert by_key[delivered_key][2:6] == (
        "final_ready",
        finals["delivered"][0],
        finals["delivered"][1],
        "delivered",
    )
    for label in ("hold-a", "hold-b"):
        key = _final_key(*finals[label])
        assert by_key[key][2:6] == (
            "final_migration_hold",
            finals[label][0],
            finals[label][1],
            "dead_letter",
        )
    for row in rows_after_first_migration:
        assert row[1].startswith("turn-final:revision:twfinal1.")
        root_payload = json.loads(str(row[6]))
        assert root_payload["schema_version"] == 2
        assert root_payload["stable_key"] == _STABLE_KEY
        assert root_payload["stable_key_version"] == 1
        assert "worker_fingerprint" not in root_payload
        _assert_no_private_or_raw(root_payload)

    with sqlite3.connect(str(db_path)) as conn:
        delivered_source_id = int(by_key[delivered_key][0])
        assert conn.execute(
            """
            SELECT state, source_outbox_id
            FROM turn_presentation_plans
            WHERE plan_token = 'twplan1.legacy-completed'
            """
        ).fetchone() == ("completed", delivered_source_id)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []

    init_store(db_path)
    assert _anchor_rows(db_path) == rows_after_first_migration

    api = ConnectorOutboxAPI(db_path, _HOST_ID)
    assert api.poll({"name": "turn-final", "limit": 100})["items"] == []
    inspected = api.inspect(
        {
            "schema_version": 1,
            "name": "turn-final",
            "status": "dead_letter",
            "limit": 100,
        }
    )
    assert inspected["ok"] is True
    assert inspected["total"] == 2
    assert {item["key"] for item in inspected["items"]} == hold_keys
    _assert_no_private_or_raw(inspected)

    with sqlite3.connect(str(db_path)) as conn:
        new_turn = "turn-04-new-ready"
        new_revision = _insert_current_final(
            conn,
            turn_id=new_turn,
            user_text=f"{_RAW_USER_MARKER}-new",
            final_text=f"{_RAW_FINAL_MARKER}-new",
        )
        new_anchor_id = store_sqlite._ensure_final_ready_anchor_conn(
            conn,
            host_id=_HOST_ID,
            turn_id=new_turn,
            content_revision_value=new_revision,
            now="2026-01-01T00:01:00+00:00",
        )
        assert new_anchor_id is not None
    new_key = _final_key(new_turn, new_revision)

    new_work = api.poll({"name": "turn-final", "limit": 100})
    assert [item["key"] for item in new_work["items"]] == [new_key]
    _assert_no_private_or_raw(new_work)
    assert delivered_key not in {item["key"] for item in new_work["items"]}

    selected_key = _final_key(*finals["hold-a"])
    untouched_key = _final_key(*finals["hold-b"])
    retried = api.retry(
        {
            "schema_version": 1,
            "name": "turn-final",
            "key": selected_key,
        }
    )
    assert retried["ok"] is True
    assert retried["status"] == "requeued"
    assert retried["key"] == selected_key
    _assert_no_private_or_raw(retried)

    with sqlite3.connect(str(db_path)) as conn:
        retry_states = dict(
            conn.execute(
                """
                SELECT delivery_key, delivery_kind || ':' || status
                FROM connector_outbox
                WHERE delivery_key IN (?, ?)
                """,
                (selected_key, untouched_key),
            ).fetchall()
        )
    assert retry_states == {
        selected_key: "final_ready:queued",
        untouched_key: "final_migration_hold:dead_letter",
    }

    after_retry_inspect = api.inspect(
        {
            "schema_version": 1,
            "name": "turn-final",
            "status": "dead_letter",
            "limit": 100,
        }
    )
    assert after_retry_inspect["total"] == 1
    assert [item["key"] for item in after_retry_inspect["items"]] == [untouched_key]
    _assert_no_private_or_raw(after_retry_inspect)

    released = api.poll({"name": "turn-final", "limit": 100})
    assert [item["key"] for item in released["items"]] == [selected_key]
    _assert_no_private_or_raw(released)
    assert delivered_key not in {item["key"] for item in released["items"]}


@pytest.mark.parametrize(
    "invalid_meta",
    [
        {},
        {"stable_key": _STABLE_KEY},
        {"stable_key": "wsk1_invalid", "stable_key_version": 1},
        {"stable_key": _STABLE_KEY, "stable_key_version": True},
        {"stable_key": _STABLE_KEY, "stable_key_version": 2},
    ],
)
def test_v10_to_v11_missing_stable_key_pair_becomes_nonroutable_hold(
    tmp_path: Path,
    invalid_meta: dict[str, object],
) -> None:
    db_path = tmp_path / "retention-v10-stable-key-hold.db"
    _create_v10_store(db_path)
    finals = _seed_v10_finals(db_path)
    turn_id, revision = finals["delivered"]
    with sqlite3.connect(str(db_path)) as conn:
        raw_payload = conn.execute(
            """
            SELECT payload_json
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (_HOST_ID, turn_id),
        ).fetchone()[0]
        turn_payload = json.loads(str(raw_payload))
        turn_payload["meta"] = invalid_meta
        conn.execute(
            """
            UPDATE turns
            SET payload_json = ?
            WHERE host_id = ? AND turn_id = ?
            """,
            (json.dumps(turn_payload, sort_keys=True), _HOST_ID, turn_id),
        )

    init_store(db_path)
    key = _final_key(turn_id, revision)
    row = {
        str(anchor[1]): anchor
        for anchor in _anchor_rows(db_path)
    }[key]
    payload = json.loads(str(row[6]))

    assert row[2:6] == (
        "final_migration_hold",
        turn_id,
        revision,
        "dead_letter",
    )
    assert payload["schema_version"] == 1
    assert "stable_key" not in payload
    assert "stable_key_version" not in payload
    assert "worker_fingerprint" not in payload
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT source_outbox_id
            FROM turn_presentation_plans
            WHERE plan_token = 'twplan1.legacy-completed'
            """
        ).fetchone() == (None,)
    api = ConnectorOutboxAPI(db_path, _HOST_ID)
    assert api.poll({"name": "turn-final", "limit": 100})["items"] == []
    inspected = api.inspect(
        {
            "schema_version": 1,
            "name": "turn-final",
            "status": "dead_letter",
            "limit": 100,
        }
    )
    assert key in {item.get("key") for item in inspected["items"]}
    _assert_no_private_or_raw(inspected)


@pytest.mark.parametrize(
    "proof_gap",
    [
        "declared_part_missing",
        "delivered_attempt_missing",
        "delivered_attempt_contradicted",
        "ack_time_missing",
        "foreign_host_part",
    ],
)
def test_v10_to_v11_migration_requires_complete_host_bound_ack_proof(
    tmp_path: Path,
    proof_gap: str,
) -> None:
    db_path = tmp_path / f"retention-v10-{proof_gap}.db"
    _create_v10_store(db_path)
    finals = _seed_v10_finals(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        if proof_gap == "declared_part_missing":
            conn.execute(
                """
                UPDATE turn_presentation_plans
                SET part_count = 2
                WHERE plan_token = 'twplan1.legacy-completed'
                """
            )
        elif proof_gap == "delivered_attempt_missing":
            conn.execute(
                """
                DELETE FROM connector_deliveries
                WHERE delivery_key = 'turn-final:legacy-completed:000000'
                """
            )
        elif proof_gap == "delivered_attempt_contradicted":
            conn.execute(
                """
                UPDATE connector_deliveries
                SET status = 'failed', delivered_at = NULL
                WHERE delivery_key = 'turn-final:legacy-completed:000000'
                """
            )
        elif proof_gap == "ack_time_missing":
            conn.execute(
                """
                UPDATE turn_presentation_plans
                SET completed_at = NULL
                WHERE plan_token = 'twplan1.legacy-completed'
                """
            )
        else:
            conn.execute(
                """
                UPDATE connector_outbox
                SET host_id = 'foreign-host'
                WHERE delivery_key = 'turn-final:legacy-completed:000000'
                """
            )
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []

    init_store(db_path)
    delivered_key = _final_key(*finals["delivered"])
    migrated = {
        str(row[1]): row
        for row in _anchor_rows(db_path)
    }[delivered_key]
    assert migrated[2:6] == (
        "final_migration_hold",
        finals["delivered"][0],
        finals["delivered"][1],
        "dead_letter",
    )
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT source_outbox_id
            FROM turn_presentation_plans
            WHERE plan_token = 'twplan1.legacy-completed'
            """
        ).fetchone() == (None,)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []

    api = ConnectorOutboxAPI(db_path, _HOST_ID)
    assert api.poll({"name": "turn-final", "limit": 100})["items"] == []
    inspected = api.inspect(
        {
            "schema_version": 1,
            "name": "turn-final",
            "status": "dead_letter",
            "limit": 100,
        }
    )
    assert delivered_key in {item.get("key") for item in inspected["items"]}
    _assert_no_private_or_raw(inspected)

@pytest.mark.parametrize("owner_matches", [True, False])
def test_v10_failed_plan_links_only_with_exact_immutable_job_route(
    tmp_path: Path,
    owner_matches: bool,
) -> None:
    db_path = tmp_path / f"retention-v10-owner-{owner_matches}.db"
    _create_v10_store(db_path)
    turn_id = "turn-legacy-owner"
    with sqlite3.connect(str(db_path)) as conn:
        revision = _insert_current_final(
            conn,
            turn_id=turn_id,
            user_text="legacy owner prompt",
            final_text="legacy owner final",
        )
        final_identity = store_sqlite.turn_final_delivery_identity(
            _HOST_ID,
            turn_id,
            revision,
        )
        route = {
            "schema_version": 2,
            "turn_id": turn_id,
            "content_revision": revision,
            "final_identity": final_identity,
            "stable_key": _STABLE_KEY if owner_matches else "wsk1_" + ("b" * 64),
            "stable_key_version": 1,
        }
        outbox_cursor = conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES (?, 'turn-final', 'turn-final:legacy-owner:000000',
                      'dead_letter', ?, '{}', ?, ?, NULL)
            """,
            (
                _HOST_ID,
                json.dumps({"turn": route}, sort_keys=True),
                _CREATED_AT,
                _CREATED_AT,
            ),
        )
        plan_cursor = conn.execute(
            """
            INSERT INTO turn_presentation_plans (
                host_id, name, plan_token, turn_id, content_revision,
                presentation_version, generation, part_count, state,
                created_at, activated_at
            ) VALUES (?, 'turn-final', 'twplan1.legacy-owner', ?, ?,
                      'legacy-owner-v1', 1, 1, 'failed', ?, ?)
            """,
            (_HOST_ID, turn_id, revision, _CREATED_AT, _CREATED_AT),
        )
        conn.execute(
            """
            INSERT INTO turn_presentation_jobs (
                plan_id, sequence_index, operation, part_ordinal,
                spans_json, outbox_id, created_at
            ) VALUES (?, 0, 'upsert', 0, '[]', ?, ?)
            """,
            (int(plan_cursor.lastrowid), int(outbox_cursor.lastrowid), _CREATED_AT),
        )

    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        root = conn.execute(
            """
            SELECT id, delivery_kind, status
            FROM connector_outbox
            WHERE delivery_kind IN ('final_ready', 'final_migration_hold')
            """
        ).fetchone()
        source_outbox_id = conn.execute(
            """
            SELECT source_outbox_id
            FROM turn_presentation_plans
            WHERE plan_token = 'twplan1.legacy-owner'
            """
        ).fetchone()[0]

    assert root is not None
    if owner_matches:
        assert root[1:3] == ("final_ready", "awaiting_ack")
        assert source_outbox_id == root[0]
    else:
        assert root[1:3] == ("final_migration_hold", "dead_letter")
        assert source_outbox_id is None



def test_v10_to_v11_migration_failure_rolls_back_the_entire_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "retention-v10-rollback.db"
    _create_v10_store(db_path)
    _seed_v10_finals(db_path)
    before = _database_dump(db_path)

    real_payload = store_sqlite._final_ready_payload_conn
    calls = 0

    def fail_after_first_anchor(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("controlled v11 migration failure")
        return real_payload(*args, **kwargs)

    monkeypatch.setattr(
        store_sqlite,
        "_final_ready_payload_conn",
        fail_after_first_anchor,
    )

    with pytest.raises(RuntimeError, match="controlled v11 migration failure"):
        init_store(db_path)

    assert calls == 2
    assert _database_dump(db_path) == before
    with sqlite3.connect(str(db_path)) as conn:
        _assert_v10_shape(conn)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'turn-final'"
        ).fetchone()[0] == 1
