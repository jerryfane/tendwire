"""Transactional retention contracts for authoritative snapshot projection."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tendwire.config import Config
from tendwire.core.models import Snapshot, WorkerBinding
from tendwire.core.projector import project_from_raw
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    init_store,
    latest_snapshot,
    list_worker_bindings,
    merge_turn_content,
    save_snapshot,
    upsert_command_pending_turn,
)


HOST_ID = "snapshot-retention-host"
WORKER_ID = "worker-1"
FINAL_TEXT = "authoritative snapshot final"
USER_TEXT = "authoritative snapshot prompt"
STABLE_KEY_A = "wsk1_" + ("e" * 64)
STABLE_KEY_B = "wsk1_" + ("f" * 64)


def _snapshot(db_path: Path, *, status: str = "active", second: int = 0) -> Snapshot:
    return project_from_raw(
        Config(host_id=HOST_ID, db_path=db_path),
        workers=[
            {
                "id": WORKER_ID,
                "name": "Projection Worker",
                "status": status,
                "space_id": "space-1",
                "meta": {
                    "stable_key": STABLE_KEY_A,
                    "stable_key_version": 1,
                },
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
    )


def _empty_snapshot(db_path: Path, *, second: int) -> Snapshot:
    return project_from_raw(
        Config(host_id=HOST_ID, db_path=db_path),
        workers=[],
        timestamp=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
    )


def _seed_complete_final(db_path: Path, snapshot: Snapshot) -> tuple[str, str]:
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "user_text": USER_TEXT,
            "assistant_final_text": FINAL_TEXT,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:01+00:00",
    ) == 1
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT turns.turn_id, revisions.content_revision
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = ? AND turns.worker_id = ?
            """,
            (HOST_ID, WORKER_ID),
        ).fetchone()
    assert row is not None
    return str(row[0]), str(row[1])


def test_snapshot_complete_final_has_one_neutral_idempotent_anchor(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot-final-anchor.db"
    original = _snapshot(db_path)
    turn_id, revision = _seed_complete_final(db_path, original)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM connector_outbox WHERE host_id = ? AND turn_id = ?",
            (HOST_ID, turn_id),
        )
        conn.commit()

    rewritten = Snapshot.from_dict(
        {**original.to_dict(), "updated_at": "2026-01-01T00:00:02+00:00"}
    )
    save_snapshot(db_path, rewritten)
    save_snapshot(db_path, rewritten)

    with sqlite3.connect(db_path) as conn:
        anchors = conn.execute(
            """
            SELECT connector, delivery_kind, turn_id, content_revision, payload_json
            FROM connector_outbox
            WHERE host_id = ? AND delivery_kind = 'final_ready'
            """,
            (HOST_ID,),
        ).fetchall()

    assert len(anchors) == 1
    connector, delivery_kind, anchored_turn, anchored_revision, raw_payload = anchors[0]
    assert (connector, delivery_kind, anchored_turn, anchored_revision) == (
        "turn-final",
        "final_ready",
        turn_id,
        revision,
    )
    payload = json.loads(raw_payload)
    assert payload["operation"] == "materialize"
    assert payload["turn_id"] == turn_id
    assert payload["content_revision"] == revision
    assert payload["schema_version"] == 2
    assert payload["stable_key"] == STABLE_KEY_A
    assert payload["stable_key_version"] == 1
    encoded = json.dumps(payload, sort_keys=True)
    assert FINAL_TEXT not in encoded
    assert USER_TEXT not in encoded
    assert payload["content"]["fields"]["assistant_final_text"]["inline"] is False
    assert payload["content"]["fields"]["user_text"]["inline"] is False


@pytest.mark.parametrize(
    "invalid_meta",
    [
        {},
        {"stable_key": STABLE_KEY_A},
        {"stable_key": "wsk1_invalid", "stable_key_version": 1},
        {"stable_key": STABLE_KEY_A, "stable_key_version": True},
        {"stable_key": STABLE_KEY_A, "stable_key_version": 2},
    ],
)
def test_final_anchor_fails_closed_without_public_stable_key_pair(
    tmp_path: Path,
    invalid_meta: dict[str, object],
) -> None:
    db_path = tmp_path / "snapshot-final-stable-key.db"
    original = _snapshot(db_path)
    turn_id, revision = _seed_complete_final(db_path, original)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM connector_outbox WHERE host_id = ? AND turn_id = ?",
            (HOST_ID, turn_id),
        )
        raw_payload = conn.execute(
            """
            SELECT payload_json
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (HOST_ID, turn_id),
        ).fetchone()[0]
        turn_payload = json.loads(str(raw_payload))
        turn_payload["meta"] = invalid_meta
        conn.execute(
            """
            UPDATE turns
            SET payload_json = ?
            WHERE host_id = ? AND turn_id = ?
            """,
            (json.dumps(turn_payload, sort_keys=True), HOST_ID, turn_id),
        )
        anchor_id = store_sqlite._ensure_final_ready_anchor_conn(
            conn,
            host_id=HOST_ID,
            turn_id=turn_id,
            content_revision_value=revision,
            now="2026-01-01T00:00:02+00:00",
        )
        anchor = conn.execute(
            """
            SELECT delivery_kind, status, payload_json
            FROM connector_outbox
            WHERE host_id = ? AND turn_id = ?
            """,
            (HOST_ID, turn_id),
        ).fetchone()

    assert anchor_id is not None
    assert anchor is not None
    assert anchor[0:2] == ("final_migration_hold", "dead_letter")
    hold_payload = json.loads(str(anchor[2]))
    assert hold_payload["schema_version"] == 1
    assert "stable_key" not in hold_payload
    assert "stable_key_version" not in hold_payload


def test_missing_identity_merge_persists_nonpollable_hold_on_current_placeholder(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "missing-identity-merge.db"
    config = Config(host_id=HOST_ID, db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Worker Missing Identity",
                "status": "active",
                "kind": "worker",
            }
        ],
    )
    init_store(db_path)
    assert save_snapshot(db_path, snapshot) is True

    changed = merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "source_turn_id": "turnsrc-missing-identity",
            "assistant_final_text": "must remain durably held",
        },
        observed_at="2026-01-01T00:00:01+00:00",
    )
    assert changed == 1
    with sqlite3.connect(str(db_path)) as conn:
        anchor = conn.execute(
            """
            SELECT outbox.delivery_kind, outbox.status, outbox.payload_json,
                   turns.payload_json
            FROM connector_outbox AS outbox
            JOIN turns
              ON turns.host_id = outbox.host_id
             AND turns.turn_id = outbox.turn_id
            WHERE outbox.host_id = ?
            """,
            (HOST_ID,),
        ).fetchone()

    assert anchor is not None
    assert anchor[0:2] == ("final_migration_hold", "dead_letter")
    hold_payload = json.loads(str(anchor[2]))
    turn_payload = json.loads(str(anchor[3]))
    assert hold_payload["schema_version"] == 1
    assert "stable_key" not in hold_payload
    assert turn_payload["source_turn_id"].startswith("turnsrc-")


def test_worker_id_reuse_binds_each_root_to_immutable_stable_key(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "snapshot-worker-reuse.db"
    config = Config(host_id=HOST_ID, db_path=db_path)
    first = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Worker Instance One",
                "status": "active",
                "space_id": "space-1",
                "meta": {
                    "stable_key": STABLE_KEY_A,
                    "stable_key_version": 1,
                },
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    )
    init_store(db_path)
    save_snapshot(db_path, first)
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "assistant_final_text": "first instance final",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "instance-one-source",
        },
        observed_at="2026-01-01T00:00:01+00:00",
    ) == 1

    second = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Worker Instance Two",
                "status": "active",
                "space_id": "space-1",
                "meta": {
                    "stable_key": STABLE_KEY_B,
                    "stable_key_version": 1,
                },
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
    )
    assert second.workers[0].fingerprint != first.workers[0].fingerprint
    save_snapshot(db_path, second)
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "assistant_final_text": "second instance final",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "instance-two-source",
        },
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 1

    with sqlite3.connect(db_path) as conn:
        roots = [
            json.loads(str(row[0]))
            for row in conn.execute(
                """
                SELECT payload_json
                FROM connector_outbox
                WHERE host_id = ? AND delivery_kind = 'final_ready'
                ORDER BY id
                """,
                (HOST_ID,),
            ).fetchall()
        ]
        persisted = {
            str(turn_id): json.loads(str(payload_json)).get("meta", {}).get("stable_key")
            for turn_id, payload_json in conn.execute(
                """
                SELECT turn_id, payload_json
                FROM turns
                WHERE host_id = ?
                """,
                (HOST_ID,),
            ).fetchall()
        }

    assert len(roots) == 2
    assert {root["stable_key"] for root in roots} == {
        STABLE_KEY_A,
        STABLE_KEY_B,
    }
    assert all(root["schema_version"] == 2 for root in roots)
    assert all(root["stable_key_version"] == 1 for root in roots)
    for root in roots:
        assert root["stable_key"] == persisted[root["turn_id"]]


@pytest.mark.parametrize("old_status", ["queued", "delivered"])
def test_same_source_content_isolated_across_owner_change_and_restart(
    tmp_path: Path,
    old_status: str,
) -> None:
    db_path = tmp_path / f"same-source-owner-{old_status}.db"
    config = Config(host_id=HOST_ID, db_path=db_path)

    def owner_snapshot(stable_key: str, second: int) -> Snapshot:
        return project_from_raw(
            config,
            workers=[
                {
                    "id": WORKER_ID,
                    "name": "Reused Worker",
                    "status": "active",
                    "meta": {
                        "stable_key": stable_key,
                        "stable_key_version": 1,
                    },
                }
            ],
            timestamp=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
        )

    first = owner_snapshot(STABLE_KEY_A, 0)
    init_store(db_path)
    assert save_snapshot(db_path, first) is True
    content = {
        "source_turn_id": "same-backend-source",
        "assistant_final_text": "identical owner-sensitive final",
        "complete": True,
        "has_open_turn": False,
    }
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:01+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        first_root = conn.execute(
            """
            SELECT id, turn_id
            FROM connector_outbox
            WHERE host_id = ? AND delivery_kind = 'final_ready'
            """,
            (HOST_ID,),
        ).fetchone()
        assert first_root is not None
        if old_status == "delivered":
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = 'delivered'
                WHERE id = ?
                """,
                (int(first_root[0]),),
            )

    second = owner_snapshot(STABLE_KEY_B, 2)
    assert save_snapshot(db_path, second) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 1
    init_store(db_path)
    assert save_snapshot(db_path, second) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:04+00:00",
    ) == 0

    with sqlite3.connect(str(db_path)) as conn:
        roots = [
            (str(turn_id), str(status), json.loads(str(payload_json)))
            for turn_id, status, payload_json in conn.execute(
                """
                SELECT turn_id, status, payload_json
                FROM connector_outbox
                WHERE host_id = ? AND delivery_kind = 'final_ready'
                ORDER BY id
                """,
                (HOST_ID,),
            ).fetchall()
        ]
    assert len(roots) == 2
    assert roots[0][0] != roots[1][0]
    assert roots[0][1] == old_status
    assert roots[1][1] == "queued"
    assert [root[2]["stable_key"] for root in roots] == [
        STABLE_KEY_A,
        STABLE_KEY_B,
    ]


def test_missing_owner_replay_holds_without_overwriting_existing_owner_root(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "same-source-missing-owner.db"
    config = Config(host_id=HOST_ID, db_path=db_path)
    first = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Owned Worker",
                "status": "active",
                "meta": {
                    "stable_key": STABLE_KEY_A,
                    "stable_key_version": 1,
                },
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    )
    init_store(db_path)
    assert save_snapshot(db_path, first) is True
    content = {
        "source_turn_id": "same-backend-source",
        "assistant_final_text": "same final during continuity loss",
        "complete": True,
        "has_open_turn": False,
    }
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:01+00:00",
    ) == 1

    missing = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Unowned Worker",
                "status": "active",
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
    )
    assert save_snapshot(db_path, missing) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 1
    init_store(db_path)
    assert save_snapshot(db_path, missing) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:04+00:00",
    ) == 0

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT delivery_kind, status, payload_json
            FROM connector_outbox
            WHERE host_id = ?
            ORDER BY id
            """,
            (HOST_ID,),
        ).fetchall()
    assert [(str(row[0]), str(row[1])) for row in rows] == [
        ("final_ready", "queued"),
        ("final_migration_hold", "dead_letter"),
    ]
    assert json.loads(str(rows[0][2]))["stable_key"] == STABLE_KEY_A
    assert json.loads(str(rows[1][2]))["schema_version"] == 1


def test_equal_public_snapshot_replay_cannot_replace_private_binding_route(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "equal-public-private-route.db"
    config = Config(host_id=HOST_ID, db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Equal Public Worker",
                "status": "active",
                "meta": {
                    "stable_key": STABLE_KEY_A,
                    "stable_key_version": 1,
                },
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    )
    private_identity = "binding-private-identity"
    first_binding = WorkerBinding(
        host_id=HOST_ID,
        worker_id=WORKER_ID,
        worker_fingerprint=snapshot.workers[0].fingerprint,
        backend="herdr",
        target_kind="terminal_id",
        target_value="first-private-target",
        sendable=True,
        observed_at=snapshot.updated_at,
        private_fingerprint=private_identity,
    )
    conflicting_binding = WorkerBinding(
        host_id=HOST_ID,
        worker_id=WORKER_ID,
        worker_fingerprint=snapshot.workers[0].fingerprint,
        backend="herdr",
        target_kind="terminal_id",
        target_value="conflicting-private-target",
        sendable=True,
        observed_at=snapshot.updated_at,
        private_fingerprint=private_identity,
    )
    init_store(db_path)
    assert save_snapshot(
        db_path,
        snapshot,
        worker_bindings=[first_binding],
        binding_backend="herdr",
        binding_observation_authoritative=True,
    ) is True
    assert save_snapshot(
        db_path,
        snapshot,
        worker_bindings=[conflicting_binding],
        binding_backend="herdr",
        binding_observation_authoritative=True,
    ) is True

    stored = list_worker_bindings(
        db_path,
        HOST_ID,
        backend="herdr",
        include_expired=True,
    )
    assert len(stored) == 1
    assert stored[0].target_value == "first-private-target"


def test_snapshot_projection_and_anchor_roll_back_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "snapshot-projection-rollback.db"
    original = _snapshot(db_path)
    turn_id, _revision = _seed_complete_final(db_path, original)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM connector_outbox WHERE host_id = ? AND turn_id = ?",
            (HOST_ID, turn_id),
        )
        conn.commit()

    def fail_after_projection(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fail after snapshot projection")

    monkeypatch.setattr(
        store_sqlite,
        "_apply_attention_observation_conn",
        fail_after_projection,
    )
    with pytest.raises(RuntimeError, match="fail after snapshot projection"):
        save_snapshot(db_path, _snapshot(db_path, status="waiting", second=3))

    with sqlite3.connect(db_path) as conn:
        status = conn.execute(
            "SELECT status FROM turns WHERE host_id = ? AND turn_id = ?",
            (HOST_ID, turn_id),
        ).fetchone()
        anchor_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ? AND turn_id = ? AND delivery_kind = 'final_ready'
            """,
            (HOST_ID, turn_id),
        ).fetchone()[0]
        snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE host_id = ?", (HOST_ID,)
        ).fetchone()[0]

    assert status == ("active",)
    assert anchor_count == 0
    assert snapshot_count == 1


def test_snapshot_omission_preserves_real_command_and_source_provenance(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "snapshot-provenance.db"
    original = _snapshot(db_path)
    init_store(db_path)
    save_snapshot(db_path, original)
    pending = upsert_command_pending_turn(
        db_path,
        HOST_ID,
        original.workers[0],
        request_id="command-retained",
        instruction_text=USER_TEXT,
        observed_at="2026-01-01T00:00:01+00:00",
    )
    assert pending is not None
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "user_text": USER_TEXT,
            "assistant_final_text": FINAL_TEXT,
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "backend-source-retained",
        },
        observed_at="2026-01-01T00:00:02+00:00",
    ) == 1

    with sqlite3.connect(db_path) as conn:
        source_turns = [
            (str(turn_id), json.loads(str(raw_payload)))
            for turn_id, raw_payload in conn.execute(
                "SELECT turn_id, payload_json FROM turns WHERE host_id = ?",
                (HOST_ID,),
            ).fetchall()
            if json.loads(str(raw_payload)).get("source_turn_id")
        ]
    assert len(source_turns) == 1
    turn_id, source_payload = source_turns[0]
    assert source_payload["origin_command_id"] == "command-retained"
    source_identity = str(source_payload["source_turn_id"])
    assert source_identity.startswith("turnsrc-")

    save_snapshot(db_path, _snapshot(db_path, status="waiting", second=4))
    save_snapshot(db_path, _empty_snapshot(db_path, second=6))

    with sqlite3.connect(db_path) as conn:
        surviving = conn.execute(
            """
            SELECT payload_json
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (HOST_ID, turn_id),
        ).fetchone()
        anchor_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ?
              AND turn_id = ?
              AND delivery_kind = 'final_ready'
            """,
            (HOST_ID, turn_id),
        ).fetchone()[0]
    assert surviving is not None
    surviving_payload = json.loads(str(surviving[0]))
    assert surviving_payload["origin_command_id"] == "command-retained"
    assert surviving_payload["source_turn_id"] == source_identity
    assert anchor_count == 1


def test_stale_different_fingerprint_snapshot_cannot_regress_or_prune_projection(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "snapshot-stale-order.db"
    original = _snapshot(db_path, status="active", second=0)
    turn_id, _revision = _seed_complete_final(db_path, original)
    newer = _snapshot(db_path, status="waiting", second=8)
    save_snapshot(db_path, newer)

    save_snapshot(db_path, _snapshot(db_path, status="active", second=4))
    save_snapshot(db_path, _empty_snapshot(db_path, second=5))

    with sqlite3.connect(db_path) as conn:
        worker_status = conn.execute(
            "SELECT status FROM workers WHERE host_id = ?",
            (HOST_ID,),
        ).fetchone()
        turn_status = conn.execute(
            "SELECT status FROM turns WHERE host_id = ? AND turn_id = ?",
            (HOST_ID, turn_id),
        ).fetchone()
        snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE host_id = ?",
            (HOST_ID,),
        ).fetchone()[0]
        anchor_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ?
              AND turn_id = ?
              AND delivery_kind = 'final_ready'
            """,
            (HOST_ID, turn_id),
        ).fetchone()[0]
    latest = latest_snapshot(db_path, HOST_ID)

    assert worker_status == ("waiting",)
    assert turn_status == ("waiting",)
    assert snapshot_count == 2
    assert anchor_count == 1
    assert latest is not None
    assert latest.updated_at == newer.updated_at
