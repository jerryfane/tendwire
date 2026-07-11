"""Tests for the neutral connector outbox boundary."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from tendwire.core.models import AttentionSignal, Snapshot
from tendwire.connectors import ConnectorOutboxAPI
from tendwire.store.sqlite import (
    SnapshotObservationContext,
    ack_connector_delivery,
    defer_connector_delivery,
    fail_connector_delivery,
    init_store,
    poll_connector_outbox,
    reclaim_expired_connector_leases,
    save_snapshot,
)


FORBIDDEN = {
    "private_state_json",
    "backend_target",
    "pane_id",
    "session_id",
    "terminal_id",
    "chat_id",
    "topic_id",
    "message_id",
    "bot_token",
    "telegram",
    "herdr",
    "herdres",
    "shell",
    "argv",
    "connector",
    "delivery",
    "backend.target",
    "pane.id",
    "message.id",
    "bot.token",
    "backend target",
    "pane id",
    "session id",
    "terminal id",
    "chat id",
    "topic id",
    "message id",
    "bot token",
}


def _assert_no_forbidden(value: Any) -> None:
    encoded = json.dumps(value, sort_keys=True).lower()
    for forbidden in FORBIDDEN:
        assert forbidden not in encoded


def _enqueue(db_path: Path, *, key: str = "job-1", status: str = "queued") -> None:
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "host-a",
                "attention",
                key,
                status,
                json.dumps(
                    {
                        "schema_version": 1,
                        "event_type": "attention_created",
                        "safe": "kept",
                        "transport": "telegram",
                        "backend_name": "herdres",
                        "chat_id": "must-strip",
                        "nested": {
                            "message_id": "must-strip",
                            "safe": "nested",
                            "backend_value": "herdr",
                            "list": ["ok", "telegram", "bot.token"],
                            "dot_value": "message.id",
                        },
                        "dot_private": "backend.target",
                    }
                ),
                json.dumps({"route": "private", "token": "secret"}),
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                None,
            ),
        )


def _delivery_rows(db_path: Path) -> list[tuple[Any, ...]]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT d.status, d.response_json, o.status, o.next_attempt_at, d.attempt
            FROM connector_deliveries d
            JOIN connector_outbox o ON o.id = d.outbox_id
            ORDER BY d.id
            """
        ).fetchall()


def test_poll_leases_sanitized_item_and_skips_duplicate_live_lease(tmp_path: Path) -> None:
    db_path = tmp_path / "connector.db"
    _enqueue(db_path)
    api = ConnectorOutboxAPI(db_path, "host-a")

    first = api.poll({"name": "attention", "limit": 10, "lease_seconds": 60})
    second = api.poll({"name": "attention", "limit": 10})

    assert first["ok"] is True
    assert first["items"][0]["key"] == "job-1"
    assert first["items"][0]["attempt"] == 1
    assert first["items"][0]["payload"]["safe"] == "kept"
    assert first["items"][0]["payload"]["nested"]["safe"] == "nested"
    assert first["items"][0]["ref"].startswith("twref1.")
    assert "host-a" not in first["items"][0]["ref"]
    assert "attention" not in first["items"][0]["ref"]
    assert "job-1" not in first["items"][0]["ref"]
    assert "lease" not in first["items"][0]["ref"].lower()
    assert "transport" not in first["items"][0]["payload"]
    assert "backend_name" not in first["items"][0]["payload"]
    assert "backend_value" not in first["items"][0]["payload"]["nested"]
    assert first["items"][0]["payload"]["nested"]["list"] == ["ok"]
    assert second["items"] == []
    _assert_no_forbidden(first)


def test_poll_uses_configured_default_lease_and_explicit_lease_wins(tmp_path: Path) -> None:
    db_path = tmp_path / "lease-default.db"
    _enqueue(db_path, key="default-lease")
    default_api = ConnectorOutboxAPI(db_path, "host-a", default_lease_seconds=3600)
    default_item = default_api.poll({"name": "attention"})["items"][0]
    default_delta = (
        datetime.fromisoformat(default_item["leased_until"])
        - datetime.fromisoformat(default_item["available_at"])
    ).total_seconds()

    reclaim_expired_connector_leases(
        db_path,
        "host-a",
        "attention",
        now="9999-01-01T00:00:00+00:00",
    )
    explicit_item = default_api.poll({"name": "attention", "lease_seconds": 5})["items"][0]
    explicit_delta = (
        datetime.fromisoformat(explicit_item["leased_until"])
        - datetime.fromisoformat(explicit_item["available_at"])
    ).total_seconds()

    assert default_delta == 3600
    assert explicit_delta == 5


def test_reclaim_allows_fresh_ref_and_rejects_stale_ref(tmp_path: Path) -> None:
    db_path = tmp_path / "reclaim.db"
    _enqueue(db_path)
    api = ConnectorOutboxAPI(db_path, "host-a")
    old = api.poll({"name": "attention", "lease_seconds": 60})["items"][0]["ref"]

    reclaim = reclaim_expired_connector_leases(
        db_path,
        "host-a",
        "attention",
        now="9999-01-01T00:00:00+00:00",
    )
    fresh = api.poll({"name": "attention", "lease_seconds": 60})["items"][0]
    stale_ack = api.ack({"name": "attention", "ref": old, "response": {"safe": "stale"}})

    assert reclaim["reclaimed"] == 1
    assert fresh["attempt"] == 2
    assert fresh["ref"] != old
    assert stale_ack["ok"] is False
    assert stale_ack["status"] in {"stale_ref", "expired_ref", "invalid_ref"}
    assert _delivery_rows(db_path)[-1][0] == "leased"


def test_ack_delivers_sanitized_response_and_blocks_future_poll(tmp_path: Path) -> None:
    db_path = tmp_path / "ack.db"
    _enqueue(db_path)
    api = ConnectorOutboxAPI(db_path, "host-a")
    ref = api.poll({"name": "attention"})["items"][0]["ref"]

    ack = api.ack(
        {
            "name": "attention",
            "ref": ref,
            "response": {
                "ok": True,
                "ref": "opaque-provider-ref",
                "provider": "telegram",
                "message_id": "must-strip",
                "dot_value": "message.id",
                "space_value": "message id must strip",
                "nested": {
                    "bot_token": "must-strip",
                    "safe": "kept",
                    "transport": "herdres",
                    "dot_list": ["safe", "bot.token"],
                    "space_list": ["safe", "bot token"],
                },
            },
        }
    )
    again = api.poll({"name": "attention"})

    assert ack["ok"] is True
    assert ack["status"] == "acknowledged"
    assert again["items"] == []
    rows = _delivery_rows(db_path)
    assert rows[0][0] == "delivered"
    assert rows[0][2] == "delivered"
    stored = json.loads(rows[0][1])
    assert "ref" not in stored["response"]
    assert stored["response"]["nested"]["safe"] == "kept"
    assert stored["response"]["nested"]["dot_list"] == ["safe"]
    assert stored["response"]["nested"]["space_list"] == ["safe"]
    assert "provider" not in stored["response"]
    assert "space_value" not in stored["response"]
    assert "transport" not in stored["response"]["nested"]
    _assert_no_forbidden(stored)


def test_fail_and_defer_schedule_future_availability(tmp_path: Path) -> None:
    db_path = tmp_path / "schedule.db"
    _enqueue(db_path, key="fail-job")
    api = ConnectorOutboxAPI(db_path, "host-a")
    first_ref = api.poll({"name": "attention"})["items"][0]["ref"]

    failed = api.fail(
        {
            "name": "attention",
            "ref": first_ref,
            "reason": "backend target chat id bot token",
            "available_at": "9999-01-01T00:00:00+00:00",
            "response": {"safe": "kept", "chat_id": "must-strip"},
        }
    )
    blocked_retry = api.poll({"name": "attention"})
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "UPDATE connector_outbox SET next_attempt_at = ? WHERE delivery_key = ?",
            ("2000-01-01T00:00:00+00:00", "fail-job"),
        )
    retry = api.poll({"name": "attention"})["items"][0]

    deferred = api.defer(
        {
            "name": "attention",
            "ref": retry["ref"],
            "available_at": "9999-01-01T00:00:00+00:00",
            "reason": "pane id terminal id session id message id",
        }
    )
    blocked_defer = api.poll({"name": "attention"})
    rows = _delivery_rows(db_path)
    failed_payload = json.loads(rows[0][1])
    deferred_payload = json.loads(rows[1][1])

    assert failed["status"] == "retry_scheduled"
    assert blocked_retry["items"] == []
    assert retry["attempt"] == 2
    assert deferred["status"] == "deferred"
    assert blocked_defer["items"] == []
    assert failed_payload["reason"] == ""
    assert deferred_payload["reason"] == ""
    _assert_no_forbidden(failed_payload)
    _assert_no_forbidden(deferred_payload)
    _assert_no_forbidden(failed)
    _assert_no_forbidden(deferred)


def test_max_outbox_attempts_dead_letters_exhausted_failures(tmp_path: Path) -> None:
    db_path = tmp_path / "attempts.db"
    _enqueue(db_path, key="attempt-job")
    api = ConnectorOutboxAPI(db_path, "host-a", max_attempts=2)
    first_ref = api.poll({"name": "attention"})["items"][0]["ref"]
    first_fail = api.fail({"name": "attention", "ref": first_ref, "delay_seconds": 0})

    second = api.poll({"name": "attention"})["items"][0]
    exhausted = api.fail({"name": "attention", "ref": second["ref"], "delay_seconds": 0})
    after = api.poll({"name": "attention"})

    with sqlite3.connect(str(db_path)) as conn:
        outbox_status, next_attempt_at = conn.execute(
            "SELECT status, next_attempt_at FROM connector_outbox WHERE delivery_key = ?",
            ("attempt-job",),
        ).fetchone()

    assert first_fail["status"] == "retry_scheduled"
    assert second["attempt"] == 2
    assert exhausted["status"] == "attempts_exhausted"
    assert "available_at" not in exhausted
    assert outbox_status == "dead_letter"
    assert next_attempt_at is None
    assert after["items"] == []
    _assert_no_forbidden(exhausted)


def test_poll_dead_letters_expired_lease_at_max_attempts_before_repoll(tmp_path: Path) -> None:
    db_path = tmp_path / "expired-max-attempt.db"
    _enqueue(db_path, key="expired-max")
    first = poll_connector_outbox(
        db_path,
        "host-a",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:00+00:00",
    )["items"][0]
    fail_connector_delivery(
        db_path,
        host_id="host-a",
        name="attention",
        ref=first["ref"],
        delay_seconds=0,
        max_attempts=2,
        now="2026-01-01T00:00:01+00:00",
    )
    second = poll_connector_outbox(
        db_path,
        "host-a",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:02+00:00",
    )["items"][0]

    after = poll_connector_outbox(
        db_path,
        "host-a",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:04+00:00",
    )

    with sqlite3.connect(str(db_path)) as conn:
        outbox_status = conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            ("expired-max",),
        ).fetchone()[0]
        attempts = conn.execute(
            """
            SELECT COUNT(*), COALESCE(MAX(attempt), 0)
            FROM connector_deliveries
            WHERE delivery_key = ?
            """,
            ("expired-max",),
        ).fetchone()

    assert first["attempt"] == 1
    assert second["attempt"] == 2
    assert after["items"] == []
    assert outbox_status == "dead_letter"
    assert attempts == (2, 2)


def test_invalid_wrong_host_and_wrong_name_refs_do_not_mutate(tmp_path: Path) -> None:
    db_path = tmp_path / "invalid.db"
    _enqueue(db_path)
    api = ConnectorOutboxAPI(db_path, "host-a")
    ref = api.poll({"name": "attention"})["items"][0]["ref"]

    wrong_host = ConnectorOutboxAPI(db_path, "other-host").ack({"name": "attention", "ref": ref})
    wrong_name = api.fail({"name": "other-name", "ref": ref, "reason": "nope"})
    malformed = api.defer({"name": "attention", "ref": "not-a-ref"})

    assert wrong_host["ok"] is False
    assert wrong_name["ok"] is False
    assert malformed["ok"] is False
    assert _delivery_rows(db_path)[0][0] == "leased"


def test_connector_api_rejects_non_neutral_public_names_without_echoing_them(tmp_path: Path) -> None:
    db_path = tmp_path / "connector-name.db"
    _enqueue(db_path)
    api = ConnectorOutboxAPI(db_path, "host-a")

    payloads = [
        api.poll({"name": "telegram"}),
        api.reclaim({"name": "herdres"}),
        api.ack({"name": "attention/chat", "ref": "twref1.publicSafeRef"}),
        api.fail({"name": "backend_target", "ref": "twref1.publicSafeRef"}),
        api.defer({"name": "attention delivery", "ref": "twref1.publicSafeRef"}),
        api.poll({"name": "backend.target"}),
        api.reclaim({"name": "pane.id"}),
        api.ack({"name": "message.id", "ref": "twref1.publicSafeRef"}),
        api.fail({"name": "bot.token", "ref": "twref1.publicSafeRef"}),
    ]
    unsafe_ref_payloads = [
        api.ack({"name": "attention", "ref": "telegram-message-id"}),
        api.fail({"name": "attention", "ref": "herdres-route"}),
        api.defer({"name": "attention", "ref": "backend_target"}),
    ]
    still_pollable = api.poll({"name": "attention"})

    for payload in payloads:
        assert payload["ok"] is False
        assert payload["status"] == "invalid_params"
        assert payload["name"] == ""
        _assert_no_forbidden(payload)
    for payload in unsafe_ref_payloads:
        assert payload["ok"] is False
        assert payload["status"] == "invalid_ref"
        assert "ref" not in payload
        _assert_no_forbidden(payload)
    assert still_pollable["items"][0]["key"] == "job-1"


def test_lifecycle_delivery_key_survives_fail_defer_reclaim_and_ack(tmp_path: Path) -> None:
    db_path = tmp_path / "lifecycle-delivery.db"
    host_id = "host-lifecycle"
    observed_at = "2026-01-01T00:00:00+00:00"
    snapshot = Snapshot(
        host_id=host_id,
        updated_at=observed_at,
        attention=[
            AttentionSignal(
                kind="worker_status",
                severity="warning",
                status="waiting",
                reason="Review the worker",
                source="worker:worker-1",
                updated_at=observed_at,
                meta={"worker_id": "worker-1", "needs_human": True},
                host_id=host_id,
            )
        ],
    )
    observation = SnapshotObservationContext(authority="complete", observed_at=observed_at)
    save_snapshot(db_path, snapshot, observation=observation)

    with sqlite3.connect(str(db_path)) as conn:
        generated = conn.execute(
            """
            SELECT delivery_key, status
            FROM connector_outbox
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()

    assert len(generated) == 1
    stable_key = generated[0][0]
    assert stable_key.startswith("attention:attention_created:")
    assert generated[0][1] == "queued"

    first = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=10,
        now="2026-01-01T00:00:01+00:00",
    )["items"][0]
    assert first["key"] == stable_key
    assert first["attempt"] == 1
    assert first["payload"]["event_type"] == "attention_created"

    save_snapshot(db_path, snapshot, observation=observation)
    with sqlite3.connect(str(db_path)) as conn:
        leased_rows = conn.execute(
            """
            SELECT delivery_key, status
            FROM connector_outbox
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()
    assert leased_rows == [(stable_key, "leased")]

    failed = fail_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=first["ref"],
        delay_seconds=1,
        now="2026-01-01T00:00:02+00:00",
    )
    second = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=10,
        now="2026-01-01T00:00:03+00:00",
    )["items"][0]
    deferred = defer_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=second["ref"],
        available_at="2026-01-01T00:00:05+00:00",
        now="2026-01-01T00:00:04+00:00",
    )
    third = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=10,
        now="2026-01-01T00:00:05+00:00",
    )["items"][0]

    not_expired = reclaim_expired_connector_leases(
        db_path,
        host_id,
        "attention",
        now="2026-01-01T00:00:14+00:00",
    )
    reclaimed = reclaim_expired_connector_leases(
        db_path,
        host_id,
        "attention",
        now="2026-01-01T00:00:15+00:00",
    )
    fourth = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=10,
        now="2026-01-01T00:00:15+00:00",
    )["items"][0]
    acknowledged = ack_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=fourth["ref"],
        response={"safe": "delivered"},
        now="2026-01-01T00:00:16+00:00",
    )
    after_ack = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        now="2026-01-01T00:00:17+00:00",
    )

    keyed_responses = (first, failed, second, deferred, third, fourth, acknowledged)
    assert {response["key"] for response in keyed_responses} == {stable_key}
    assert [first["attempt"], second["attempt"], third["attempt"], fourth["attempt"]] == [1, 2, 3, 4]
    assert len({first["ref"], second["ref"], third["ref"], fourth["ref"]}) == 4
    assert failed["status"] == "retry_scheduled"
    assert deferred["status"] == "deferred"
    assert not_expired["reclaimed"] == 0
    assert reclaimed["reclaimed"] == 1
    assert acknowledged["status"] == "acknowledged"
    assert after_ack["items"] == []

    with sqlite3.connect(str(db_path)) as conn:
        outbox_rows = conn.execute(
            """
            SELECT delivery_key, status
            FROM connector_outbox
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()
        delivery_rows = conn.execute(
            """
            SELECT delivery_key, attempt, status
            FROM connector_deliveries
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()

    assert outbox_rows == [(stable_key, "delivered")]
    assert [row[0] for row in delivery_rows] == [stable_key] * 4
    assert [row[1] for row in delivery_rows] == [1, 2, 3, 4]
    assert [row[2] for row in delivery_rows] == ["failed", "deferred", "expired", "delivered"]


def test_migration_terminalizes_noncanonical_live_leases_through_public_helpers(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "leased-duplicate-migration.db"
    host_id = "host-migration-leases"
    observed_at = "2026-01-01T00:00:00+00:00"
    snapshot = Snapshot(
        host_id=host_id,
        updated_at=observed_at,
        attention=[
            AttentionSignal(
                kind="worker_status",
                severity="warning",
                status="waiting",
                reason="Review the worker",
                source="worker:worker-1",
                updated_at=observed_at,
                host_id=host_id,
            )
        ],
    )
    save_snapshot(
        db_path,
        snapshot,
        observation=SnapshotObservationContext(
            authority="complete",
            observed_at=observed_at,
        ),
    )

    with sqlite3.connect(str(db_path)) as conn:
        for suffix in range(1, 4):
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                )
                SELECT
                    host_id, connector, ?, 'queued', payload_json,
                    '{}', created_at, updated_at, NULL
                FROM connector_outbox
                WHERE host_id = ? AND connector = 'attention'
                ORDER BY id
                LIMIT 1
                """,
                (f"legacy-duplicate-{suffix}", host_id),
            )

    canonical = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=100,
        now="2026-01-01T00:00:01+00:00",
    )["items"][0]
    failed_lease = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=100,
        now="2026-01-01T00:00:02+00:00",
    )["items"][0]
    deferred_lease = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=100,
        now="2026-01-01T00:00:03+00:00",
    )["items"][0]
    expiring_lease = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=5,
        now="2026-01-01T00:00:04+00:00",
    )["items"][0]

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA user_version = 4")
    init_store(db_path)

    failed = fail_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=failed_lease["ref"],
        delay_seconds=0,
        now="2026-01-01T00:00:05+00:00",
    )
    deferred = defer_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=deferred_lease["ref"],
        delay_seconds=0,
        now="2026-01-01T00:00:06+00:00",
    )
    reclaimed = reclaim_expired_connector_leases(
        db_path,
        host_id,
        "attention",
        now="2026-01-01T00:00:09+00:00",
    )
    acknowledged = ack_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=canonical["ref"],
        now="2026-01-01T00:00:10+00:00",
    )
    after = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        now="2026-01-01T00:00:11+00:00",
    )

    assert failed["status"] == "superseded"
    assert failed["key"] == failed_lease["key"]
    assert deferred["status"] == "superseded"
    assert deferred["key"] == deferred_lease["key"]
    assert reclaimed["reclaimed"] == 1
    assert acknowledged["status"] == "acknowledged"
    assert acknowledged["key"] == canonical["key"]
    assert after["items"] == []

    with sqlite3.connect(str(db_path)) as conn:
        outbox_rows = conn.execute(
            """
            SELECT delivery_key, status
            FROM connector_outbox
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()
        delivery_rows = conn.execute(
            """
            SELECT delivery_key, status
            FROM connector_deliveries
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()

    assert outbox_rows == [
        (canonical["key"], "delivered"),
        (failed_lease["key"], "superseded"),
        (deferred_lease["key"], "superseded"),
        (expiring_lease["key"], "superseded"),
    ]
    assert delivery_rows == [
        (canonical["key"], "delivered"),
        (failed_lease["key"], "failed"),
        (deferred_lease["key"], "deferred"),
        (expiring_lease["key"], "expired"),
    ]
