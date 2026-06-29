"""Tests for public turn and pending-interaction contracts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from tendwire.config import Config
from tendwire.core.models import AttentionSignal, Snapshot, SuggestedAction, Worker
from tendwire.core.projector import project_from_raw
from tendwire.core.turns import (
    InteractionChoice,
    PendingInteraction,
    Turn,
    payload_to_json,
    pending_from_snapshot,
    pending_payload_from_snapshot,
    turns_from_snapshot,
    turns_payload_from_snapshot,
)


_FORBIDDEN_FIELDS = {
    "telegram",
    "chat_id",
    "topic_id",
    "message_id",
    "thread_id",
    "token",
    "bot_token",
    "delivery",
    "route",
    "herdres_delivery",
    "command",
    "backend_target",
    "terminal_id",
    "pane_id",
    "agent_session",
    "session_id",
    "herdr_state",
    "herdres_state",
    "target_kind",
    "target_value",
    "turn_target_kind",
    "turn_target_value",
    "private_fingerprint",
    "argv",
    "env",
    "stderr",
    "stdout",
    "secret",
    "secrets",
    "password",
    "api_key",
}


def _assert_no_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _FORBIDDEN_FIELDS, f"forbidden field {path}.{key}"
            _assert_no_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, f"{path}[{index}]")


def test_turn_roundtrip_sanitizes_fields_and_ignores_volatile_timestamps() -> None:
    turn = Turn(
        host_id="turn-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        status="running",
        kind="message",
        title="Worker One",
        summary="Summarizing work",
        started_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:01+00:00",
        completed_at=None,
        source="worker:worker-1",
        origin_command_id="cmd-public",
        meta={
            "safe": {"nested": True, "pane_id": "pane-private"},
            "updated_at": "2026-01-01T00:00:02+00:00",
            "chat_id": 123,
            "token": "secret",
        },
    )
    same_logical_turn = Turn(
        host_id="turn-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        status="running",
        kind="message",
        title="Worker One",
        summary="Summarizing work",
        started_at="2026-01-02T00:00:00+00:00",
        updated_at="2026-01-02T00:00:01+00:00",
        completed_at="2026-01-02T00:00:02+00:00",
        source="worker:worker-1",
        origin_command_id="cmd-public",
        meta={"safe": {"nested": True}, "updated_at": "2026-01-02T00:00:02+00:00"},
    )
    changed_summary = Turn(
        host_id="turn-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        status="running",
        kind="message",
        title="Worker One",
        summary="Different public summary",
        source="worker:worker-1",
        origin_command_id="cmd-public",
        meta={"safe": {"nested": True}},
    )

    payload = json.loads(turn.to_json())

    assert payload["schema_version"] == 1
    assert payload["status"] == "active"
    assert payload["kind"] == "message"
    assert payload["meta"] == {
        "safe": {"nested": True},
        "updated_at": "2026-01-01T00:00:02+00:00",
    }
    assert same_logical_turn.id == turn.id
    assert same_logical_turn.fingerprint == turn.fingerprint
    assert changed_summary.id == turn.id
    assert changed_summary.fingerprint != turn.fingerprint
    assert Turn.from_json(turn.to_json()).to_dict() == payload
    _assert_no_forbidden_fields(payload)


def test_pending_interaction_roundtrip_sanitizes_choices_and_ignores_timestamps() -> None:
    choice = InteractionChoice(
        label="Approve",
        value={"decision": "yes", "backend_target": "agent-private"},
        params={"safe": "kept", "route": "telegram", "terminal_id": "term-private"},
    )
    pending = PendingInteraction(
        host_id="pending-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        kind="confirm",
        question="Delete generated files?",
        choices=[choice],
        status="pending",
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:01+00:00",
        expires_at="2026-01-01T00:05:00+00:00",
        meta={"source": "attention", "message_id": 99},
    )
    same_logical_pending = PendingInteraction(
        host_id="pending-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        kind="confirm_destructive_action",
        question="Delete generated files?",
        choices=[choice],
        status="open",
        created_at="2026-01-02T00:00:00+00:00",
        updated_at="2026-01-02T00:00:01+00:00",
        expires_at="2026-01-02T00:05:00+00:00",
        meta={"source": "attention"},
    )

    payload = json.loads(pending.to_json())

    assert payload["schema_version"] == 1
    assert payload["kind"] == "confirm_destructive_action"
    assert payload["status"] == "open"
    assert payload["choices"][0]["value"] == {"decision": "yes"}
    assert payload["choices"][0]["params"] == {"safe": "kept"}
    assert pending.id == same_logical_pending.id
    assert pending.fingerprint == same_logical_pending.fingerprint
    assert PendingInteraction.from_json(pending.to_json()).to_dict() == payload
    _assert_no_forbidden_fields(payload)


def test_turn_projection_from_snapshot_is_public_safe_and_timestamp_stable() -> None:
    config = Config(host_id="projection-host")
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": "running",
                "space_id": "space-1",
                "last_seen_at": "2026-01-01T00:00:00+00:00",
                "summary": "Current work",
                "backend_target": {"value": "agent-private"},
                "meta": {
                    "origin_command_id": "cmd-public",
                    "pane_id": "pane-private",
                    "safe": "kept",
                },
            }
        ],
    )
    later_snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": "running",
                "space_id": "space-1",
                "last_seen_at": "2026-01-02T00:00:00+00:00",
                "summary": "Current work",
                "meta": {"origin_command_id": "cmd-public", "safe": "kept"},
            }
        ],
    )

    turn = turns_from_snapshot(snapshot)[0]
    later_turn = turns_from_snapshot(later_snapshot)[0]
    payload = turn.to_dict()

    assert payload["host_id"] == "projection-host"
    assert payload["worker_id"] == "worker-1"
    assert payload["source"] == "worker:worker-1"
    assert payload["status"] == "active"
    assert payload["kind"] == "task"
    assert payload["title"] == "Worker One"
    assert payload["origin_command_id"] == "cmd-public"
    assert payload["meta"] == {
        "origin_command_id": "cmd-public",
        "raw_status": "running",
        "safe": "kept",
    }
    assert later_turn.id == turn.id
    assert later_turn.fingerprint == turn.fingerprint
    assert "agent-private" not in json.dumps(payload)
    assert "pane-private" not in json.dumps(payload)
    _assert_no_forbidden_fields(payload)


def test_pending_projection_is_conservative_and_uses_attention_signals() -> None:
    config = Config(host_id="pending-projection-host")
    generic_waiting = project_from_raw(
        config,
        workers=[
            {
                "id": "waiting",
                "name": "Waiting",
                "status": "waiting",
                "summary": "waiting for response",
            }
        ],
    )
    approval_snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "approval",
                "name": "Approval",
                "status": "pending",
                "space_id": "space-1",
                "summary": "human approval required before continuing",
                "meta": {"requires_approval": True, "backend_target": {"value": "agent-private"}},
            }
        ],
    )

    pending = pending_from_snapshot(approval_snapshot)

    assert pending_from_snapshot(generic_waiting) == []
    assert len(pending) == 1
    payload = pending[0].to_dict()
    assert payload["schema_version"] == 1
    assert payload["host_id"] == "pending-projection-host"
    assert payload["worker_id"] == "approval"
    assert payload["space_id"] == "space-1"
    assert payload["kind"] == "approval"
    assert payload["status"] == "open"
    assert payload["choices"] == []
    assert "approval" in payload["question"]
    assert "agent-private" not in json.dumps(payload)
    _assert_no_forbidden_fields(payload)


def test_pending_projection_reuses_public_suggested_actions_as_choices() -> None:
    action = SuggestedAction(
        action_id="approve-action",
        label="Approve",
        tendwire_action="approve",
        params={
            "worker_id": "worker-1",
            "safe": "kept",
            "route": "telegram",
            "terminal_id": "term-private",
        },
    )
    signal = AttentionSignal(
        kind="worker_status",
        severity="warning",
        status="waiting",
        reason="Approval required before continuing",
        source="worker:worker-1",
        updated_at="2026-01-01T00:00:00+00:00",
        suggested_actions=[action],
        meta={"worker_id": "worker-1", "space_id": "space-1", "needs_human": True, "chat_id": 123},
        host_id="choice-host",
    )
    snapshot = Snapshot(
        host_id="choice-host",
        updated_at="2026-01-01T00:00:01+00:00",
        workers=[Worker(id="worker-1", name="Worker One", status="waiting", space_id="space-1")],
        attention=[signal],
    )

    pending = pending_from_snapshot(snapshot)
    payload = pending[0].to_dict()

    assert len(pending) == 1
    assert payload["kind"] == "approval"
    assert payload["choices"] == [
        {
            "choice_id": "approve-action",
            "label": "Approve",
            "params": {"safe": "kept", "worker_id": "worker-1"},
            "value": "approve",
        }
    ]
    assert payload["meta"]["needs_human"] is True
    assert "term-private" not in json.dumps(payload)
    _assert_no_forbidden_fields(payload)


def test_turn_and_pending_payload_fingerprints_ignore_wrapper_timestamps() -> None:
    config = Config(host_id="wrapper-host")
    timestamp_a = datetime(2026, 1, 1, tzinfo=timezone.utc)
    timestamp_b = datetime(2026, 1, 2, tzinfo=timezone.utc)
    health = {
        "name": "herdr",
        "status": "healthy",
        "outcome": "empty_healthy",
        "observed_at": "2026-01-01T00:00:00+00:00",
        "counts": {"spaces": 0, "workers": 0},
    }
    health_later = {
        **health,
        "observed_at": "2026-01-02T00:00:00+00:00",
    }
    snapshot_a = project_from_raw(config, backend_health=[health], timestamp=timestamp_a)
    snapshot_b = project_from_raw(config, backend_health=[health_later], timestamp=timestamp_b)

    turns_payload_a = turns_payload_from_snapshot(snapshot_a)
    turns_payload_b = turns_payload_from_snapshot(snapshot_b)
    pending_payload_a = pending_payload_from_snapshot(snapshot_a)
    pending_payload_b = pending_payload_from_snapshot(snapshot_b)

    assert turns_payload_a["schema_version"] == 1
    assert turns_payload_a["turns"] == []
    assert pending_payload_a["pending_interactions"] == []
    assert turns_payload_a["content_fingerprint"] == turns_payload_b["content_fingerprint"]
    assert pending_payload_a["content_fingerprint"] == pending_payload_b["content_fingerprint"]
    assert json.loads(payload_to_json(turns_payload_a)) == turns_payload_a
    assert json.loads(payload_to_json(pending_payload_a)) == pending_payload_a
    _assert_no_forbidden_fields(turns_payload_a)
    _assert_no_forbidden_fields(pending_payload_a)
