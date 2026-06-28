"""Contract tests for neutral model serialization and fingerprints."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from tendwire.config import Config
from tendwire.core.models import (
    AttentionSignal,
    Snapshot,
    Space,
    SuggestedAction,
    Worker,
    normalize_status,
    utc_timestamp,
)
from tendwire.core.projector import project_empty, project_from_raw


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
}


def _assert_no_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _FORBIDDEN_FIELDS, f"forbidden field {path}.{key}"
            _assert_no_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, f"{path}[{index}]")


def _snapshot_payload(snapshot: Snapshot) -> dict[str, Any]:
    return json.loads(snapshot.to_json())


def _stable_item_key(item: dict[str, Any]) -> str:
    stable_id = item.get("id") or item.get("fingerprint")
    if stable_id is not None:
        return str(stable_id)
    return json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _strip_volatile_fingerprint_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_volatile_fingerprint_fields(item)
            for key, item in value.items()
            if key not in {"updated_at", "content_fingerprint"}
        }
    if isinstance(value, list):
        return [_strip_volatile_fingerprint_fields(item) for item in value]
    return value



def _expected_snapshot_fingerprint(payload: dict[str, Any]) -> str:
    content = _strip_volatile_fingerprint_fields(
        json.loads(json.dumps(payload, ensure_ascii=False))
    )
    for collection in ("spaces", "workers", "attention"):
        content[collection] = sorted(content.get(collection, []), key=_stable_item_key)
    encoded = json.dumps(
        content,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def test_space_worker_and_attention_serialization_include_contract_fields() -> None:
    space = Space.from_dict(
        {
            "id": "space-1",
            "name": "Alpha",
            "status": "ACTIVE",
            "updated_at": "2026-01-01T00:00:00+00:00",
            "status_line": "all green",
            "meta": {"safe": True},
        }
    )
    worker = Worker.from_dict(
        {
            "id": "worker-1",
            "name": "Agent One",
            "status": "waiting",
            "space_id": "space-1",
            "last_seen_at": "2026-01-01T00:00:01+00:00",
            "summary": "awaiting input",
            "meta": {"safe": "worker"},
        }
    )
    signal = AttentionSignal.from_dict(
        {
            "kind": "worker_status",
            "severity": "warning",
            "status": "blocked",
            "reason": "Worker is blocked",
            "source": "worker:worker-1",
            "updated_at": "2026-01-01T00:00:02+00:00",
            "suggested_actions": [
                {
                    "action_id": "inspect-worker",
                    "label": "Inspect worker",
                    "tendwire_action": "snapshot",
                    "params": {"worker_id": "worker-1"},
                }
            ],
            "meta": {"safe": "attention"},
        }
    )

    space_payload = space.to_dict()
    worker_payload = worker.to_dict()
    signal_payload = signal.to_dict()

    assert space_payload["status"] == "active"
    assert {"updated_at", "status_line", "fingerprint", "meta"} <= set(space_payload)
    assert {"last_seen_at", "summary", "fingerprint", "meta"} <= set(worker_payload)
    assert {
        "id",
        "fingerprint",
        "kind",
        "severity",
        "status",
        "reason",
        "source",
        "updated_at",
        "suggested_actions",
        "meta",
    } <= set(signal_payload)
    assert len(space_payload["fingerprint"]) == 24
    assert len(worker_payload["fingerprint"]) == 24
    assert len(signal_payload["fingerprint"]) == 24
    assert signal_payload["id"]
    assert signal_payload["suggested_actions"][0]["action_id"] == "inspect-worker"
    assert signal_payload["suggested_actions"][0]["tendwire_action"] == "snapshot"
    assert "command" not in signal_payload["suggested_actions"][0]
    assert Space.from_dict(space_payload).to_dict() == space_payload
    assert Worker.from_dict(worker_payload).to_dict() == worker_payload
    assert AttentionSignal.from_dict(signal_payload).to_dict() == signal_payload
    _assert_no_forbidden_fields(signal_payload)


def test_worker_private_backend_target_and_raw_backend_meta_do_not_serialize() -> None:
    worker = Worker(
        id="public-worker",
        name="Agent One",
        status="active",
        meta={
            "safe": "kept",
            "terminal_id": "term-1",
            "pane_id": "pane-1",
            "agent_session": {"value": "sess-1"},
            "session_id": "session-1",
            "backend_target": {"kind": "agent_id", "value": "agent-1"},
        },
        backend_target={"kind": "agent_id", "value": "agent-1"},
    )

    payload = worker.to_dict()

    assert payload["id"] == "public-worker"
    assert payload["meta"] == {"safe": "kept"}
    assert worker.backend_target == {"kind": "agent_id", "value": "agent-1"}
    _assert_no_forbidden_fields(payload)


def test_attention_signal_direct_identity_and_dataclass_actions_are_neutral() -> None:
    action = SuggestedAction(
        action_id="inspect-worker",
        label="Inspect worker",
        command="tendwire snapshot --json",
        params={"worker_id": "worker-1", "route": "telegram", "safe": "kept"},
    )

    def signal(**overrides: Any) -> AttentionSignal:
        data = {
            "kind": "worker_status",
            "severity": "warning",
            "status": "blocked",
            "reason": "Worker is blocked",
            "source": "worker:worker-1",
            "updated_at": "2026-01-01T00:00:00+00:00",
            "suggested_actions": [action],
            "host_id": "attention-host",
        }
        data.update(overrides)
        return AttentionSignal(**data)

    base = signal()
    same = signal(updated_at="2026-01-02T00:00:00+00:00")
    changed_status = signal(status="failed")
    changed_reason = signal(reason="Worker is failed")
    changed_source = signal(source="worker:worker-2")
    payload = base.to_dict()

    assert same.id == base.id
    assert same.fingerprint == base.fingerprint
    assert changed_status.fingerprint != base.fingerprint
    assert changed_reason.fingerprint != base.fingerprint
    assert changed_source.fingerprint != base.fingerprint
    assert payload["suggested_actions"][0]["params"] == {
        "worker_id": "worker-1",
        "safe": "kept",
    }
    assert payload["suggested_actions"][0]["tendwire_action"] == "tendwire snapshot --json"
    assert "command" not in payload["suggested_actions"][0]
    _assert_no_forbidden_fields(payload)


def test_done_status_aliases_canonicalize_to_sendable_done() -> None:
    for raw_status in ("done", "complete", "completed", "success"):
        assert normalize_status(raw_status) == "done"


def test_snapshot_json_has_schema_version_content_fingerprint_and_legacy_keys() -> None:
    config = Config(host_id="testhost")
    snapshot = project_empty(config)
    payload = _snapshot_payload(snapshot)

    assert {
        "schema_version",
        "content_fingerprint",
        "host_id",
        "updated_at",
        "spaces",
        "workers",
        "attention",
    } <= set(payload)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "testhost"
    assert len(payload["content_fingerprint"]) == 24
    assert payload["content_fingerprint"] == _expected_snapshot_fingerprint(payload)
    assert isinstance(payload["updated_at"], str)
    assert isinstance(payload["spaces"], list)
    assert isinstance(payload["workers"], list)
    assert isinstance(payload["attention"], list)
    _assert_no_forbidden_fields(payload)


def test_snapshot_from_json_roundtrip_preserves_fingerprints() -> None:
    config = Config(host_id="testhost")
    snapshot = project_from_raw(
        config,
        spaces=[{"id": "space-1", "name": "Alpha", "status": "active"}],
        workers=[{"id": "worker-1", "name": "Agent One", "status": "blocked"}],
    )

    restored = Snapshot.from_json(snapshot.to_json())

    assert restored == snapshot
    assert restored.content_fingerprint == snapshot.content_fingerprint
    assert restored.workers[0].fingerprint == snapshot.workers[0].fingerprint
    assert restored.attention[0].fingerprint == snapshot.attention[0].fingerprint


def test_attention_ids_are_stable_and_fingerprints_track_logical_changes() -> None:
    config = Config(host_id="attention-host")
    timestamp_a = datetime(2026, 1, 1, tzinfo=timezone.utc)
    timestamp_b = datetime(2026, 1, 2, tzinfo=timezone.utc)

    def signal_for(worker: dict[str, Any], timestamp: datetime = timestamp_a) -> dict[str, Any]:
        snapshot = project_from_raw(config, workers=[worker], timestamp=timestamp)
        payload = _snapshot_payload(snapshot)
        assert len(payload["attention"]) == 1
        return payload["attention"][0]

    base = signal_for({"id": "worker-1", "name": "Agent One", "status": "blocked"})
    same = signal_for(
        {"id": "worker-1", "name": "Agent One", "status": "blocked"},
        timestamp=timestamp_b,
    )
    changed_status = signal_for(
        {"id": "worker-1", "name": "Agent One", "status": "failed"}
    )
    changed_reason = signal_for(
        {"id": "worker-1", "name": "Agent Two", "status": "blocked"}
    )
    changed_source = signal_for(
        {"id": "worker-2", "name": "Agent One", "status": "blocked"}
    )

    assert same["id"] == base["id"]
    assert same["fingerprint"] == base["fingerprint"]
    assert same["updated_at"] != base["updated_at"]
    assert changed_status["fingerprint"] != base["fingerprint"]
    assert changed_reason["fingerprint"] != base["fingerprint"]
    assert changed_source["fingerprint"] != base["fingerprint"]


def test_snapshot_content_fingerprint_ignores_updated_at_and_sorts_content() -> None:
    config = Config(host_id="fingerprint-host")
    timestamp_a = datetime(2026, 1, 1, tzinfo=timezone.utc)
    timestamp_b = datetime(2026, 1, 2, tzinfo=timezone.utc)
    spaces = [
        {"id": "space-b", "name": "Beta", "status": "active"},
        {"id": "space-a", "name": "Alpha", "status": "idle"},
    ]
    workers = [
        {"id": "worker-b", "name": "Bravo", "status": "active", "space_id": "space-b"},
        {"id": "worker-a", "name": "Alpha", "status": "active", "space_id": "space-a"},
    ]

    snapshot_a = project_from_raw(config, spaces=spaces, workers=workers, timestamp=timestamp_a)
    snapshot_b = project_from_raw(
        config,
        spaces=list(reversed(spaces)),
        workers=list(reversed(workers)),
        timestamp=timestamp_b,
    )
    changed = project_from_raw(
        config,
        spaces=spaces,
        workers=[
            {"id": "worker-b", "name": "Bravo", "status": "active", "space_id": "space-b"},
            {"id": "worker-a", "name": "Alpha", "status": "waiting", "space_id": "space-a"},
        ],
        timestamp=timestamp_b,
    )

    payload_a = _snapshot_payload(snapshot_a)
    payload_b = _snapshot_payload(snapshot_b)

    assert payload_a["updated_at"] != payload_b["updated_at"]
    assert payload_a["content_fingerprint"] == payload_b["content_fingerprint"]
    assert payload_a["content_fingerprint"] == _expected_snapshot_fingerprint(payload_a)
    assert changed.content_fingerprint != snapshot_a.content_fingerprint


def test_worker_attention_includes_local_target_metadata() -> None:
    config = Config(host_id="attention-host")
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Agent One", "status": "blocked", "space_id": "space-1"}],
    )
    signal = _snapshot_payload(snapshot)["attention"][0]

    assert signal["meta"]["needs_human"] is True
    assert signal["meta"]["worker_id"] == "worker-1"
    assert signal["meta"]["space_id"] == "space-1"


def test_project_from_raw_normalizes_status_and_strips_connector_fields() -> None:
    config = Config(host_id="strip-host")
    snapshot = project_from_raw(
        config,
        spaces=[
            {
                "id": "space-1",
                "name": "Alpha",
                "status": "running",
                "telegram": "forbidden",
                "chat_id": 123,
                "meta": {"token": "secret", "safe": "kept"},
            }
        ],
        workers=[
            {
                "id": "worker-1",
                "name": "Agent One",
                "status": "panic",
                "space_id": "space-1",
                "topic_id": 456,
                "message_id": 789,
                "delivery": {"route": "telegram"},
                "meta": {"bot_token": "secret", "safe": "worker-kept"},
            }
        ],
    )
    payload = _snapshot_payload(snapshot)

    assert payload["spaces"][0]["status"] == "active"
    assert payload["spaces"][0]["meta"]["safe"] == "kept"
    assert payload["workers"][0]["status"] == "failed"
    assert payload["workers"][0]["meta"]["raw_status"] == "panic"
    assert payload["workers"][0]["meta"]["safe"] == "worker-kept"
    _assert_no_forbidden_fields(payload)


def test_utc_timestamp_is_iso8601_with_z_offset() -> None:
    ts = utc_timestamp()
    assert ts.endswith("+00:00")
    assert "T" in ts
