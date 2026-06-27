"""Tests for core model serialization and snapshot shape."""

from __future__ import annotations

import json

from tendwire.core.attention import update_snapshot_attention
from tendwire.core.models import (
    AttentionSignal,
    Snapshot,
    Space,
    Worker,
    utc_timestamp,
)
from tendwire.core.projector import project_empty, project_from_raw
from tendwire.config import Config


def test_space_to_dict_roundtrip() -> None:
    space = Space(id="space-1", name="alpha", status="active")
    data = space.to_dict()
    restored = Space.from_dict(data)
    assert restored == space


def test_worker_to_dict_roundtrip() -> None:
    worker = Worker(id="w-1", name="agent-1", status="error", space_id="space-1")
    data = worker.to_dict()
    restored = Worker.from_dict(data)
    assert restored == worker


def test_attention_signal_to_dict_roundtrip() -> None:
    signal = AttentionSignal(id="a-1", level="warn", reason="stalled", source="worker:w-1")
    data = signal.to_dict()
    restored = AttentionSignal.from_dict(data)
    assert restored == signal


def test_snapshot_to_json_has_required_keys() -> None:
    config = Config(host_id="testhost")
    snapshot = project_empty(config)
    payload = json.loads(snapshot.to_json())

    assert set(payload.keys()) == {
        "host_id",
        "updated_at",
        "spaces",
        "workers",
        "attention",
    }
    assert payload["host_id"] == "testhost"
    assert isinstance(payload["updated_at"], str)
    assert payload["spaces"] == []
    assert payload["workers"] == []
    assert isinstance(payload["attention"], list)


def test_snapshot_from_json_roundtrip() -> None:
    config = Config(host_id="testhost")
    snapshot = project_empty(config)
    restored = Snapshot.from_json(snapshot.to_json())
    assert restored == snapshot


def test_utc_timestamp_is_iso8601_with_z_offset() -> None:
    ts = utc_timestamp()
    assert ts.endswith("+00:00")
    assert "T" in ts


def test_projected_snapshot_attention_from_critical_worker() -> None:
    config = Config(host_id="testhost")
    snapshot = project_from_raw(
        config,
        workers=[{"id": "w-1", "name": "agent-1", "status": "error"}],
    )
    levels = {a.level for a in snapshot.attention}
    assert "critical" in levels
    assert any("w-1" in a.source for a in snapshot.attention)
