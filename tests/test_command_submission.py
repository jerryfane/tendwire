"""Tests for the authoritative daemon command submission path."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from tendwire.backends.herdr_socket import HerdrSocketTimeoutError
from tendwire.command_submission import submit_command
from tendwire.config import Config
from tendwire.core.commands import (
    STATUS_ACCEPTED,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DUPLICATE_REQUEST,
    STATUS_NOT_FOUND,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_STALE_TARGET,
)
from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from tendwire.store.sqlite import (
    get_command_receipt,
    init_store,
    save_snapshot,
    upsert_worker_bindings,
)


_FORBIDDEN_PUBLIC_KEYS = {
    "pane_id",
    "terminal_id",
    "backend_target",
    "agent_session",
    "argv",
    "shell",
    "target_kind",
    "target_value",
    "private_fingerprint",
}


def _assert_no_private_json(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _FORBIDDEN_PUBLIC_KEYS, f"forbidden field {path}.{key}"
            _assert_no_private_json(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_private_json(item, f"{path}[{index}]")


def _config(tmp_path: Path, *, backend: str = "socket") -> Config:
    return Config(
        host_id="cmd-host",
        data_dir=tmp_path,
        db_path=tmp_path / "commands.db",
        herdr_backend=backend,
    )


def _request(
    *,
    request_id: str = "req-1",
    worker_id: str = "w-1",
    text: str = "hello",
    worker_fingerprint: str | None = None,
) -> dict[str, Any]:
    target: dict[str, Any] = {"worker_id": worker_id}
    if worker_fingerprint is not None:
        target["worker_fingerprint"] = worker_fingerprint
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": request_id,
        "dry_run": False,
        "target": target,
        "instruction": {"text": text},
    }


def _healthy_backend() -> BackendHealth:
    return BackendHealth(
        name="herdr",
        status="healthy",
        outcome="healthy_non_empty",
        observed_at="2026-01-01T00:00:00+00:00",
        counts={"workers": 1},
    )


def _seed(
    config: Config,
    workers: list[Worker],
    bindings: list[WorkerBinding] | None = None,
    *,
    health: BackendHealth | None = None,
) -> None:
    assert config.db_path is not None
    init_store(config.db_path)
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:00:00+00:00",
            workers=workers,
            backend_health=[health or _healthy_backend()],
        ),
    )
    if bindings:
        upsert_worker_bindings(config.db_path, bindings)


def _binding(
    worker: Worker,
    *,
    value: str = "agent-secret",
    sendable: bool = True,
    reason: str | None = None,
    fingerprint: str | None = None,
    private_fingerprint: str = "private-secret",
) -> WorkerBinding:
    return WorkerBinding(
        host_id="cmd-host",
        worker_id=worker.id,
        worker_fingerprint=fingerprint or worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value=value,
        sendable=sendable,
        reason=reason,
        observed_at="2026-01-01T00:00:00+00:00",
        private_fingerprint=private_fingerprint,
    )


class _FakeSocketClient:
    def __init__(self, calls: list[dict[str, Any]], *, raises: BaseException | None = None) -> None:
        self.calls = calls
        self.raises = raises

    def connect(self) -> "_FakeSocketClient":
        return self

    def agent_send(self, params: dict[str, Any], *, timeout: float | None = None) -> dict[str, Any]:
        self.calls.append(dict(params))
        if self.raises is not None:
            raise self.raises
        return {"accepted": True}

    def close(self) -> None:
        return None


def _factory(calls: list[dict[str, Any]], *, raises: BaseException | None = None):
    def make_client(config: Config) -> _FakeSocketClient:
        return _FakeSocketClient(calls, raises=raises)

    return make_client


def test_submit_command_uses_socket_agent_send_once_and_caches_result(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    first = submit_command(config, _request(), socket_client_factory=_factory(calls))
    second = submit_command(config, _request(), socket_client_factory=_factory(calls))
    duplicate = submit_command(
        config,
        _request(text="changed"),
        socket_client_factory=_factory(calls),
    )

    assert first.status == STATUS_ACCEPTED
    assert second.to_dict() == first.to_dict()
    assert duplicate.status == STATUS_DUPLICATE_REQUEST
    assert calls == [{"agent_id": "agent-secret", "text": "hello"}]

    assert config.db_path is not None
    receipt = get_command_receipt(config.db_path, "cmd-host", "req-1", "send_instruction")
    assert receipt is not None
    assert receipt["status"] == STATUS_ACCEPTED
    assert receipt["uncertain"] is False

    with sqlite3.connect(str(config.db_path)) as conn:
        event_rows = conn.execute("SELECT event_type, payload_json FROM events ORDER BY id").fetchall()
        command_row = conn.execute(
            "SELECT request_json, result_json FROM commands WHERE request_id = 'req-1'"
        ).fetchone()
    assert [row[0] for row in event_rows] == [
        "snapshot.saved",
        "command.reserved",
        "command.send_started",
        "command.submitted",
        "command.cached",
        "command.duplicate",
    ]
    assert command_row is not None
    assert json.loads(command_row[0])["request_id"] == "req-1"

    public_surfaces = [
        first.to_dict(),
        duplicate.to_dict(),
        json.loads(receipt["result_json"]),
        json.loads(command_row[1]),
        *[json.loads(row[1]) for row in event_rows],
    ]
    encoded = json.dumps(public_surfaces)
    assert "agent-secret" not in encoded
    assert "private-secret" not in encoded
    for surface in public_surfaces:
        _assert_no_private_json(surface)


def test_submit_command_backend_unavailable_prevents_not_found_and_send(tmp_path: Path) -> None:
    config = _config(tmp_path)
    health = BackendHealth(
        name="herdr",
        status="degraded",
        outcome="protocol_error",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    _seed(config, [], [], health=health)
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _request(worker_id="missing", request_id="degraded-1"),
        socket_client_factory=_factory(calls),
    )

    assert envelope.status == STATUS_BACKEND_UNAVAILABLE
    assert calls == []
    assert envelope.status != STATUS_NOT_FOUND


def test_submit_command_missing_worker_can_return_not_found_when_backend_healthy(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _seed(config, [], [])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _request(worker_id="missing", request_id="missing-1"),
        socket_client_factory=_factory(calls),
    )

    assert envelope.status == STATUS_NOT_FOUND
    assert calls == []


def test_submit_command_rejects_stale_worker_fingerprint_and_binding(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker, fingerprint="old-fingerprint")])
    calls: list[dict[str, Any]] = []

    stale_request = submit_command(
        config,
        _request(request_id="stale-request", worker_fingerprint="old-fingerprint"),
        socket_client_factory=_factory(calls),
    )
    stale_binding = submit_command(
        config,
        _request(request_id="stale-binding"),
        socket_client_factory=_factory(calls),
    )

    assert stale_request.status == STATUS_STALE_TARGET
    assert stale_binding.status == STATUS_STALE_TARGET
    assert calls == []


def test_submit_command_rejects_duplicate_missing_and_unsendable_bindings(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")

    _seed(config, [worker], [])
    calls: list[dict[str, Any]] = []
    missing = submit_command(
        config,
        _request(request_id="missing-binding"),
        socket_client_factory=_factory(calls),
    )
    assert missing.status == STATUS_BACKEND_UNSUPPORTED

    config = _config(tmp_path / "unsendable")
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker, sendable=False, reason="disabled")])
    unsendable = submit_command(
        config,
        _request(request_id="unsendable-binding"),
        socket_client_factory=_factory(calls),
    )
    assert unsendable.status == STATUS_BACKEND_UNSUPPORTED

    config = _config(tmp_path / "duplicate")
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [
            _binding(worker, value="agent-a", private_fingerprint="private-a"),
            _binding(worker, value="agent-b", private_fingerprint="private-b"),
        ],
    )
    duplicate = submit_command(
        config,
        _request(request_id="duplicate-binding"),
        socket_client_factory=_factory(calls),
    )
    assert duplicate.status == STATUS_AMBIGUOUS_BACKEND_TARGET
    assert calls == []


def test_submit_command_timeout_after_send_start_is_uncertain_and_not_retried(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config,
        _request(request_id="timeout-1"),
        socket_client_factory=_factory(calls, raises=HerdrSocketTimeoutError("timeout")),
    )
    second = submit_command(
        config,
        _request(request_id="timeout-1"),
        socket_client_factory=_factory(calls),
    )

    assert first.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert second.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert calls == [{"agent_id": "agent-secret", "text": "hello"}]

    assert config.db_path is not None
    receipt = get_command_receipt(config.db_path, "cmd-host", "timeout-1", "send_instruction")
    assert receipt is not None
    assert receipt["uncertain"] is True
    with sqlite3.connect(str(config.db_path)) as conn:
        events = [row[0] for row in conn.execute("SELECT event_type FROM events ORDER BY id").fetchall()]
    assert "command.send_started" in events
    assert "command.submitted" in events
    assert "command.uncertain" in events
