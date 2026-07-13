"""Tests for the authoritative daemon command submission path."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
import tendwire.command_submission as command_submission

from tendwire.backends.herdr_protocol import HerdrProtocolError
from tendwire.backends.herdr_socket import (
    HerdrSocketDisconnectedError,
    HerdrSocketTimeoutError,
)
from tendwire.command_submission import submit_command
from tendwire.config import Config
from tendwire.core.commands import (
    STATUS_ACCEPTED,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DUPLICATE_INSTRUCTION,
    STATUS_DUPLICATE_REQUEST,
    STATUS_INVALID_REQUEST,
    STATUS_NOT_FOUND,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_STALE_TARGET,
)
from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from tendwire.core.turns import PendingObservation, PendingObservedChoice
from tendwire.store.sqlite import (
    apply_backend_pending_observation,
    get_command_receipt,
    init_store,
    merge_turn_content,
    pending_payload_from_store,
    save_snapshot,
    turns_payload_from_store,
    upsert_command_pending_turn,
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
    target_kind: str = "agent_id",
    value: str = "agent-secret",
    sendable: bool = True,
    reason: str | None = None,
    fingerprint: str | None = None,
    private_fingerprint: str = "private-secret",
    turn_target_kind: str | None = "pane_id",
    turn_target_value: str | None = "pane-secret",
) -> WorkerBinding:
    return WorkerBinding(
        host_id="cmd-host",
        worker_id=worker.id,
        worker_fingerprint=fingerprint or worker.fingerprint,
        backend="herdr",
        target_kind=target_kind,
        target_value=value,
        turn_target_kind=turn_target_kind,
        turn_target_value=turn_target_value,
        sendable=sendable,
        reason=reason,
        observed_at="2026-01-01T00:00:00+00:00",
        private_fingerprint=private_fingerprint,
    )


class _FakeSocketClient:
    def __init__(
        self,
        calls: list[dict[str, Any]],
        *,
        raises: BaseException | None = None,
        pane_id: str = "pane-secret",
    ) -> None:
        self.calls = calls
        self.raises = raises
        self.pane_id = pane_id

    def connect(self) -> "_FakeSocketClient":
        return self

    def request(self, method: str, params: dict[str, Any], *, timeout: float | None = None) -> dict[str, Any]:
        self.calls.append({"method": method, "params": dict(params)})
        if self.raises is not None and method == "pane.send_text":
            raise self.raises
        if method == "agent.get":
            return {"result": {"agent": {"pane_id": self.pane_id}}}
        return {"accepted": True}

    def close(self) -> None:
        return None


def _factory(calls: list[dict[str, Any]], *, raises: BaseException | None = None, pane_id: str = "pane-secret"):
    def make_client(config: Config) -> _FakeSocketClient:
        return _FakeSocketClient(calls, raises=raises, pane_id=pane_id)

    return make_client


def _expected_submit_calls(target: str = "agent-secret", *, pane_id: str = "pane-secret") -> list[dict[str, Any]]:
    return [
        {"method": "agent.get", "params": {"target": target}},
        *_expected_private_clear_calls(pane_id),
        {"method": "pane.send_text", "params": {"pane_id": pane_id, "text": "hello"}},
        {"method": "pane.send_keys", "params": {"pane_id": pane_id, "keys": ["enter"]}},
    ]


def _expected_private_clear_calls(pane_id: str = "pane-secret") -> list[dict[str, Any]]:
    return [
        {"method": "pane.send_keys", "params": {"pane_id": pane_id, "keys": ["ctrl+u"]}},
        {"method": "pane.send_keys", "params": {"pane_id": pane_id, "keys": ["ctrl+a", "ctrl+k"]}},
        {"method": "pane.send_keys", "params": {"pane_id": pane_id, "keys": ["ctrl+a", "backspace"]}},
    ]

@pytest.mark.parametrize(
    ("request_id", "include_request_id"),
    [
        (None, False),
        (None, True),
        ("", True),
        ("   \t", True),
    ],
)
def test_submit_command_rejects_invalid_request_id_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request_id: Any,
    include_request_id: bool,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    calls: list[str] = []

    def guarded_observation(config: Config) -> Snapshot:
        calls.append("observe")
        raise AssertionError("invalid request_id must not observe")

    def guarded_socket_factory(config: Config) -> _FakeSocketClient:
        calls.append("socket")
        raise AssertionError("invalid request_id must not construct a socket client")

    monkeypatch.setattr("tendwire.command_submission.project_from_observations", guarded_observation)
    payload = _request()
    if include_request_id:
        payload["request_id"] = request_id
    else:
        del payload["request_id"]

    envelope = submit_command(config, payload, socket_client_factory=guarded_socket_factory)

    assert envelope.status == STATUS_INVALID_REQUEST
    assert envelope.error is not None
    assert envelope.error["code"] == STATUS_INVALID_REQUEST
    assert calls == []
    with sqlite3.connect(str(config.db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM commands").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


@pytest.mark.parametrize(
    ("label", "factory_exception", "connect_exception"),
    [
        ("factory", RuntimeError("raw setup failure with private detail"), None),
        ("path", ValueError("bad socket path /private/herdr.sock"), None),
        ("missing", None, FileNotFoundError("missing /private/herdr.sock")),
        ("refused", None, ConnectionRefusedError("refused /private/herdr.sock")),
        ("permission", None, PermissionError("denied /private/herdr.sock")),
    ],
)
def test_submit_command_socket_setup_failures_are_backend_unavailable(
    tmp_path: Path,
    label: str,
    factory_exception: BaseException | None,
    connect_exception: BaseException | None,
) -> None:
    config = _config(tmp_path / label)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    class SetupFailsClient:
        def connect(self) -> "SetupFailsClient":
            assert connect_exception is not None
            raise connect_exception

        def request(self, method: str, params: dict[str, Any], *, timeout: float | None = None) -> dict[str, Any]:
            calls.append({"method": method, "params": dict(params)})
            raise AssertionError("private send request must not run before setup succeeds")

        def close(self) -> None:
            return None

    def make_client(config: Config) -> SetupFailsClient:
        if factory_exception is not None:
            raise factory_exception
        return SetupFailsClient()

    envelope = submit_command(
        config,
        _request(request_id=f"setup-{label}"),
        socket_client_factory=make_client,
    )

    assert envelope.status == STATUS_BACKEND_UNAVAILABLE
    assert envelope.error is not None
    assert envelope.error["code"] == STATUS_BACKEND_UNAVAILABLE
    assert "private" not in json.dumps(envelope.to_dict())
    assert calls == []
    assert envelope.status != STATUS_NOT_FOUND
    assert envelope.status != STATUS_REQUEST_STATE_UNCERTAIN
    assert config.db_path is not None
    receipt = get_command_receipt(config.db_path, "cmd-host", f"setup-{label}", "send_instruction")
    assert receipt is not None
    assert receipt["status"] == STATUS_BACKEND_UNAVAILABLE
    assert receipt["uncertain"] is False
    with sqlite3.connect(str(config.db_path)) as conn:
        events = [row[0] for row in conn.execute("SELECT event_type FROM events ORDER BY id").fetchall()]
    assert "command.send_started" not in events


@pytest.mark.parametrize(
    "exc",
    [
        HerdrSocketDisconnectedError("disconnected"),
        HerdrProtocolError("malformed response"),
        OSError("transport failed after write"),
    ],
)
def test_submit_command_post_send_transport_failures_are_uncertain(
    tmp_path: Path,
    exc: BaseException,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _request(request_id=f"uncertain-{type(exc).__name__}"),
        socket_client_factory=_factory(calls, raises=exc),
    )

    assert envelope.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert envelope.status != STATUS_BACKEND_UNAVAILABLE
    assert calls == [
        {"method": "agent.get", "params": {"target": "agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_text", "params": {"pane_id": "pane-secret", "text": "hello"}},
    ]
    assert config.db_path is not None
    receipt = get_command_receipt(config.db_path, "cmd-host", f"uncertain-{type(exc).__name__}", "send_instruction")
    assert receipt is not None
    assert receipt["uncertain"] is True
    with sqlite3.connect(str(config.db_path)) as conn:
        events = [row[0] for row in conn.execute("SELECT event_type FROM events ORDER BY id").fetchall()]
    assert "command.send_started" in events
    assert "command.submitted" in events


def test_submit_command_uses_socket_pane_input_once_and_caches_result(tmp_path: Path) -> None:
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
    assert first.result == {
        "target": {"worker_id": "w-1"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "target_state_at_send": "active",
        "observed_turn_state": "pending_observation",
    }
    assert second.to_dict() == first.to_dict()
    assert duplicate.status == STATUS_DUPLICATE_REQUEST
    assert calls == _expected_submit_calls()

    assert config.db_path is not None
    receipt = get_command_receipt(config.db_path, "cmd-host", "req-1", "send_instruction")
    assert receipt is not None
    assert receipt["status"] == STATUS_ACCEPTED
    assert receipt["uncertain"] is False
    turns_payload = turns_payload_from_store(config.db_path, "cmd-host")
    command_turns = [
        turn
        for turn in turns_payload["turns"]
        if turn.get("origin_command_id") == "req-1"
    ]
    assert len(command_turns) == 1
    command_turn = command_turns[0]
    assert command_turn["worker_id"] == "w-1"
    assert command_turn["worker_fingerprint"] == worker.fingerprint
    assert command_turn["status"] == "active"
    assert command_turn["user_text"] == "hello"
    assert command_turn["assistant_final_text"] is None
    assert command_turn["complete"] is False
    assert command_turn["has_open_turn"] is True
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:01:00+00:00",
            workers=[worker],
            backend_health=[_healthy_backend()],
        ),
    )
    turns_after_snapshot = turns_payload_from_store(config.db_path, "cmd-host")
    assert any(turn.get("origin_command_id") == "req-1" for turn in turns_after_snapshot["turns"])

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
        turns_payload,
        turns_after_snapshot,
        json.loads(receipt["result_json"]),
        json.loads(command_row[1]),
        *[json.loads(row[1]) for row in event_rows],
    ]
    encoded = json.dumps(public_surfaces)
    assert "agent-secret" not in encoded
    assert "private-secret" not in encoded
    for surface in public_surfaces:
        _assert_no_private_json(surface)


def test_submit_command_suppresses_recent_same_worker_long_instruction_with_new_request_id(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    text = "When this exact long Telegram instruction appears again, it should be treated as replay."

    first = submit_command(
        config,
        _request(request_id="long-1", text=text),
        socket_client_factory=_factory(calls),
    )
    second = submit_command(
        config,
        _request(request_id="long-2", text=text),
        socket_client_factory=_factory(calls),
    )

    assert first.status == STATUS_ACCEPTED
    assert second.ok is True
    assert second.status == STATUS_DUPLICATE_INSTRUCTION
    assert second.result == {
        "target": {"worker_id": "w-1"},
        "delivery_state": "duplicate_suppressed",
        "deduplicated": True,
        "replay_window_seconds": 21600,
    }
    assert calls == [
        {"method": "agent.get", "params": {"target": "agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_text", "params": {"pane_id": "pane-secret", "text": text}},
        {"method": "pane.send_keys", "params": {"pane_id": "pane-secret", "keys": ["enter"]}},
    ]

    assert config.db_path is not None
    first_receipt = get_command_receipt(config.db_path, "cmd-host", "long-1", "send_instruction")
    second_receipt = get_command_receipt(config.db_path, "cmd-host", "long-2", "send_instruction")
    assert first_receipt is not None
    assert first_receipt["status"] == STATUS_ACCEPTED
    assert second_receipt is not None
    assert second_receipt["status"] == STATUS_DUPLICATE_INSTRUCTION
    with sqlite3.connect(str(config.db_path)) as conn:
        events = [row[0] for row in conn.execute("SELECT event_type FROM events ORDER BY id").fetchall()]
    assert events == [
        "snapshot.saved",
        "command.reserved",
        "command.send_started",
        "command.submitted",
        "command.reserved",
        "command.duplicate_instruction",
    ]
    public_json = json.dumps([second.to_dict(), json.loads(second_receipt["result_json"])])
    assert text not in public_json
    assert "agent-secret" not in public_json
    assert "private-secret" not in public_json


def test_submit_command_allows_same_instruction_after_worker_fingerprint_changes(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    old_worker = Worker(id="w-1", name="Alpha", status="active", fingerprint="old-fp")
    new_worker = Worker(id="w-1", name="Alpha", status="active", fingerprint="new-fp")
    _seed(
        config,
        [old_worker],
        [_binding(old_worker, value="old-agent-secret", private_fingerprint="old-private-secret")],
    )
    calls: list[dict[str, Any]] = []
    text = "When this exact long Telegram instruction appears for a new binding, it should send."

    first = submit_command(
        config,
        _request(request_id="fingerprint-1", text=text, worker_fingerprint="old-fp"),
        socket_client_factory=_factory(calls),
    )
    assert first.status == STATUS_ACCEPTED

    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:01:00+00:00",
            workers=[new_worker],
            backend_health=[_healthy_backend()],
        ),
    )
    upsert_worker_bindings(
        config.db_path,
        [_binding(new_worker, value="new-agent-secret", private_fingerprint="new-private-secret")],
    )

    second = submit_command(
        config,
        _request(request_id="fingerprint-2", text=text, worker_fingerprint="new-fp"),
        socket_client_factory=_factory(calls),
    )

    assert second.status == STATUS_ACCEPTED
    assert calls == [
        {"method": "agent.get", "params": {"target": "old-agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_text", "params": {"pane_id": "pane-secret", "text": text}},
        {"method": "pane.send_keys", "params": {"pane_id": "pane-secret", "keys": ["enter"]}},
        {"method": "agent.get", "params": {"target": "new-agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_text", "params": {"pane_id": "pane-secret", "text": text}},
        {"method": "pane.send_keys", "params": {"pane_id": "pane-secret", "keys": ["enter"]}},
    ]


def test_submit_command_waits_for_text_to_stage_before_enter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        "tendwire.command_submission.time.sleep",
        lambda seconds: calls.append({"sleep": seconds}),
    )

    envelope = submit_command(config, _request(), socket_client_factory=_factory(calls))

    assert envelope.status == STATUS_ACCEPTED
    assert calls == [
        {"method": "agent.get", "params": {"target": "agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_text", "params": {"pane_id": "pane-secret", "text": "hello"}},
        {"sleep": 0.2},
        {"method": "pane.send_keys", "params": {"pane_id": "pane-secret", "keys": ["enter"]}},
    ]


def test_submit_command_reports_submitted_transport_and_worker_state(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="working")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(config, _request(), socket_client_factory=_factory(calls))

    assert envelope.status == STATUS_ACCEPTED
    assert envelope.result == {
        "target": {"worker_id": "w-1"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "target_state_at_send": "active",
        "observed_turn_state": "pending_observation",
    }


def test_submit_command_marks_idle_worker_delivery_as_submitted(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="idle")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(config, _request(), socket_client_factory=_factory(calls))

    assert envelope.status == STATUS_ACCEPTED
    assert envelope.result == {
        "target": {"worker_id": "w-1"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "target_state_at_send": "idle",
        "observed_turn_state": "pending_observation",
    }


def test_submit_command_terminal_binding_resolves_pane_and_submits_input(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker, target_kind="terminal_id", value="term-secret")])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(config, _request(), socket_client_factory=_factory(calls, pane_id="pane-private"))

    assert envelope.status == STATUS_ACCEPTED
    assert calls == _expected_submit_calls("term-secret", pane_id="pane-private")
    public_json = json.dumps(envelope.to_dict())
    assert "term-secret" not in public_json
    assert "pane-private" not in public_json
    _assert_no_private_json(envelope.to_dict())


def test_submit_command_pane_binding_submits_without_public_pane_leak(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker, target_kind="pane_id", value="pane-private")])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(config, _request(), socket_client_factory=_factory(calls))

    assert envelope.status == STATUS_ACCEPTED
    assert calls == [
        *_expected_private_clear_calls("pane-private"),
        {"method": "pane.send_text", "params": {"pane_id": "pane-private", "text": "hello"}},
        {"method": "pane.send_keys", "params": {"pane_id": "pane-private", "keys": ["enter"]}},
    ]
    public_json = json.dumps(envelope.to_dict())
    assert "pane-private" not in public_json
    _assert_no_private_json(envelope.to_dict())


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
    assert calls == [
        {"method": "agent.get", "params": {"target": "agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_text", "params": {"pane_id": "pane-secret", "text": "hello"}},
    ]

    assert config.db_path is not None
    receipt = get_command_receipt(config.db_path, "cmd-host", "timeout-1", "send_instruction")
    assert receipt is not None
    assert receipt["uncertain"] is True
    with sqlite3.connect(str(config.db_path)) as conn:
        events = [row[0] for row in conn.execute("SELECT event_type FROM events ORDER BY id").fetchall()]
    assert "command.send_started" in events
    assert "command.submitted" in events
    assert "command.uncertain" in events


def _answer_request(
    *,
    request_id: str = "answer-1",
    dry_run: bool = False,
    pending_id: str = "pending-public",
    pending_fingerprint: str = "revision-public",
    choice_id: str = "choice-public",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "answer_pending",
        "request_id": request_id,
        "dry_run": dry_run,
        "params": {
            "pending_id": pending_id,
            "pending_fingerprint": pending_fingerprint,
            "choice_id": choice_id,
        },
    }


class _PendingClaim:
    def __init__(
        self,
        status: str,
        worker: Worker,
        *,
        claim_token: str | None,
        private_fingerprint: str = "binding-private",
        turn_target_value: str = "pane-secret",
        picker_ordinal: int = 2,
    ) -> None:
        self.status = status
        self.claim_token = claim_token
        self.worker_id = worker.id if status in {"claimed", "validated"} else None
        self.worker_fingerprint = worker.fingerprint if status in {"claimed", "validated"} else None
        self.binding_private_fingerprint = (
            private_fingerprint if status in {"claimed", "validated"} else None
        )
        self.turn_target_value = (
            turn_target_value if status in {"claimed", "validated"} else None
        )
        self.picker_ordinal = picker_ordinal if status in {"claimed", "validated"} else None


class _PendingSend:
    def __init__(
        self,
        status: str,
        worker: Worker,
        *,
        private_fingerprint: str = "binding-private",
        turn_target_value: str = "pane-secret",
        picker_ordinal: int = 2,
    ) -> None:
        self.status = status
        self.worker_id = worker.id if status == "started" else None
        self.worker_fingerprint = worker.fingerprint if status == "started" else None
        self.binding_private_fingerprint = private_fingerprint if status == "started" else None
        self.turn_target_value = turn_target_value if status == "started" else None
        self.picker_ordinal = picker_ordinal if status == "started" else None


def _patch_pending_store_flow(
    monkeypatch: pytest.MonkeyPatch,
    worker: Worker,
    *,
    claim_status: str = "claimed",
    private_fingerprint: str = "binding-private",
    picker_ordinal: int = 2,
    turn_target_value: str = "pane-secret",
    finish_result: bool = True,
) -> list[tuple[Any, ...]]:
    transitions: list[tuple[Any, ...]] = []

    def claim(
        db_path: Path,
        host_id: str,
        pending_id: str,
        pending_fingerprint: str,
        choice_id: str,
        *,
        claim: bool = True,
        observed_at: str | None = None,
    ) -> _PendingClaim:
        transitions.append(
            ("claim", claim, pending_id, pending_fingerprint, choice_id, observed_at)
        )
        status = claim_status
        token: str | None = "claim-private" if status == "claimed" else None
        if not claim and status == "claimed":
            status = "validated"
        return _PendingClaim(
            status,
            worker,
            claim_token=token,
            private_fingerprint=private_fingerprint,
            turn_target_value=turn_target_value,
            picker_ordinal=picker_ordinal,
        )

    def start(
        db_path: Path,
        host_id: str,
        claim_token: str,
        *,
        observed_at: str | None = None,
    ) -> _PendingSend:
        transitions.append(("start", claim_token, observed_at))
        return _PendingSend(
            "started",
            worker,
            private_fingerprint=private_fingerprint,
            turn_target_value=turn_target_value,
            picker_ordinal=picker_ordinal,
        )

    def finish(
        db_path: Path,
        host_id: str,
        claim_token: str,
        *,
        accepted: bool,
    ) -> bool:
        transitions.append(("finish", claim_token, accepted))
        return finish_result

    def abandon(db_path: Path, host_id: str, claim_token: str) -> bool:
        transitions.append(("abandon", claim_token))
        return True

    monkeypatch.setattr(command_submission, "claim_backend_pending_choice", claim)
    monkeypatch.setattr(command_submission, "start_backend_pending_choice_send", start)
    monkeypatch.setattr(command_submission, "finish_backend_pending_choice_send", finish)
    monkeypatch.setattr(command_submission, "abandon_backend_pending_choice_claim", abandon)
    monkeypatch.setattr(command_submission, "_SUBMIT_ENTER_DELAY_SECONDS", 0)
    return transitions


def _expected_answer_calls(
    *,
    pane_id: str = "pane-secret",
    ordinal: int = 2,
) -> list[dict[str, Any]]:
    return [
        *_expected_private_clear_calls(pane_id),
        {"method": "pane.send_text", "params": {"pane_id": pane_id, "text": str(ordinal)}},
        {"method": "pane.send_keys", "params": {"pane_id": pane_id, "keys": ["enter"]}},
    ]


def test_answer_pending_dry_run_validates_current_choice_without_claim_or_socket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(monkeypatch, worker)
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _answer_request(request_id="", dry_run=True),
        socket_client_factory=_factory(calls),
    )

    assert envelope.ok is True
    assert envelope.status == "dry_run"
    assert envelope.result == {
        "target": {"worker_id": "w-1"},
        "pending": {"id": "pending-public", "fingerprint": "revision-public"},
        "choice": {"choice_id": "choice-public"},
        "delivery_state": "not_submitted",
    }
    assert transitions == [
        (
            "claim",
            False,
            "pending-public",
            "revision-public",
            "choice-public",
            None,
        )
    ]
    assert calls == []
    assert config.db_path is not None
    assert get_command_receipt(config.db_path, "cmd-host", "", "answer_pending") is None


def test_answer_pending_claims_sends_only_ordinal_and_replays_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(monkeypatch, worker)
    monkeypatch.setattr(
        command_submission,
        "list_worker_bindings",
        lambda *args, **kwargs: pytest.fail(
            "answer_pending must use the binding authenticated by the claim API"
        ),
    )
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config,
        _answer_request(),
        socket_client_factory=_factory(calls),
    )
    monkeypatch.setattr(
        command_submission,
        "_append_command_event",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            OSError("cached audit unavailable")
        ),
    )
    replay = submit_command(
        config,
        _answer_request(),
        socket_client_factory=_factory(calls),
    )
    changed_payload = submit_command(
        config,
        _answer_request(choice_id="different-choice"),
        socket_client_factory=_factory(calls),
    )

    assert first.ok is True
    assert first.status == STATUS_ACCEPTED
    assert first.result == {
        "target": {"worker_id": "w-1"},
        "pending": {"id": "pending-public", "fingerprint": "revision-public"},
        "choice": {"choice_id": "choice-public"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "observed_pending_state": "pending_observation",
    }
    assert replay.to_dict() == first.to_dict()
    assert changed_payload.status == STATUS_DUPLICATE_REQUEST
    assert calls == _expected_answer_calls()
    assert transitions == [
        (
            "claim",
            True,
            "pending-public",
            "revision-public",
            "choice-public",
            None,
        ),
        ("start", "claim-private", None),
        ("finish", "claim-private", True),
    ]

    assert config.db_path is not None
    receipt = get_command_receipt(config.db_path, "cmd-host", "answer-1", "answer_pending")
    assert receipt is not None
    assert receipt["status"] == STATUS_ACCEPTED
    turns = turns_payload_from_store(config.db_path, config.host_id)["turns"]
    assert not any(turn.get("origin_command_id") == "answer-1" for turn in turns)


@pytest.mark.parametrize(
    "claim_status",
    ["not_found", "stale", "changed", "unknown_choice", "already_claimed"],
)
def test_answer_pending_changed_disappeared_stale_or_unknown_fails_before_socket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    claim_status: str,
) -> None:
    config = _config(tmp_path / claim_status)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(
        monkeypatch,
        worker,
        claim_status=claim_status,
    )
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _answer_request(request_id=f"answer-{claim_status}"),
        socket_client_factory=_factory(calls),
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_STALE_TARGET
    assert envelope.error == {
        "code": STATUS_STALE_TARGET,
        "message": "pending interaction changed or is no longer answerable",
        "details": {},
    }
    assert calls == []
    assert len(transitions) == 1
    assert transitions[0][0] == "claim"




def test_answer_pending_second_request_loses_claim_race_without_socket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(monkeypatch, worker)
    original_claim = command_submission.claim_backend_pending_choice
    claim_count = 0

    def racing_claim(*args: Any, **kwargs: Any) -> _PendingClaim:
        nonlocal claim_count
        claim_count += 1
        if claim_count == 2:
            transitions.append(("claim_race_lost",))
            return _PendingClaim("already_claimed", worker, claim_token=None)
        return original_claim(*args, **kwargs)

    monkeypatch.setattr(command_submission, "claim_backend_pending_choice", racing_claim)
    calls: list[dict[str, Any]] = []
    losing_result: list[Any] = []

    class _RacingClient(_FakeSocketClient):
        def connect(self) -> "_FakeSocketClient":
            losing_result.append(
                submit_command(
                    config,
                    _answer_request(request_id="answer-race-loser"),
                    socket_client_factory=lambda _config: pytest.fail(
                        "losing request must not create a socket client"
                    ),
                )
            )
            return self

    winner = submit_command(
        config,
        _answer_request(request_id="answer-race-winner"),
        socket_client_factory=lambda _config: _RacingClient(calls),
    )

    assert winner.status == STATUS_ACCEPTED
    assert len(losing_result) == 1
    assert losing_result[0].status == STATUS_STALE_TARGET
    assert calls == _expected_answer_calls()


def test_answer_pending_post_send_failure_is_uncertain_and_not_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(monkeypatch, worker)
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config,
        _answer_request(request_id="answer-uncertain"),
        socket_client_factory=_factory(
            calls,
            raises=HerdrSocketDisconnectedError("disconnected"),
        ),
    )
    replay = submit_command(
        config,
        _answer_request(request_id="answer-uncertain"),
        socket_client_factory=_factory(calls),
    )

    assert first.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert replay.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert transitions[-1] == ("finish", "claim-private", False)
    assert calls == [
        *_expected_private_clear_calls(),
        {"method": "pane.send_text", "params": {"pane_id": "pane-secret", "text": "2"}},
    ]
    assert config.db_path is not None
    receipt = get_command_receipt(
        config.db_path,
        "cmd-host",
        "answer-uncertain",
        "answer_pending",
    )
    assert receipt is not None
    assert receipt["uncertain"] is True


def test_answer_pending_public_surfaces_recursively_exclude_private_route_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    private_binding = "raw-binding-sentinel"
    private_target = "raw-target-sentinel"
    private_pane = "raw-pane-sentinel"
    _seed(
        config,
        [worker],
        [
            _binding(
                worker,
                value=private_target,
                private_fingerprint=private_binding,
                turn_target_kind="pane_id",
                turn_target_value=private_pane,
            )
        ],
    )
    _patch_pending_store_flow(
        monkeypatch,
        worker,
        private_fingerprint=private_binding,
        turn_target_value=private_pane,
        picker_ordinal=3,
    )
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _answer_request(request_id="answer-private"),
        socket_client_factory=_factory(calls, pane_id=private_pane),
    )

    assert envelope.status == STATUS_ACCEPTED
    assert calls == _expected_answer_calls(
        pane_id=private_pane,
        ordinal=3,
    )
    assert config.db_path is not None
    receipt = get_command_receipt(
        config.db_path,
        "cmd-host",
        "answer-private",
        "answer_pending",
    )
    assert receipt is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        event_payloads = [
            json.loads(row[0])
            for row in conn.execute(
                "SELECT payload_json FROM events WHERE aggregate_id = ? ORDER BY id",
                ("answer-private",),
            ).fetchall()
        ]
    public_surfaces = [
        envelope.to_dict(),
        json.loads(receipt["result_json"]),
        *event_payloads,
    ]
    encoded = json.dumps(public_surfaces)
    assert private_binding not in encoded
    assert private_target not in encoded
    assert private_pane not in encoded
    for surface in public_surfaces:
        _assert_no_private_json(surface)


def test_answer_pending_integrates_with_durable_two_phase_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    binding = _binding(
        worker,
        private_fingerprint="binding-private",
        turn_target_kind="pane_id",
        turn_target_value="pane-secret",
    )
    _seed(config, [worker], [binding])
    assert config.db_path is not None
    assert apply_backend_pending_observation(
        config.db_path,
        config.host_id,
        worker.id,
        PendingObservation(
            kind="open_prompt",
            question="Choose a safe option",
            pending_kind="choice",
            choices=(
                PendingObservedChoice(
                    choice_id="choice-aaaaaaaaaaaaaaaaaaaaaaaa",
                    label="First",
                    picker_ordinal=1,
                ),
                PendingObservedChoice(
                    choice_id="choice-bbbbbbbbbbbbbbbbbbbbbbbb",
                    label="Second",
                    picker_ordinal=2,
                ),
            ),
            revision_digest="private-revision-digest",
        ),
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    pending_before = pending_payload_from_store(config.db_path, config.host_id)
    assert len(pending_before["pending_interactions"]) == 1
    interaction = pending_before["pending_interactions"][0]
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(command_submission, "_SUBMIT_ENTER_DELAY_SECONDS", 0)

    envelope = submit_command(
        config,
        _answer_request(
            request_id="answer-real-claim",
            pending_id=interaction["id"],
            pending_fingerprint=interaction["fingerprint"],
            choice_id=interaction["choices"][1]["choice_id"],
        ),
        socket_client_factory=_factory(calls),
    )

    assert envelope.status == STATUS_ACCEPTED
    assert envelope.result == {
        "target": {"worker_id": worker.id},
        "pending": {
            "id": interaction["id"],
            "fingerprint": interaction["fingerprint"],
        },
        "choice": {"choice_id": interaction["choices"][1]["choice_id"]},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "observed_pending_state": "pending_observation",
    }
    assert calls == _expected_answer_calls(ordinal=2)
    pending_after = pending_payload_from_store(config.db_path, config.host_id)
    assert pending_after["pending_interactions"] == []


@pytest.mark.parametrize("start_status", ["changed", "stale", "binding_changed"])
def test_answer_pending_second_cas_rejects_change_before_pane_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    start_status: str,
) -> None:
    config = _config(tmp_path / start_status)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(monkeypatch, worker)

    def changed_start(
        db_path: Path,
        host_id: str,
        claim_token: str,
        *,
        observed_at: str | None = None,
    ) -> _PendingSend:
        transitions.append(("start", claim_token, start_status))
        return _PendingSend(start_status, worker)

    monkeypatch.setattr(
        command_submission,
        "start_backend_pending_choice_send",
        changed_start,
    )
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _answer_request(request_id=f"answer-start-{start_status}"),
        socket_client_factory=_factory(calls),
    )

    assert envelope.status == STATUS_STALE_TARGET
    assert envelope.error == {
        "code": STATUS_STALE_TARGET,
        "message": "pending interaction changed or is no longer answerable",
        "details": {},
    }
    assert calls == []
    assert transitions[-1] == ("abandon", "claim-private")


def test_answer_pending_failed_pre_send_release_is_uncertain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="replacement-private")],
    )
    transitions = _patch_pending_store_flow(
        monkeypatch,
        worker,
        private_fingerprint="claimed-private",
    )

    def failed_abandon(db_path: Path, host_id: str, claim_token: str) -> bool:
        transitions.append(("abandon_failed", claim_token))
        return False

    monkeypatch.setattr(
        command_submission,
        "abandon_backend_pending_choice_claim",
        failed_abandon,
    )
    def failed_factory(config: Config) -> Any:
        raise OSError("socket unavailable")

    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _answer_request(request_id="answer-release-failed"),
        socket_client_factory=failed_factory,
    )

    assert envelope.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert envelope.error == {
        "code": STATUS_REQUEST_STATE_UNCERTAIN,
        "message": "pending answer claim could not be safely released",
        "details": {},
    }
    assert calls == []
    assert transitions[-1] == ("abandon_failed", "claim-private")
    assert config.db_path is not None
    receipt = get_command_receipt(
        config.db_path,
        config.host_id,
        "answer-release-failed",
        "answer_pending",
    )
    assert receipt is not None
    assert receipt["uncertain"] is True


def test_stable_owner_pending_command_survives_worker_churn_and_source_wins(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    stable_key = "wsk1_" + ("6" * 64)
    request_id = "stable-owner-request"
    worker_a = Worker(
        id="owner-worker-a",
        name="Owner Worker A",
        status="active",
        space_id="owner-space-a",
        fingerprint="owner-fingerprint-a",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    worker_b = Worker(
        id="owner-worker-b",
        name="Owner Worker B",
        status="waiting",
        space_id="owner-space-b",
        fingerprint="owner-fingerprint-b",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    binding_a = _binding(
        worker_a,
        value="owner-agent-a-private",
        private_fingerprint="owner-binding-a-private",
        turn_target_value="owner-pane-a-private",
    )
    _seed(config, [worker_a], [binding_a])
    calls: list[dict[str, Any]] = []

    accepted = submit_command(
        config,
        _request(request_id=request_id, worker_id=worker_a.id),
        socket_client_factory=_factory(calls, pane_id="owner-pane-a-private"),
    )
    assert accepted.status == STATUS_ACCEPTED
    command_before = next(
        turn
        for turn in turns_payload_from_store(config.db_path, config.host_id)["turns"]
        if turn.get("origin_command_id") == request_id
    )
    with sqlite3.connect(str(config.db_path)) as conn:
        command_sequence = conn.execute(
            """
            SELECT list_sequence
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (config.host_id, command_before["id"]),
        ).fetchone()[0]

    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-07-13T04:01:00+00:00",
            workers=[worker_b],
            backend_health=[_healthy_backend()],
        ),
    )
    upsert_worker_bindings(
        config.db_path,
        [
            _binding(
                worker_b,
                value="owner-agent-b-private",
                private_fingerprint="owner-binding-b-private",
                turn_target_value="owner-pane-b-private",
            )
        ],
    )
    command_after = upsert_command_pending_turn(
        config.db_path,
        config.host_id,
        worker_b,
        request_id=request_id,
        instruction_text="hello",
        observed_at="2026-07-13T04:01:01+00:00",
    )
    assert command_after is not None
    assert command_after["id"] == command_before["id"]
    assert command_after["worker_id"] == worker_b.id
    assert command_after["worker_fingerprint"] == worker_b.fingerprint
    assert command_after["space_id"] == worker_b.space_id
    assert command_after["complete"] is False
    assert command_after["has_open_turn"] is True
    with sqlite3.connect(str(config.db_path)) as conn:
        rows = conn.execute(
            """
            SELECT turn_id, list_sequence
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.origin_command_id') = ?
            """,
            (config.host_id, request_id),
        ).fetchall()
    assert rows == [(command_before["id"], command_sequence)]

    raw_source = "019f5590-3333-7333-8333-333333333333"
    assert merge_turn_content(
        config.db_path,
        config.host_id,
        worker_b.id,
        {
            "source_turn_id": raw_source,
            "user_text": "hello",
            "assistant_final_text": "durable owner answer",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-13T04:01:02+00:00",
    ) == 1
    completed_payload = turns_payload_from_store(
        config.db_path,
        config.host_id,
        schema_version=2,
    )
    completed_source = next(
        turn
        for turn in completed_payload["turns"]
        if turn.get("assistant_final_text") == "durable owner answer"
    )
    assert completed_source["id"] != command_before["id"]
    assert completed_source["origin_command_id"] == request_id
    assert completed_source["source_turn_id"].startswith("turnsrc-")
    assert completed_source["source_turn_id"] != raw_source
    assert completed_source["complete"] is True
    assert completed_source["has_open_turn"] is False
    with sqlite3.connect(str(config.db_path)) as conn:
        source_before_retry = conn.execute(
            """
            SELECT turns.turn_id, turns.list_sequence, turns.payload_json,
                   revisions.content_revision
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = ? AND turns.turn_id = ?
            """,
            (config.host_id, completed_source["id"]),
        ).fetchone()
        list_state_before_retry = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (config.host_id,),
        ).fetchone()

    source_wins = upsert_command_pending_turn(
        config.db_path,
        config.host_id,
        worker_b,
        request_id=request_id,
        instruction_text="hello",
        observed_at="2026-07-13T04:01:03+00:00",
    )
    assert source_wins is not None
    assert source_wins["id"] == completed_source["id"]
    assert source_wins["source_turn_id"] == completed_source["source_turn_id"]
    assert source_wins["assistant_final_text"] == "durable owner answer"
    assert source_wins["complete"] is True
    assert source_wins["has_open_turn"] is False
    with sqlite3.connect(str(config.db_path)) as conn:
        source_after_retry = conn.execute(
            """
            SELECT turns.turn_id, turns.list_sequence, turns.payload_json,
                   revisions.content_revision
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = ? AND turns.turn_id = ?
            """,
            (config.host_id, completed_source["id"]),
        ).fetchone()
        list_state_after_retry = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (config.host_id,),
        ).fetchone()
        origin_rows = conn.execute(
            """
            SELECT turn_id, json_extract(payload_json, '$.source_turn_id')
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.origin_command_id') = ?
            """,
            (config.host_id, request_id),
        ).fetchall()
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert source_after_retry == source_before_retry
    assert list_state_after_retry == list_state_before_retry
    assert origin_rows == [
        (completed_source["id"], completed_source["source_turn_id"])
    ]
    assert foreign_keys == []
    assert calls == _expected_submit_calls(
        "owner-agent-a-private",
        pane_id="owner-pane-a-private",
    )

    receipt = get_command_receipt(
        config.db_path,
        config.host_id,
        request_id,
        "send_instruction",
    )
    assert receipt is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        event_payloads = [
            json.loads(str(row[0]))
            for row in conn.execute(
                "SELECT payload_json FROM events WHERE aggregate_id = ? ORDER BY id",
                (request_id,),
            ).fetchall()
        ]
    public_surfaces = [
        accepted.to_dict(),
        command_before,
        command_after,
        completed_payload,
        source_wins,
        json.loads(receipt["result_json"]),
        *event_payloads,
    ]
    encoded = json.dumps(public_surfaces, sort_keys=True)
    for private_value in (
        raw_source,
        "owner-agent-a-private",
        "owner-binding-a-private",
        "owner-pane-a-private",
        "owner-agent-b-private",
        "owner-binding-b-private",
        "owner-pane-b-private",
    ):
        assert private_value not in encoded
    for surface in public_surfaces:
        _assert_no_private_json(surface)


def test_completed_source_command_replay_after_owner_churn_adopts_current_projection_without_reopen(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    stable_key = "wsk1_" + ("8" * 64)
    request_id = "completed-owner-request"
    raw_source = "019f5590-5555-7555-8555-555555555555"
    worker_a = Worker(
        id="completed-worker-a",
        name="Completed Worker A",
        status="active",
        space_id="completed-space-a",
        fingerprint="completed-fingerprint-a",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    worker_b = Worker(
        id="completed-worker-b",
        name="Completed Worker B",
        status="waiting",
        space_id="completed-space-b",
        fingerprint="completed-fingerprint-b",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    _seed(config, [worker_a])
    command = upsert_command_pending_turn(
        config.db_path,
        config.host_id,
        worker_a,
        request_id=request_id,
        instruction_text="complete this command",
        observed_at="2026-07-13T06:00:00+00:00",
    )
    assert command is not None
    assert merge_turn_content(
        config.db_path,
        config.host_id,
        worker_a.id,
        {
            "source_turn_id": raw_source,
            "user_text": "complete this command",
            "assistant_final_text": "terminal answer from A",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-13T06:00:01+00:00",
    ) == 1
    before_public = turns_payload_from_store(
        config.db_path,
        config.host_id,
        schema_version=2,
    )
    source_before = next(
        turn
        for turn in before_public["turns"]
        if turn.get("origin_command_id") == request_id
        and turn.get("source_turn_id")
    )
    assert source_before["worker_id"] == worker_a.id
    assert source_before["assistant_final_text"] == "terminal answer from A"
    assert source_before["complete"] is True
    assert source_before["has_open_turn"] is False

    def durable_identity() -> tuple[Any, ...]:
        with sqlite3.connect(str(config.db_path)) as conn:
            row = conn.execute(
                """
                SELECT turns.turn_id,
                       turns.list_sequence,
                       json_extract(turns.payload_json, '$.source_turn_id'),
                       revisions.content_revision,
                       outbox.id,
                       outbox.delivery_key,
                       json_extract(outbox.payload_json, '$.final_identity'),
                       outbox.status
                FROM turns
                JOIN turn_content_revisions AS revisions
                  ON revisions.host_id = turns.host_id
                 AND revisions.turn_id = turns.turn_id
                 AND revisions.is_current = 1
                JOIN connector_outbox AS outbox
                  ON outbox.host_id = turns.host_id
                 AND outbox.turn_id = turns.turn_id
                 AND outbox.content_revision = revisions.content_revision
                 AND outbox.delivery_kind = 'final_ready'
                WHERE turns.host_id = ?
                  AND turns.turn_id = ?
                """,
                (config.host_id, source_before["id"]),
            ).fetchone()
            assert row is not None
            return tuple(row)

    durable_before = durable_identity()
    with sqlite3.connect(str(config.db_path)) as conn:
        list_state_before = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (config.host_id,),
        ).fetchone()

    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-07-13T06:01:00+00:00",
            workers=[worker_b],
            backend_health=[_healthy_backend()],
        ),
    )
    replayed = upsert_command_pending_turn(
        config.db_path,
        config.host_id,
        worker_b,
        request_id=request_id,
        instruction_text="complete this command",
        observed_at="2026-07-13T06:01:01+00:00",
    )
    assert replayed is not None
    assert replayed["id"] == source_before["id"]
    assert replayed["source_turn_id"] == source_before["source_turn_id"]
    assert replayed["worker_id"] == worker_b.id
    assert replayed["worker_fingerprint"] == worker_b.fingerprint
    assert replayed["space_id"] == worker_b.space_id
    assert replayed["assistant_final_text"] == "terminal answer from A"
    assert replayed["complete"] is True
    assert replayed["has_open_turn"] is False

    after_public = turns_payload_from_store(
        config.db_path,
        config.host_id,
        schema_version=2,
    )
    source_after = next(
        turn
        for turn in after_public["turns"]
        if turn.get("id") == source_before["id"]
    )
    assert source_after["worker_id"] == worker_b.id
    assert source_after["worker_fingerprint"] == worker_b.fingerprint
    assert source_after["space_id"] == worker_b.space_id
    assert source_after["assistant_final_text"] == "terminal answer from A"
    assert source_after["complete"] is True
    assert source_after["has_open_turn"] is False
    assert durable_identity() == durable_before
    with sqlite3.connect(str(config.db_path)) as conn:
        list_state_after = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (config.host_id,),
        ).fetchone()
        origin_rows = conn.execute(
            """
            SELECT turn_id,
                   json_extract(payload_json, '$.source_turn_id'),
                   json_extract(payload_json, '$.complete')
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.origin_command_id') = ?
            """,
            (config.host_id, request_id),
        ).fetchall()
        root_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ?
              AND delivery_kind = 'final_ready'
              AND turn_id = ?
            """,
            (config.host_id, source_before["id"]),
        ).fetchone()[0]
        current_revision_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (config.host_id, source_before["id"]),
        ).fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert list_state_after == list_state_before
    assert origin_rows == [
        (source_before["id"], source_before["source_turn_id"], 1)
    ]
    assert root_count == 1
    assert current_revision_count == 1
    assert foreign_keys == []
    assert raw_source not in json.dumps(after_public, sort_keys=True)
