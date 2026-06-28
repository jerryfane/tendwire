"""Tests for pure command action execution."""

from __future__ import annotations

import json
from typing import Any

import pytest

from tendwire.config import Config
from tendwire.core.actions import CommandContext, execute_command
from tendwire.core.commands import (
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DRY_RUN,
    STATUS_INVALID_REQUEST,
    STATUS_NOT_FOUND,
    STATUS_REJECTED,
    STATUS_RESOLVED,
    CommandEnvelope,
    CommandRequest,
)
from tendwire.core.models import Snapshot, Worker
from tendwire.core.projector import project_from_raw


def _snapshot(host_id: str = "action-host") -> Snapshot:
    return project_from_raw(
        Config(host_id=host_id),
        spaces=[{"id": "s-1", "name": "Space", "status": "active"}],
        workers=[
            {"id": "w-1", "name": "Alpha", "status": "active", "space_id": "s-1"},
            {"id": "w-2", "name": "Beta", "status": "idle", "space_id": "s-1"},
            {"id": "w-3", "name": "Alpha", "status": "waiting", "space_id": "s-2"},
            {"id": "w-4", "name": "Failed", "status": "failed", "space_id": "s-1"},
            {"id": "w-5", "name": "Done", "status": "done", "space_id": "s-1"},
            {"id": "w-6", "name": "Closed", "status": "closed", "space_id": "s-1"},
            {"id": "w-7", "name": "Unknown", "status": "unknown", "space_id": "s-1"},
            {"id": "w-8", "name": "Mystery", "status": "mystery", "space_id": "s-1"},
        ],
    )


def _workers(snapshot: Snapshot) -> list[Worker]:
    return list(snapshot.workers)


def test_noop_action_succeeds() -> None:
    request = CommandRequest(action="noop")
    context = CommandContext(host_id="host", workers=[])
    envelope = execute_command(request, context)
    assert envelope.ok is True
    assert envelope.status == "noop"
    assert envelope.action == "noop"


def test_read_snapshot_returns_snapshot_shaped_result() -> None:
    snapshot = _snapshot()
    request = CommandRequest(action="read_snapshot")
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot), snapshot=snapshot)
    envelope = execute_command(request, context)
    assert envelope.ok is True
    assert envelope.status == "snapshot"
    result = envelope.result or {}
    assert "snapshot" in result
    assert result["snapshot"]["schema_version"] == 2
    assert result["snapshot"]["host_id"] == snapshot.host_id


def test_unknown_action_rejected_without_backend_call() -> None:
    calls: list[tuple[Any, Any]] = []

    def fake_backend(target: Any, instruction: Any) -> CommandEnvelope:
        calls.append((target, instruction))
        return CommandEnvelope(ok=True, status="accepted", action="send_instruction")

    request = CommandRequest(action="bad_action")
    context = CommandContext(host_id="host", workers=[], backend_sender=fake_backend)
    envelope = execute_command(request, context)
    assert envelope.ok is False
    assert envelope.status == STATUS_REJECTED
    assert calls == []


def test_resolve_target_exact() -> None:
    snapshot = _snapshot()
    request = CommandRequest(action="resolve_target", target={"worker_id": "w-1"})
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot))
    envelope = execute_command(request, context)
    assert envelope.ok is True
    assert envelope.status == STATUS_RESOLVED
    assert envelope.result["target"]["worker_id"] == "w-1"


def test_resolve_target_not_found() -> None:
    snapshot = _snapshot()
    request = CommandRequest(action="resolve_target", target={"worker_id": "missing"})
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot))
    envelope = execute_command(request, context)
    assert envelope.ok is False
    assert envelope.status == STATUS_NOT_FOUND


def test_resolve_target_ambiguous() -> None:
    snapshot = _snapshot()
    request = CommandRequest(action="resolve_target", target={"name": "Alpha"})
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot))
    envelope = execute_command(request, context)
    assert envelope.ok is False
    assert envelope.status == "ambiguous_target"
    assert len(envelope.result["candidates"]) == 2


def test_resolve_target_stale_fingerprint() -> None:
    snapshot = _snapshot()
    request = CommandRequest(
        action="resolve_target",
        target={"worker_id": "w-1", "worker_fingerprint": "deadbeef"},
    )
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot))
    envelope = execute_command(request, context)
    assert envelope.ok is False
    assert envelope.status == "stale_target"


def test_resolve_target_disallowed_status() -> None:
    snapshot = _snapshot()
    request = CommandRequest(action="resolve_target", target={"worker_id": "w-4"})
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot))
    envelope = execute_command(request, context)
    assert envelope.ok is False
    assert envelope.status == STATUS_REJECTED


def test_send_instruction_dry_run_does_not_call_backend() -> None:
    calls: list[tuple[Any, Any]] = []

    def fake_backend(target: Any, instruction: Any) -> CommandEnvelope:
        calls.append((target, instruction))
        return CommandEnvelope(ok=True, status="accepted", action="send_instruction")

    snapshot = _snapshot()
    request = CommandRequest(
        action="send_instruction",
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
        dry_run=True,
    )
    context = CommandContext(
        host_id=snapshot.host_id,
        workers=_workers(snapshot),
        backend_sender=fake_backend,
    )
    envelope = execute_command(request, context)
    assert envelope.ok is True
    assert envelope.status == STATUS_DRY_RUN
    assert calls == []


def test_send_instruction_non_dry_run_requires_request_id() -> None:
    snapshot = _snapshot()
    request = CommandRequest(
        action="send_instruction",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
    )
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot))
    envelope = execute_command(request, context)
    assert envelope.ok is False
    assert envelope.status == STATUS_INVALID_REQUEST


def test_send_instruction_non_dry_run_returns_backend_unsupported() -> None:
    snapshot = _snapshot()
    request = CommandRequest(
        action="send_instruction",
        request_id="req-1",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
    )
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot))
    envelope = execute_command(request, context)
    assert envelope.ok is False
    assert envelope.status == STATUS_BACKEND_UNSUPPORTED
    assert envelope.request_id == "req-1"
    assert envelope.dry_run is False


def test_send_instruction_rejects_empty_target_before_only_worker_fallback() -> None:
    calls: list[tuple[Any, Any]] = []

    def fake_backend(target: Any, instruction: Any) -> CommandEnvelope:
        calls.append((target, instruction))
        return CommandEnvelope(ok=True, status="accepted", action="send_instruction")

    request = CommandRequest(
        action="send_instruction",
        request_id="req-empty",
        dry_run=False,
        target={},
        instruction={"text": "hello"},
    )
    context = CommandContext(
        host_id="host",
        workers=[Worker(id="only-worker", name="Only", status="active")],
        backend_sender=fake_backend,
    )

    envelope = execute_command(request, context)

    assert envelope.ok is False
    assert envelope.status == STATUS_INVALID_REQUEST
    assert calls == []


def test_send_instruction_allows_done_status() -> None:
    calls: list[tuple[Any, Any]] = []

    def fake_backend(target: Any, instruction: Any) -> CommandEnvelope:
        calls.append((target, instruction))
        return CommandEnvelope(ok=True, status="accepted", action="send_instruction")

    snapshot = _snapshot()
    request = CommandRequest(
        action="send_instruction",
        request_id="req-done",
        dry_run=False,
        target={"worker_id": "w-5"},
        instruction={"text": "hello"},
    )
    context = CommandContext(
        host_id=snapshot.host_id,
        workers=_workers(snapshot),
        backend_sender=fake_backend,
    )

    envelope = execute_command(request, context)

    assert envelope.ok is True
    assert envelope.status == "accepted"
    assert calls[0][0]["status"] == "done"


@pytest.mark.parametrize("worker_id", ["w-4", "w-6", "w-7", "w-8"])
def test_send_instruction_rejects_closed_failed_unknown_statuses(worker_id: str) -> None:
    snapshot = _snapshot()
    request = CommandRequest(
        action="send_instruction",
        request_id=f"req-{worker_id}",
        dry_run=False,
        target={"worker_id": worker_id},
        instruction={"text": "hello"},
    )
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot))

    envelope = execute_command(request, context)

    assert envelope.ok is False
    assert envelope.status == STATUS_REJECTED


def test_send_instruction_backend_receives_resolved_worker_id() -> None:
    calls: list[tuple[Any, Any]] = []

    def fake_backend(target: Any, instruction: Any) -> CommandEnvelope:
        calls.append((target, instruction))
        return CommandEnvelope(ok=True, status="accepted", action="send_instruction")

    snapshot = _snapshot()
    request = CommandRequest(
        action="send_instruction",
        request_id="req-1",
        dry_run=False,
        target={"name": "Beta"},
        instruction={"text": "hello"},
    )
    context = CommandContext(
        host_id=snapshot.host_id,
        workers=_workers(snapshot),
        backend_sender=fake_backend,
    )

    envelope = execute_command(request, context)

    assert envelope.ok is True
    assert envelope.status == "accepted"
    assert calls == [
        (
            {
                "worker_id": "w-2",
                "name": "Beta",
                "space_id": "s-1",
                "status": "idle",
                "worker_fingerprint": snapshot.workers[1].fingerprint,
            },
            {"text": "hello"},
        )
    ]

def test_send_instruction_respects_ambiguous_target_before_backend() -> None:
    snapshot = _snapshot()
    request = CommandRequest(
        action="send_instruction",
        request_id="req-1",
        dry_run=False,
        target={"name": "Alpha"},
        instruction={"text": "hello"},
    )
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot))
    envelope = execute_command(request, context)
    assert envelope.ok is False
    assert envelope.status == "ambiguous_target"


def test_send_instruction_respects_rejected_status_before_backend() -> None:
    snapshot = _snapshot()
    request = CommandRequest(
        action="send_instruction",
        request_id="req-1",
        dry_run=False,
        target={"worker_id": "w-4"},
        instruction={"text": "hello"},
    )
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot))
    envelope = execute_command(request, context)
    assert envelope.ok is False
    assert envelope.status == STATUS_REJECTED


def test_public_result_contains_no_connector_fields() -> None:
    snapshot = _snapshot()
    request = CommandRequest(action="read_snapshot")
    context = CommandContext(host_id=snapshot.host_id, workers=_workers(snapshot), snapshot=snapshot)
    envelope = execute_command(request, context)
    payload = json.loads(envelope.to_json())

    def check(value: Any, path: str = "$") -> None:
        if isinstance(value, dict):
            for key in value:
                assert key not in {
                    "telegram",
                    "chat_id",
                    "topic_id",
                    "message_id",
                    "thread_id",
                    "route",
                    "delivery",
                    "token",
                    "bot_token",
                    "pane_id",
                    "terminal_id",
                    "tty",
                    "pty",
                    "pid",
                    "tmux",
                    "screen_session",
                    "window_id",
                    "tab_id",
                    "argv",
                    "command",
                    "shell",
                    "backend_target",
                    "agent_session",
                    "session_id",
                }, f"forbidden field {path}.{key}"
                check(value[key], f"{path}.{key}")
        elif isinstance(value, list):
            for i, item in enumerate(value):
                check(item, f"{path}[{i}]")

    check(payload)


def test_send_instruction_backend_receives_private_target_but_public_result_is_sanitized() -> None:
    calls: list[tuple[Any, Any]] = []
    worker = Worker(
        id="public-worker",
        name="Agent",
        status="active",
        backend_target={"kind": "agent_id", "value": "send-agent"},
    )

    def fake_backend(target: Any, instruction: Any) -> CommandEnvelope:
        calls.append((target, instruction))
        return CommandEnvelope(
            ok=True,
            status="accepted",
            action="send_instruction",
            result={"target": target},
        )

    request = CommandRequest(
        action="send_instruction",
        request_id="req-private-target",
        dry_run=False,
        target={"worker_id": "public-worker"},
        instruction={"text": "hello"},
    )
    envelope = execute_command(
        request,
        CommandContext(host_id="host", workers=[worker], backend_sender=fake_backend),
    )
    payload = json.loads(envelope.to_json())

    assert envelope.ok is True
    assert calls[0][0]["backend_target"] == {"kind": "agent_id", "value": "send-agent"}
    assert payload["result"]["target"] == {
        "worker_id": "public-worker",
        "name": "Agent",
        "space_id": None,
        "status": "active",
        "worker_fingerprint": worker.fingerprint,
    }
