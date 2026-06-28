"""Tests for the neutral command request/result/envelope contract."""

from __future__ import annotations

import json
from typing import Any

import pytest

from tendwire.core.commands import (
    ALLOWED_ACTIONS,
    STATUS_AMBIGUOUS_TARGET,
    STATUS_INVALID_REQUEST,
    STATUS_NOT_FOUND,
    STATUS_REJECTED,
    STATUS_RESOLVED,
    STATUS_STALE_TARGET,
    CommandEnvelope,
    CommandRequest,
    MAX_INSTRUCTION_LENGTH,
    parse_command_request,
    resolve_target,
    sanitize_command_result,
    validate_instruction_text,
    validate_request,
    worker_candidate,
)
from tendwire.core.models import Worker


_FORBIDDEN_COMMAND_FIELDS = {
    "telegram",
    "chat_id",
    "topic_id",
    "message_id",
    "thread_id",
    "route",
    "routes",
    "delivery",
    "deliveries",
    "token",
    "tokens",
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
    "connector",
    "connectors",
    "backend_target",
    "agent_session",
    "session_id",
}


def _assert_no_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _FORBIDDEN_COMMAND_FIELDS, f"forbidden field {path}.{key}"
            _assert_no_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, f"{path}[{index}]")


def test_allowed_actions_frozen() -> None:
    assert ALLOWED_ACTIONS == {"noop", "read_snapshot", "resolve_target", "send_instruction"}


def test_command_request_defaults_are_dry_run() -> None:
    request = CommandRequest(action="noop")
    assert request.dry_run is True
    assert request.schema_version == 1
    assert request.request_id is None


@pytest.mark.parametrize("value", [True, False])
def test_parse_command_request_accepts_literal_boolean_dry_run(value: bool) -> None:
    request, error = parse_command_request(json.dumps({"schema_version": 1, "action": "noop", "dry_run": value}))
    assert error is None
    assert request is not None
    assert request.dry_run is value


@pytest.mark.parametrize("value", ["false", "true", 0, 1, None, [], {}])
def test_parse_command_request_rejects_non_boolean_dry_run(value: Any) -> None:
    request, error = parse_command_request(json.dumps({"schema_version": 1, "action": "noop", "dry_run": value}))
    assert request is None
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize("value", ["false", "true", 0, 1, None, [], {}])
def test_validate_request_rejects_non_boolean_dry_run(value: Any) -> None:
    request = CommandRequest(action="noop", dry_run=value)
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


def test_command_request_to_dict_roundtrip() -> None:
    request = CommandRequest(
        action="send_instruction",
        request_id="req-1",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
        params={"extra": True},
    )
    data = request.to_dict()
    restored = CommandRequest.from_dict(data)
    assert restored == request


def test_command_envelope_shape_matches_contract() -> None:
    request = CommandRequest(action="noop")
    envelope = CommandEnvelope.from_result(request, ok=True, status="noop")
    payload = envelope.to_dict()
    assert {
        "schema_version",
        "action",
        "request_id",
        "ok",
        "dry_run",
        "status",
        "result",
        "error",
        "warnings",
    } <= set(payload)
    assert payload["schema_version"] == 1
    assert payload["ok"] is True
    assert payload["status"] == "noop"
    _assert_no_forbidden_fields(payload)


def test_validate_request_rejects_bad_schema_version() -> None:
    request = CommandRequest(action="noop", schema_version=2)
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


def test_validate_request_rejects_unknown_action() -> None:
    request = CommandRequest(action="explode")
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_REJECTED


def test_validate_request_rejects_missing_action() -> None:
    request = CommandRequest(action="")
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize("field", sorted(_FORBIDDEN_COMMAND_FIELDS))
def test_validate_request_rejects_forbidden_connector_and_terminal_fields(field: str) -> None:
    request = CommandRequest(action="noop", params={field: "leaked"})
    error = validate_request(request)
    assert error is not None, field
    assert error["code"] == STATUS_INVALID_REQUEST
    assert field in str(error.get("details", {}))


def test_validate_request_rejects_forbidden_nested_fields() -> None:
    request = CommandRequest(
        action="send_instruction",
        target={"worker_id": "w-1"},
        instruction={"text": "ok"},
        params={"nested": {"pane_id": "leaked"}},
    )
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


@pytest.mark.parametrize("field", sorted(_FORBIDDEN_COMMAND_FIELDS))
def test_parse_command_request_rejects_raw_top_level_forbidden_field(field: str) -> None:
    """Raw decoded JSON is rejected before from_dict drops unknown top-level keys."""
    payload = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "raw-rej",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
            field: "leaked",
        }
    )
    request, error = parse_command_request(payload)
    assert request is None, field
    assert error is not None, field
    assert error["code"] == STATUS_INVALID_REQUEST, field
    assert field in str(error.get("details", {}))


def test_parse_command_request_rejects_unknown_top_level_fields() -> None:
    payload = json.dumps({"schema_version": 1, "action": "noop", "surprise": True})
    request, error = parse_command_request(payload)

    assert request is None
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert "$.surprise" in str(error.get("details", {}))


def test_parse_command_request_rejects_raw_nested_forbidden_field() -> None:
    payload = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "raw-nested-rej",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
            "params": {"nested": [{"raw": {"shell": "bash"}}]},
        }
    )
    request, error = parse_command_request(payload)

    assert request is not None
    assert request.request_id == "raw-nested-rej"
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert "$.params.nested[0].raw.shell" in str(error.get("details", {}))


def test_command_request_from_dict_drops_unknown_top_level_keys() -> None:
    """Unknown top-level keys disappear from the canonical request shape."""
    request = CommandRequest.from_dict(
        {
            "schema_version": 1,
            "action": "noop",
            "telegram": "leaked",
            "pane_id": "p-1",
        }
    )
    assert "telegram" not in request.to_dict()
    assert "pane_id" not in request.to_dict()


@pytest.mark.parametrize("field", ["pane_id", "terminal_id", "argv", "shell"])
def test_validate_request_rejects_disallowed_target_fields(field: str) -> None:
    request = CommandRequest(
        action="resolve_target",
        target={"worker_id": "w-1", field: "leaked"},
    )
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert field in str(error.get("details", {}))

@pytest.mark.parametrize("field", ["argv", "command", "shell"])
def test_validate_request_rejects_disallowed_instruction_fields(field: str) -> None:
    request = CommandRequest(
        action="send_instruction",
        target={"worker_id": "w-1"},
        instruction={"text": "ok", field: "leaked"},
    )
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert field in str(error.get("details", {}))


def test_validate_send_instruction_requires_target_and_text() -> None:
    missing_target = CommandRequest(
        action="send_instruction",
        instruction={"text": "ok"},
    )
    error = validate_request(missing_target)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST

    empty_target = CommandRequest(
        action="send_instruction",
        target={},
        instruction={"text": "ok"},
    )
    error = validate_request(empty_target)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert "explicit target selector" in error["message"]

    missing_text = CommandRequest(
        action="send_instruction",
        target={"worker_id": "w-1"},
        instruction={"text": ""},
    )
    error = validate_request(missing_text)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST


def test_validate_send_instruction_non_dry_run_requires_request_id() -> None:
    request = CommandRequest(
        action="send_instruction",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "ok"},
    )
    error = validate_request(request)
    assert error is not None
    assert error["code"] == STATUS_INVALID_REQUEST
    assert "request_id" in error["message"]


@pytest.mark.parametrize(
    "text,expected_code",
    [
        ("", STATUS_INVALID_REQUEST),
        ("a" * (MAX_INSTRUCTION_LENGTH + 1), STATUS_INVALID_REQUEST),
        ("hello\x00world", STATUS_INVALID_REQUEST),
        ("hello\x1b[31mworld", STATUS_INVALID_REQUEST),
        ("hello\x1b]0;title\x07world", STATUS_INVALID_REQUEST),
        ("hello\x9b31mworld", STATUS_INVALID_REQUEST),
        ("hello\x7fworld", STATUS_INVALID_REQUEST),
        ("\x1b[200~paste\x1b[201~", STATUS_INVALID_REQUEST),
        ("hello\rworld", STATUS_INVALID_REQUEST),
        ("hello\x07world", STATUS_INVALID_REQUEST),
        ("hello\bworld", STATUS_INVALID_REQUEST),
        ("hello\fworld", STATUS_INVALID_REQUEST),
        ("hello\vworld", STATUS_INVALID_REQUEST),
        ("hello\x01world", STATUS_INVALID_REQUEST),
        ("hello\tworld", None),
        ("hello\nworld", None),
        ("plain text", None),
        ("unicode 🚀", None),
    ],
)
def test_validate_instruction_text_rejects_unsafe_input(text: str, expected_code: str | None) -> None:
    error = validate_instruction_text(text)
    if expected_code is None:
        assert error is None
    else:
        assert error is not None
        assert error["code"] == expected_code


def test_worker_candidate_is_neutral() -> None:
    worker = Worker(
        id="w-1",
        name="Agent One",
        status="active",
        space_id="s-1",
        summary="working",
        meta={"raw_status": "executing"},
    )
    candidate = worker_candidate(worker)
    assert set(candidate.keys()) <= {
        "worker_id",
        "name",
        "space_id",
        "status",
        "worker_fingerprint",
        "summary",
    }
    assert candidate["worker_id"] == "w-1"
    assert candidate["worker_fingerprint"] == worker.fingerprint
    _assert_no_forbidden_fields(candidate)


def _workers() -> list[Worker]:
    return [
        Worker(id="w-1", name="Alpha", status="active", space_id="s-1"),
        Worker(id="w-2", name="Beta", status="idle", space_id="s-1"),
        Worker(id="w-3", name="Alpha", status="waiting", space_id="s-2"),
        Worker(id="w-4", name="Closed", status="closed", space_id="s-1"),
    ]


def test_resolve_target_by_exact_worker_id() -> None:
    resolved, candidates, status = resolve_target({"worker_id": "w-2"}, _workers())
    assert status == STATUS_RESOLVED
    assert resolved is not None
    assert resolved["worker_id"] == "w-2"
    assert len(candidates) == 1


def test_resolve_target_not_found() -> None:
    resolved, candidates, status = resolve_target({"worker_id": "missing"}, _workers())
    assert status == STATUS_NOT_FOUND
    assert resolved is None
    assert candidates == []


def test_resolve_target_ambiguous_by_name() -> None:
    resolved, candidates, status = resolve_target({"name": "Alpha"}, _workers())
    assert status == STATUS_AMBIGUOUS_TARGET
    assert resolved is None
    assert len(candidates) == 2


def test_resolve_target_name_unique_after_space_filter() -> None:
    resolved, candidates, status = resolve_target({"name": "Alpha", "space_id": "s-2"}, _workers())
    assert status == STATUS_RESOLVED
    assert resolved is not None
    assert resolved["worker_id"] == "w-3"


def test_resolve_target_stale_fingerprint() -> None:
    workers = _workers()
    stale_fp = "deadbeef"
    resolved, candidates, status = resolve_target(
        {"worker_id": "w-1", "worker_fingerprint": stale_fp},
        workers,
    )
    assert status == STATUS_STALE_TARGET
    assert resolved is None
    assert len(candidates) == 1
    assert candidates[0]["worker_id"] == "w-1"


def test_resolve_target_matching_fingerprint() -> None:
    workers = _workers()
    fp = workers[0].fingerprint
    resolved, candidates, status = resolve_target(
        {"worker_id": "w-1", "worker_fingerprint": fp},
        workers,
    )
    assert status == STATUS_RESOLVED
    assert resolved is not None
    assert resolved["worker_id"] == "w-1"


def test_resolve_target_rejects_disallowed_status() -> None:
    resolved, candidates, status = resolve_target({"worker_id": "w-4"}, _workers())
    assert status == STATUS_REJECTED
    assert resolved is None
    assert len(candidates) == 1
    assert candidates[0]["status"] == "closed"


def test_command_envelope_roundtrip_via_dict() -> None:
    request = CommandRequest(action="resolve_target", request_id="r-1", dry_run=False)
    envelope = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_RESOLVED,
        result={"target": {"worker_id": "w-1"}},
        warnings=["one"],
    )
    restored = CommandEnvelope.from_dict(envelope.to_dict())
    assert restored.to_dict() == envelope.to_dict()


_COMMAND_RESULT_FORBIDDEN_KEYS = {
    "pane_id",
    "terminal_id",
    "pid",
    "tty",
    "pty",
    "tmux",
    "screen_session",
    "window_id",
    "tab_id",
    "argv",
    "shell",
    "command",
    "route",
    "routes",
    "delivery",
    "deliveries",
    "token",
    "tokens",
    "connector",
    "connectors",
    "telegram",
    "chat_id",
    "topic_id",
    "message_id",
    "thread_id",
    "bot_token",
    "herdres_delivery",
    "backend_target",
    "agent_session",
    "session_id",
}


def test_sanitize_command_result_strips_plural_and_variant_forbidden_keys() -> None:
    raw = {
        "safe": "kept",
        "pane_id": "p-1",
        "terminal_id": "t-1",
        "pid": 123,
        "tty": "/dev/pts/0",
        "shell": "bash",
        "command": "python app.py",
        "routes": ["r1"],
        "deliveries": [{"id": 1}],
        "tokens": "secret",
        "connectors": {"telegram": "leaked"},
        "backend_target": {"kind": "agent_id", "value": "agent-1"},
        "agent_session": {"value": "sess-1"},
        "session_id": "session-1",
        "nested": {
            "safe": "kept",
            "window_id": "w-1",
            "tab_id": "t-1",
            "argv": ["-c"],
            "backend_target": {"value": "nested"},
        },
        "list": [
            {"safe": "kept", "route": "r"},
        ],
    }
    sanitized = sanitize_command_result(raw)
    assert sanitized == {
        "safe": "kept",
        "nested": {"safe": "kept"},
        "list": [{"safe": "kept"}],
    }


def test_sanitize_command_result_preserves_legitimate_envelope_and_target_fields() -> None:
    envelope = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": "req-1",
        "ok": True,
        "dry_run": False,
        "status": "accepted",
        "result": {
            "target": {
                "worker_id": "w-1",
                "space_id": "s-1",
                "worker_fingerprint": "fp",
                "name": "Alpha",
            },
            "snapshot": {
                "schema_version": 2,
                "host_id": "host",
                "content_fingerprint": "fp",
            },
        },
        "error": None,
        "warnings": [],
    }
    sanitized = sanitize_command_result(envelope)
    assert sanitized == envelope
