"""Tests for the Herdr CLI backend adapter contract."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Sequence
from typing import Any

import pytest

from tendwire.backends import herdr_cli, herdr_command
from tendwire.backends.herdr_cli import fetch_herdr_state
from tendwire.config import Config
from tendwire.core.commands import (
    STATUS_ACCEPTED,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_REQUEST_STATE_UNCERTAIN,
)
from tendwire.core.projector import project_from_observations


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
    "pane_id",
    "terminal_id",
    "backend_target",
    "agent_session",
    "session_id",
    "argv",
    "command",
    "shell",
    "connector",
    "connectors",
}


def _completed(stdout: str, returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["herdr", "workspace", "list", "--json"],
        returncode=returncode,
        stdout=stdout,
        stderr="",
    )


def _respond(args: Sequence[str], responses: dict[tuple[str, ...], Any]) -> subprocess.CompletedProcess[str] | None:
    """Return a canned response for a herdr command tuple."""
    key = tuple(args)
    if key not in responses:
        return _completed("", returncode=1)
    response = responses[key]
    if isinstance(response, subprocess.CompletedProcess):
        return response
    if isinstance(response, str):
        return _completed(response)
    return _completed(json.dumps(response))


def _assert_no_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _FORBIDDEN_FIELDS, f"forbidden field {path}.{key}"
            _assert_no_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, f"{path}[{index}]")


def _send_completed(returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["herdr", "agent", "send", "worker-1", "hello"],
        returncode=returncode,
        stdout="",
        stderr="",
    )


def test_send_instruction_uses_agent_send_argv(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        return _send_completed()

    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command.subprocess, "run", fake_run)

    envelope = herdr_command.send_instruction(
        config,
        {
            "worker_id": "public-worker-1",
            "backend_target": {"kind": "agent_id", "value": "agent-send-1", "sendable": True, "reason": None},
            "terminal_id": "ignored",
        },
        {"text": "hello"},
    )

    assert envelope.ok is True
    assert envelope.status == STATUS_ACCEPTED
    assert envelope.result == {"target": {"worker_id": "public-worker-1"}}
    assert calls == [
        (
            ["herdr", "agent", "send", "agent-send-1", "hello"],
            {
                "capture_output": True,
                "text": True,
                "check": False,
                "timeout": config.herdr_timeout_seconds,
            },
        )
    ]
    assert "shell" not in calls[0][1]


def test_send_instruction_requires_private_backend_target(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[Any] = []

    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command, "_run_agent_send", lambda *args: calls.append(args))

    envelope = herdr_command.send_instruction(
        config,
        {"worker_id": "public-worker-1", "agent_session": {"value": "sess-ignored"}},
        {"text": "hello"},
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_BACKEND_UNSUPPORTED
    assert calls == []
    _assert_no_forbidden_fields(envelope.to_dict())


def test_send_instruction_maps_missing_binary_to_backend_unavailable(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="missing-herdr")
    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: None)
    monkeypatch.setattr(
        herdr_command,
        "_run_agent_send",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not run")),
    )

    envelope = herdr_command.send_instruction(
        config,
        {"worker_id": "worker-1", "backend_target": {"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None}},
        {"text": "hello"},
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_BACKEND_UNAVAILABLE
    _assert_no_forbidden_fields(envelope.to_dict())


def test_send_instruction_maps_nonzero_exit_to_backend_failed(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command, "_run_agent_send", lambda *args: _send_completed(returncode=2))

    envelope = herdr_command.send_instruction(
        config,
        {"worker_id": "worker-1", "backend_target": {"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None}},
        {"text": "hello"},
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_BACKEND_FAILED
    assert envelope.error["details"] == {"exit_code": 2}
    _assert_no_forbidden_fields(envelope.to_dict())


def test_send_instruction_maps_timeout_to_uncertain(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")

    def raise_timeout(*args: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=["herdr", "agent", "send"], timeout=5.0)

    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command, "_run_agent_send", raise_timeout)

    envelope = herdr_command.send_instruction(
        config,
        {"worker_id": "worker-1", "backend_target": {"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None}},
        {"text": "hello"},
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_REQUEST_STATE_UNCERTAIN
    _assert_no_forbidden_fields(envelope.to_dict())


def test_send_instruction_rejects_ambiguous_private_backend_target(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[Any] = []

    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command, "_run_agent_send", lambda *args: calls.append(args))

    envelope = herdr_command.send_instruction(
        config,
        {
            "worker_id": "public-worker-1",
            "backend_target": {
                "kind": "agent_id",
                "value": "agent-1",
                "sendable": False,
                "reason": "duplicate_backend_target",
            },
        },
        {"text": "hello"},
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_AMBIGUOUS_BACKEND_TARGET
    assert calls == []
    _assert_no_forbidden_fields(envelope.to_dict())


def test_fetch_herdr_state_returns_empty_when_binary_missing() -> None:
    config = Config(host_id="testhost", herdr_bin="definitely-not-a-real-herdr-binary")
    spaces, workers = fetch_herdr_state(config)
    assert spaces == []
    assert workers == []


def test_fetch_herdr_state_returns_empty_on_cli_failure(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(
        herdr_cli,
        "_run_herdr",
        lambda args, cfg: _completed('{"workers":[{"id":"leaked"}]}', returncode=2),
    )

    spaces, workers = fetch_herdr_state(config)

    assert spaces == []
    assert workers == []


def test_fetch_herdr_state_returns_empty_on_malformed_json(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _completed("not json"))

    spaces, workers = fetch_herdr_state(config)

    assert spaces == []
    assert workers == []


def test_sample_herdr_projection_is_neutral_and_fingerprinted(monkeypatch) -> None:
    config = Config(host_id="herdr-host", herdr_bin="herdr")
    sample_payload = {
        "spaces": [
            {
                "id": "space-1",
                "name": "Build",
                "status": "running",
                "status_line": "building package",
                "telegram": "forbidden",
                "chat_id": 111,
                "safe": "space-meta",
            }
        ],
        "workers": [
            {
                "id": "worker-1",
                "name": "Agent One",
                "status": "panic",
                "space_id": "space-1",
                "summary": "crashed",
                "topic_id": 222,
                "message_id": 333,
                "route": "telegram",
                "delivery": {"chat_id": 444},
                "safe": "worker-meta",
            }
        ],
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(
        herdr_cli,
        "_run_herdr",
        lambda args, cfg: _completed(json.dumps(sample_payload)),
    )

    spaces, workers = fetch_herdr_state(config)
    snapshot = project_from_observations(config, spaces=spaces, workers=workers)
    payload = json.loads(snapshot.to_json())

    assert payload["schema_version"] == 2
    assert len(payload["content_fingerprint"]) == 24
    assert payload["spaces"][0]["status"] == "active"
    assert payload["spaces"][0]["fingerprint"]
    assert payload["spaces"][0]["meta"]["safe"] == "space-meta"
    assert payload["workers"][0]["status"] == "failed"
    assert payload["workers"][0]["fingerprint"]
    assert payload["workers"][0]["meta"]["raw_status"] == "panic"
    assert payload["workers"][0]["meta"]["safe"] == "worker-meta"
    assert payload["attention"][0]["source"] == "worker:worker-1"
    assert payload["attention"][0]["fingerprint"]
    _assert_no_forbidden_fields(payload)


def test_herdr_agent_public_id_and_private_backend_target_are_separate(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-worker",
                        "agent_id": "send-agent",
                        "agent": "Coder",
                        "agent_session": {"value": "sess-must-not-leak"},
                        "terminal_id": "term-must-not-leak",
                        "pane_id": "pane-must-not-leak",
                        "session_id": "session-must-not-leak",
                        "workspace_id": "ws-1",
                    }
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)
    snapshot = project_from_observations(config, spaces=spaces, workers=workers)
    payload = json.loads(snapshot.to_json())

    assert len(workers) == 1
    assert workers[0].id == "public-worker"
    assert workers[0].backend_target is not None
    assert workers[0].backend_target["kind"] == "agent_id"
    assert workers[0].backend_target["value"] == "send-agent"
    assert workers[0].backend_target["sendable"] is True
    assert workers[0].backend_target["reason"] is None
    assert payload["workers"][0]["id"] == "public-worker"
    assert payload["workers"][0]["name"] == "Coder"
    assert "sess-must-not-leak" not in json.dumps(payload)
    assert "term-must-not-leak" not in json.dumps(payload)
    assert "pane-must-not-leak" not in json.dumps(payload)
    assert "session-must-not-leak" not in json.dumps(payload)
    _assert_no_forbidden_fields(payload)


def test_herdr_backend_target_precedence_and_pane_fallback(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "id": "public-id",
                        "agent_id": "agent-send",
                        "agent": "agent-name",
                        "label": "agent-label",
                        "terminal_id": "term-fallback",
                        "pane_id": "pane-fallback",
                    },
                    {
                        "slug": "pane-public",
                        "agent_session": {"value": "sess-not-sendable"},
                        "terminal_id": "term-send",
                        "pane_id": "pane-send",
                        "state_labels": ["agent"],
                    },
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    _, workers = fetch_herdr_state(config)
    by_id = {worker.id: worker for worker in workers}

    assert by_id["public-id"].backend_target is not None
    assert by_id["public-id"].backend_target["kind"] == "agent_id"
    assert by_id["public-id"].backend_target["value"] == "agent-send"
    assert by_id["public-id"].backend_target["sendable"] is True
    assert by_id["pane-public"].backend_target is not None
    assert by_id["pane-public"].backend_target["kind"] == "terminal_id"
    assert by_id["pane-public"].backend_target["value"] == "term-send"
    assert by_id["pane-public"].backend_target["sendable"] is True
    assert all(
        (worker.backend_target or {}).get("value") != "sess-not-sendable"
        for worker in workers
    )


def test_duplicate_sendable_backend_targets_are_marked_not_sendable(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {"worker_id": "public-a", "agent_id": "same-send", "agent": "A"},
                    {"worker_id": "public-b", "agent_id": "same-send", "agent": "B"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    _, workers = fetch_herdr_state(config)

    assert {worker.id for worker in workers} == {"public-a", "public-b"}
    assert all((worker.backend_target or {}).get("kind") == "agent_id" for worker in workers)
    assert all((worker.backend_target or {}).get("value") == "same-send" for worker in workers)
    assert all((worker.backend_target or {}).get("sendable") is False for worker in workers)
    assert all(
        (worker.backend_target or {}).get("reason") == "duplicate_backend_target"
        for worker in workers
    )


def test_name_and_label_backend_fallbacks_are_sendable_only_when_unique(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {"worker_id": "name-unique", "name": "NameUnique"},
                    {"worker_id": "label-unique", "label": "LabelUnique"},
                    {"worker_id": "name-dupe-a", "name": "DupText"},
                    {"worker_id": "label-dupe-b", "label": "DupText"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    _, workers = fetch_herdr_state(config)
    by_id = {worker.id: worker for worker in workers}

    assert by_id["name-unique"].backend_target == {
        "kind": "name",
        "value": "NameUnique",
        "sendable": True,
        "reason": None,
    }
    assert by_id["label-unique"].backend_target == {
        "kind": "label",
        "value": "LabelUnique",
        "sendable": True,
        "reason": None,
    }
    assert by_id["name-dupe-a"].backend_target["sendable"] is False
    assert by_id["name-dupe-a"].backend_target["reason"] == "not_unique"
    assert by_id["label-dupe-b"].backend_target["sendable"] is False
    assert by_id["label-dupe-b"].backend_target["reason"] == "not_unique"


def test_fetch_herdr_command_observation_reports_healthy_empty(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {"result": {"panes": []}},
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_probe_herdr", lambda args, cfg: ("ok", _respond(args, responses).stdout and json.loads(_respond(args, responses).stdout)))

    observation = herdr_cli.fetch_herdr_command_observation(config)

    assert observation.healthy is True
    assert observation.outcome == "empty_healthy"
    assert observation.workers == []


@pytest.mark.parametrize("outcome", ["timeout", "malformed_json", "nonzero"])
def test_fetch_herdr_command_observation_degraded_agent_probe(monkeypatch, outcome: str) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")

    def fake_probe(args: Sequence[str], cfg: Config) -> tuple[str, Any]:
        if tuple(args) == ("workspace", "list"):
            return "ok", {"result": {"workspaces": []}}
        if tuple(args) == ("workspace", "list", "--json"):
            return "ok", {"result": {"workspaces": []}}
        return outcome, None

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_probe_herdr", fake_probe)

    observation = herdr_cli.fetch_herdr_command_observation(config)

    assert observation.healthy is False
    assert observation.status == "degraded"
    assert observation.outcome == outcome
    assert observation.workers == []


def test_no_flag_workspace_and_agent_lists_preferred_without_json_calls(monkeypatch) -> None:
    """Herdr 0.7.0 no-flag envelopes are used before compatibility --json fallbacks."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {
            "result": {
                "workspaces": [
                    {
                        "workspace_id": "ws-1",
                        "label": "Build",
                        "agent_status": "working",
                        "active_tab_id": "tab-1",
                    }
                ]
            }
        },
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "agent_session": {"value": "sess-1"},
                        "agent": "Coder",
                        "workspace_id": "ws-1",
                        "agent_status": "done",
                        "cwd": "/home/dev",
                    }
                ]
            }
        },
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str] | None:
        calls.append(tuple(args))
        if "--json" in args:
            raise AssertionError("--json fallback should not be called after valid no-flag output")
        return _respond(args, responses)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert calls == [("workspace", "list"), ("agent", "list")]
    assert len(spaces) == 1
    assert spaces[0].id == "ws-1"
    assert spaces[0].name == "Build"
    assert spaces[0].status == "active"
    assert spaces[0].meta.get("active_tab_id") == "tab-1"
    assert spaces[0].meta.get("raw_status") == "working"
    assert len(workers) == 1
    assert workers[0].id == "Coder"
    assert workers[0].name == "Coder"
    assert workers[0].status == "done"
    assert workers[0].space_id == "ws-1"
    assert workers[0].backend_target is not None
    assert workers[0].backend_target["kind"] == "agent"
    assert workers[0].backend_target["value"] == "Coder"
    assert workers[0].backend_target["sendable"] is True
    assert workers[0].meta.get("cwd") == "/home/dev"
    assert "raw_status" not in workers[0].meta


def test_json_workspace_list_fallback_when_no_flag_fails(monkeypatch) -> None:
    """Compatibility --json workspace list is tried when no-flag output fails."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): _completed("usage", returncode=2),
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [{"workspace_id": "ws-json", "label": "Compat", "agent_status": "idle"}]
            }
        },
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {"result": {"panes": []}},
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str] | None:
        calls.append(tuple(args))
        return _respond(args, responses)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert calls[:2] == [("workspace", "list"), ("workspace", "list", "--json")]
    assert len(spaces) == 1
    assert spaces[0].id == "ws-json"
    assert spaces[0].name == "Compat"
    assert workers == []


def test_json_agent_list_fallback_when_no_flag_is_malformed(monkeypatch) -> None:
    """Compatibility --json agent list is tried when no-flag output is malformed."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): _completed("not json"),
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {
                        "agent_session": {"value": "sess-json"},
                        "agent": "CompatAgent",
                        "workspace_id": "ws-1",
                    }
                ]
            }
        },
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str] | None:
        calls.append(tuple(args))
        return _respond(args, responses)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert calls == [("workspace", "list"), ("agent", "list"), ("agent", "list", "--json")]
    assert spaces == []
    assert len(workers) == 1
    assert workers[0].id == "CompatAgent"
    assert workers[0].name == "CompatAgent"
    assert workers[0].space_id == "ws-1"


def test_result_envelopes_parse(monkeypatch) -> None:
    """result.workspaces, result.agents, and result.panes envelopes parse."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [{"workspace_id": "ws-result", "label": "ResultSpace", "agent_status": "idle"}]
            }
        },
        ("agent", "list", "--json"): {
            "result": {"agents": [{"agent_session": {"value": "sess-result"}, "agent": "Agent"}]}
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(spaces) == 1
    assert spaces[0].id == "ws-result"
    assert len(workers) == 1
    assert workers[0].id == "Agent"


def test_pane_fallback_only_when_agent_list_yields_none(monkeypatch) -> None:
    """Pane list fallback runs only when agents are empty and only for agent-bearing panes."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): _completed("", returncode=1),
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {
            "result": {
                "panes": [
                    {"pane_id": "pane-agent", "agent": "Runner", "workspace_id": "ws-1"},
                    {"pane_id": "pane-plain", "workspace_id": "ws-1"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 1
    assert workers[0].id == "Runner"
    assert workers[0].name == "Runner"
    assert workers[0].backend_target is not None
    assert workers[0].backend_target["kind"] == "pane_id"
    assert workers[0].backend_target["value"] == "pane-agent"
    assert workers[0].backend_target["sendable"] is True


def test_pane_fallback_skipped_when_agents_present(monkeypatch) -> None:
    """Pane list is not used as a fallback when agent list already produced workers."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): {
            "result": {"agents": [{"agent_session": {"value": "sess-1"}, "agent": "Agent"}]}
        },
        ("pane", "list"): {"result": {"panes": [{"pane_id": "sess-1", "agent": "Agent"}]}},
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 1
    assert workers[0].id == "Agent"


def test_agent_and_pane_duplicates_emit_one_worker(monkeypatch) -> None:
    """A worker described by both agent and pane payloads is deduplicated."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): _completed("", returncode=1),
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {
            "result": {
                "panes": [
                    {"pane_id": "pane-1", "agent": "Runner", "workspace_id": "ws-1"},
                    {"pane_id": "pane-1", "agent": "Runner", "workspace_id": "ws-1"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 1
    assert workers[0].id == "Runner"


def test_repeated_agent_names_with_distinct_ids_remain_distinct(monkeypatch) -> None:
    """Workers with the same display name but different session ids are kept."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {"agent_session": {"value": "sess-a"}, "agent": "Coder", "workspace_id": "ws-1"},
                    {"agent_session": {"value": "sess-b"}, "agent": "Coder", "workspace_id": "ws-1"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 2
    assert {w.id for w in workers} == {"Coder-1", "Coder-2"}
    assert workers[0].id < workers[1].id
    assert all((w.backend_target or {}).get("kind") == "agent" for w in workers)
    assert all((w.backend_target or {}).get("value") == "Coder" for w in workers)
    assert all((w.backend_target or {}).get("sendable") is False for w in workers)
    assert all((w.backend_target or {}).get("reason") == "not_unique" for w in workers)


def test_status_aliases_for_live_herdr(monkeypatch) -> None:
    """Working maps to active, done maps to done, and raw_status is preserved."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [
                    {"workspace_id": "ws-1", "label": "Space", "agent_status": "working"},
                    {"workspace_id": "ws-2", "label": "Space2", "agent_status": "responding"},
                ]
            }
        },
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {"agent_session": {"value": "sess-1"}, "agent": "Agent", "workspace_id": "ws-1", "agent_status": "done"},
                    {"agent_session": {"value": "sess-2"}, "agent": "Agent", "workspace_id": "ws-2", "agent_status": "awaiting-input"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert spaces[0].status == "active"
    assert spaces[0].meta.get("raw_status") == "working"
    assert spaces[1].status == "waiting"
    assert spaces[1].meta.get("raw_status") == "responding"
    by_space = {w.space_id: w for w in workers}
    assert by_space["ws-1"].status == "done"
    assert "raw_status" not in by_space["ws-1"].meta
    assert by_space["ws-2"].status == "waiting"
    assert by_space["ws-2"].meta.get("raw_status") == "awaiting-input"


def test_forbidden_connector_fields_stripped_in_herdr_070(monkeypatch) -> None:
    """Connector/delivery fields are stripped from live Herdr 0.7.0 payloads."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [
                    {
                        "workspace_id": "ws-1",
                        "label": "Space",
                        "agent_status": "idle",
                        "telegram": "leaked",
                        "chat_id": 123,
                    }
                ]
            }
        },
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {
                        "agent_session": {"value": "sess-1"},
                        "agent": "Agent",
                        "workspace_id": "ws-1",
                        "agent_status": "active",
                        "route": "telegram",
                        "delivery": {"topic_id": 456},
                        "herdres_delivery": {"message_id": 789},
                    }
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)
    snapshot = project_from_observations(config, spaces=spaces, workers=workers)
    payload = json.loads(snapshot.to_json())

    _assert_no_forbidden_fields(payload)
    assert "telegram" not in payload["spaces"][0]["meta"]
    assert "route" not in payload["workers"][0]["meta"]
