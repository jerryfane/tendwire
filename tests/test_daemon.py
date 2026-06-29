"""Tests for the PR7 Tendwire daemon skeleton and local JSON API."""

from __future__ import annotations

import io
import json
import socket
import threading
from pathlib import Path
from typing import Any

from tendwire.backends.herdr_cli import HerdrCommandObservation
from tendwire.cli import main
from tendwire.config import Config
from tendwire.core.commands import STATUS_ACCEPTED, STATUS_INVALID_REQUEST, CommandEnvelope
from tendwire.core.models import (
    AttentionSignal,
    BackendHealth,
    Snapshot,
    SuggestedAction,
    Worker,
    WorkerBinding,
)
from tendwire.core.projector import project_from_raw
from tendwire.daemon import DaemonHooks, TendwireDaemon
from tendwire.daemon_api import DaemonAPIClient, TendwireDaemonAPI
from tendwire.store.sqlite import get_command_receipt, init_store, latest_snapshot, save_snapshot


_PUBLIC_JSON_FORBIDDEN_KEYS = {
    "tty",
    "pty",
    "pid",
    "process_id",
    "pane_id",
    "terminal_id",
    "backend_target",
    "session_id",
    "private",
    "private_binding",
    "private_fingerprint",
    "route",
    "delivery",
    "connector",
    "command",
    "raw_command",
    "chat_id",
    "topic_id",
    "message_id",
    "token",
    "secret",
    "password",
    "credentials",
}
_PUBLIC_JSON_FORBIDDEN_COMPACT = {key.replace("_", "") for key in _PUBLIC_JSON_FORBIDDEN_KEYS}


def _assert_no_public_json_forbidden(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            assert (
                normalized not in _PUBLIC_JSON_FORBIDDEN_KEYS
                and normalized.replace("_", "") not in _PUBLIC_JSON_FORBIDDEN_COMPACT
            ), f"forbidden field {path}.{key}"
            _assert_no_public_json_forbidden(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_public_json_forbidden(item, f"{path}[{index}]")


def _public_snapshot() -> Snapshot:
    return Snapshot(
        host_id="daemon-host",
        updated_at="2026-01-01T00:00:00+00:00",
        workers=[
            Worker(
                id="worker-1",
                name="Worker One",
                status="waiting",
                summary="approval required before continuing",
                meta={
                    "safe": "kept",
                    "tty": "sentinel-private-tty",
                    "pane_id": "sentinel-private-pane",
                    "connectorId": "sentinel-private-connector",
                    "authToken": "sentinel-private-token",
                },
                backend_target={
                    "kind": "agent_id",
                    "value": "sentinel-private-target",
                    "sendable": True,
                },
            )
        ],
        attention=[
            AttentionSignal(
                kind="worker_status",
                severity="warning",
                status="waiting",
                reason="approval required before continuing",
                source="worker:worker-1",
                updated_at="2026-01-01T00:00:00+00:00",
                suggested_actions=[
                    SuggestedAction(
                        action_id="approve",
                        label="Approve",
                        tendwire_action="approve",
                        params={"safe": "kept", "message_id": "sentinel-private-message"},
                    )
                ],
                meta={"needs_human": True, "space_id": "space-1", "private": "sentinel-private-meta"},
            )
        ],
        backend_health=[
            BackendHealth(
                name="herdr",
                status="healthy",
                outcome="healthy_non_empty",
                observed_at="2026-01-01T00:00:00+00:00",
                message="healthy",
                counts={"workers": 1},
            )
        ],
    )


def test_daemon_api_required_methods_are_public_safe() -> None:
    snapshot = _public_snapshot()
    calls: list[dict[str, Any]] = []
    api = TendwireDaemonAPI(
        get_snapshot=lambda: snapshot,
        get_health=lambda: {
            "schema_version": 1,
            "status": "ok",
            "host_id": snapshot.host_id,
            "backend_health": [health.to_dict() for health in snapshot.backend_health],
        },
        submit_command=lambda params: calls.append(dict(params))
        or CommandEnvelope.error(
            None,
            {
                "code": STATUS_INVALID_REQUEST,
                "message": "bad command",
                "details": {"fields": ["$.tty"]},
            },
        ),
    )

    for method in ("ping", "health.get", "snapshot.get", "attention.list", "turn.list", "pending.list"):
        response = api.dispatch({"method": method})
        assert response["ok"] is True
        encoded = json.dumps(response)
        assert "sentinel-private" not in encoded
        _assert_no_public_json_forbidden(response)

    command_response = api.dispatch(
        {
            "method": "command.submit",
            "params": {
                "schema_version": 1,
                "action": "noop",
                "tty": "sentinel-private-tty",
            },
        }
    )
    assert command_response["ok"] is True
    assert command_response["result"]["ok"] is False
    assert calls[0]["tty"] == "sentinel-private-tty"
    assert "sentinel-private" not in json.dumps(command_response)
    _assert_no_public_json_forbidden(command_response)


def test_daemon_starts_observes_persists_serves_and_removes_socket(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon.db"
    socket_path = tmp_path / "daemon.sock"
    config = Config(host_id="daemon-host", data_dir=tmp_path, db_path=db_path, socket_path=socket_path)

    def observe(config: Config) -> Snapshot:
        snapshot = project_from_raw(
            config,
            workers=[{"id": "worker-1", "name": "Worker One", "status": "active"}],
            backend_health=[
                {
                    "name": "herdr",
                    "status": "healthy",
                    "outcome": "healthy_non_empty",
                    "observed_at": "2026-01-01T00:00:00+00:00",
                    "counts": {"workers": 1},
                }
            ],
        )
        save_snapshot(db_path, snapshot)
        return snapshot

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(observe_initial_snapshot=observe),
    )
    daemon.start()
    thread = threading.Thread(target=daemon.serve_forever)
    thread.start()
    try:
        assert latest_snapshot(db_path, "daemon-host") is not None
        ping = DaemonAPIClient(socket_path).request("ping")
        snapshot_response = DaemonAPIClient(socket_path).request("snapshot.get")
        health_response = DaemonAPIClient(socket_path).request("health.get")

        assert ping["ok"] is True
        assert ping["result"]["pong"] is True
        assert snapshot_response["result"]["host_id"] == "daemon-host"
        assert snapshot_response["result"]["workers"][0]["id"] == "worker-1"
        assert health_response["result"]["store"]["status"] == "healthy"
    finally:
        daemon.stop()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert not socket_path.exists()


def test_daemon_command_submit_uses_existing_receipt_idempotency(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "commands.db"
    socket_path = tmp_path / "commands.sock"
    config = Config(host_id="cmd-host", data_dir=tmp_path, db_path=db_path, socket_path=socket_path)
    init_store(db_path)
    calls: list[tuple[Any, Any]] = []
    worker = Worker(id="w-1", name="Alpha", status="active")
    binding = WorkerBinding(
        host_id="cmd-host",
        worker_id="w-1",
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-private",
        sendable=True,
        reason=None,
        observed_at="2026-01-01T00:00:00+00:00",
        private_fingerprint="private-binding",
    )

    def observe(config: Config) -> Snapshot:
        snapshot = project_from_raw(
            config,
            workers=[{"id": "w-1", "name": "Alpha", "status": "active"}],
        )
        save_snapshot(db_path, snapshot)
        return snapshot

    def command_observation(config: Config, stored_bindings: list[WorkerBinding] | None = None) -> HerdrCommandObservation:
        return HerdrCommandObservation(
            spaces=[],
            workers=[worker],
            status="healthy",
            outcome="healthy_non_empty",
            bindings=[binding],
        )

    def send_instruction(config: Config, target: Any, instruction: Any) -> CommandEnvelope:
        calls.append((target, instruction))
        return CommandEnvelope(
            ok=True,
            status=STATUS_ACCEPTED,
            action="send_instruction",
            request_id=None,
            dry_run=False,
            result={"target": {"worker_id": target["worker_id"]}},
        )

    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", command_observation)
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", send_instruction)

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(observe_initial_snapshot=observe),
    )
    daemon.start()
    thread = threading.Thread(target=daemon.serve_forever)
    thread.start()
    try:
        client = DaemonAPIClient(socket_path)
        request = {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "req-1",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
        }
        first = client.request("command.submit", request)
        second = client.request("command.submit", request)

        assert first["ok"] is True
        assert first["result"]["status"] == STATUS_ACCEPTED
        assert second["ok"] is True
        assert second["result"]["status"] == STATUS_ACCEPTED
        assert len(calls) == 1
        assert get_command_receipt(db_path, "cmd-host", "req-1", "send_instruction") is not None
        assert "agent-private" not in json.dumps(first)
        _assert_no_public_json_forbidden(first)
    finally:
        daemon.stop()
        thread.join(timeout=2)


def test_cli_snapshot_falls_back_when_configured_socket_is_absent(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    socket_path = tmp_path / "absent.sock"

    def fake_state(config: Config) -> tuple[list[Any], list[Worker]]:
        return [], [Worker(id="fallback-worker", name="Fallback", status="active")]

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", fake_state)

    code = main(
        [
            "--host-id",
            "fallback-host",
            "--socket-path",
            str(socket_path),
            "snapshot",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["workers"][0]["id"] == "fallback-worker"


def test_cli_command_falls_back_when_configured_socket_is_stale(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    socket_path = tmp_path / "stale.sock"
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(str(socket_path))
    stale.close()

    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps({"schema_version": 1, "action": "noop"})))

    code = main(["--socket-path", str(socket_path), "command", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["ok"] is True
    assert payload["status"] == "noop"
