"""Tests for tendwire CLI snapshot JSON output and optional storage."""

from __future__ import annotations

import io
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from tendwire.backends import herdr_cli
from tendwire.cli import main
from tendwire.config import Config
from tendwire.core.models import AttentionSignal, Snapshot, Space, SuggestedAction, Worker
from tendwire.core.projector import project_from_raw
from tendwire.store.sqlite import append_event, init_store, latest_snapshot, list_worker_bindings, save_snapshot


_PUBLIC_JSON_FORBIDDEN_KEYS = {
    "tty",
    "pty",
    "pid",
    "pids",
    "process_id",
    "process_ids",
    "tmux",
    "tmux_session",
    "tmux_sessions",
    "screen_session",
    "screen_sessions",
    "window_id",
    "window_ids",
    "tab_id",
    "tab_ids",
    "pane_id",
    "pane_ids",
    "terminal_id",
    "terminal_ids",
    "backend_target",
    "backend_targets",
    "session_id",
    "private",
    "private_binding",
    "private_bindings",
    "private_fingerprint",
    "private_fingerprints",
    "route",
    "routes",
    "delivery",
    "deliveries",
    "connector",
    "connectors",
    "command",
    "command_args",
    "command_argv",
    "command_line",
    "command_payload",
    "command_text",
    "raw_args",
    "raw_argv",
    "raw_command",
    "raw_command_line",
    "shell_command",
    "chat_id",
    "chat_ids",
    "topic_id",
    "topic_ids",
    "message_id",
    "message_ids",
    "token",
    "tokens",
    "secret",
    "secrets",
    "password",
    "passwords",
    "credentials",
    "cookie",
    "auth_token",
    "auth_tokens",
}
_PUBLIC_JSON_FORBIDDEN_COMPACT = {
    key.replace("_", "") for key in _PUBLIC_JSON_FORBIDDEN_KEYS
}


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


def test_cli_snapshot_json_prints_contract_json_only(capsys) -> None:
    code = main(
        [
            "--host-id",
            "cli-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "cli-host"
    assert len(payload["content_fingerprint"]) == 24
    assert {"updated_at", "spaces", "workers", "attention", "backend_health"} <= set(payload)
    assert payload["backend_health"][0]["name"] == "herdr"
    assert payload["backend_health"][0]["status"] == "unavailable"
    assert payload["backend_health"][0]["outcome"] == "missing_binary"


def test_cli_snapshot_no_herdr_works() -> None:
    """Empty snapshot works even when herdr is not installed."""
    code = main(["--herdr-bin", "definitely-not-a-real-herdr-binary", "snapshot", "--json"])
    assert code == 0


def test_cli_turns_json_no_herdr_prints_public_empty_collection(capsys) -> None:
    code = main(
        [
            "--host-id",
            "turns-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "turns",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["schema_version"] == 1
    assert payload["host_id"] == "turns-host"
    assert payload["turns"] == []
    assert len(payload["content_fingerprint"]) == 24
    assert payload["backend_health"][0]["name"] == "herdr"
    assert payload["backend_health"][0]["status"] == "unavailable"
    assert payload["backend_health"][0]["outcome"] == "missing_binary"


def test_cli_pending_json_no_herdr_prints_public_empty_collection(capsys) -> None:
    code = main(
        [
            "--host-id",
            "pending-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "pending",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["schema_version"] == 1
    assert payload["host_id"] == "pending-host"
    assert payload["pending_interactions"] == []
    assert len(payload["content_fingerprint"]) == 24
    assert payload["backend_health"][0]["name"] == "herdr"
    assert payload["backend_health"][0]["status"] == "unavailable"
    assert payload["backend_health"][0]["outcome"] == "missing_binary"


def test_cli_store_hooks_print_json_only_and_support_dry_run(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "store-cli.db"
    init_store(db_path)
    append_event(
        db_path,
        "store-cli",
        "private.event",
        {"pane_id": "sentinel-private-pane", "raw_payload": "sentinel-private-raw"},
        observed_at="2026-01-01T00:00:00+00:00",
    )
    append_event(
        db_path,
        "store-cli",
        "public.event",
        {"safe": "kept"},
        observed_at="9999-01-09T00:00:00+00:00",
    )

    status_code = main(["--host-id", "store-cli", "store", "status", "--db-path", str(db_path)])
    status_captured = capsys.readouterr()
    status_payload = json.loads(status_captured.out)

    tail_code = main(
        [
            "--host-id",
            "store-cli",
            "store",
            "events-tail",
            "--db-path",
            str(db_path),
            "--limit",
            "5",
        ]
    )
    tail_captured = capsys.readouterr()
    tail_payload = json.loads(tail_captured.out)

    cleanup_code = main(
        [
            "--host-id",
            "store-cli",
            "store",
            "cleanup",
            "--db-path",
            str(db_path),
            "--retention-days",
            "7",
            "--dry-run",
        ]
    )
    cleanup_captured = capsys.readouterr()
    cleanup_payload = json.loads(cleanup_captured.out)

    missing_code = main(
        [
            "--host-id",
            "store-cli",
            "store",
            "status",
            "--db-path",
            str(tmp_path / "missing.db"),
        ]
    )
    missing_captured = capsys.readouterr()
    missing_payload = json.loads(missing_captured.out)

    with sqlite3.connect(str(db_path)) as conn:
        event_count = conn.execute("SELECT COUNT(*) FROM events WHERE host_id = ?", ("store-cli",)).fetchone()[0]

    assert status_code == 0
    assert tail_code == 0
    assert cleanup_code == 0
    assert missing_code == 1
    assert status_captured.err == tail_captured.err == cleanup_captured.err == missing_captured.err == ""
    assert status_payload["counts"]["events"] == 2
    assert tail_payload["events"]
    assert "sentinel-private" not in json.dumps(tail_payload)
    assert "payload_json" not in json.dumps(tail_payload)
    assert cleanup_payload["dry_run"] is True
    assert cleanup_payload["retention"]["deleted"] == 1
    assert event_count == 2
    assert missing_payload["status"] == "store_unavailable"


def test_cli_turns_and_pending_project_from_snapshot_observation(capsys, monkeypatch) -> None:
    def _fake_herdr_state(config):
        return [
            Space(id="space-1", name="Space One", status="active"),
        ], [
            Worker(
                id="worker-1",
                name="Worker One",
                status="pending",
                space_id="space-1",
                summary="human approval required before continuing",
                meta={
                    "needs_human": True,
                    "safe": "kept",
                    "pane_id": "pane-private",
                    "tty": "sentinel-cli-tty",
                    "pty": "sentinel-cli-pty",
                    "pid": "sentinel-cli-pid",
                    "processId": "sentinel-cli-process",
                    "tmux-session": "sentinel-cli-tmux",
                    "screenSession": "sentinel-cli-screen",
                    "window_id": "sentinel-cli-window",
                    "tabId": "sentinel-cli-tab",
                    "terminalid": "sentinel-cli-terminal",
                    "backendTarget": "sentinel-cli-backend",
                    "session-id": "sentinel-cli-session",
                    "messageIds": "sentinel-cli-message-ids",
                    "terminalIds": "sentinel-cli-terminal-ids",
                    "terminal": "sentinel-cli-terminal-object",
                    "telegramMessageId": "sentinel-cli-telegram-message",
                    "routeId": "sentinel-cli-route-id",
                    "connectorId": "sentinel-cli-connector-id",
                    "tmuxPaneId": "sentinel-cli-tmux-pane-id",
                    "screenWindowId": "sentinel-cli-screen-window-id",
                    "agentSessionId": "sentinel-cli-agent-session-id",
                    "session": "sentinel-cli-session-object",
                    "privateFingerprints": "sentinel-cli-private-fingerprints",
                    "passwords": "sentinel-cli-passwords",
                    "privateBinding": "sentinel-cli-private-binding",
                    "authToken": "sentinel-cli-auth",
                },
                backend_target={"kind": "agent_id", "value": "agent-private", "sendable": True},
            )
        ]

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)

    turns_code = main(["--host-id", "projection-cli", "--herdr-bin", "herdr", "turns", "--json"])
    turns_captured = capsys.readouterr()
    turns_payload = json.loads(turns_captured.out)
    pending_code = main(["--host-id", "projection-cli", "--herdr-bin", "herdr", "pending", "--json"])
    pending_captured = capsys.readouterr()
    pending_payload = json.loads(pending_captured.out)

    encoded_turns = json.dumps(turns_payload)
    encoded_pending = json.dumps(pending_payload)
    assert turns_code == 0
    assert pending_code == 0
    assert turns_captured.err == ""
    assert pending_captured.err == ""
    assert turns_payload["turns"][0]["worker_id"] == "worker-1"
    assert turns_payload["turns"][0]["status"] == "waiting"
    assert turns_payload["turns"][0]["kind"] == "task"
    assert pending_payload["pending_interactions"][0]["worker_id"] == "worker-1"
    assert pending_payload["pending_interactions"][0]["kind"] == "approval"
    assert pending_payload["pending_interactions"][0]["status"] == "open"
    assert "agent-private" not in encoded_turns
    assert "pane-private" not in encoded_turns
    assert "agent-private" not in encoded_pending
    assert "pane-private" not in encoded_pending
    assert "sentinel-cli-" not in encoded_turns
    assert "sentinel-cli-" not in encoded_pending
    _assert_no_public_json_forbidden(turns_payload)
    _assert_no_public_json_forbidden(pending_payload)


def test_cli_turns_and_pending_json_strip_raw_command_action_material(capsys, monkeypatch) -> None:
    def _fake_snapshot(config):
        return Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:00:00+00:00",
            workers=[
                Worker(
                    id="worker-1",
                    name="Worker One",
                    status="waiting",
                    space_id="space-1",
                    summary="waiting for action",
                )
            ],
            attention=[
                AttentionSignal(
                    kind="worker_status",
                    severity="warning",
                    status="waiting",
                    reason="Choose next action",
                    source="worker:worker-1",
                    updated_at="2026-01-01T00:00:00+00:00",
                    suggested_actions=[
                        SuggestedAction(
                            command="sentinel-cli-safe-looking-command-alias",
                            params={
                                "safe_choice": "kept",
                                "commandLine": "sentinel-cli-command-line",
                                "terminal_id": "sentinel-cli-terminal",
                                "backendTarget": "sentinel-cli-backend",
                                "session-id": "sentinel-cli-session",
                                "token": "sentinel-cli-token",
                                "secret": "sentinel-cli-secret",
                            },
                        )
                    ],
                    meta={"worker_id": "worker-1", "space_id": "space-1", "needs_human": True},
                    host_id=config.host_id,
                )
            ],
        )

    monkeypatch.setattr("tendwire.cli._current_public_snapshot", _fake_snapshot)

    turns_code = main(["--host-id", "raw-command-cli", "turns", "--json"])
    turns_captured = capsys.readouterr()
    pending_code = main(["--host-id", "raw-command-cli", "pending", "--json"])
    pending_captured = capsys.readouterr()
    turns_payload = json.loads(turns_captured.out)
    pending_payload = json.loads(pending_captured.out)
    encoded_turns = json.dumps(turns_payload, sort_keys=True)
    encoded_pending = json.dumps(pending_payload, sort_keys=True)

    assert turns_code == 0
    assert pending_code == 0
    assert turns_captured.err == ""
    assert pending_captured.err == ""
    assert turns_payload["turns"][0]["worker_id"] == "worker-1"
    assert pending_payload["pending_interactions"][0]["choices"] == [
        {
            "choice_id": pending_payload["pending_interactions"][0]["choices"][0]["choice_id"],
            "label": "Action",
            "params": {"safe_choice": "kept"},
        }
    ]
    assert "sentinel-cli-" not in encoded_turns
    assert "sentinel-cli-" not in encoded_pending
    _assert_no_public_json_forbidden(turns_payload)
    _assert_no_public_json_forbidden(pending_payload)


def test_cli_snapshot_json_reports_healthy_empty_herdr(capsys, monkeypatch) -> None:
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {"result": {"panes": []}},
    }

    def _fake_run_herdr(args, cfg):
        if tuple(args) in responses:
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps(responses[tuple(args)]),
                stderr="",
            )
        return subprocess.CompletedProcess(args=list(args), returncode=1, stdout="", stderr="")

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", _fake_run_herdr)

    code = main(["--host-id", "cli-empty", "--herdr-bin", "herdr", "snapshot", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["spaces"] == []
    assert payload["workers"] == []
    assert payload["backend_health"][0]["status"] == "healthy"
    assert payload["backend_health"][0]["outcome"] == "empty_healthy"
    assert payload["backend_health"][0]["counts"] == {"spaces": 0, "workers": 0}


def test_cli_snapshot_store_persists_printed_snapshot(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "cli.db"
    code = main(
        [
            "--host-id",
            "cli-store",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--db-path",
            str(db_path),
            "--json",
            "--store",
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    payload = json.loads(captured.out)
    assert captured.err == ""
    restored = latest_snapshot(db_path)
    assert restored is not None
    assert restored.host_id == "cli-store"
    assert restored.content_fingerprint == payload["content_fingerprint"]


def test_cli_attention_json_reads_store_backed_lifecycle(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "attention.db"
    socket_path = tmp_path / "absent.sock"
    config = Config(host_id="cli-attention", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": "blocked",
                "meta": {
                    "safe": "kept",
                    "pane_id": "sentinel-private-pane",
                    "terminalId": "sentinel-private-terminal",
                    "backendTarget": "sentinel-private-backend",
                    "authToken": "sentinel-private-token",
                },
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "counts": {"workers": 1},
            }
        ],
        timestamp=datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
    )
    save_snapshot(db_path, snapshot)

    code = main(
        [
            "--host-id",
            "cli-attention",
            "--socket-path",
            str(socket_path),
            "attention",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["host_id"] == "cli-attention"
    assert payload["attention"][0]["lifecycle_status"] == "open"
    assert payload["attention"][0]["first_seen_at"] == "2026-01-01T00:00:00+00:00"
    assert payload["attention"][0]["signal_count"] == 1
    assert "sentinel-private" not in json.dumps(payload, sort_keys=True)
    _assert_no_public_json_forbidden(payload)


def test_cli_attention_json_falls_back_to_snapshot_when_store_is_unavailable(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "missing.db"
    socket_path = tmp_path / "absent.sock"

    def _fake_herdr_state(config):
        return [], [
            Worker(
                id="worker-1",
                name="Worker One",
                status="blocked",
                meta={"pane_id": "sentinel-private-pane"},
            )
        ]

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)

    code = main(
        [
            "--host-id",
            "cli-attention-fallback",
            "--socket-path",
            str(socket_path),
            "attention",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert len(payload["attention"]) == 1
    assert payload["attention"][0]["status"] == "blocked"
    assert "first_seen_at" not in payload["attention"][0]
    assert "sentinel-private" not in json.dumps(payload, sort_keys=True)
    _assert_no_public_json_forbidden(payload)


def test_cli_public_json_does_not_emit_connector_private_store_rows(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "connector-private.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "public-host",
                "sentinel-connector-private",
                "sentinel-delivery-key",
                "queued",
                '{"safe":"kept"}',
                '{"chat_id":"sentinel-chat","route":"sentinel-route"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        outbox_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outbox_id,
                "public-host",
                "sentinel-connector-private",
                "sentinel-delivery-key",
                1,
                "delivered",
                '{"ok":true}',
                '{"message_id":"sentinel-message","token":"sentinel-token"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )

    payloads: list[dict[str, Any]] = []

    snapshot_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
            "--store",
            "--db-path",
            str(db_path),
        ]
    )
    snapshot_captured = capsys.readouterr()
    payloads.append(json.loads(snapshot_captured.out))

    turns_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "turns",
            "--json",
        ]
    )
    turns_captured = capsys.readouterr()
    payloads.append(json.loads(turns_captured.out))

    pending_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "pending",
            "--json",
        ]
    )
    pending_captured = capsys.readouterr()
    payloads.append(json.loads(pending_captured.out))

    doctor_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "doctor",
            "--json",
        ]
    )
    doctor_captured = capsys.readouterr()
    payloads.append(json.loads(doctor_captured.out))

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "read_snapshot"})),
    )
    command_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    command_captured = capsys.readouterr()
    payloads.append(json.loads(command_captured.out))

    with sqlite3.connect(str(db_path)) as conn:
        private_counts = (
            conn.execute("SELECT COUNT(*) FROM connector_outbox").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM connector_deliveries").fetchone()[0],
        )

    encoded = json.dumps(payloads, sort_keys=True)
    assert snapshot_code == 0
    assert turns_code == 0
    assert pending_code == 0
    assert doctor_code == 1
    assert command_code == 0
    assert private_counts == (1, 1)
    assert "sentinel-" not in encoded


def test_cli_snapshot_store_persists_private_bindings_outside_snapshot_payload(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "bindings.db"
    responses = {
        ("workspace", "list"): {
            "result": {
                "workspaces": [
                    {"workspace_id": "wA", "label": "Bindings"}
                ]
            }
        },
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-worker",
                        "agent_id": "agent-secret",
                        "agent": "Worker",
                        "workspace_id": "wA",
                        "pane_id": "wA:p1",
                    }
                ]
            }
        },
        ("pane", "list"): {
            "result": {
                "panes": [
                    {
                        "workspace_id": "wA",
                        "pane_id": "wA:p1",
                        "terminal_id": "terminal-secret",
                        "agent": "Worker",
                    }
                ]
            }
        },
    }

    def _fake_run_herdr(args, cfg):
        if tuple(args) in responses:
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps(responses[tuple(args)]),
                stderr="",
            )
        return subprocess.CompletedProcess(args=list(args), returncode=1, stdout="", stderr="")

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", _fake_run_herdr)

    code = main(
        [
            "--host-id",
            "cli-bindings",
            "--herdr-bin",
            "herdr",
            "snapshot",
            "--db-path",
            str(db_path),
            "--json",
            "--store",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    bindings = list_worker_bindings(db_path, "cli-bindings", backend="herdr")

    assert code == 0
    assert len(bindings) == 1
    assert bindings[0].worker_id == "public-worker"
    assert bindings[0].target_kind == "agent_id"
    assert bindings[0].target_value == "agent-secret"
    encoded = json.dumps(payload)
    assert "agent-secret" not in encoded
    assert "wA:p1" not in encoded
    assert "target_kind" not in encoded


def test_cli_module_invocation() -> None:
    """python -m tendwire.cli snapshot --json works."""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), "..", "src")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tendwire.cli",
            "--host-id",
            "module-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "module-host"
    assert len(payload["content_fingerprint"]) == 24


def test_cli_snapshot_with_live_shaped_herdr_fixtures(capsys, monkeypatch) -> None:
    """CLI emits schema v2 JSON with non-empty spaces and workers from Herdr fixtures."""

    def _fake_run_herdr(args, cfg):
        if tuple(args) == ("workspace", "list", "--json"):
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps({
                    "result": {
                        "workspaces": [
                            {
                                "workspace_id": "wA",
                                "label": "CLI Space",
                                "agent_status": "working",
                                "focused": True,
                            }
                        ]
                    }
                }),
                stderr="",
            )
        if tuple(args) == ("agent", "list", "--json"):
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps({
                    "result": {
                        "agents": [
                            {
                                "agent_session": {"value": "sess-cli"},
                                "agent": "CLI Agent",
                                "workspace_id": "wA",
                                "pane_id": "wA:p1",
                                "agent_status": "executing",
                                "cwd": "/tmp",
                            }
                        ]
                    }
                }),
                stderr="",
            )
        if tuple(args) == ("pane", "list"):
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps({
                    "result": {
                        "panes": [
                            {
                                "workspace_id": "wA",
                                "pane_id": "wA:p1",
                                "terminal_id": "terminal-cli",
                                "agent": "CLI Agent",
                                "agent_session": {"value": "sess-cli"},
                                "agent_status": "executing",
                            }
                        ]
                    }
                }),
                stderr="",
            )
        return subprocess.CompletedProcess(args=list(args), returncode=1, stdout="", stderr="")

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", _fake_run_herdr)

    code = main(["--host-id", "cli-live", "--herdr-bin", "herdr", "snapshot", "--json"])
    captured = capsys.readouterr()

    assert code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "cli-live"
    assert len(payload["spaces"]) == 1
    assert payload["spaces"][0]["id"] == "wA"
    assert payload["spaces"][0]["status"] == "active"
    assert len(payload["workers"]) == 1
    assert payload["workers"][0]["id"] == "CLI Agent"
    assert payload["workers"][0]["status"] == "active"
    assert payload["backend_health"][0]["name"] == "herdr"
    assert payload["backend_health"][0]["status"] == "healthy"
    assert payload["backend_health"][0]["outcome"] == "healthy_non_empty"
    assert payload["backend_health"][0]["counts"] == {"spaces": 1, "workers": 1}
    assert "agent_session" not in json.dumps(payload)
    assert "sess-cli" not in json.dumps(payload)
