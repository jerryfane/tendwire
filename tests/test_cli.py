"""Tests for tendwire CLI snapshot JSON output and optional storage."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from tendwire.backends import herdr_cli
from tendwire.cli import main
from tendwire.core.models import Space, Worker
from tendwire.store.sqlite import latest_snapshot, list_worker_bindings


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


def test_cli_snapshot_store_persists_private_bindings_outside_snapshot_payload(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "bindings.db"
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-worker",
                        "agent_id": "agent-secret",
                        "agent": "Worker",
                        "pane_id": "pane-secret",
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
    assert "pane-secret" not in encoded
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
                                "workspace_id": "ws-cli",
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
                                "workspace_id": "ws-cli",
                                "agent_status": "executing",
                                "cwd": "/tmp",
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
    assert payload["spaces"][0]["id"] == "ws-cli"
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
