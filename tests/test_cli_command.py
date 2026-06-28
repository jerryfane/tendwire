"""Tests for the `tendwire command --json` CLI orchestration."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

from tendwire.cli import main
from tendwire.core.commands import (
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DRY_RUN,
    STATUS_DUPLICATE_REQUEST,
    STATUS_INVALID_REQUEST,
    STATUS_REQUEST_STATE_UNCERTAIN,
)
from tendwire.core.models import Space, Worker
from tendwire.store.sqlite import get_command_receipt


def _fake_herdr_state(config: Any) -> tuple[list[Space], list[Worker]]:
    workers = [
        Worker(id="w-1", name="Alpha", status="active", space_id="s-1"),
        Worker(id="w-2", name="Beta", status="idle", space_id="s-1"),
    ]
    return [], workers


def test_cli_command_invalid_json(capsys, monkeypatch) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO("not json"))
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["status"] == STATUS_INVALID_REQUEST


def test_cli_command_noop_success(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "noop"})),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["status"] == "noop"
    assert payload["schema_version"] == 1
    assert captured.err == ""


def test_cli_command_unknown_action_rejected(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "explode"})),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert captured.err == ""


def test_cli_command_read_snapshot_neutral_result(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "read_snapshot"})),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["status"] == "snapshot"
    assert payload["result"]["snapshot"]["schema_version"] == 2
    assert captured.err == ""


def test_cli_command_send_instruction_dry_run_no_receipt(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    db_path = tmp_path / "cmd.db"
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["status"] == STATUS_DRY_RUN
    assert payload["dry_run"] is True
    # Dry-runs never create receipts.
    assert get_command_receipt(db_path, "cmd-host", "", "send_instruction") is None


def test_cli_command_send_instruction_non_dry_run_requires_request_id(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["status"] == STATUS_INVALID_REQUEST


def test_cli_command_duplicate_request_id_same_payload_returns_cached(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    db_path = tmp_path / "cmd.db"
    payload = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "dup-1",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
        }
    )

    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    code1 = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured1 = capsys.readouterr()
    assert code1 == 1
    result1 = json.loads(captured1.out)
    assert result1["status"] == STATUS_BACKEND_UNSUPPORTED

    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    code2 = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured2 = capsys.readouterr()
    assert code2 == 1
    result2 = json.loads(captured2.out)
    assert result2["status"] == STATUS_BACKEND_UNSUPPORTED
    assert result2 == result1


def test_cli_command_duplicate_request_id_different_payload_rejects(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    db_path = tmp_path / "cmd.db"
    payload1 = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "dup-2",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
        }
    )
    payload2 = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "dup-2",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "world"},
        }
    )

    monkeypatch.setattr("sys.stdin", io.StringIO(payload1))
    main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    capsys.readouterr()

    monkeypatch.setattr("sys.stdin", io.StringIO(payload2))
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    result = json.loads(captured.out)
    assert result["status"] == STATUS_DUPLICATE_REQUEST


def test_cli_command_pending_receipt_rejects_without_retry(capsys, monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "cmd.db"
    # Seed an uncertain receipt directly.
    from tendwire.store.sqlite import init_store, save_command_receipt

    init_store(db_path)
    save_command_receipt(
        db_path,
        host_id="cmd-host",
        request_id="uncertain-1",
        action="send_instruction",
        payload_fingerprint="fp",
        status=STATUS_REQUEST_STATE_UNCERTAIN,
        result_json=json.dumps(
            {
                "schema_version": 1,
                "action": "send_instruction",
                "request_id": "uncertain-1",
                "ok": False,
                "dry_run": False,
                "status": STATUS_REQUEST_STATE_UNCERTAIN,
                "result": None,
                "error": {"code": STATUS_REQUEST_STATE_UNCERTAIN, "message": "pending", "details": {}},
                "warnings": [],
            }
        ),
        uncertain=True,
    )

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "uncertain-1",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    result = json.loads(captured.out)
    assert result["status"] == STATUS_REQUEST_STATE_UNCERTAIN


def test_cli_command_forbidden_field_rejected(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "noop",
                    "params": {"pane_id": "leaked"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["status"] == STATUS_INVALID_REQUEST


def test_cli_command_backend_unsupported_result_is_sanitized(capsys, monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "cmd.db"
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "sup-1",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["status"] == STATUS_BACKEND_UNSUPPORTED
    assert "send_instruction" in str(payload)
    assert captured.err == ""
