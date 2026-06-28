"""Tests for the `tendwire command --json` CLI orchestration."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

from tendwire.cli import main
from tendwire.core.commands import (
    STATUS_ACCEPTED,
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_DRY_RUN,
    STATUS_DUPLICATE_REQUEST,
    STATUS_INVALID_REQUEST,
    STATUS_REQUEST_STATE_UNCERTAIN,
    CommandEnvelope,
)
from tendwire.core.models import Space, Worker
from tendwire.store.sqlite import get_command_receipt


def _fake_herdr_state(config: Any) -> tuple[list[Space], list[Worker]]:
    workers = [
        Worker(id="w-1", name="Alpha", status="active", space_id="s-1"),
        Worker(id="w-2", name="Beta", status="idle", space_id="s-1"),
    ]
    return [], workers

def _accepted_backend(calls: list[tuple[Any, Any]]):
    def send(config: Any, target: Any, instruction: Any) -> CommandEnvelope:
        calls.append((target, instruction))
        return CommandEnvelope(
            ok=True,
            status=STATUS_ACCEPTED,
            action="send_instruction",
            result={"target": target},
        )

    return send


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
    calls: list[str] = []

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("noop must not fetch Herdr state")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
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
    assert calls == []


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
    monkeypatch.setattr(
        "tendwire.cli.herdr_send_instruction",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("backend called")),
    )
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


def test_cli_command_send_instruction_empty_target_rejects_before_fetch(capsys, monkeypatch) -> None:
    calls: list[str] = []

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("empty target must reject before Herdr fetch")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "empty-target",
                    "dry_run": False,
                    "target": {},
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
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert calls == []


def test_cli_command_duplicate_request_id_same_payload_returns_cached(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    calls: list[tuple[Any, Any]] = []
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", _accepted_backend(calls))
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
    assert code1 == 0
    result1 = json.loads(captured1.out)
    assert result1["status"] == STATUS_ACCEPTED

    receipt = get_command_receipt(db_path, "cmd-host", "dup-1", "send_instruction")
    assert receipt is not None
    assert receipt["uncertain"] is False
    assert receipt["completed_at"] is not None

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
    assert code2 == 0
    result2 = json.loads(captured2.out)
    assert result2["status"] == STATUS_ACCEPTED
    assert result2 == result1
    assert len(calls) == 1


def test_cli_command_duplicate_request_id_different_payload_rejects(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    calls: list[tuple[Any, Any]] = []
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", _accepted_backend(calls))
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
    capsys.readouterr()
    assert code1 == 0

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
    assert len(calls) == 1


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


def test_cli_command_backend_result_and_receipt_are_sanitized(capsys, monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "cmd.db"
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)

    def leaky_backend(config: Any, target: Any, instruction: Any) -> CommandEnvelope:
        return CommandEnvelope(
            ok=False,
            status=STATUS_BACKEND_FAILED,
            action="send_instruction",
            result={
                "target": target,
                "pane_id": "p-1",
                "nested": {"argv": ["herdr"], "safe": "kept"},
            },
            error={
                "code": STATUS_BACKEND_FAILED,
                "message": "failed",
                "details": {"terminal_id": "t-1", "safe": "kept"},
            },
        )

    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", leaky_backend)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "sanitize-1",
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
    assert payload["status"] == STATUS_BACKEND_FAILED
    assert payload["result"]["nested"]["safe"] == "kept"
    assert payload["error"]["details"] == {"safe": "kept"}
    _assert_no_command_public_forbidden_fields(payload)

    receipt = get_command_receipt(db_path, "cmd-host", "sanitize-1", "send_instruction")
    assert receipt is not None
    cached = json.loads(receipt["result_json"])
    _assert_no_command_public_forbidden_fields(cached)
    assert captured.err == ""


_COMMAND_PUBLIC_FORBIDDEN_KEYS = {
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
}


def _assert_no_command_public_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _COMMAND_PUBLIC_FORBIDDEN_KEYS, f"forbidden field {path}.{key}"
            _assert_no_command_public_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_command_public_forbidden_fields(item, f"{path}[{index}]")


def _fake_herdr_state_with_terminal(config: Any) -> tuple[list[Space], list[Worker]]:
    return [], [
        Worker(
            id="w-terminal",
            name="Terminal",
            status="active",
            space_id="s-1",
            meta={
                "pane_id": "p-1",
                "terminal_id": "t-1",
                "pid": 123,
                "tty": "/dev/pts/0",
                "pty": "pts",
                "tmux": "sess",
                "screen_session": "scr",
                "window_id": "win-1",
                "tab_id": "tab-1",
                "argv": ["bash"],
                "shell": "bash",
                "command": "python app.py",
                "route": "telegram",
                "routes": ["r1"],
                "delivery": {"id": 1},
                "deliveries": [{"id": 2}],
                "token": "secret",
                "tokens": ["t1"],
                "connector": {"x": 1},
                "connectors": [{"y": 2}],
                "safe": "kept",
            },
        )
    ]


def test_cli_command_read_snapshot_strips_command_public_terminal_fields(
    capsys, monkeypatch
) -> None:
    """Command-public read_snapshot strips terminal/connector identifiers while
    leaving the ordinary snapshot --json output unchanged.
    """
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state_with_terminal)
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
    assert payload["action"] == "read_snapshot"
    assert "request_id" in payload
    meta = payload["result"]["snapshot"]["workers"][0]["meta"]
    for key in _COMMAND_PUBLIC_FORBIDDEN_KEYS:
        assert key not in meta, key
    assert meta["safe"] == "kept"

    # The same worker data still flows through the standalone snapshot path.
    code2 = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
        ]
    )
    captured2 = capsys.readouterr()
    assert code2 == 0
    snapshot = json.loads(captured2.out)
    assert snapshot["schema_version"] == 2
    snap_meta = snapshot["workers"][0]["meta"]
    assert snap_meta["pane_id"] == "p-1"
    assert snap_meta["terminal_id"] == "t-1"
    assert snap_meta["safe"] == "kept"


def test_cli_command_forbidden_field_rejects_before_backend_and_store(
    capsys, monkeypatch
) -> None:
    """A contract-invalid request must be rejected before any backend or store call."""
    calls: list[str] = []

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("fetch_herdr_state called before validation")

    def guarded_get_receipt(*args: Any, **kwargs: Any) -> Any:
        calls.append("get_receipt")
        raise AssertionError("get_command_receipt called before validation")

    def guarded_save_receipt(*args: Any, **kwargs: Any) -> None:
        calls.append("save_receipt")
        raise AssertionError("save_command_receipt called before validation")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
    monkeypatch.setattr("tendwire.cli.get_command_receipt", guarded_get_receipt)
    monkeypatch.setattr("tendwire.cli.save_command_receipt", guarded_save_receipt)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "rej-1",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
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
    assert payload["request_id"] == "rej-1"
    assert calls == []


def test_cli_command_raw_top_level_forbidden_rejects_before_pipeline(
    capsys, monkeypatch
) -> None:
    """Raw top-level forbidden fields reject before store, projection, or backend work."""
    calls: list[str] = []

    def guarded_get_receipt(*args: Any, **kwargs: Any) -> Any:
        calls.append("get_receipt")
        raise AssertionError("get_command_receipt called before raw validation")

    def guarded_save_receipt(*args: Any, **kwargs: Any) -> None:
        calls.append("save_receipt")
        raise AssertionError("save_command_receipt called before raw validation")

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("fetch_herdr_state called before raw validation")

    def guarded_project(*args: Any, **kwargs: Any) -> Any:
        calls.append("project")
        raise AssertionError("project_from_observations called before raw validation")

    def guarded_execute(*args: Any, **kwargs: Any) -> Any:
        calls.append("execute")
        raise AssertionError("execute_command called before raw validation")

    def guarded_send(*args: Any, **kwargs: Any) -> Any:
        calls.append("backend")
        raise AssertionError("backend sender called before raw validation")

    monkeypatch.setattr("tendwire.cli.get_command_receipt", guarded_get_receipt)
    monkeypatch.setattr("tendwire.cli.save_command_receipt", guarded_save_receipt)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
    monkeypatch.setattr("tendwire.cli.project_from_observations", guarded_project)
    monkeypatch.setattr("tendwire.cli.execute_command", guarded_execute)
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", guarded_send)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "raw-rej",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                    "pane_id": "leaked",
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
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert payload["request_id"] is None
    assert "pane_id" in str(payload["error"]["details"])
    assert calls == []


def test_cli_command_backend_unavailable_preserves_request_id(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    """Non-dry-run send_instruction preserves request_id in stdout and receipt."""
    db_path = tmp_path / "req.db"
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "req-visible",
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
    assert payload["status"] == STATUS_BACKEND_UNAVAILABLE
    assert payload["request_id"] == "req-visible"

    receipt = get_command_receipt(db_path, "cmd-host", "req-visible", "send_instruction")
    assert receipt is not None
    cached = json.loads(receipt["result_json"])
    assert cached["request_id"] == "req-visible"
    assert cached["status"] == STATUS_BACKEND_UNAVAILABLE
