"""Tests for the read-only Herdr doctor diagnostics."""

from __future__ import annotations

import json
import os
import subprocess
import socket
import sqlite3
import stat
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from tendwire.backends import herdr_cli
from tendwire.backends.herdr_cli import diagnose_herdr, fetch_herdr_state
from tendwire.cli import main
from tendwire.config import Config, load_config
from tendwire.local_state import (
    ConfigStateReport,
    LocalStateErrorCode,
    LocalStateIssue,
    LocalStateKind,
    PermissionResult,
    PermissionState,
)
from tendwire.store.sqlite import init_store


FIXTURES = Path(__file__).parent / "fixtures" / "herdr"

_FORBIDDEN_TEXT = (
    "telegram",
    "chat_id",
    "topic_id",
    "message_id",
    "thread_id",
    "argv",
    "token",
    "bot_token",
    "delivery",
    "route",
    "backend_target",
)


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _completed(args: Sequence[str], stdout: str = "", stderr: str = "", returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["herdr", *args],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def _doctor_outcomes(payload: dict[str, Any]) -> dict[str, str]:
    return {str(check["name"]): str(check["outcome"]) for check in payload["checks"]}

_HERDR_CHECK_NAMES = frozenset(
    {
        "workspace_list",
        "workspace_list_json",
        "agent_list",
        "agent_list_json",
        "pane_list",
        "pane_list_json",
    }
)
_LOCAL_STATE_CHECK_NAMES = (
    "state_directory_permissions",
    "database_permissions",
    "identity_permissions",
    "daemon_socket_permissions",
)


def _herdr_checks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        check
        for check in payload["checks"]
        if str(check["name"]) in _HERDR_CHECK_NAMES
    ]


def _local_state_checks(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(check["name"]): check
        for check in payload["checks"]
        if str(check["name"]) in _LOCAL_STATE_CHECK_NAMES
    }


def _maintenance_check(payload: dict[str, Any]) -> dict[str, Any]:
    matches = [
        check
        for check in payload["checks"]
        if check.get("name") == "store_maintenance"
    ]
    assert len(matches) == 1
    return matches[0]
def _pending_check(payload: dict[str, Any]) -> dict[str, Any]:
    matches = [
        check
        for check in payload["checks"]
        if check.get("name") == "pending_ingestion"
    ]
    assert len(matches) == 1
    return matches[0]




def _patch_healthy_herdr(monkeypatch, calls: list[tuple[str, ...]] | None = None) -> None:
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if calls is not None:
            calls.append(tuple(args[1:]))
        return _completed(args[1:], stdout='{"items": []}')

    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)


def _write_mode(path: Path, mode: int) -> None:
    path.write_bytes(b"local-state")
    os.chmod(path, mode)


def test_doctor_reports_missing_herdr_binary(monkeypatch, tmp_path: Path) -> None:
    config = Config(
        host_id="testhost",
        herdr_bin="missing-herdr",
        data_dir=tmp_path / "state",
    )
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: None)

    payload = diagnose_herdr(config)

    assert payload["status"] == "unavailable"
    assert _doctor_outcomes(payload)["workspace_list"] == "missing_binary"
    assert all(check["ok"] is False for check in _herdr_checks(payload))


def test_doctor_reports_timeout_and_skips_remaining_checks(monkeypatch, tmp_path: Path) -> None:
    config = Config(
        host_id="testhost",
        herdr_bin="herdr",
        herdr_timeout_seconds=0.25,
        data_dir=tmp_path / "state",
    )
    calls: list[tuple[str, ...]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(args[1:]))
        assert kwargs["timeout"] == 0.25
        raise subprocess.TimeoutExpired(cmd=args, timeout=kwargs["timeout"])

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)

    payload = diagnose_herdr(config)
    outcomes = _doctor_outcomes(payload)

    assert payload["status"] == "timeout"
    assert payload["aggregate_deadline_seconds"] == 1.5
    assert calls == [("workspace", "list")]
    assert outcomes["workspace_list"] == "timeout"
    assert outcomes["agent_list"] == "skipped_after_timeout"


def test_doctor_distinguishes_nonzero_malformed_empty_and_nonempty(monkeypatch, tmp_path: Path) -> None:
    config = Config(
        host_id="testhost",
        herdr_bin="herdr",
        data_dir=tmp_path / "state",
    )
    responses = {
        ("workspace", "list"): _completed(
            ["workspace", "list"],
            stderr=_fixture("nonzero_stderr.txt"),
            returncode=2,
        ),
        ("workspace", "list", "--json"): _completed(
            ["workspace", "list", "--json"],
            stdout=_fixture("workspace_list_json_empty.json"),
        ),
        ("agent", "list"): _completed(
            ["agent", "list"],
            stdout=_fixture("malformed.txt"),
        ),
        ("agent", "list", "--json"): _completed(
            ["agent", "list", "--json"],
            stdout=_fixture("agent_list_no_flag_nonempty.json"),
        ),
        ("pane", "list"): _completed(
            ["pane", "list"],
            stdout=_fixture("pane_list_empty.json"),
        ),
    }

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return responses[tuple(args[1:])]

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)

    payload = diagnose_herdr(config)
    outcomes = _doctor_outcomes(payload)

    assert payload["status"] == "degraded"
    assert outcomes["workspace_list"] == "nonzero"
    assert outcomes["workspace_list_json"] == "empty_healthy"
    assert outcomes["agent_list"] == "malformed_json"
    assert outcomes["agent_list_json"] == "healthy_non_empty"
    assert outcomes["pane_list"] == "empty_healthy"
    assert "herdr fixture error" in json.dumps(payload)


def test_doctor_cli_outputs_json_only_and_sanitizes_samples(capsys, monkeypatch, tmp_path: Path) -> None:
    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return _completed(args[1:], stdout="not json token=must-not-leak", returncode=0)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("TENDWIRE_DB_PATH", str(tmp_path / "state" / "tendwire.db"))

    code = main(["--host-id", "doctor-host", "--herdr-bin", "herdr", "doctor", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["schema_version"] == 1
    assert payload["command"] == "doctor"
    serialized = json.dumps(payload).lower()
    assert not any(forbidden in serialized for forbidden in _FORBIDDEN_TEXT)


def test_cli_herdr_timeout_knob_is_used_by_doctor(capsys, monkeypatch, tmp_path: Path) -> None:
    timeouts: list[float] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        timeouts.append(float(kwargs["timeout"]))
        return _completed(args[1:], stdout=_fixture("pane_list_empty.json"))

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("TENDWIRE_DB_PATH", str(tmp_path / "state" / "tendwire.db"))

    code = main(["--herdr-timeout", "0.75", "--herdr-bin", "herdr", "doctor", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert timeouts == [0.75, 0.75, 0.75]
    assert payload["timeout_seconds"] == 0.75
    assert payload["aggregate_deadline_seconds"] == 4.5
    assert all("aggregate_deadline_seconds" in check for check in _herdr_checks(payload))
    assert _pending_check(payload)["outcome"] == "store_unavailable"


def test_doctor_appends_fixed_compliant_local_state_checks(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o700)
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=state_dir / "tendwire.db",
        socket_path=state_dir / "tendwire.sock",
    )
    assert config.db_path is not None
    init_store(config.db_path)
    for identity_path in (
        config.installation_key_path,
        config.installation_key_marker_path,
        config.installation_key_sentinel_path,
    ):
        _write_mode(identity_path, 0o600)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(config.socket_path))
    assert config.socket_path is not None
    os.chmod(config.socket_path, 0o600)
    calls: list[tuple[str, ...]] = []
    _patch_healthy_herdr(monkeypatch, calls)
    try:
        payload = diagnose_herdr(config)
    finally:
        listener.close()
        config.socket_path.unlink(missing_ok=True)

    assert set(payload) == {
        "schema_version",
        "command",
        "herdr_bin",
        "timeout_seconds",
        "aggregate_deadline_seconds",
        "status",
        "checks",
    }
    assert payload["schema_version"] == 1
    assert payload["command"] == "doctor"
    assert payload["status"] == "degraded"
    assert calls == [
        ("workspace", "list"),
        ("agent", "list"),
        ("pane", "list"),
    ]
    assert len(_herdr_checks(payload)) == 6
    local_checks = _local_state_checks(payload)
    assert tuple(local_checks) == _LOCAL_STATE_CHECK_NAMES
    assert all(
        check == {
            "name": name,
            "ok": True,
            "outcome": "compliant",
            "remediation": "No action required.",
        }
        for name, check in local_checks.items()
    )
    assert _maintenance_check(payload)["outcome"] == "overdue"


def test_doctor_treats_uninitialized_state_and_stopped_socket_as_neutral(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "never-created-state"
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=state_dir / "never-created.db",
    )
    assert config.socket_path is None
    calls: list[tuple[str, ...]] = []
    _patch_healthy_herdr(monkeypatch, calls)

    payload = diagnose_herdr(config)

    assert payload["status"] == "degraded"
    assert not state_dir.exists()
    assert calls == [
        ("workspace", "list"),
        ("agent", "list"),
        ("pane", "list"),
    ]
    local_checks = _local_state_checks(payload)
    assert {
        name: (check["ok"], check["outcome"], check["remediation"])
        for name, check in local_checks.items()
    } == {
        "state_directory_permissions": (
            True,
            "not_initialized",
            "No action required while local state is uninitialized.",
        ),
        "database_permissions": (
            True,
            "not_initialized",
            "No action required while local state is uninitialized.",
        ),
        "identity_permissions": (
            True,
            "not_initialized",
            "No action required while local state is uninitialized.",
        ),
        "daemon_socket_permissions": (
            True,
            "not_running",
            "No action required while the daemon is stopped.",
        ),
    }
    assert _maintenance_check(payload) == {
        "name": "store_maintenance",
        "ok": True,
        "outcome": "not_initialized",
        "remediation": "No action required while the store is uninitialized.",
        "snapshot_retention_days": 14,
        "snapshot_retention_count": 4096,
        "maintenance_batch_size": 100,
        "maintenance_cadence_seconds": 3600,
        "snapshot_count": 0,
        "last_completed_at": None,
    }
    assert _pending_check(payload) == {
        "name": "pending_ingestion",
        "ok": False,
        "outcome": "store_unavailable",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
        "stale_grace_seconds": 30.0,
    }


def test_doctor_inspects_broad_default_socket_without_mutating_or_disclosing_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "s"
    state_dir.mkdir()
    os.chmod(state_dir, 0o700)
    default_socket = state_dir / "tendwire.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(default_socket))
    os.chmod(default_socket, 0o666)
    before = (
        os.lstat(default_socket).st_ino,
        stat.S_IMODE(os.lstat(default_socket).st_mode),
    )
    config = Config(host_id="doctor-host", herdr_bin="herdr", data_dir=state_dir)
    assert config.socket_path is None
    calls: list[tuple[str, ...]] = []
    _patch_healthy_herdr(monkeypatch, calls)
    try:
        payload = diagnose_herdr(config)
        after = (
            os.lstat(default_socket).st_ino,
            stat.S_IMODE(os.lstat(default_socket).st_mode),
        )
    finally:
        listener.close()
        default_socket.unlink(missing_ok=True)

    assert payload["status"] == "degraded"
    assert calls == [
        ("workspace", "list"),
        ("agent", "list"),
        ("pane", "list"),
    ]
    assert after == before
    assert _local_state_checks(payload)["daemon_socket_permissions"] == {
        "name": "daemon_socket_permissions",
        "ok": False,
        "outcome": "repair_required",
        "remediation": "Restart Tendwire to repair local state permissions.",
    }
    serialized = json.dumps(payload)
    assert str(state_dir) not in serialized
    assert str(default_socket) not in serialized
    assert "tendwire.sock" not in serialized


def test_doctor_rejects_wrong_type_default_socket_without_mutating_or_disclosing_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "wrong-type-default-socket-state-value"
    state_dir.mkdir()
    os.chmod(state_dir, 0o700)
    default_socket = state_dir / "tendwire.sock"
    private_contents = b"wrong-default-socket-content-value"
    default_socket.write_bytes(private_contents)
    os.chmod(default_socket, 0o600)
    before = (
        os.lstat(default_socket).st_ino,
        stat.S_IMODE(os.lstat(default_socket).st_mode),
    )
    config = Config(host_id="doctor-host", herdr_bin="herdr", data_dir=state_dir)
    assert config.socket_path is None
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(config)

    after = (
        os.lstat(default_socket).st_ino,
        stat.S_IMODE(os.lstat(default_socket).st_mode),
    )
    assert payload["status"] == "degraded"
    assert after == before
    assert default_socket.read_bytes() == private_contents
    assert _local_state_checks(payload)["daemon_socket_permissions"] == {
        "name": "daemon_socket_permissions",
        "ok": False,
        "outcome": "unsafe",
        "remediation": "Move unsafe local state aside and restore from a trusted backup.",
    }
    serialized = json.dumps(payload)
    assert str(state_dir) not in serialized
    assert str(default_socket) not in serialized
    assert private_contents.decode() not in serialized


def test_doctor_rejects_symlink_default_socket_without_following_or_disclosing_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "symlink-default-socket-state-value"
    state_dir.mkdir()
    os.chmod(state_dir, 0o700)
    target = state_dir / "default-socket-target-value"
    private_contents = b"default-socket-target-content-value"
    target.write_bytes(private_contents)
    os.chmod(target, 0o600)
    default_socket = state_dir / "tendwire.sock"
    default_socket.symlink_to(target)
    before_target = (
        os.lstat(target).st_ino,
        stat.S_IMODE(os.lstat(target).st_mode),
    )
    config = Config(host_id="doctor-host", herdr_bin="herdr", data_dir=state_dir)
    assert config.socket_path is None
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(config)

    after_target = (
        os.lstat(target).st_ino,
        stat.S_IMODE(os.lstat(target).st_mode),
    )
    assert payload["status"] == "degraded"
    assert default_socket.is_symlink()
    assert after_target == before_target
    assert target.read_bytes() == private_contents
    assert _local_state_checks(payload)["daemon_socket_permissions"] == {
        "name": "daemon_socket_permissions",
        "ok": False,
        "outcome": "unsafe",
        "remediation": "Move unsafe local state aside and restore from a trusted backup.",
    }
    serialized = json.dumps(payload)
    for forbidden in (
        str(state_dir),
        str(default_socket),
        str(target),
        private_contents.decode(),
    ):
        assert forbidden not in serialized


def test_doctor_reports_broad_modes_without_mutating_and_cli_exits_degraded(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "tendwire.db"
    identity_paths = (
        state_dir / "installation.key",
        state_dir / "installation.key.sha256",
        state_dir / "installation.key.initialized",
    )
    for path in (db_path, *identity_paths):
        _write_mode(path, 0o644)
    socket_path = state_dir / "tendwire.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path))
    os.chmod(socket_path, 0o666)
    observed_paths = (state_dir, db_path, *identity_paths, socket_path)
    before = {
        str(path): (os.lstat(path).st_ino, stat.S_IMODE(os.lstat(path).st_mode))
        for path in observed_paths
    }
    calls: list[tuple[str, ...]] = []
    _patch_healthy_herdr(monkeypatch, calls)
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(state_dir))
    monkeypatch.setenv("TENDWIRE_DB_PATH", str(db_path))
    try:
        code = main(
            [
                "--herdr-bin",
                "herdr",
                "--socket-path",
                str(socket_path),
                "doctor",
                "--json",
            ]
        )
        captured = capsys.readouterr()
    finally:
        listener.close()
        socket_path.unlink(missing_ok=True)

    payload = json.loads(captured.out)
    after = {
        str(path): (os.lstat(path).st_ino, stat.S_IMODE(os.lstat(path).st_mode))
        for path in observed_paths
        if path != socket_path
    }
    assert code == 1
    assert captured.err == ""
    assert payload["status"] == "degraded"
    assert calls == [
        ("workspace", "list"),
        ("agent", "list"),
        ("pane", "list"),
    ]
    assert after == {key: value for key, value in before.items() if key != str(socket_path)}
    assert all(
        check == {
            "name": name,
            "ok": False,
            "outcome": "repair_required",
            "remediation": "Restart Tendwire to repair local state permissions.",
        }
        for name, check in _local_state_checks(payload).items()
    )
    assert _maintenance_check(payload)["outcome"] == "unsafe"
    assert _maintenance_check(payload)["ok"] is False


def test_doctor_degrades_for_symlinks_and_wrong_entry_types_without_following(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state-value-that-must-not-leak"
    state_dir.mkdir()
    os.chmod(state_dir, 0o700)
    target = state_dir / "symlink-target-that-must-not-leak"
    _write_mode(target, 0o600)
    db_path = state_dir / "database-value-that-must-not-leak"
    db_path.symlink_to(target)
    identity_path = state_dir / "installation.key"
    identity_path.mkdir()
    socket_path = state_dir / "socket-value-that-must-not-leak"
    socket_path.write_bytes(b"wrong socket type")
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=db_path,
        socket_path=socket_path,
        socket_group="group-value-that-must-not-leak",
    )
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(config)

    local_checks = _local_state_checks(payload)
    assert payload["status"] == "degraded"
    assert local_checks["state_directory_permissions"]["outcome"] == "compliant"
    assert local_checks["database_permissions"]["outcome"] == "unsafe"
    assert local_checks["identity_permissions"]["outcome"] == "unsafe"
    assert local_checks["daemon_socket_permissions"]["outcome"] == "unsafe"
    assert db_path.is_symlink()
    assert target.read_bytes() == b"local-state"
    assert _maintenance_check(payload)["outcome"] == "unsafe"
    assert _maintenance_check(payload)["ok"] is False
    serialized = json.dumps(payload)
    for forbidden in (
        str(state_dir),
        str(db_path),
        str(socket_path),
        str(target),
        "group-value-that-must-not-leak",
        "wrong socket type",
    ):
        assert forbidden not in serialized


def test_store_maintenance_rejects_wrong_database_type_without_mutation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "wrong-type-private-state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "wrong-type-private-database"
    db_path.mkdir(mode=0o700)
    sentinel = db_path / "private-sentinel"
    sentinel.write_text("wrong-type-private-content", encoding="utf-8")
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=db_path,
    )
    before = (stat.S_IMODE(db_path.stat().st_mode), sentinel.read_bytes())
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(config)

    assert payload["status"] == "degraded"
    assert _maintenance_check(payload)["outcome"] == "unsafe"
    assert (stat.S_IMODE(db_path.stat().st_mode), sentinel.read_bytes()) == before
    serialized = json.dumps(payload)
    for private in (
        str(state_dir),
        str(db_path),
        str(sentinel),
        "wrong-type-private-content",
    ):
        assert private not in serialized


def test_store_maintenance_refuses_outdated_schema_without_migration(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "outdated-private-state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "outdated-private-store.db"
    private_table = "private_schema_sentinel"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(f"CREATE TABLE {private_table} (private_value TEXT)")
        conn.execute(
            f"INSERT INTO {private_table} (private_value) VALUES (?)",
            ("outdated-private-content",),
        )
        conn.execute("PRAGMA user_version = 1")
    os.chmod(db_path, 0o600)
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=db_path,
    )
    before = db_path.read_bytes()
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(config)

    assert payload["status"] == "degraded"
    assert _maintenance_check(payload)["outcome"] == "unavailable"
    assert db_path.read_bytes() == before
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
        assert conn.execute(
            f"SELECT private_value FROM {private_table}"
        ).fetchone()[0] == "outdated-private-content"
    serialized = json.dumps(payload)
    for private in (
        str(state_dir),
        str(db_path),
        private_table,
        "outdated-private-content",
    ):
        assert private not in serialized


def _seed_maintenance_store(
    config: Config,
    *,
    last_completed_at: str,
    snapshot_count: int,
) -> tuple[bytes, str]:
    assert config.db_path is not None
    init_store(config.db_path)
    private = "maintenance-private-payload-sentinel"
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.execute(
            """
            UPDATE store_maintenance_state
            SET last_started_at = ?,
                last_completed_at = ?,
                last_status = 'ok'
            WHERE scope = 'automatic'
            """,
            (last_completed_at, last_completed_at),
        )
        for index in range(snapshot_count):
            conn.execute(
                """
                INSERT INTO snapshots (
                    host_id, created_at, content_fingerprint, payload
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    config.host_id,
                    f"2026-01-01T00:00:0{index}+00:00",
                    f"private-fingerprint-{index}",
                    json.dumps({"private": private, "index": index}),
                ),
            )
    return config.db_path.read_bytes(), private


def test_store_maintenance_reports_current_fixed_aggregate_without_mutation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "current-private-state"
    state_dir.mkdir(mode=0o700)
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=state_dir / "current-private-store.db",
        snapshot_retention_days=21,
        snapshot_retention_count=10,
        snapshot_maintenance_batch_size=7,
        store_maintenance_cadence_seconds=3600,
    )
    before, private = _seed_maintenance_store(
        config,
        last_completed_at="2026-01-01T00:00:00+00:00",
        snapshot_count=1,
    )
    monkeypatch.setattr(
        herdr_cli,
        "utc_timestamp",
        lambda *_args, **_kwargs: "2026-01-01T00:30:00+00:00",
    )
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(config)

    assert payload["status"] == "ok"
    assert _maintenance_check(payload) == {
        "name": "store_maintenance",
        "ok": True,
        "outcome": "ok",
        "remediation": "No action required.",
        "snapshot_retention_days": 21,
        "snapshot_retention_count": 10,
        "maintenance_batch_size": 7,
        "maintenance_cadence_seconds": 3600,
        "snapshot_count": 1,
        "last_completed_at": "2026-01-01T00:00:00+00:00",
    }
    assert _pending_check(payload) == {
        "name": "pending_ingestion",
        "ok": True,
        "outcome": "healthy",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
        "stale_grace_seconds": 30.0,
    }
    assert config.db_path is not None
    assert config.db_path.read_bytes() == before
    assert private not in json.dumps(payload)


def test_doctor_pending_ingestion_is_fixed_nonmutating_and_public_safe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    state_dir = tmp_path / "pending-health-private-state"
    state_dir.mkdir(mode=0o700)
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=state_dir / "pending-health-private.db",
        pending_stale_grace_seconds=17,
    )
    before, private = _seed_maintenance_store(
        config,
        last_completed_at="2026-01-01T00:00:00+00:00",
        snapshot_count=1,
    )
    herdr_calls: list[tuple[str, ...]] = []
    _patch_healthy_herdr(monkeypatch, herdr_calls)
    monkeypatch.setattr(
        herdr_cli,
        "utc_timestamp",
        lambda *_args, **_kwargs: "2026-01-01T00:30:00+00:00",
    )
    durable_calls: list[tuple[Path, str]] = []

    def durable_health(db_path: Path, host_id: str) -> dict[str, Any]:
        durable_calls.append((db_path, host_id))
        return {
            "status": "degraded",
            "counts": {"fresh": 2, "stale": 1, "total": 3},
            "pane_id": "sentinel-private-pane",
            "source_path": str(tmp_path / "sentinel-private-source"),
            "tool_id": "sentinel-private-tool",
            "error": "sentinel-private-error",
        }

    monkeypatch.setattr(store_sqlite, "backend_pending_health", durable_health)

    payload = diagnose_herdr(config)

    assert payload["status"] == "degraded"
    assert durable_calls == [(config.db_path, config.host_id)]
    assert herdr_calls == [
        ("workspace", "list"),
        ("agent", "list"),
        ("pane", "list"),
    ]
    assert _pending_check(payload) == {
        "name": "pending_ingestion",
        "ok": False,
        "outcome": "degraded",
        "counts": {"fresh": 2, "stale": 1, "total": 3},
        "stale_grace_seconds": 17.0,
    }
    assert config.db_path.read_bytes() == before
    serialized = json.dumps(payload, sort_keys=True)
    assert private not in serialized
    assert "sentinel-private" not in serialized
    monkeypatch.setattr(
        store_sqlite,
        "backend_pending_health",
        lambda *_args: {
            "status": "healthy",
            "counts": {"fresh": 1, "stale": 1, "total": 2},
        },
    )
    fail_closed = diagnose_herdr(config)
    assert _pending_check(fail_closed) == {
        "name": "pending_ingestion",
        "ok": False,
        "outcome": "store_unavailable",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
        "stale_grace_seconds": 17.0,
    }
    monkeypatch.setattr(
        store_sqlite,
        "backend_pending_health",
        lambda *_args: {
            "status": "healthy",
            "counts": {"fresh": 3, "stale": 0, "total": 3},
        },
    )
    recovered = diagnose_herdr(config)
    assert recovered["status"] == "ok"
    assert _pending_check(recovered) == {
        "name": "pending_ingestion",
        "ok": True,
        "outcome": "healthy",
        "counts": {"fresh": 3, "stale": 0, "total": 3},
        "stale_grace_seconds": 17.0,
    }


def test_store_maintenance_reports_overdue_without_mutation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "overdue-private-state"
    state_dir.mkdir(mode=0o700)
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=state_dir / "overdue-private-store.db",
        snapshot_retention_count=10,
        store_maintenance_cadence_seconds=3600,
    )
    before, private = _seed_maintenance_store(
        config,
        last_completed_at="2026-01-01T00:00:00+00:00",
        snapshot_count=1,
    )
    monkeypatch.setattr(
        herdr_cli,
        "utc_timestamp",
        lambda *_args, **_kwargs: "2026-01-01T02:00:00+00:00",
    )
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(config)

    check = _maintenance_check(payload)
    assert payload["status"] == "degraded"
    assert check["outcome"] == "overdue"
    assert check["ok"] is False
    assert check["snapshot_count"] == 1
    assert check["last_completed_at"] == "2026-01-01T00:00:00+00:00"
    assert config.db_path is not None
    assert config.db_path.read_bytes() == before
    assert private not in json.dumps(payload)


def test_store_maintenance_reports_backlog_before_cadence_without_mutation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "backlog-private-state"
    state_dir.mkdir(mode=0o700)
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=state_dir / "backlog-private-store.db",
        snapshot_retention_days=36500,
        snapshot_retention_count=1,
        snapshot_maintenance_batch_size=1,
        store_maintenance_cadence_seconds=3600,
    )
    before, private = _seed_maintenance_store(
        config,
        last_completed_at="2026-01-01T00:00:00+00:00",
        snapshot_count=2,
    )
    monkeypatch.setattr(
        herdr_cli,
        "utc_timestamp",
        lambda *_args, **_kwargs: "2026-01-01T00:30:00+00:00",
    )
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(config)

    check = _maintenance_check(payload)
    assert payload["status"] == "degraded"
    assert check["outcome"] == "backlog"
    assert check["ok"] is False
    assert check["snapshot_count"] == 2
    assert check["last_completed_at"] == "2026-01-01T00:00:00+00:00"
    assert config.db_path is not None
    assert config.db_path.read_bytes() == before
    assert private not in json.dumps(payload)


def test_doctor_maps_owner_and_group_failures_to_fixed_unsafe_records(
    monkeypatch,
    tmp_path: Path,
) -> None:
    private_remediation = "private-remediation-value-that-must-not-leak"
    report = ConfigStateReport(
        ok=False,
        entries=(
            PermissionResult(
                LocalStateKind.STATE_DIRECTORY,
                PermissionState.PRIVATE,
                0o700,
            ),
        ),
        issues=(
            LocalStateIssue(
                LocalStateKind.PRIVATE_FILE,
                LocalStateErrorCode.WRONG_OWNER,
                private_remediation,
            ),
            LocalStateIssue(
                LocalStateKind.SOCKET_GROUP,
                LocalStateErrorCode.WRONG_GROUP,
                private_remediation,
            ),
        ),
    )
    monkeypatch.setattr(herdr_cli, "inspect_config_state", lambda *args, **kwargs: report)
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(
        Config(
            host_id="doctor-host",
            herdr_bin="herdr",
            data_dir=tmp_path / "private-state",
        )
    )

    local_checks = _local_state_checks(payload)
    assert payload["status"] == "degraded"
    assert local_checks["identity_permissions"] == {
        "name": "identity_permissions",
        "ok": False,
        "outcome": "unsafe",
        "remediation": "Move unsafe local state aside and restore from a trusted backup.",
    }
    assert local_checks["daemon_socket_permissions"] == {
        "name": "daemon_socket_permissions",
        "ok": False,
        "outcome": "unsafe",
        "remediation": "Move unsafe local state aside and restore from a trusted backup.",
    }
    assert private_remediation not in json.dumps(payload)


def test_doctor_recursively_redacts_configured_and_subprocess_private_values(
    monkeypatch,
    tmp_path: Path,
) -> None:
    private_bin = str(tmp_path / "configured-bin-value" / "herdr")
    private_state = tmp_path / "configured-state-value"
    private_db = private_state / "configured-database-value"
    private_socket = private_state / "configured-socket-value"
    private_group = "configured-group-value-that-must-not-leak"
    private_sample = "subprocess-sample-value-that-must-not-leak"
    which_values: list[str] = []
    subprocess_values: list[str] = []

    def fake_which(value: str) -> str:
        which_values.append(value)
        return "/usr/bin/herdr"

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        subprocess_values.append(args[0])
        return _completed(
            args[1:],
            stdout=f"stdout: {private_sample} /home/alice/private-output",
            stderr=f"stderr: {private_sample} /run/user/1000/private.sock",
            returncode=2,
        )

    monkeypatch.setattr(herdr_cli.shutil, "which", fake_which)
    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)
    payload = diagnose_herdr(
        Config(
            host_id="doctor-host",
            herdr_bin=private_bin,
            data_dir=private_state,
            db_path=private_db,
            socket_path=private_socket,
            socket_group=private_group,
        )
    )

    assert which_values == [private_bin]
    assert subprocess_values == [private_bin] * 6
    assert isinstance(payload["herdr_bin"], str)
    assert payload["herdr_bin"] != private_bin
    assert all(
        "stdout_sample" in check and "stderr_sample" in check
        for check in _herdr_checks(payload)
    )
    serialized = json.dumps(payload)
    for forbidden in (
        private_bin,
        str(private_state),
        str(private_db),
        str(private_socket),
        private_group,
        private_sample,
        "/home/alice/private-output",
        "/run/user/1000/private.sock",
    ):
        assert forbidden not in serialized


def test_doctor_never_serializes_raw_inspection_or_launch_exceptions(
    monkeypatch,
    tmp_path: Path,
) -> None:
    raw_error = "raw-exception-value-that-must-not-leak"

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise OSError(raw_error)

    monkeypatch.setattr(herdr_cli.shutil, "which", fail)
    monkeypatch.setattr(herdr_cli, "inspect_config_state", fail)

    payload = diagnose_herdr(
        Config(
            host_id="doctor-host",
            herdr_bin=str(tmp_path / "private-bin"),
            data_dir=tmp_path / "private-state",
        )
    )

    assert payload["status"] == "unavailable"
    assert raw_error not in json.dumps(payload)
    assert all(
        check["outcome"] == "unsafe" and check["ok"] is False
        for check in _local_state_checks(payload).values()
    )


def test_tilde_path_expansion_for_configured_paths(monkeypatch) -> None:
    monkeypatch.setenv("TENDWIRE_DATA_DIR", "~/tendwire-data")
    monkeypatch.setenv("TENDWIRE_DB_PATH", "~/tendwire-data/tendwire.db")
    monkeypatch.setenv("TENDWIRE_HERDR_BIN", "~/bin/herdr")
    monkeypatch.setenv("TENDWIRE_HERDR_TIMEOUT_SECONDS", "0.5")

    config = load_config()

    assert str(config.data_dir).startswith(str(Path.home()))
    assert str(config.db_path).startswith(str(Path.home()))
    assert config.herdr_bin.startswith(str(Path.home()))
    assert config.herdr_timeout_seconds == 0.5


def test_fixture_outputs_parse_through_snapshot_fail_soft_path(monkeypatch) -> None:
    config = Config(host_id="fixture-host", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): _completed(
            ["workspace", "list"],
            stdout=_fixture("workspace_list_no_flag_nonempty.json"),
        ),
        ("agent", "list"): _completed(
            ["agent", "list"],
            stdout=_fixture("agent_list_no_flag_nonempty.json"),
        ),
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(args))
        return responses.get(tuple(args), _completed(args, stdout="", returncode=1))

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert calls == [("workspace", "list"), ("agent", "list"), ("pane", "list")]
    assert len(spaces) == 1
    assert spaces[0].id == "ws-fixture"
    assert len(workers) == 1
    assert workers[0].id == "Fixture Agent"


def test_doctor_initialized_store_with_absent_sqlite_sidecars_is_validation_only(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "private-doctor-state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "private-doctor-database"
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=db_path,
    )
    init_store(db_path)
    sidecars = tuple(
        Path(f"{db_path}{suffix}") for suffix in ("-wal", "-shm", "-journal")
    )
    assert all(not os.path.lexists(path) for path in sidecars)
    before = (
        tuple(sorted(path.name for path in state_dir.iterdir())),
        db_path.stat().st_ino,
        db_path.stat().st_size,
        db_path.stat().st_mtime_ns,
        stat.S_IMODE(db_path.stat().st_mode),
    )
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(config)

    after = (
        tuple(sorted(path.name for path in state_dir.iterdir())),
        db_path.stat().st_ino,
        db_path.stat().st_size,
        db_path.stat().st_mtime_ns,
        stat.S_IMODE(db_path.stat().st_mode),
    )
    assert _local_state_checks(payload)["database_permissions"] == {
        "name": "database_permissions",
        "ok": True,
        "outcome": "compliant",
        "remediation": "No action required.",
    }
    assert after == before
    assert all(not os.path.lexists(path) for path in sidecars)


@pytest.mark.parametrize("hostile_member", ["main", "wal"])
def test_doctor_sqlite_failures_are_fixed_typed_and_path_free(
    monkeypatch: Any,
    tmp_path: Path,
    hostile_member: str,
) -> None:
    state_dir = tmp_path / "private-doctor-hostile-state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "private-doctor-hostile-database"
    target = state_dir / "private-doctor-hostile-target"
    private_contents = b"raw-OSError-private-sidecar-target"
    target.write_bytes(private_contents)
    os.chmod(target, 0o600)
    if hostile_member == "main":
        hostile_path = db_path
    else:
        init_store(db_path)
        hostile_path = Path(f"{db_path}-wal")
    hostile_path.symlink_to(target)
    target_before = (
        target.read_bytes(),
        target.stat().st_ino,
        stat.S_IMODE(target.stat().st_mode),
    )
    hostile_inode = str(os.lstat(hostile_path).st_ino)
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=db_path,
    )
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(config)

    database_check = _local_state_checks(payload)["database_permissions"]
    assert database_check == {
        "name": "database_permissions",
        "ok": False,
        "outcome": "unsafe",
        "remediation": "Move unsafe local state aside and restore from a trusted backup.",
    }
    assert _maintenance_check(payload)["outcome"] == "unsafe"
    assert hostile_path.is_symlink()
    assert (
        target.read_bytes(),
        target.stat().st_ino,
        stat.S_IMODE(target.stat().st_mode),
    ) == target_before
    serialized = json.dumps(payload, sort_keys=True)
    for forbidden in (
        str(state_dir),
        str(db_path),
        db_path.name,
        str(hostile_path),
        hostile_path.name,
        str(target),
        target.name,
        private_contents.decode(),
        hostile_inode,
        "-wal",
        "-shm",
        "-journal",
        "OSError",
        "[Errno",
        '"uid"',
        '"gid"',
        '"inode"',
    ):
        assert forbidden not in serialized


def test_doctor_selected_main_disappearance_is_typed_and_publicly_fixed(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    from tendwire import local_state as local_state_module

    state_dir = tmp_path / "private-doctor-main-race-state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "private-doctor-main-race-database"
    config = Config(
        host_id="doctor-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=db_path,
    )
    init_store(db_path)
    selected_inode = str(db_path.stat().st_ino)
    removed = False
    observed_codes: list[LocalStateErrorCode] = []
    original_inspect = herdr_cli.inspect_config_state

    def remove_selected_main(phase: str, kind: LocalStateKind) -> None:
        nonlocal removed
        if phase == "captured" and kind is LocalStateKind.DATABASE and not removed:
            removed = True
            db_path.unlink()

    def capture_typed_failure(*args: Any, **kwargs: Any) -> ConfigStateReport:
        report = original_inspect(*args, **kwargs)
        observed_codes.extend(
            issue.code
            for issue in report.issues
            if issue.kind is LocalStateKind.DATABASE
        )
        return report

    monkeypatch.setattr(
        local_state_module,
        "_sqlite_family_test_phase",
        remove_selected_main,
    )
    monkeypatch.setattr(
        herdr_cli,
        "inspect_config_state",
        capture_typed_failure,
    )
    _patch_healthy_herdr(monkeypatch)

    payload = diagnose_herdr(config)

    assert removed
    assert observed_codes == [LocalStateErrorCode.ENTRY_CHANGED]
    assert not os.path.lexists(db_path)
    assert _local_state_checks(payload)["database_permissions"] == {
        "name": "database_permissions",
        "ok": False,
        "outcome": "unsafe",
        "remediation": "Move unsafe local state aside and restore from a trusted backup.",
    }
    assert _maintenance_check(payload)["outcome"] == "unsafe"
    serialized = json.dumps(payload, sort_keys=True)
    for forbidden in (
        str(state_dir),
        str(db_path),
        db_path.name,
        selected_inode,
        "-wal",
        "-shm",
        "-journal",
        "OSError",
        "[Errno",
        '"uid"',
        '"gid"',
        '"inode"',
    ):
        assert forbidden not in serialized
