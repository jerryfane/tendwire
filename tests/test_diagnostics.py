"""Tests for the read-only Herdr doctor diagnostics."""

from __future__ import annotations

import json
import os
import subprocess
import socket
import stat
from collections.abc import Sequence
from pathlib import Path
from typing import Any

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

    assert code == 0
    assert captured.err == ""
    assert timeouts == [0.75, 0.75, 0.75]
    assert payload["timeout_seconds"] == 0.75
    assert payload["aggregate_deadline_seconds"] == 4.5
    assert all("aggregate_deadline_seconds" in check for check in _herdr_checks(payload))


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
    _write_mode(config.db_path, 0o600)
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
    assert payload["status"] == "ok"
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

    assert payload["status"] == "ok"
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
