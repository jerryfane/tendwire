"""Tests for tendwire CLI snapshot JSON output and optional storage."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from tendwire.cli import main
from tendwire.store.sqlite import latest_snapshot


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
    assert {"updated_at", "spaces", "workers", "attention"} <= set(payload)


def test_cli_snapshot_no_herdr_works() -> None:
    """Empty snapshot works even when herdr is not installed."""
    code = main(["--herdr-bin", "definitely-not-a-real-herdr-binary", "snapshot", "--json"])
    assert code == 0


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
