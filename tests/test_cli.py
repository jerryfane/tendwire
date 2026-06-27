"""Tests for tendwire CLI snapshot --json."""

from __future__ import annotations

import json

from tendwire.cli import main


def test_cli_snapshot_json_prints_valid_json(capsys) -> None:
    code = main(["snapshot", "--json"])
    captured = capsys.readouterr()

    assert code == 0
    payload = json.loads(captured.out)
    assert set(payload.keys()) == {
        "host_id",
        "updated_at",
        "spaces",
        "workers",
        "attention",
    }


def test_cli_snapshot_no_herdr_works() -> None:
    """Empty snapshot works even when herdr is not installed."""
    code = main(["--herdr-bin", "definitely-not-a-real-herdr-binary", "snapshot", "--json"])
    assert code == 0


def test_cli_module_invocation() -> None:
    """python -m tendwire.cli snapshot --json works."""
    import os
    import subprocess
    import sys

    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), "..", "src")
    result = subprocess.run(
        [sys.executable, "-m", "tendwire.cli", "snapshot", "--json"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert set(payload.keys()) == {
        "host_id",
        "updated_at",
        "spaces",
        "workers",
        "attention",
    }
