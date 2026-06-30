"""Tests for Tendwire runtime configuration."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from tendwire.config import Config, load_config


def test_herdr_backend_defaults_to_cli(monkeypatch) -> None:
    monkeypatch.delenv("TENDWIRE_HERDR_BACKEND", raising=False)

    assert Config().herdr_backend == "cli"
    assert load_config().herdr_backend == "cli"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("cli", "cli"),
        ("socket", "socket"),
        (" CLI ", "cli"),
        ("SOCKET", "socket"),
    ],
)
def test_herdr_backend_accepts_explicit_values(monkeypatch, raw: str, expected: str) -> None:
    monkeypatch.delenv("TENDWIRE_HERDR_BACKEND", raising=False)

    assert Config(herdr_backend=raw).herdr_backend == expected
    assert load_config(herdr_backend=raw).herdr_backend == expected


def test_herdr_backend_reads_environment(monkeypatch) -> None:
    monkeypatch.setenv("TENDWIRE_HERDR_BACKEND", "socket")

    assert load_config().herdr_backend == "socket"


def test_herdr_backend_invalid_value_fails_clearly(monkeypatch) -> None:
    monkeypatch.setenv("TENDWIRE_HERDR_BACKEND", "events")

    with pytest.raises(ValueError, match="herdr_backend must be one of: cli, socket"):
        load_config()


def test_cli_default_import_does_not_load_socket_event_backend() -> None:
    code = """
import sys
before = set(sys.modules)
import tendwire.cli
loaded = set(sys.modules) - before
for name in sorted(loaded):
    if name in {
        "tendwire.backends.herdr_events",
        "tendwire.backends.herdr_socket",
        "tendwire.backends.herdr_protocol",
    }:
        print(name)
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), "..", "src")
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    assert result.stdout == ""
