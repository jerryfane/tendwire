"""Tests for Tendwire runtime configuration."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from tendwire.config import Config, load_config


def test_pr16_runtime_knobs_have_documented_defaults(monkeypatch) -> None:
    for name in (
        "TENDWIRE_EVENT_DEBOUNCE_SECONDS",
        "TENDWIRE_RECONCILE_INTERVAL_SECONDS",
        "TENDWIRE_EVENT_RETENTION_DAYS",
        "TENDWIRE_OUTPUT_EXCERPT_CHARS",
        "TENDWIRE_MAX_WORKERS",
        "TENDWIRE_MAX_OUTBOX_ATTEMPTS",
        "TENDWIRE_CONNECTOR_CLAIM_TTL_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)

    config = load_config()

    assert config.event_debounce_seconds == 0.05
    assert config.reconcile_interval_seconds == 300.0
    assert config.event_retention_days == 7
    assert config.output_excerpt_chars == 200
    assert config.max_workers == 512
    assert config.max_outbox_attempts == 10
    assert config.connector_claim_ttl_seconds == 60


def test_pr16_runtime_knobs_accept_constructor_and_env(monkeypatch) -> None:
    monkeypatch.setenv("TENDWIRE_EVENT_DEBOUNCE_SECONDS", "0.25")
    monkeypatch.setenv("TENDWIRE_RECONCILE_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("TENDWIRE_EVENT_RETENTION_DAYS", "14")
    monkeypatch.setenv("TENDWIRE_OUTPUT_EXCERPT_CHARS", "123")
    monkeypatch.setenv("TENDWIRE_MAX_WORKERS", "64")
    monkeypatch.setenv("TENDWIRE_MAX_OUTBOX_ATTEMPTS", "3")
    monkeypatch.setenv("TENDWIRE_CONNECTOR_CLAIM_TTL_SECONDS", "45")

    env_config = load_config()
    explicit = load_config(
        event_debounce_seconds="0.1",
        reconcile_interval_seconds="5",
        event_retention_days="2",
        output_excerpt_chars="50",
        max_workers="9",
        max_outbox_attempts="4",
        connector_claim_ttl_seconds="15",
    )

    assert env_config.event_debounce_seconds == 0.25
    assert env_config.reconcile_interval_seconds == 0
    assert env_config.event_retention_days == 14
    assert env_config.output_excerpt_chars == 123
    assert env_config.max_workers == 64
    assert env_config.max_outbox_attempts == 3
    assert env_config.connector_claim_ttl_seconds == 45
    assert explicit.event_debounce_seconds == 0.1
    assert explicit.reconcile_interval_seconds == 5
    assert explicit.event_retention_days == 2
    assert explicit.output_excerpt_chars == 50
    assert explicit.max_workers == 9
    assert explicit.max_outbox_attempts == 4
    assert explicit.connector_claim_ttl_seconds == 15


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("event_debounce_seconds", -0.1, "event_debounce_seconds must be non-negative"),
        ("reconcile_interval_seconds", -1, "reconcile_interval_seconds must be non-negative"),
        ("event_retention_days", 0, "event_retention_days must be >= 1"),
        ("output_excerpt_chars", 0, "output_excerpt_chars must be >= 1"),
        ("max_workers", 0, "max_workers must be >= 1"),
        ("max_outbox_attempts", 0, "max_outbox_attempts must be >= 1"),
        ("connector_claim_ttl_seconds", 0, "connector_claim_ttl_seconds must be >= 1"),
    ],
)
def test_pr16_runtime_knobs_reject_invalid_values(field: str, value: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Config(**{field: value})


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
