"""Tests for Tendwire runtime configuration."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from tendwire.config import (
    DEFAULT_TURN_REFRESH_INTERVAL_SECONDS,
    DEFAULT_TURN_REFRESH_WORKERS,
    Config,
    load_config,
)


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


TURN_REFRESH_ENV_NAMES = (
    "TENDWIRE_TURN_REFRESH_INTERVAL_SECONDS",
    "TENDWIRE_TURN_REFRESH_WORKERS",
)


def test_turn_refresh_knobs_have_documented_defaults(monkeypatch) -> None:
    for name in TURN_REFRESH_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)

    config = load_config()

    assert DEFAULT_TURN_REFRESH_INTERVAL_SECONDS == 2.0
    assert DEFAULT_TURN_REFRESH_WORKERS == 4
    assert config.turn_refresh_interval_seconds == 2.0
    assert config.turn_refresh_workers == 4


def test_turn_refresh_knobs_use_explicit_before_environment(monkeypatch) -> None:
    monkeypatch.setenv("TENDWIRE_TURN_REFRESH_INTERVAL_SECONDS", "3.5")
    monkeypatch.setenv("TENDWIRE_TURN_REFRESH_WORKERS", "8")

    env_config = load_config(max_workers=16)
    explicit = load_config(
        max_workers=16,
        turn_refresh_interval_seconds="0.25",
        turn_refresh_workers="6",
    )

    assert env_config.turn_refresh_interval_seconds == 3.5
    assert env_config.turn_refresh_workers == 8
    assert explicit.turn_refresh_interval_seconds == 0.25
    assert explicit.turn_refresh_workers == 6


@pytest.mark.parametrize("value", [0, -0.01, "nan", "inf", "-inf"])
def test_turn_refresh_interval_rejects_nonpositive_or_nonfinite(value: object) -> None:
    with pytest.raises(
        ValueError,
        match="turn_refresh_interval_seconds must be a finite positive number",
    ):
        Config(turn_refresh_interval_seconds=value)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (True, "turn_refresh_workers must be an integer >= 1"),
        (0, "turn_refresh_workers must be >= 1"),
        (-1, "turn_refresh_workers must be >= 1"),
        (33, "turn_refresh_workers must be <= 32"),
        (1.5, "turn_refresh_workers must be an integer >= 1"),
    ],
)
def test_turn_refresh_workers_reject_invalid_bounds(
    value: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        Config(turn_refresh_workers=value)


def test_turn_refresh_workers_cannot_exceed_observed_worker_max(monkeypatch) -> None:
    with pytest.raises(
        ValueError,
        match="turn_refresh_workers must be <= max_workers",
    ):
        Config(max_workers=3, turn_refresh_workers=4)

    monkeypatch.setenv("TENDWIRE_TURN_REFRESH_WORKERS", "5")
    with pytest.raises(
        ValueError,
        match="turn_refresh_workers must be <= max_workers",
    ):
        load_config(max_workers=4)


SNAPSHOT_MAINTENANCE_ENV_NAMES = (
    "TENDWIRE_SNAPSHOT_RETENTION_DAYS",
    "TENDWIRE_SNAPSHOT_RETENTION_COUNT",
    "TENDWIRE_SNAPSHOT_MAINTENANCE_BATCH_SIZE",
    "TENDWIRE_STORE_MAINTENANCE_CADENCE_SECONDS",
)


def test_snapshot_maintenance_knobs_have_documented_defaults(monkeypatch) -> None:
    for name in SNAPSHOT_MAINTENANCE_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)

    config = load_config()

    assert config.snapshot_retention_days == 14
    assert config.snapshot_retention_count == 4096
    assert config.snapshot_maintenance_batch_size == 100
    assert config.store_maintenance_cadence_seconds == 3600


def test_snapshot_maintenance_knobs_use_constructor_before_environment(monkeypatch) -> None:
    monkeypatch.setenv("TENDWIRE_SNAPSHOT_RETENTION_DAYS", "21")
    monkeypatch.setenv("TENDWIRE_SNAPSHOT_RETENTION_COUNT", "5000")
    monkeypatch.setenv("TENDWIRE_SNAPSHOT_MAINTENANCE_BATCH_SIZE", "250")
    monkeypatch.setenv("TENDWIRE_STORE_MAINTENANCE_CADENCE_SECONDS", "1800")

    env_config = load_config()
    explicit = load_config(
        snapshot_retention_days="7",
        snapshot_retention_count="2048",
        snapshot_maintenance_batch_size="50",
        store_maintenance_cadence_seconds="7200",
    )

    assert env_config.snapshot_retention_days == 21
    assert env_config.snapshot_retention_count == 5000
    assert env_config.snapshot_maintenance_batch_size == 250
    assert env_config.store_maintenance_cadence_seconds == 1800
    assert explicit.snapshot_retention_days == 7
    assert explicit.snapshot_retention_count == 2048
    assert explicit.snapshot_maintenance_batch_size == 50
    assert explicit.store_maintenance_cadence_seconds == 7200


@pytest.mark.parametrize(
    "field",
    [
        "snapshot_retention_days",
        "snapshot_retention_count",
        "snapshot_maintenance_batch_size",
        "store_maintenance_cadence_seconds",
    ],
)
@pytest.mark.parametrize(
    ("value", "message"),
    [
        (True, "must be an integer >= 1"),
        (0, "must be >= 1"),
        (-1, "must be >= 1"),
        ("malformed", "must be an integer >= 1"),
        (1.5, "must be an integer >= 1"),
    ],
)
def test_snapshot_maintenance_knobs_reject_invalid_values(
    field: str,
    value: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        Config(**{field: value})


def test_snapshot_maintenance_batch_size_rejects_values_above_maximum(monkeypatch) -> None:
    with pytest.raises(
        ValueError,
        match="snapshot_maintenance_batch_size must be <= 1000",
    ):
        Config(snapshot_maintenance_batch_size=1001)

    monkeypatch.setenv("TENDWIRE_SNAPSHOT_MAINTENANCE_BATCH_SIZE", "1001")
    with pytest.raises(
        ValueError,
        match="snapshot_maintenance_batch_size must be <= 1000",
    ):
        load_config()


def test_socket_group_defaults_private_and_normalizes_without_lookup(monkeypatch) -> None:
    monkeypatch.delenv("TENDWIRE_SOCKET_GROUP", raising=False)
    unresolved_group = "tendwire-no-such-config-group-7f6d4b2c"

    assert Config().socket_group is None
    assert load_config().socket_group is None
    assert Config(socket_group=f"  {unresolved_group}  ").socket_group == unresolved_group

    monkeypatch.setenv("TENDWIRE_SOCKET_GROUP", "  daemon-clients  ")
    assert load_config().socket_group == "daemon-clients"
    assert load_config(socket_group="  explicit-clients  ").socket_group == "explicit-clients"
    assert load_config(socket_group="   ").socket_group is None


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
