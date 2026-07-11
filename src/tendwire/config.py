"""Tendwire runtime configuration.

Loads settings from a simple defaults + optional environment override set.
No external config-file parser is required.
"""

from __future__ import annotations

import os
import platform
import socket
from dataclasses import dataclass, field
from pathlib import Path

HERDR_BACKENDS = frozenset({"cli", "socket"})
DEFAULT_EVENT_DEBOUNCE_SECONDS = 0.05
DEFAULT_RECONCILE_INTERVAL_SECONDS = 300.0
DEFAULT_EVENT_RETENTION_DAYS = 7
DEFAULT_OUTPUT_EXCERPT_CHARS = 200
DEFAULT_MAX_WORKERS = 512
DEFAULT_MAX_OUTBOX_ATTEMPTS = 10
DEFAULT_CONNECTOR_CLAIM_TTL_SECONDS = 60


@dataclass(frozen=True)
class Config:
    """Neutral runtime configuration for Tendwire."""

    host_id: str = field(default_factory=lambda: platform.node() or "unknown")
    herdr_bin: str = "herdr"
    data_dir: Path = field(default_factory=lambda: Path.home() / ".local" / "share" / "tendwire")
    db_path: Path | None = None
    socket_path: Path | None = None
    herdr_timeout_seconds: float = 5.0
    herdr_backend: str = "cli"
    event_debounce_seconds: float = DEFAULT_EVENT_DEBOUNCE_SECONDS
    reconcile_interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS
    event_retention_days: int = DEFAULT_EVENT_RETENTION_DAYS
    output_excerpt_chars: int = DEFAULT_OUTPUT_EXCERPT_CHARS
    max_workers: int = DEFAULT_MAX_WORKERS
    max_outbox_attempts: int = DEFAULT_MAX_OUTBOX_ATTEMPTS
    connector_claim_ttl_seconds: int = DEFAULT_CONNECTOR_CLAIM_TTL_SECONDS
    socket_group: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "herdr_bin", os.path.expanduser(self.herdr_bin))
        object.__setattr__(self, "data_dir", Path(self.data_dir).expanduser())
        if self.db_path is None:
            object.__setattr__(
                self,
                "db_path",
                self.data_dir / "tendwire.db",
            )
        else:
            object.__setattr__(self, "db_path", Path(self.db_path).expanduser())
        if self.socket_path is not None:
            object.__setattr__(self, "socket_path", Path(self.socket_path).expanduser())
        if self.socket_group is not None:
            normalized_socket_group = str(self.socket_group).strip()
            object.__setattr__(self, "socket_group", normalized_socket_group or None)

        if self.herdr_timeout_seconds <= 0:
            raise ValueError("herdr_timeout_seconds must be positive")
        backend = str(self.herdr_backend or "").strip().lower()
        if backend not in HERDR_BACKENDS:
            allowed = ", ".join(sorted(HERDR_BACKENDS))
            raise ValueError(f"herdr_backend must be one of: {allowed}")
        object.__setattr__(self, "herdr_backend", backend)
        object.__setattr__(
            self,
            "event_debounce_seconds",
            _non_negative_float(self.event_debounce_seconds, "event_debounce_seconds"),
        )
        object.__setattr__(
            self,
            "reconcile_interval_seconds",
            _non_negative_float(self.reconcile_interval_seconds, "reconcile_interval_seconds"),
        )
        object.__setattr__(
            self,
            "event_retention_days",
            _positive_int(self.event_retention_days, "event_retention_days", minimum=1),
        )
        object.__setattr__(
            self,
            "output_excerpt_chars",
            _positive_int(self.output_excerpt_chars, "output_excerpt_chars", minimum=1),
        )
        object.__setattr__(
            self,
            "max_workers",
            _positive_int(self.max_workers, "max_workers", minimum=1),
        )
        object.__setattr__(
            self,
            "max_outbox_attempts",
            _positive_int(self.max_outbox_attempts, "max_outbox_attempts", minimum=1),
        )
        object.__setattr__(
            self,
            "connector_claim_ttl_seconds",
            _positive_int(
                self.connector_claim_ttl_seconds,
                "connector_claim_ttl_seconds",
                minimum=1,
            ),
        )

    @property
    def installation_key_path(self) -> Path:
        """Private stable-worker installation key path."""
        return self.data_dir / "installation.key"

    @property
    def installation_key_marker_path(self) -> Path:
        """Nonsecret digest marker used to detect installation key loss."""
        return self.data_dir / "installation.key.sha256"

    @property
    def installation_key_sentinel_path(self) -> Path:
        """Nonsecret durable marker that the installation identity was initialized."""
        return self.data_dir / "installation.key.initialized"


def _non_negative_float(value: float | str, name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a non-negative number") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be non-negative")
    return parsed


def _positive_int(value: int | str, name: str, *, minimum: int = 1) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer >= {minimum}") from exc
    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return parsed


def _resolve_value(explicit: object, env_name: str, default: object) -> object:
    if explicit is not None:
        return explicit
    env_value = os.environ.get(env_name)
    if env_value is not None:
        return env_value
    return default


def load_config(
    *,
    host_id: str | None = None,
    herdr_bin: str | None = None,
    data_dir: str | Path | None = None,
    db_path: str | Path | None = None,
    socket_path: str | Path | None = None,
    socket_group: str | None = None,
    herdr_timeout_seconds: float | str | None = None,
    herdr_backend: str | None = None,
    event_debounce_seconds: float | str | None = None,
    reconcile_interval_seconds: float | str | None = None,
    event_retention_days: int | str | None = None,
    output_excerpt_chars: int | str | None = None,
    max_workers: int | str | None = None,
    max_outbox_attempts: int | str | None = None,
    connector_claim_ttl_seconds: int | str | None = None,
) -> Config:
    """Build a Config from explicit args, then environment, then defaults."""
    env_host_id = os.environ.get("TENDWIRE_HOST_ID")
    env_herdr_bin = os.environ.get("TENDWIRE_HERDR_BIN")
    env_data_dir = os.environ.get("TENDWIRE_DATA_DIR")
    env_db_path = os.environ.get("TENDWIRE_DB_PATH")
    env_socket_path = os.environ.get("TENDWIRE_SOCKET_PATH")
    env_socket_group = os.environ.get("TENDWIRE_SOCKET_GROUP")
    env_herdr_timeout_seconds = os.environ.get("TENDWIRE_HERDR_TIMEOUT_SECONDS")
    env_herdr_backend = os.environ.get("TENDWIRE_HERDR_BACKEND")

    resolved_host_id = host_id or env_host_id or (platform.node() or "unknown")
    resolved_herdr_bin = herdr_bin or env_herdr_bin or "herdr"

    if db_path is not None:
        resolved_db_path = Path(db_path)
    elif env_db_path is not None:
        resolved_db_path = Path(env_db_path)
    else:
        resolved_db_path = None

    if data_dir is not None:
        resolved_data_dir = Path(data_dir)
    elif env_data_dir is not None:
        resolved_data_dir = Path(env_data_dir)
    else:
        resolved_data_dir = Path.home() / ".local" / "share" / "tendwire"

    if socket_path is not None:
        resolved_socket_path = Path(socket_path)
    elif env_socket_path is not None:
        resolved_socket_path = Path(env_socket_path)
    else:
        resolved_socket_path = None
    if socket_group is not None:
        resolved_socket_group = socket_group
    else:
        resolved_socket_group = env_socket_group

    raw_timeout = herdr_timeout_seconds
    if raw_timeout is None:
        raw_timeout = env_herdr_timeout_seconds
    if raw_timeout is None:
        resolved_herdr_timeout_seconds = 5.0
    else:
        try:
            resolved_herdr_timeout_seconds = float(raw_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError("herdr timeout must be a positive number") from exc

    resolved_herdr_backend = herdr_backend
    if resolved_herdr_backend is None:
        resolved_herdr_backend = env_herdr_backend
    if resolved_herdr_backend is None:
        resolved_herdr_backend = "cli"

    return Config(
        host_id=resolved_host_id,
        herdr_bin=resolved_herdr_bin,
        data_dir=resolved_data_dir,
        db_path=resolved_db_path,
        socket_path=resolved_socket_path,
        herdr_timeout_seconds=resolved_herdr_timeout_seconds,
        herdr_backend=resolved_herdr_backend,
        event_debounce_seconds=_resolve_value(
            event_debounce_seconds,
            "TENDWIRE_EVENT_DEBOUNCE_SECONDS",
            DEFAULT_EVENT_DEBOUNCE_SECONDS,
        ),
        reconcile_interval_seconds=_resolve_value(
            reconcile_interval_seconds,
            "TENDWIRE_RECONCILE_INTERVAL_SECONDS",
            DEFAULT_RECONCILE_INTERVAL_SECONDS,
        ),
        event_retention_days=_resolve_value(
            event_retention_days,
            "TENDWIRE_EVENT_RETENTION_DAYS",
            DEFAULT_EVENT_RETENTION_DAYS,
        ),
        output_excerpt_chars=_resolve_value(
            output_excerpt_chars,
            "TENDWIRE_OUTPUT_EXCERPT_CHARS",
            DEFAULT_OUTPUT_EXCERPT_CHARS,
        ),
        max_workers=_resolve_value(
            max_workers,
            "TENDWIRE_MAX_WORKERS",
            DEFAULT_MAX_WORKERS,
        ),
        max_outbox_attempts=_resolve_value(
            max_outbox_attempts,
            "TENDWIRE_MAX_OUTBOX_ATTEMPTS",
            DEFAULT_MAX_OUTBOX_ATTEMPTS,
        ),
        connector_claim_ttl_seconds=_resolve_value(
            connector_claim_ttl_seconds,
            "TENDWIRE_CONNECTOR_CLAIM_TTL_SECONDS",
            DEFAULT_CONNECTOR_CLAIM_TTL_SECONDS,
        ),
        socket_group=resolved_socket_group,
    )
