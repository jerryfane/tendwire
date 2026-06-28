"""Project neutral Snapshot objects from backend observations and config.

The projector imports only stdlib and sibling core modules. It must not import
Telegram, Herdres, or concrete backend connector modules.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ..config import Config
from .attention import update_snapshot_attention
from .models import Snapshot, Space, Worker, utc_timestamp


def project_empty(config: Config) -> Snapshot:
    """Return an empty neutral snapshot for the configured host."""
    return update_snapshot_attention(
        Snapshot(
            host_id=config.host_id,
            updated_at=utc_timestamp(),
            spaces=[],
            workers=[],
            attention=[],
        )
    )


def project_from_observations(
    config: Config,
    *,
    spaces: list[Space] | None = None,
    workers: list[Worker] | None = None,
    timestamp: datetime | None = None,
) -> Snapshot:
    """Build a neutral snapshot from backend-neutral observations."""
    snapshot = Snapshot(
        host_id=config.host_id,
        updated_at=utc_timestamp(timestamp),
        spaces=list(spaces or []),
        workers=list(workers or []),
        attention=[],
    )
    return update_snapshot_attention(snapshot)


def project_from_raw(
    config: Config,
    *,
    spaces: list[dict[str, Any]] | None = None,
    workers: list[dict[str, Any]] | None = None,
    timestamp: datetime | None = None,
) -> Snapshot:
    """Build a neutral snapshot from raw dict observations."""
    snapshot = Snapshot(
        host_id=config.host_id,
        updated_at=utc_timestamp(timestamp),
        spaces=[Space.from_dict(s) for s in (spaces or [])],
        workers=[Worker.from_dict(w) for w in (workers or [])],
        attention=[],
    )
    return update_snapshot_attention(snapshot)
