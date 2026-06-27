"""Neutral data models for Tendwire snapshots.

These models are intentionally device-neutral. They contain no Telegram,
Herdres delivery, chat/topic/message ID, or connector-specific routing state.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Space:
    """A neutral space observation (e.g. a Herdr space / project context)."""

    id: str
    name: str
    status: str = "unknown"
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Space":
        return cls(
            id=str(data["id"]),
            name=str(data.get("name", data["id"])),
            status=str(data.get("status", "unknown")),
            meta=dict(data.get("meta", {})),
        )


@dataclass(frozen=True)
class Worker:
    """A neutral worker observation (e.g. a running terminal agent)."""

    id: str
    name: str
    status: str = "unknown"
    space_id: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Worker":
        return cls(
            id=str(data["id"]),
            name=str(data.get("name", data["id"])),
            status=str(data.get("status", "unknown")),
            space_id=data.get("space_id"),
            meta=dict(data.get("meta", {})),
        )


@dataclass(frozen=True)
class AttentionSignal:
    """A pure, neutral attention signal produced from snapshot state."""

    id: str
    level: str  # e.g. "info", "warn", "critical"
    reason: str
    source: str  # e.g. "worker:abc" or "space:def"
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AttentionSignal":
        return cls(
            id=str(data["id"]),
            level=str(data.get("level", "info")),
            reason=str(data.get("reason", "")),
            source=str(data.get("source", "")),
            meta=dict(data.get("meta", {})),
        )


@dataclass(frozen=True)
class Snapshot:
    """Device-neutral top-level snapshot shape."""

    host_id: str
    updated_at: str
    spaces: list[Space] = field(default_factory=list)
    workers: list[Worker] = field(default_factory=list)
    attention: list[AttentionSignal] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "host_id": self.host_id,
            "updated_at": self.updated_at,
            "spaces": [s.to_dict() for s in self.spaces],
            "workers": [w.to_dict() for w in self.workers],
            "attention": [a.to_dict() for a in self.attention],
        }

    def to_json(self, indent: int | None = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Snapshot":
        return cls(
            host_id=str(data["host_id"]),
            updated_at=str(data["updated_at"]),
            spaces=[Space.from_dict(s) for s in data.get("spaces", [])],
            workers=[Worker.from_dict(w) for w in data.get("workers", [])],
            attention=[AttentionSignal.from_dict(a) for a in data.get("attention", [])],
        )

    @classmethod
    def from_json(cls, payload: str) -> "Snapshot":
        return cls.from_dict(json.loads(payload))


def utc_timestamp(dt: datetime | None = None) -> str:
    """Return an ISO-8601 UTC timestamp string."""
    if dt is None:
        dt = _utc_now()
    return dt.astimezone(timezone.utc).isoformat()
