"""Thin adapter boundary around the Herdr CLI.

This module shells out read-only to a `herdr` binary when available and parses
output on a best-effort basis. If the binary is missing or fails, it returns
empty neutral data rather than blocking the snapshot contract.

This module must not import Herdres code or leak delivery/routing state into
core models.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from ..config import Config
from ..core.models import Space, Worker, normalize_status


_HERDR_TIMEOUT_SECONDS = 5.0

_FORBIDDEN_CONNECTOR_FIELDS = {
    "telegram",
    "chat_id",
    "topic_id",
    "message_id",
    "thread_id",
    "token",
    "bot_token",
    "delivery",
    "route",
    "herdres_delivery",
}

_STATUS_KEYS = (
    "status",
    "state",
    "phase",
    "lifecycle",
    "lifecycle_state",
    "raw_status",
)


_FORBIDDEN_CONNECTOR_FIELDS_COMPACT = {field.replace("_", "") for field in _FORBIDDEN_CONNECTOR_FIELDS}


def _compact_field_name(key: object) -> str:
    """Normalize field names for conservative connector/status-key matching."""
    return str(key).lower().replace("-", "_").replace("_", "")


def _field_matches(key: object, expected: str) -> bool:
    """Return True when a payload key matches snake_case or camelCase spelling."""
    return _compact_field_name(key) == _compact_field_name(expected)


def _is_forbidden_connector_field(key: object) -> bool:
    """Return True for forbidden connector fields, including common case variants."""
    compact = _compact_field_name(key)
    return str(key).lower().replace("-", "_") in _FORBIDDEN_CONNECTOR_FIELDS or compact in _FORBIDDEN_CONNECTOR_FIELDS_COMPACT


def _run_herdr(args: Sequence[str], config: Config) -> subprocess.CompletedProcess[str] | None:
    """Run the Herdr CLI with read-only arguments; return None on any CLI failure."""
    try:
        return subprocess.run(
            [config.herdr_bin, *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=_HERDR_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired, UnicodeDecodeError, ValueError, TypeError):
        return None


def _parse_json_output(stdout: str | None) -> Any:
    """Best-effort parse of herdr JSON output; None on failure."""
    if not stdout or not stdout.strip():
        return None
    try:
        return json.loads(stdout)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def _command_payload(args: Sequence[str], config: Config) -> Any:
    """Return parsed JSON for a single herdr command, or None on any bad output."""
    result = _run_herdr(args, config)
    if result is None or result.returncode != 0:
        return None
    return _parse_json_output(result.stdout)


def _strip_connector_fields(value: Any) -> Any:
    """Recursively drop connector/delivery fields from arbitrary JSON-like values."""
    if isinstance(value, Mapping):
        clean: dict[str, Any] = {}
        for key, child in value.items():
            key_text = str(key)
            if _is_forbidden_connector_field(key):
                continue
            clean[key_text] = _strip_connector_fields(child)
        return clean
    if isinstance(value, list):
        return [_strip_connector_fields(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_connector_fields(item) for item in value]
    return value


def _strip_status_fields(value: Any) -> Any:
    """Recursively drop raw status fields from metadata values."""
    if isinstance(value, Mapping):
        clean: dict[str, Any] = {}
        for key, child in value.items():
            if any(_field_matches(key, status_key) for status_key in _STATUS_KEYS):
                continue
            clean[str(key)] = _strip_status_fields(child)
        return clean
    if isinstance(value, list):
        return [_strip_status_fields(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_status_fields(item) for item in value]
    return value


def _payload_items(payload: Any, keys: Sequence[str]) -> list[dict[str, Any]]:
    """Extract object records from conservative herdr list payload shapes."""
    if isinstance(payload, list):
        candidates: Iterable[Any] = payload
    elif isinstance(payload, Mapping):
        candidates = ()
        for key in keys:
            value = _value_for_key(payload, key)
            if isinstance(value, list):
                candidates = value
                break
            if isinstance(value, Mapping):
                nested = _payload_items(value, keys)
                if nested:
                    return nested
    else:
        return []

    items: list[dict[str, Any]] = []
    for item in candidates:
        if isinstance(item, Mapping):
            stripped = _strip_connector_fields(item)
            if isinstance(stripped, dict):
                items.append(stripped)
    return items


def _first_text(item: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    """Return the first scalar string value for any key."""
    for key in keys:
        value = _value_for_key(item, key)
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            return str(value)
    return None


def _related_id(value: Any) -> str | None:
    """Return a neutral related-object id from a scalar or mapping."""
    if value is None:
        return None
    if isinstance(value, Mapping):
        return _first_text(value, ("id", "workspace_id", "space_id", "slug", "name"))
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return None


def _normalize_status(raw_status: Any) -> tuple[str, str | None]:
    """Return canonical status plus original raw string when normalization changed it."""
    if raw_status is None:
        return "unknown", None

    raw_text = str(raw_status)
    canonical = normalize_status(raw_text)
    raw_key = raw_text.strip().lower().replace("_", "-")
    raw_meta = raw_text if raw_text and raw_key != canonical else None
    return canonical, raw_meta


def _value_for_key(item: Mapping[str, Any], expected_key: str) -> Any:
    """Return a value by exact key or snake/camel-case equivalent."""
    if expected_key in item:
        return item[expected_key]
    for key, value in item.items():
        if _field_matches(key, expected_key):
            return value
    return None


def _status_from_item(item: Mapping[str, Any]) -> tuple[str, str | None]:
    """Extract and normalize status-like fields from a herdr record."""
    for key in _STATUS_KEYS:
        value = _value_for_key(item, key)
        if value is not None:
            return _normalize_status(value)
    return "unknown", None


def _meta_from_item(item: Mapping[str, Any], excluded_keys: set[str], raw_status: str | None) -> dict[str, Any]:
    """Build sanitized neutral metadata for a projected model."""
    explicit_meta = _value_for_key(item, "meta")
    meta = {
        str(key): _strip_status_fields(value)
        for key, value in item.items()
        if not _field_matches(key, "meta")
        and not any(_field_matches(key, excluded_key) for excluded_key in excluded_keys)
        and not any(_field_matches(key, status_key) for status_key in _STATUS_KEYS)
    }
    if isinstance(explicit_meta, Mapping):
        for key, value in explicit_meta.items():
            if _is_forbidden_connector_field(key):
                continue
            if any(_field_matches(key, status_key) for status_key in _STATUS_KEYS):
                continue
            meta[str(key)] = _strip_status_fields(value)
    if raw_status is not None:
        meta["raw_status"] = raw_status
    return meta


def _spaces_from_payload(payload: Any) -> list[Space]:
    """Extract neutral Space objects from a herdr workspace-list payload."""
    spaces: list[Space] = []
    for item in _payload_items(payload, ("workspaces", "spaces", "data", "items", "results")):
        space_id = _first_text(item, ("id", "workspace_id", "space_id", "slug", "name")) or "unknown"
        name = _first_text(item, ("name", "title", "label", "slug", "id", "workspace_id", "space_id")) or space_id
        status, raw_status = _status_from_item(item)
        updated_at = _first_text(item, ("updated_at", "last_seen_at", "observed_at", "timestamp"))
        status_line = _first_text(item, ("status_line", "summary", "description"))
        meta = _meta_from_item(
            item,
            {
                "id",
                "workspace_id",
                "space_id",
                "slug",
                "name",
                "title",
                "label",
                "meta",
                "updated_at",
                "last_seen_at",
                "observed_at",
                "timestamp",
                "status_line",
                "summary",
                "description",
                "fingerprint",
            },
            raw_status,
        )
        spaces.append(
            Space(
                id=space_id,
                name=name,
                status=status,
                meta=meta,
                updated_at=updated_at,
                status_line=status_line,
            )
        )
    return spaces


def _workers_from_payload(payload: Any) -> list[Worker]:
    """Extract neutral Worker objects from a herdr agent-list payload."""
    workers: list[Worker] = []
    for item in _payload_items(payload, ("agents", "workers", "data", "items", "results")):
        worker_id = _first_text(item, ("id", "agent_id", "worker_id", "slug", "name")) or "unknown"
        name = _first_text(item, ("name", "title", "label", "slug", "id", "agent_id", "worker_id")) or worker_id
        status, raw_status = _status_from_item(item)
        last_seen_at = _first_text(item, ("last_seen_at", "updated_at", "observed_at", "timestamp"))
        summary = _first_text(item, ("summary", "status_line", "description"))
        space_id = (
            _first_text(item, ("space_id", "workspace_id", "spaceId", "workspaceId"))
            or _related_id(_value_for_key(item, "space"))
            or _related_id(_value_for_key(item, "workspace"))
        )
        meta = _meta_from_item(
            item,
            {
                "id",
                "agent_id",
                "worker_id",
                "slug",
                "name",
                "title",
                "label",
                "meta",
                "space_id",
                "workspace_id",
                "spaceId",
                "workspaceId",
                "space",
                "workspace",
                "last_seen_at",
                "updated_at",
                "observed_at",
                "timestamp",
                "summary",
                "status_line",
                "description",
                "fingerprint",
            },
            raw_status,
        )
        workers.append(
            Worker(
                id=worker_id,
                name=name,
                status=status,
                space_id=space_id,
                meta=meta,
                last_seen_at=last_seen_at,
                summary=summary,
            )
        )
    return workers


def fetch_herdr_state(config: Config) -> tuple[list[Space], list[Worker]]:
    """Return neutral spaces and workers from the Herdr CLI, or empty lists."""
    try:
        if shutil.which(config.herdr_bin) is None:
            return [], []
    except (TypeError, ValueError, OSError):
        return [], []

    workspace_payload = _command_payload(["workspace", "list", "--json"], config)
    agent_payload = _command_payload(["agent", "list", "--json"], config)
    return _spaces_from_payload(workspace_payload), _workers_from_payload(agent_payload)
