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
from typing import Any

from ..config import Config
from ..core.models import Space, Worker


def _run_herdr(args: list[str], config: Config) -> subprocess.CompletedProcess[str]:
    """Run the Herdr CLI with the supplied read-only arguments."""
    return subprocess.run(
        [config.herdr_bin, *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _parse_json_output(stdout: str) -> dict[str, Any]:
    """Best-effort parse of herdr JSON output; empty dict on failure."""
    if not stdout.strip():
        return {}
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {}


def _spaces_from_payload(payload: dict[str, Any]) -> list[Space]:
    """Extract neutral Space objects from a herdr payload."""
    spaces: list[Space] = []
    for item in payload.get("spaces", payload.get("data", [])):
        if not isinstance(item, dict):
            continue
        spaces.append(
            Space(
                id=str(item.get("id", item.get("name", "unknown"))),
                name=str(item.get("name", item.get("id", "unknown"))),
                status=str(item.get("status", "unknown")),
                meta={k: v for k, v in item.items() if k not in {"id", "name", "status"}},
            )
        )
    return spaces


def _workers_from_payload(payload: dict[str, Any]) -> list[Worker]:
    """Extract neutral Worker objects from a herdr payload."""
    workers: list[Worker] = []
    for item in payload.get("workers", []):
        if not isinstance(item, dict):
            continue
        workers.append(
            Worker(
                id=str(item.get("id", item.get("name", "unknown"))),
                name=str(item.get("name", item.get("id", "unknown"))),
                status=str(item.get("status", "unknown")),
                space_id=item.get("space_id") or item.get("space"),
                meta={k: v for k, v in item.items() if k not in {"id", "name", "status", "space_id", "space"}},
            )
        )
    return workers


def fetch_herdr_state(config: Config) -> tuple[list[Space], list[Worker]]:
    """Return neutral spaces and workers from the Herdr CLI, or empty lists."""
    if shutil.which(config.herdr_bin) is None:
        return [], []

    result = _run_herdr(["status", "--json"], config)
    if result.returncode != 0:
        return [], []

    payload = _parse_json_output(result.stdout)
    return _spaces_from_payload(payload), _workers_from_payload(payload)
