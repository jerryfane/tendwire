"""Structured turn ingestion through Tendwire-owned private Herdr bindings."""

from __future__ import annotations

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Mapping
from typing import Any

from ..config import Config
from ..store.sqlite import list_worker_bindings, merge_turn_content


_TURN_CONTENT_KEYS = (
    "user_text",
    "assistant_final_text",
    "assistant_stream_text",
    "complete",
    "has_open_turn",
)


def _extract_turn_payload(value: Any) -> Mapping[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    result = value.get("result")
    if isinstance(result, Mapping) and isinstance(result.get("turn"), Mapping):
        return result["turn"]
    if isinstance(value.get("turn"), Mapping):
        return value["turn"]
    return value


def _read_private_turn(config: Config, pane_id: str) -> Mapping[str, Any] | None:
    try:
        completed = subprocess.run(
            [
                config.herdr_bin,
                "pane",
                "turn",
                pane_id,
                "--last",
                "--format",
                "json",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=config.herdr_timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired, UnicodeDecodeError, ValueError):
        return None
    if completed.returncode != 0:
        return None
    try:
        payload = json.loads(completed.stdout)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    turn = _extract_turn_payload(payload)
    if not isinstance(turn, Mapping) or turn.get("available") is False:
        return None
    content = {key: turn.get(key) for key in _TURN_CONTENT_KEYS if key in turn}
    if not any(value not in (None, "", False) for value in content.values()):
        return None
    return content


def refresh_structured_turn_content(config: Config) -> dict[str, Any]:
    """Refresh public turn text from private turn targets, if a turn-capable Herdr bin exists."""
    if config.db_path is None:
        return {"ok": False, "status": "store_unavailable", "updated": 0, "attempted": 0}
    bindings = list_worker_bindings(config.db_path, config.host_id, backend="herdr")
    turn_bindings = [
        binding
        for binding in bindings
        if binding.turn_target_kind == "pane_id" and binding.turn_target_value
    ]
    updated = 0
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(turn_bindings)))) as pool:
        futures = {
            pool.submit(_read_private_turn, config, str(binding.turn_target_value)): binding
            for binding in turn_bindings
        }
        for future in as_completed(futures):
            binding = futures[future]
            try:
                content = future.result()
            except Exception:
                content = None
            if content is None:
                continue
            updated += merge_turn_content(
                config.db_path,
                config.host_id,
                binding.worker_id,
                content,
            )
    return {"ok": True, "status": "ok", "updated": updated, "attempted": len(turn_bindings)}
