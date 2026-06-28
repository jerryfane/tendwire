"""Backend command boundary for Herdr.

This module is the narrow adapter for mutating commands directed at Herdr. In
milestone 1 no safe high-level Herdr instruction API is used, so
send_instruction returns backend_unsupported rather than performing unsafe
terminal control (send-keys, send-text, pane commands, PTY manipulation, etc.).
"""

from __future__ import annotations

from typing import Any

from ..config import Config
from ..core.commands import (
    STATUS_BACKEND_UNSUPPORTED,
    CommandEnvelope,
    error_value,
)


def send_instruction(
    config: Config,
    target: dict[str, Any],
    instruction: dict[str, Any],
) -> CommandEnvelope:
    """Return backend_unsupported for send_instruction in this milestone."""
    return CommandEnvelope(
        ok=False,
        status=STATUS_BACKEND_UNSUPPORTED,
        action="send_instruction",
        request_id=None,
        dry_run=False,
        result=None,
        error=error_value(
            STATUS_BACKEND_UNSUPPORTED,
            "send_instruction is not supported by this backend in this milestone",
            details={"target": target.get("worker_id")},
        ),
    )
