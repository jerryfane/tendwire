"""Release-readiness guards for the public Tendwire contract.

Consolidates the RC invariants: public snapshot/turns/pending JSON carry no
forbidden connector/terminal keys and no pseudo pane-id string values, even
when the raw backend observation embeds pane ids, terminal ids, and send-key
payloads.
"""

from __future__ import annotations

import json
import re
from typing import Any

from tendwire.config import Config
from tendwire.core.projector import project_from_raw
from tendwire.core.turns import (
    pending_payload_from_snapshot,
    turns_payload_from_snapshot,
)


_FORBIDDEN_KEYS = {
    "pane_id",
    "pane_ids",
    "terminal_id",
    "terminal_ids",
    "backend_target",
    "backend_targets",
    "send_keys",
    "session_id",
    "private_fingerprint",
    "chat_id",
    "topic_id",
    "message_id",
    "token",
    "secret",
}
_FORBIDDEN_COMPACT = {key.replace("_", "") for key in _FORBIDDEN_KEYS}

# Herdr pane/terminal identifiers look like ``w<hex>:p<n>`` / ``w<hex>:t<n>``.
_PSEUDO_PANE_ID_RE = re.compile(r"\bw[0-9a-f]+:(?:p|t)[0-9a-f]+\b", re.IGNORECASE)

# Private values seeded into the raw observation below; none may surface publicly.
_PRIVATE_VALUES = ("wX8:p1", "term_655private", "send-secret", "019f-private-session")


def _assert_public_clean(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            assert normalized not in _FORBIDDEN_KEYS, f"forbidden key {path}.{key}"
            assert normalized.replace("_", "") not in _FORBIDDEN_COMPACT, f"forbidden key {path}.{key}"
            _assert_public_clean(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_public_clean(item, f"{path}[{index}]")
    elif isinstance(value, str):
        assert not _PSEUDO_PANE_ID_RE.search(value), f"pseudo pane id in {path}: {value!r}"


def _public_payloads() -> list[dict[str, Any]]:
    config = Config(host_id="rc-host")
    snapshot = project_from_raw(
        config,
        spaces=[{"id": "wX8", "name": "projectx", "status": "active"}],
        workers=[
            {
                "id": "codex",
                "name": "codex",
                "status": "working",
                "space_id": "wX8",
                "pane_id": "wX8:p1",
                "terminal_id": "term_655private",
                "agent_session": {"agent": "codex", "kind": "id", "value": "019f-private-session"},
                "meta": {"send_keys": "send-secret", "backend_target": {"kind": "pane_id", "value": "wX8:p1"}},
            }
        ],
    )
    return [
        json.loads(snapshot.to_json()),
        turns_payload_from_snapshot(snapshot),
        pending_payload_from_snapshot(snapshot),
    ]


def test_public_payloads_have_zero_forbidden_keys_and_no_pseudo_pane_ids():
    for payload in _public_payloads():
        _assert_public_clean(payload)


def test_private_values_never_appear_in_public_payloads():
    blob = json.dumps(_public_payloads())
    for private in _PRIVATE_VALUES:
        assert private not in blob, f"private value leaked into public JSON: {private!r}"


def test_public_worker_identity_is_neutral():
    snapshot_payload, _turns, _pending = _public_payloads()
    workers = snapshot_payload["workers"]
    assert workers, "expected a projected worker"
    # Public id is the neutral agent id, not a pane/terminal identifier.
    assert workers[0]["id"] == "codex"
    assert "pane_id" not in workers[0]
    assert "backend_target" not in workers[0]
