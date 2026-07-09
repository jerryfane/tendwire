"""meta.stable_key: a session-independent hash of a worker's DURABLE terminal identity, emitted so a
connector can reconcile a re-lettered worker id (herdr reassigns ids positionally across restarts, and
renumbers the positional pane_id when a sibling closes) back to the same terminal instead of stranding a
duplicate. The key must follow the terminal_id, not the churning pane_id, and never leak a raw id."""
from __future__ import annotations

import json
import re
from pathlib import Path

from tendwire.backends.herdr_cli import _workers_and_bindings_from_records
from tendwire.backends.herdr_events import HerdrEventBackend
from tendwire.config import Config
from tendwire.store.sqlite import init_store

_HEX24 = re.compile(r"^[0-9a-f]{24}$")


def _config(tmp_path: Path) -> Config:
    return Config(
        host_id="stable-host",
        data_dir=tmp_path,
        db_path=tmp_path / "stable-host.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
    )


def _agent_item(*, agent_id="agent-1", pane_id="ws-1:p2Q", terminal_id="term-1", name="claude", agent="claude"):
    item = {"name": name, "agent": agent, "workspace_id": "ws-1", "status": "waiting",
            "agent_session": {"kind": "id", "value": "sess-1"}}
    if agent_id is not None:
        item["agent_id"] = agent_id
    if pane_id is not None:
        item["pane_id"] = pane_id
    if terminal_id is not None:
        item["terminal_id"] = terminal_id
    return item


def _workers(tmp_path, agents):
    config = _config(tmp_path)
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    records = backend._records_from_reconcile_payloads({"agents": agents}, {"panes": []})
    workers, _bindings = _workers_and_bindings_from_records(config, records)
    return workers


def test_stable_key_is_hex_from_terminal_identity(tmp_path: Path) -> None:
    (worker,) = _workers(tmp_path, [_agent_item()])
    key = worker.meta.get("stable_key")
    assert key and _HEX24.match(key)


def test_stable_key_stable_across_worker_id_relettering(tmp_path: Path) -> None:
    # THE point: across a herdr restart the durable terminal_id is unchanged, even though the worker/agent
    # id is re-lettered AND the positional pane_id is renumbered. The stable_key must stay the same.
    (w_before,) = _workers(tmp_path, [_agent_item(agent_id="agent-old", pane_id="ws-1:p2")])
    (w_after,) = _workers(tmp_path, [_agent_item(agent_id="agent-new", pane_id="ws-1:p5")])
    assert w_before.meta["stable_key"] == w_after.meta["stable_key"]


def test_stable_key_unchanged_when_only_pane_id_changes(tmp_path: Path) -> None:
    # A sibling pane closing renumbers this pane's positional pane_id; the terminal_id (and so the key)
    # holds. Keying off pane_id would (wrongly) treat this as a different worker.
    (w1,) = _workers(tmp_path, [_agent_item(pane_id="ws-1:p3")])
    (w2,) = _workers(tmp_path, [_agent_item(pane_id="ws-1:p1")])
    assert w1.meta["stable_key"] == w2.meta["stable_key"]


def test_stable_key_differs_across_terminal_ids(tmp_path: Path) -> None:
    # Distinct terminals -> distinct keys, even when the positional pane_id happens to be identical.
    (w1,) = _workers(tmp_path, [_agent_item(agent_id="a", terminal_id="term-a")])
    (w2,) = _workers(tmp_path, [_agent_item(agent_id="b", terminal_id="term-b")])
    assert w1.meta["stable_key"] != w2.meta["stable_key"]


def test_stable_key_differs_across_distinct_panes(tmp_path: Path) -> None:
    # Two distinct terminals in one reconcile keep distinct keys through the dedup path.
    workers = _workers(tmp_path, [
        _agent_item(agent_id="a", pane_id="ws-1:p1", terminal_id="term-a"),
        _agent_item(agent_id="b", pane_id="ws-1:p2", terminal_id="term-b"),
    ])
    keys = {w.meta.get("stable_key") for w in workers}
    assert len(keys) == 2 and all(k for k in keys)


def test_stable_key_absent_without_pane_identity(tmp_path: Path) -> None:
    # A worker exposing only a session id (no pane_id/terminal_id) gets NO stable_key — never hash a
    # {no-pane, space} tuple, which would collapse every session-only worker in a space to one key.
    (worker,) = _workers(tmp_path, [_agent_item(agent="codex", name="codex", pane_id=None, terminal_id=None)])
    assert "stable_key" not in worker.meta


def test_stable_key_does_not_leak_raw_pane_or_terminal_id(tmp_path: Path) -> None:
    (worker,) = _workers(tmp_path, [_agent_item(pane_id="ws-1:leaky-pane", terminal_id="term-leaky")])
    blob = json.dumps(worker.to_dict())
    assert worker.meta.get("stable_key")               # present
    assert "ws-1:leaky-pane" not in blob               # raw pane id never surfaces
    assert "term-leaky" not in blob                    # raw terminal id (now hashed) never surfaces


def test_stable_key_survives_to_dict_sanitizer(tmp_path: Path) -> None:
    (worker,) = _workers(tmp_path, [_agent_item()])
    assert worker.to_dict()["meta"].get("stable_key") == worker.meta["stable_key"]
