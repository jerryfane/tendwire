"""Tests for private Herdr turn ingestion into public Tendwire turns."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from tendwire.backends import herdr_turns
from tendwire.backends.herdr_turns import refresh_structured_turn_content
from tendwire.config import Config
from tendwire.core.models import WorkerBinding
from tendwire.core.projector import project_from_raw
from tendwire.store.sqlite import init_store, save_snapshot, turns_payload_from_store, upsert_worker_bindings


def test_refresh_structured_turn_content_uses_private_binding_without_public_leak(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "turns.db"
    config = Config(
        host_id="turn-host",
        db_path=db_path,
        herdr_bin="herdr_turn_adapter.py",
        herdr_timeout_seconds=2,
    )
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "codex", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id=config.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend="herdr",
                target_kind="agent_id",
                target_value="agent-private",
                turn_target_kind="pane_id",
                turn_target_value="pane-private",
                sendable=True,
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="private-binding",
            )
        ],
    )
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=json.dumps(
                {
                    "result": {
                        "turn": {
                            "available": True,
                            "user_text": "Why is Telegram showing lifecycle status?",
                            "assistant_final_text": "Use Tendwire turn text, not pane_id pane-private.",
                            "assistant_stream_text": "Checking source mode...",
                            "complete": True,
                            "has_open_turn": False,
                        }
                    }
                }
            ),
            stderr="",
        )

    monkeypatch.setattr(herdr_turns.subprocess, "run", fake_run)

    result = refresh_structured_turn_content(config)
    payload = turns_payload_from_store(db_path, config.host_id, snapshot=snapshot)

    assert result["attempted"] == 1
    assert result["updated"] == 1
    assert calls[0][0] == [
        "herdr_turn_adapter.py",
        "pane",
        "turn",
        "pane-private",
        "--last",
        "--format",
        "json",
    ]
    assert calls[0][1]["timeout"] == 2
    turn = payload["turns"][0]
    assert turn["user_text"] == "Why is Telegram showing lifecycle status?"
    assert "Use Tendwire turn text" in turn["assistant_final_text"]
    public_json = json.dumps(payload)
    assert "pane-private" not in public_json
    assert "agent-private" not in public_json
    assert turn["complete"] is True
    assert turn["has_open_turn"] is False
