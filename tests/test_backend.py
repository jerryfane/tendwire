"""Tests for the Herdr CLI backend adapter contract."""

from __future__ import annotations

import json
import subprocess
from typing import Any

from tendwire.backends import herdr_cli
from tendwire.backends.herdr_cli import fetch_herdr_state
from tendwire.config import Config
from tendwire.core.projector import project_from_observations


_FORBIDDEN_FIELDS = {
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


def _completed(stdout: str, returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["herdr", "workspace", "list", "--json"],
        returncode=returncode,
        stdout=stdout,
        stderr="",
    )


def _assert_no_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _FORBIDDEN_FIELDS, f"forbidden field {path}.{key}"
            _assert_no_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, f"{path}[{index}]")


def test_fetch_herdr_state_returns_empty_when_binary_missing() -> None:
    config = Config(host_id="testhost", herdr_bin="definitely-not-a-real-herdr-binary")
    spaces, workers = fetch_herdr_state(config)
    assert spaces == []
    assert workers == []


def test_fetch_herdr_state_returns_empty_on_cli_failure(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(
        herdr_cli,
        "_run_herdr",
        lambda args, cfg: _completed('{"workers":[{"id":"leaked"}]}', returncode=2),
    )

    spaces, workers = fetch_herdr_state(config)

    assert spaces == []
    assert workers == []


def test_fetch_herdr_state_returns_empty_on_malformed_json(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _completed("not json"))

    spaces, workers = fetch_herdr_state(config)

    assert spaces == []
    assert workers == []


def test_sample_herdr_projection_is_neutral_and_fingerprinted(monkeypatch) -> None:
    config = Config(host_id="herdr-host", herdr_bin="herdr")
    sample_payload = {
        "spaces": [
            {
                "id": "space-1",
                "name": "Build",
                "status": "running",
                "status_line": "building package",
                "telegram": "forbidden",
                "chat_id": 111,
                "safe": "space-meta",
            }
        ],
        "workers": [
            {
                "id": "worker-1",
                "name": "Agent One",
                "status": "panic",
                "space_id": "space-1",
                "summary": "crashed",
                "topic_id": 222,
                "message_id": 333,
                "route": "telegram",
                "delivery": {"chat_id": 444},
                "safe": "worker-meta",
            }
        ],
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(
        herdr_cli,
        "_run_herdr",
        lambda args, cfg: _completed(json.dumps(sample_payload)),
    )

    spaces, workers = fetch_herdr_state(config)
    snapshot = project_from_observations(config, spaces=spaces, workers=workers)
    payload = json.loads(snapshot.to_json())

    assert payload["schema_version"] == 2
    assert len(payload["content_fingerprint"]) == 24
    assert payload["spaces"][0]["status"] == "active"
    assert payload["spaces"][0]["fingerprint"]
    assert payload["spaces"][0]["meta"]["safe"] == "space-meta"
    assert payload["workers"][0]["status"] == "failed"
    assert payload["workers"][0]["fingerprint"]
    assert payload["workers"][0]["meta"]["raw_status"] == "panic"
    assert payload["workers"][0]["meta"]["safe"] == "worker-meta"
    assert payload["attention"][0]["source"] == "worker:worker-1"
    assert payload["attention"][0]["fingerprint"]
    _assert_no_forbidden_fields(payload)
