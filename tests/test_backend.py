"""Tests for the Herdr CLI backend adapter."""

from __future__ import annotations

from tendwire.backends.herdr_cli import fetch_herdr_state
from tendwire.config import Config


def test_fetch_herdr_state_returns_empty_when_binary_missing() -> None:
    config = Config(host_id="testhost", herdr_bin="definitely-not-a-real-herdr-binary")
    spaces, workers = fetch_herdr_state(config)
    assert spaces == []
    assert workers == []
