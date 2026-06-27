"""Tests for the sqlite store."""

from __future__ import annotations

import tempfile
from pathlib import Path

from tendwire.config import Config
from tendwire.core.projector import project_empty
from tendwire.store.sqlite import init_store, latest_snapshot, list_hosts, save_snapshot


def test_store_save_and_latest() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "tendwire.db"
        config = Config(host_id="storehost", db_path=db_path)
        init_store(db_path)
        assert latest_snapshot(db_path) is None

        snapshot = project_empty(config)
        save_snapshot(db_path, snapshot)
        restored = latest_snapshot(db_path)
        assert restored is not None
        assert restored.host_id == "storehost"


def test_list_hosts() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "tendwire.db"
        config = Config(host_id="host-a", db_path=db_path)
        save_snapshot(db_path, project_empty(config))
        assert list_hosts(db_path) == ["host-a"]
