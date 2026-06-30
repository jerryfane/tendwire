"""Tests for the opt-in Herdr socket event backend."""

from __future__ import annotations

import json
import socket
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from tendwire.backends.herdr_events import HerdrEventBackend
from tendwire.backends.herdr_socket import HerdrSocketClient
from tendwire.backends.herdr_socket import HerdrSocketTimeoutError
from tendwire.config import Config
from tendwire.core.models import BackendHealth, Worker
from tendwire.core.projector import project_from_observations
from tendwire.daemon import DaemonHooks, TendwireDaemon
from tendwire.store.sqlite import (
    init_store,
    latest_snapshot,
    list_worker_bindings,
    save_snapshot,
)


_PUBLIC_JSON_FORBIDDEN_KEYS = {
    "pane_id",
    "terminal_id",
    "backend_target",
    "chat_id",
    "topic_id",
    "message_id",
    "connector",
    "raw_payload",
    "socket_path",
    "target_kind",
    "target_value",
    "private_fingerprint",
}
_PUBLIC_JSON_FORBIDDEN_COMPACT = {key.replace("_", "") for key in _PUBLIC_JSON_FORBIDDEN_KEYS}


def _assert_no_public_json_forbidden(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            assert (
                normalized not in _PUBLIC_JSON_FORBIDDEN_KEYS
                and normalized.replace("_", "") not in _PUBLIC_JSON_FORBIDDEN_COMPACT
            ), f"forbidden field {path}.{key}"
            _assert_no_public_json_forbidden(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_public_json_forbidden(item, f"{path}[{index}]")


class _SocketConnection:
    def __init__(self, conn: socket.socket, requests: list[dict[str, Any]]) -> None:
        self.conn = conn
        self.requests = requests
        self._buffer = bytearray()

    def read_request(self) -> dict[str, Any]:
        while b"\n" not in self._buffer:
            chunk = self.conn.recv(4096)
            if not chunk:
                raise ConnectionError("client disconnected before request")
            self._buffer.extend(chunk)
        index = self._buffer.index(b"\n")
        line = bytes(self._buffer[: index + 1])
        del self._buffer[: index + 1]
        request = json.loads(line.decode("utf-8"))
        self.requests.append(request)
        return request

    def send_json(self, payload: Mapping[str, Any]) -> None:
        self.conn.sendall(json.dumps(dict(payload), separators=(",", ":")).encode("utf-8") + b"\n")


class _FakeHerdrSocketServer:
    def __init__(self, tmp_path: Path, handler: Callable[[_SocketConnection], None]) -> None:
        self.path = tmp_path / f"herdr-events-{time.monotonic_ns()}.sock"
        self.handler = handler
        self.requests: list[dict[str, Any]] = []
        self.errors: list[BaseException] = []
        self._ready = threading.Event()
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "_FakeHerdrSocketServer":
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.path))
        listener.listen(1)
        listener.settimeout(0.2)
        self._listener = listener
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        assert self._ready.wait(1)
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._listener is not None:
            self._listener.close()
        if self._thread is not None:
            self._thread.join(timeout=1)
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        if exc_type is None and self.errors:
            raise AssertionError(f"fake Herdr socket failed: {self.errors!r}")

    def _run(self) -> None:
        self._ready.set()
        try:
            assert self._listener is not None
            conn, _addr = self._listener.accept()
            with conn:
                self.handler(_SocketConnection(conn, self.requests))
        except OSError:
            pass
        except BaseException as exc:
            self.errors.append(exc)


class _StaticClient:
    def __init__(
        self,
        *,
        workspaces: Any | None = None,
        tabs: Any | None = None,
        panes: Any | None = None,
        agents: Any | None = None,
    ) -> None:
        self.workspaces = {"workspaces": list(workspaces or [])}
        self.tabs = {"tabs": list(tabs or [])}
        self.panes = {"panes": list(panes or [])}
        self.agents = {"agents": list(agents or [])}
        self.calls: list[str] = []

    def workspace_list(self) -> Any:
        self.calls.append("workspace.list")
        return self.workspaces

    def tab_list(self) -> Any:
        self.calls.append("tab.list")
        return self.tabs

    def pane_list(self) -> Any:
        self.calls.append("pane.list")
        return self.panes

    def agent_list(self) -> Any:
        self.calls.append("agent.list")
        return self.agents


def _config(tmp_path: Path, host_id: str = "events-host") -> Config:
    return Config(
        host_id=host_id,
        data_dir=tmp_path,
        db_path=tmp_path / f"{host_id}.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
    )


def _backend(tmp_path: Path, host_id: str = "events-host", *, debounce_seconds: float = 0) -> HerdrEventBackend:
    config = _config(tmp_path, host_id)
    init_store(Path(config.db_path))
    return HerdrEventBackend(
        config,
        debounce_seconds=debounce_seconds,
        reconnect_delay_seconds=0,
    )


def _initial_pane_client() -> _StaticClient:
    return _StaticClient(
        workspaces=[{"id": "space-1", "name": "Build", "status": "active"}],
        panes=[
            {
                "pane_id": "pane-1",
                "agent": "Agent One",
                "workspace_id": "space-1",
                "status": "running",
            }
        ],
        agents=[],
    )


def test_startup_reconcile_uses_socket_client_persists_projection_and_private_bindings(tmp_path: Path) -> None:
    def handler(conn: _SocketConnection) -> None:
        results = {
            "workspace.list": {
                "workspaces": [
                    {
                        "id": "space-1",
                        "name": "Build",
                        "status": "active",
                        "pane_id": "private-pane",
                    }
                ]
            },
            "tab.list": {"tabs": [{"id": "tab-private", "workspace_id": "space-1"}]},
            "pane.list": {
                "panes": [
                    {
                        "pane_id": "pane-1",
                        "terminal_id": "terminal-private",
                        "agent": "Agent One",
                        "workspace_id": "space-1",
                        "status": "running",
                    }
                ]
            },
            "agent.list": {
                "agents": [
                    {
                        "agent_id": "agent-private",
                        "name": "Agent One",
                        "workspace_id": "space-1",
                        "status": "waiting",
                        "pane_id": "pane-1",
                    }
                ]
            },
        }
        for _index in range(4):
            request = conn.read_request()
            conn.send_json({"id": request["id"], "result": results[request["method"]]})

    config = _config(tmp_path, "socket-reconcile")
    init_store(Path(config.db_path))
    with _FakeHerdrSocketServer(tmp_path, handler) as server:
        backend = HerdrEventBackend(config, debounce_seconds=0)
        client = HerdrSocketClient(str(server.path), timeout=1)
        snapshot = backend.reconcile_once(client=client)
        client.close()

    assert [request["method"] for request in server.requests] == [
        "workspace.list",
        "tab.list",
        "pane.list",
        "agent.list",
    ]
    assert snapshot.backend_health[0].status == "healthy"
    assert "agent-private" in {worker.id for worker in snapshot.workers}
    assert list_worker_bindings(Path(config.db_path), config.host_id, backend="herdr")
    encoded = snapshot.to_json()
    assert "private-pane" not in encoded
    assert "terminal-private" not in encoded
    _assert_no_public_json_forbidden(json.loads(encoded))


def test_agent_status_changed_updates_worker_once_for_duplicate_sequence(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "status-dedupe")
    backend.reconcile_once(client=_initial_pane_client())

    first = {
        "event": "agent_status_changed",
        "server_id": "srv-1",
        "sequence": 10,
        "payload": {"agent": "Agent One", "status": "blocked"},
    }
    duplicate = {
        "event": "agent.status_changed",
        "server_id": "srv-1",
        "sequence": 10,
        "payload": {"agent": "Agent One", "status": "failed"},
    }

    assert backend.queue_event_envelope(first) is True
    assert backend.queue_event_envelope(duplicate) is False

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "blocked"


def test_pane_moved_preserves_public_worker_and_updates_private_binding(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "pane-moved")
    backend.reconcile_once(client=_initial_pane_client())
    before = latest_snapshot(backend.db_path, backend.config.host_id)
    assert before is not None
    worker_id = before.workers[0].id
    binding = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]

    backend.queue_event_envelope(
        {
            "event": "pane.moved",
            "payload": {
                "old_pane_id": "pane-1",
                "pane_id": "pane-2",
                "agent": "Agent One",
                "workspace_id": "space-1",
            },
        }
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    moved_binding = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]
    assert after is not None
    assert after.workers[0].id == worker_id
    assert moved_binding.private_fingerprint == binding.private_fingerprint
    assert moved_binding.target_kind == "pane_id"
    assert moved_binding.target_value == "pane-2"


def test_pane_closed_closes_worker_and_expires_matching_binding(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "pane-closed")
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope({"event": "pane.closed", "payload": {"pane_id": "pane-1"}})

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "closed"
    assert list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr") == []
    expired = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
        include_expired=True,
    )
    assert expired[0].sendable is False
    assert expired[0].reason == "pane_closed"


def test_disconnect_degraded_state_preserves_workers_and_bindings(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "degraded")
    backend.reconcile_once(client=_initial_pane_client())
    binding_before = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]

    backend._mark_unhealthy("socket_disconnected")

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    binding_after = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]
    assert snapshot is not None
    assert snapshot.workers[0].status == "active"
    assert snapshot.backend_health[0].status == "unavailable"
    assert binding_after.private_fingerprint == binding_before.private_fingerprint
    assert binding_after.sendable is True


def test_healthy_empty_reconnect_closes_missing_workers_and_expires_bindings(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "healthy-empty")
    backend.reconcile_once(client=_initial_pane_client())

    backend.reconcile_once(client=_StaticClient(workspaces=[], tabs=[], panes=[], agents=[]))

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.backend_health[0].status == "healthy"
    assert snapshot.backend_health[0].outcome == "empty_healthy"
    assert snapshot.workers[0].status == "closed"
    assert list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr") == []


def test_debounce_batches_until_flush_and_shutdown_flushes(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "debounce", debounce_seconds=60)
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope(
        {"event": "agent_status_changed", "payload": {"agent": "Agent One", "status": "blocked"}}
    )
    pending = latest_snapshot(backend.db_path, backend.config.host_id)
    assert pending is not None
    assert pending.workers[0].status == "active"

    backend.stop()

    flushed = latest_snapshot(backend.db_path, backend.config.host_id)
    assert flushed is not None
    assert flushed.workers[0].status == "blocked"


def test_idle_event_timeout_keeps_polling_without_marking_backend_unhealthy(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "idle-timeout")
    backend.reconcile_once(client=_initial_pane_client())

    class IdleThenEventClient:
        def __init__(self) -> None:
            self.calls = 0

        def read_event(self, subscription_id: str, *, timeout: float | None = None) -> dict[str, Any]:
            self.calls += 1
            if self.calls == 1:
                raise HerdrSocketTimeoutError("idle")
            backend.stop_event.set()
            return {
                "id": subscription_id,
                "event": "agent_status_changed",
                "payload": {"agent": "Agent One", "status": "blocked"},
            }

    client = IdleThenEventClient()
    backend._read_event_stream(client, "sub-1")

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "blocked"
    assert snapshot.backend_health[0].status == "healthy"
    assert client.calls == 2


def test_mark_unhealthy_safe_sets_ready_even_when_persist_fails(tmp_path: Path, monkeypatch: Any) -> None:
    backend = _backend(tmp_path, "ready-on-error")

    def boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("store unavailable")

    monkeypatch.setattr("tendwire.backends.herdr_events.save_snapshot", boom)

    try:
        backend._mark_unhealthy_safe("protocol_error")
    except RuntimeError:
        pass

    assert backend.ready is True


def test_protocol_error_health_is_degraded_and_specific(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "protocol-health")

    backend._mark_unhealthy("protocol_error")

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.backend_health[0].status == "degraded"
    assert snapshot.backend_health[0].outcome == "protocol_error"


def test_daemon_starts_socket_backend_only_when_configured(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon-socket.db"
    socket_path = tmp_path / "daemon.sock"
    config = Config(
        host_id="daemon-socket",
        data_dir=tmp_path,
        db_path=db_path,
        socket_path=socket_path,
        herdr_backend="socket",
    )
    calls: list[str] = []

    class FakeBackend:
        def __init__(self, config: Config, stop_event: threading.Event) -> None:
            self.config = config
            self.stop_event = stop_event

        def start(self, *, wait_for_reconcile: bool = True) -> None:
            calls.append(f"start:{wait_for_reconcile}")
            snapshot = project_from_observations(
                self.config,
                workers=[Worker(id="worker-1", name="Worker", status="active")],
                backend_health=[
                    BackendHealth(
                        name="herdr",
                        status="healthy",
                        outcome="healthy_non_empty",
                        counts={"workers": 1},
                    )
                ],
            )
            save_snapshot(Path(self.config.db_path), snapshot)

        def stop(self) -> None:
            calls.append("stop")

    def observe_cli(_config: Config) -> Any:
        raise AssertionError("CLI observation must not run in socket mode")

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            observe_initial_snapshot=observe_cli,
            event_backend_factory=lambda cfg, stop_event: FakeBackend(cfg, stop_event),
        ),
    )
    daemon.start()
    try:
        assert calls == ["start:True"]
        assert daemon.get_snapshot().workers[0].id == "worker-1"
        _assert_no_public_json_forbidden(daemon.get_health())
    finally:
        daemon.stop()

    assert "stop" in calls


def test_daemon_socket_fallback_uses_backend_health_when_snapshot_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon-fallback.db"
    socket_path = tmp_path / "daemon-fallback.sock"
    config = Config(
        host_id="daemon-fallback",
        data_dir=tmp_path,
        db_path=db_path,
        socket_path=socket_path,
        herdr_backend="socket",
    )

    class FakeBackend:
        def __init__(self, config: Config, stop_event: threading.Event) -> None:
            self.config = config
            self.stop_event = stop_event
            self.health = HerdrEventBackend(config)._health_for("protocol_error")

        def start(self, *, wait_for_reconcile: bool = True) -> None:
            return None

        def stop(self) -> None:
            return None

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(event_backend_factory=lambda cfg, stop_event: FakeBackend(cfg, stop_event)),
    )
    daemon.start()
    try:
        snapshot = daemon.get_snapshot()
        assert snapshot.backend_health[0].status == "degraded"
        assert snapshot.backend_health[0].outcome == "protocol_error"
    finally:
        daemon.stop()
