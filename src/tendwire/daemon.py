"""Long-running Tendwire daemon lifecycle skeleton."""

from __future__ import annotations

import json
import signal
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import Config
from .core.commands import CommandEnvelope
from .core.models import Snapshot, sanitize_public_mapping, utc_timestamp
from .daemon_api import TendwireDaemonAPI, UnixSocketJSONServer
from .local_state import repair_config_state


def _valid_observation_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return value if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def _nonnegative_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return max(0, value)


def _nonnegative_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    converted = float(value)
    if converted < 0 or converted != converted or converted in {float("inf"), float("-inf")}:
        return None
    return converted


def _turn_ingestion_health(config: Config, scheduler: Any | None) -> dict[str, Any]:
    raw: Mapping[str, Any] = {}
    if scheduler is not None:
        try:
            status_value = scheduler.operational_status()
        except Exception:
            status_value = {}
        if isinstance(status_value, Mapping):
            raw = status_value
    status = raw.get("status")
    if status not in {"healthy", "stale", "degraded", "stopping"}:
        status = "stale" if scheduler is None else "degraded"
    return {
        "status": status,
        "queue": _nonnegative_int(raw.get("queue_depth")),
        "active": _nonnegative_int(raw.get("active")),
        "refreshed": _nonnegative_int(raw.get("refreshed")),
        "failed": _nonnegative_int(raw.get("failed")),
        "timed_out": _nonnegative_int(raw.get("timed_out")),
        "coalesced": _nonnegative_int(raw.get("coalesced")),
        "queue_full": _nonnegative_int(raw.get("queue_full")),
        "last_success": _valid_observation_timestamp(
            raw.get("last_success") if isinstance(raw.get("last_success"), str) else None
        ),
        "last_duration_ms": _nonnegative_float(raw.get("last_duration_ms")),
        "stale_age": _nonnegative_float(raw.get("stale_age_seconds")),
        "bounds": {
            "refresh_interval_seconds": config.turn_refresh_interval_seconds,
            "max_workers": config.turn_refresh_workers,
            "queue_capacity": _nonnegative_int(raw.get("queue_capacity")),
            "adapter_timeout_seconds": config.herdr_timeout_seconds,
        },
    }


def _pending_ingestion_health(config: Config) -> dict[str, Any]:
    """Return the fixed durable pending aggregate without exposing row identity."""
    unavailable = {
        "status": "store_unavailable",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    raw: Mapping[str, Any] = unavailable
    if config.db_path is not None:
        try:
            from .store.sqlite import backend_pending_health

            value = backend_pending_health(Path(config.db_path), config.host_id)
        except Exception:
            value = unavailable
        if isinstance(value, Mapping):
            raw = value
    raw_counts = raw.get("counts")
    status = raw.get("status")
    count_values = (
        tuple(raw_counts.get(key) for key in ("fresh", "stale", "total"))
        if isinstance(raw_counts, Mapping)
        else ()
    )
    valid_counts = len(count_values) == 3 and all(
        isinstance(value, int) and not isinstance(value, bool) and value >= 0
        for value in count_values
    )
    if (
        status not in {"healthy", "degraded", "store_unavailable"}
        or not valid_counts
        or count_values[2] != count_values[0] + count_values[1]
        or (status == "healthy" and count_values[1] != 0)
        or (status == "degraded" and count_values[1] == 0)
        or (status == "store_unavailable" and count_values != (0, 0, 0))
    ):
        status = "store_unavailable"
        counts = dict(unavailable["counts"])
    else:
        counts = dict(zip(("fresh", "stale", "total"), count_values, strict=True))
    return {
        "status": status,
        "counts": counts,
        "bounds": {
            "stale_grace_seconds": config.pending_stale_grace_seconds,
        },
    }


def default_socket_path(config: Config) -> Path:
    """Return the daemon socket path for this config."""
    if config.socket_path is not None:
        return Path(config.socket_path)
    return Path(config.data_dir) / "tendwire.sock"


def _default_init_store(db_path: Path) -> None:
    from .store.sqlite import init_store

    init_store(db_path)


def _default_observe_initial_snapshot(config: Config) -> Snapshot:
    from .cli import observe_public_snapshot

    return observe_public_snapshot(config, store_snapshot=True)


def _default_submit_command(config: Config, payload: str) -> CommandEnvelope:
    from .command_submission import submit_command

    return submit_command(config, payload)


def _default_turn_scheduler_factory(config: Config) -> Any:
    from .backends.herdr_turns import TurnIngestionScheduler

    return TurnIngestionScheduler(config)


@dataclass(frozen=True)
class DaemonHooks:
    """Dependency injection points for deterministic daemon tests."""

    init_store: Callable[[Path], None] = _default_init_store
    observe_initial_snapshot: Callable[[Config], Snapshot] = _default_observe_initial_snapshot
    submit_command: Callable[[Config, str], CommandEnvelope | Mapping[str, Any]] = _default_submit_command
    event_backend_factory: Callable[[Config, threading.Event], Any] | None = None
    turn_scheduler_factory: Callable[[Config], Any] = _default_turn_scheduler_factory


class TendwireDaemon:
    """Owns store initialization, initial observation, API dispatch, and shutdown."""

    def __init__(
        self,
        config: Config,
        *,
        socket_path: str | Path | None = None,
        hooks: DaemonHooks | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self.config = config
        self.socket_path = Path(socket_path) if socket_path is not None else default_socket_path(config)
        self._prepare_socket_parent = socket_path is None and config.socket_path is None
        self.hooks = hooks or DaemonHooks()
        self.stop_event = stop_event or threading.Event()
        self.started_at = utc_timestamp()
        self._snapshot: Snapshot | None = None
        self._server: UnixSocketJSONServer | None = None
        self._event_backend: Any | None = None
        self._turn_scheduler: Any | None = None
        self._stop_lock = threading.Lock()
        self._automatic_maintenance_status: dict[str, Any] | None = None

    @property
    def snapshot(self) -> Snapshot | None:
        return self._snapshot

    @property
    def server(self) -> UnixSocketJSONServer | None:
        return self._server

    def start(self) -> None:
        if self._server is not None and self._server.listening:
            return
        if self.stop_event.is_set():
            raise RuntimeError("daemon cannot start after shutdown")
        if self.config.db_path is None:
            raise RuntimeError("daemon requires a sqlite db path")

        server: UnixSocketJSONServer | None = None
        try:
            repair_config_state(
                self.config.data_dir,
                self.config.db_path,
                private_files=(
                    self.config.installation_key_path,
                    self.config.installation_key_marker_path,
                    self.config.installation_key_sentinel_path,
                ),
            )
            self.hooks.init_store(Path(self.config.db_path))
            if self.config.herdr_backend == "socket":
                self._snapshot = self._start_socket_event_backend()
            else:
                self._snapshot = self.hooks.observe_initial_snapshot(self.config)
                self._after_snapshot_saved()

            scheduler = self.hooks.turn_scheduler_factory(self.config)
            self._turn_scheduler = scheduler
            backend = self._event_backend
            callback_setter = (
                getattr(backend, "set_turn_refresh_callback", None)
                if backend is not None
                else None
            )
            if callable(callback_setter):
                callback_setter(scheduler.request_refresh)
            scheduler.start()
            scheduler.request_refresh()

            api = TendwireDaemonAPI(
                get_snapshot=self.get_snapshot,
                get_health=self.get_health,
                submit_command=self.submit_command,
                get_attention=self.get_attention,
                get_turns=self.get_turns,
                get_turn_content=self.get_turn_content,
                get_pending=self.get_pending,
                connector_call=self.connector_call,
            )
            server = UnixSocketJSONServer(
                self.socket_path,
                api.dispatch,
                stop_event=self.stop_event,
                socket_group=self.config.socket_group,
                prepare_parent=self._prepare_socket_parent,
            )
            self._server = server
            server.start()
        except Exception:
            self.stop_event.set()
            backend = self._event_backend
            callback_setter = (
                getattr(backend, "set_turn_refresh_callback", None)
                if backend is not None
                else None
            )
            if callable(callback_setter):
                try:
                    callback_setter(None)
                except Exception:
                    pass
            scheduler = self._turn_scheduler
            self._turn_scheduler = None
            if scheduler is not None:
                try:
                    scheduler.stop(
                        flush_timeout_seconds=self.config.herdr_timeout_seconds + 1.0
                    )
                except Exception:
                    pass
            self._event_backend = None
            if backend is not None:
                try:
                    backend.stop()
                except Exception:
                    pass
            self._server = None
            if server is not None:
                try:
                    server.close()
                except Exception:
                    pass
            self._snapshot = None
            raise

    def serve_forever(self) -> None:
        if self._server is None:
            self.start()
        server = self._server
        if server is None:
            raise RuntimeError("daemon server did not start")
        server.serve_forever()

    def stop(self) -> None:
        with self._stop_lock:
            self.stop_event.set()
            server = self._server
            backend = self._event_backend
            scheduler = self._turn_scheduler
            self._server = None
            self._event_backend = None
            self._turn_scheduler = None

        if server is not None:
            try:
                server.close()
            except Exception:
                pass

        if backend is not None:
            flush = getattr(backend, "flush", None)
            if callable(flush):
                try:
                    flush()
                except Exception:
                    pass
            callback_setter = getattr(backend, "set_turn_refresh_callback", None)
            if callable(callback_setter):
                try:
                    callback_setter(None)
                except Exception:
                    pass

        if scheduler is not None:
            try:
                scheduler.stop(
                    flush_timeout_seconds=self.config.herdr_timeout_seconds + 1.0
                )
            except Exception:
                pass

        if backend is not None:
            try:
                backend.stop()
            except Exception:
                pass

    def _after_snapshot_saved(self) -> None:
        if self.config.db_path is None:
            return
        from .store.sqlite import (
            SnapshotRetentionPolicy,
            maybe_run_automatic_store_maintenance,
        )

        policy = SnapshotRetentionPolicy(
            retention_days=self.config.snapshot_retention_days,
            retention_count=self.config.snapshot_retention_count,
            batch_size=self.config.snapshot_maintenance_batch_size,
        )
        try:
            result = maybe_run_automatic_store_maintenance(
                Path(self.config.db_path),
                policy=policy,
                cadence_seconds=self.config.store_maintenance_cadence_seconds,
            )
            snapshot_result = result.get("snapshot")
            snapshot_counts = snapshot_result if isinstance(snapshot_result, Mapping) else {}
            maintenance_status = {
                "ok": bool(result.get("ok")),
                "status": str(result.get("status") or "unknown"),
                "due": bool(result.get("due")),
                "examined": int(snapshot_counts.get("examined") or 0),
                "deleted": int(snapshot_counts.get("deleted") or 0),
                "remaining_candidates": bool(snapshot_counts.get("remaining_candidates")),
            }
        except Exception:
            self._automatic_maintenance_status = {
                "ok": False,
                "status": "failed",
                "due": False,
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
            }
        else:
            self._automatic_maintenance_status = maintenance_status

    def _start_socket_event_backend(self) -> Snapshot:
        if self.hooks.event_backend_factory is None:
            from .backends.herdr_events import HerdrEventBackend

            backend = HerdrEventBackend(self.config, stop_event=self.stop_event)
        else:
            backend = self.hooks.event_backend_factory(self.config, self.stop_event)
        self._event_backend = backend
        backend.start(wait_for_reconcile=True)
        from .store.sqlite import SnapshotObservationContext, latest_snapshot, save_snapshot

        snapshot = latest_snapshot(Path(self.config.db_path), self.config.host_id)
        if snapshot is not None:
            return snapshot
        from .backends.herdr_cli import herdr_backend_health
        from .core.projector import project_from_observations

        backend_health = (
            backend.health.to_backend_health()
            if hasattr(backend, "health")
            else herdr_backend_health("unknown")
        )
        snapshot = project_from_observations(
            self.config,
            backend_health=[backend_health],
        )
        save_snapshot(
            Path(self.config.db_path),
            snapshot,
            observation=SnapshotObservationContext(
                authority="none",
                observed_at=_valid_observation_timestamp(backend_health.observed_at),
            ),
        )
        self._after_snapshot_saved()
        return snapshot

    def get_snapshot(self) -> Snapshot:
        if self.config.db_path is not None:
            from .store.sqlite import latest_snapshot

            snapshot = latest_snapshot(Path(self.config.db_path), self.config.host_id)
            if snapshot is not None:
                self._snapshot = snapshot
                return snapshot
        if self._snapshot is not None:
            return self._snapshot
        raise RuntimeError("daemon has no initial snapshot")

    def get_health(self) -> dict[str, Any]:
        snapshot = self.get_snapshot()
        store_payload: dict[str, Any] = {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": self.config.host_id,
            "counts": {},
            "outbox": {"pending": 0, "leased": 0, "terminal": 0, "by_status": {}},
            "maintenance": {
                "last_completed_at": None,
                "status": "not_initialized",
                "snapshot_count": 0,
                "snapshot_retention_days": self.config.snapshot_retention_days,
                "snapshot_retention_count": self.config.snapshot_retention_count,
                "maintenance_batch_size": self.config.snapshot_maintenance_batch_size,
                "maintenance_cadence_seconds": self.config.store_maintenance_cadence_seconds,
                "backlog": False,
            },
        }
        if self.config.db_path is not None:
            from .store.sqlite import store_status

            store_payload = store_status(
                Path(self.config.db_path),
                self.config.host_id,
                snapshot_retention_days=self.config.snapshot_retention_days,
                snapshot_retention_count=self.config.snapshot_retention_count,
                maintenance_batch_size=self.config.snapshot_maintenance_batch_size,
                maintenance_cadence_seconds=self.config.store_maintenance_cadence_seconds,
            )
        store_ok = bool(store_payload.get("ok"))
        backend_runtime: dict[str, Any] = {}
        if self._event_backend is not None and hasattr(self._event_backend, "operational_status"):
            status_value = getattr(self._event_backend, "operational_status")
            if isinstance(status_value, Mapping):
                backend_runtime = dict(status_value)
        backend_maintenance = backend_runtime.get("automatic_maintenance")
        runtime_maintenance = (
            backend_maintenance
            if isinstance(backend_maintenance, Mapping)
            else self._automatic_maintenance_status
        )
        maintenance_payload = store_payload.get("maintenance")
        maintenance = (
            dict(maintenance_payload)
            if isinstance(maintenance_payload, Mapping)
            else {}
        )
        if runtime_maintenance is not None:
            maintenance["last_check"] = dict(runtime_maintenance)
        maintenance_degraded = maintenance.get("status") == "failed" or (
            runtime_maintenance is not None
            and not bool(runtime_maintenance.get("ok"))
        )
        pending_ingestion = _pending_ingestion_health(self.config)
        last_event_at = backend_runtime.get("last_event_at") or store_payload.get("last_event_at")
        last_snapshot_at = backend_runtime.get("last_snapshot_at") or store_payload.get("last_snapshot_at") or snapshot.updated_at
        payload = {
            "schema_version": 1,
            "status": (
                "ok"
                if store_ok
                and not maintenance_degraded
                and pending_ingestion["status"] == "healthy"
                else "degraded"
            ),
            "host_id": self.config.host_id,
            "daemon": {
                "status": "healthy",
                "started_at": self.started_at,
            },
            "store": {
                "status": (
                    "unavailable"
                    if not store_ok
                    else "degraded"
                    if maintenance_degraded
                    else "healthy"
                ),
                "counts": store_payload.get("counts", {}),
                "outbox": store_payload.get("outbox", {}),
                "last_event_at": store_payload.get("last_event_at"),
                "last_snapshot_at": store_payload.get("last_snapshot_at"),
                "maintenance": maintenance,
            },
            "snapshot": {
                "updated_at": snapshot.updated_at,
                "content_fingerprint": snapshot.content_fingerprint,
            },
            "timestamps": {
                "last_snapshot_at": last_snapshot_at,
                "last_event_at": last_event_at,
                "last_reconcile_at": backend_runtime.get("last_reconcile_at"),
            },
            "backend": {
                "status": backend_runtime.get("status"),
                "outcome": backend_runtime.get("outcome"),
                "ready": backend_runtime.get("ready"),
                "running": backend_runtime.get("running"),
                "reconcile_enabled": backend_runtime.get(
                    "reconcile_enabled",
                    self.config.reconcile_interval_seconds > 0,
                ),
            },
            "turn_ingestion": _turn_ingestion_health(
                self.config,
                self._turn_scheduler,
            ),
            "pending_ingestion": pending_ingestion,
            "limits": {
                "event_debounce_seconds": self.config.event_debounce_seconds,
                "reconcile_interval_seconds": self.config.reconcile_interval_seconds,
                "event_retention_days": self.config.event_retention_days,
                "output_excerpt_chars": self.config.output_excerpt_chars,
                "max_workers": self.config.max_workers,
                "max_outbox_attempts": self.config.max_outbox_attempts,
                "outbox_claim_ttl_seconds": self.config.connector_claim_ttl_seconds,
                "snapshot_retention_days": self.config.snapshot_retention_days,
                "snapshot_retention_count": self.config.snapshot_retention_count,
                "snapshot_maintenance_batch_size": self.config.snapshot_maintenance_batch_size,
                "store_maintenance_cadence_seconds": self.config.store_maintenance_cadence_seconds,
            },
            "backend_health": [health.to_dict() for health in snapshot.backend_health],
        }
        return sanitize_public_mapping(payload)

    def get_attention(self) -> Mapping[str, Any]:
        if self.config.db_path is not None:
            from .store.sqlite import attention_payload_from_store

            payload = attention_payload_from_store(
                Path(self.config.db_path),
                self.config.host_id,
            )
            if payload is not None:
                return payload
        from .core.attention import attention_payload_from_snapshot

        return attention_payload_from_snapshot(self.get_snapshot())

    def get_pending(self) -> Mapping[str, Any]:
        """Return the durable pending projection shared with the CLI fallback."""
        from .store.sqlite import pending_payload_from_store

        return pending_payload_from_store(
            Path(self.config.db_path),
            self.config.host_id,
        )

    def get_turns(
        self,
        *,
        schema_version: int = 1,
        limit: int = 100,
        cursor: str | None = None,
        since: str | None = None,
    ) -> Mapping[str, Any]:
        if self.config.db_path is None:
            return {
                "schema_version": schema_version,
                "host_id": self.config.host_id,
                "ok": False,
                "status": "store_unavailable",
            }
        from .store.sqlite import turns_payload_from_store

        return turns_payload_from_store(
            Path(self.config.db_path),
            self.config.host_id,
            snapshot=self.get_snapshot(),
            schema_version=schema_version,
            limit=limit,
            cursor=cursor,
            since=since,
        )

    def get_turn_content(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        """turn.content.get: return one immutable bounded canonical page."""
        if self.config.db_path is None:
            return {
                "schema_version": 1,
                "ok": False,
                "status": "store_unavailable",
                "error": {
                    "code": "store_unavailable",
                    "message": "daemon requires a sqlite db path for this method",
                },
            }
        from .store.sqlite import get_turn_content

        return get_turn_content(
            Path(self.config.db_path),
            self.config.host_id,
            turn_id=params.get("turn_id"),
            content_revision=params.get("content_revision"),
            field=params.get("field"),
            cursor=params.get("cursor"),
            schema_version=params.get("schema_version", 1),
        )

    def connector_call(self, method: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        if self.config.db_path is None:
            return {
                "schema_version": 1,
                "ok": False,
                "status": "store_unavailable",
                "host_id": self.config.host_id,
                "name": str(params.get("name") or ""),
                "error": {
                    "code": "store_unavailable",
                    "message": "daemon requires a sqlite db path for this method",
                },
            }
        from .connectors import ConnectorOutboxAPI

        return ConnectorOutboxAPI(
            Path(self.config.db_path),
            self.config.host_id,
            default_lease_seconds=self.config.connector_claim_ttl_seconds,
            max_attempts=self.config.max_outbox_attempts,
        ).dispatch(method, params)

    def submit_command(self, params: Mapping[str, Any]) -> CommandEnvelope | Mapping[str, Any]:
        # Preserve the submitted keys exactly so the existing command parser can
        # reject private/connector fields instead of receiving sanitized input.
        payload = json.dumps(
            dict(params),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return self.hooks.submit_command(self.config, payload)


def run_daemon(
    config: Config,
    *,
    socket_path: str | Path | None = None,
    hooks: DaemonHooks | None = None,
    install_signal_handlers: bool = True,
) -> int:
    """Run the daemon until SIGINT, SIGTERM, or an injected stop event."""
    daemon = TendwireDaemon(config, socket_path=socket_path, hooks=hooks)
    previous_handlers: dict[int, Any] = {}

    def _handle_stop(_signum: int, _frame: Any) -> None:
        daemon.stop()

    if install_signal_handlers:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, _handle_stop)

    try:
        daemon.start()
        daemon.serve_forever()
        return 0
    except KeyboardInterrupt:
        daemon.stop()
        return 0
    finally:
        daemon.stop()
        if install_signal_handlers:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
