"""Tendwire command-line interface.

Console script entry point: tendwire = tendwire.cli:main
Module entry point: python -m tendwire.cli snapshot --json
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .backends.herdr_cli import (
    bindings_from_workers,
    diagnose_herdr,
    fetch_herdr_command_observation,
    fetch_herdr_snapshot_observation,
    fetch_herdr_state,
    herdr_backend_health,
    rehydrate_workers_from_bindings,
)
from .backends.herdr_command import send_instruction as herdr_send_instruction
from .config import Config, load_config
from .core.actions import CommandContext, execute_command
from .core.commands import (
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_DUPLICATE_REQUEST,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_PENDING,
    CommandEnvelope,
    error_value,
    parse_command_request,
    validate_request,
)
from .core.projector import project_from_observations
from .core.models import BackendHealth, WorkerBinding, separate_duplicate_worker_bindings, utc_timestamp
from .core.turns import (
    payload_to_json,
    pending_payload_from_snapshot,
    turns_payload_from_snapshot,
)
from .store.sqlite import (
    envelope_to_receipt_json,
    expire_stale_worker_bindings,
    list_worker_bindings,
    reserve_command_receipt,
    save_command_receipt,
    upsert_worker_bindings,
)


_HERDR_BACKEND = "herdr"
_DEFAULT_FETCH_HERDR_STATE = fetch_herdr_state


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tendwire",
        description="Local-first control plane for terminal-based agents.",
    )
    parser.add_argument(
        "--host-id",
        dest="host_id",
        default=None,
        help="Override the host identifier used in snapshots.",
    )
    parser.add_argument(
        "--herdr-bin",
        dest="herdr_bin",
        default=None,
        help="Path or name of the herdr binary (default: herdr).",
    )
    parser.add_argument(
        "--herdr-timeout",
        dest="herdr_timeout_seconds",
        default=None,
        help="Seconds to wait for each Herdr CLI probe (default: 5.0).",
    )

    subparsers = parser.add_subparsers(dest="command")

    snapshot_parser = subparsers.add_parser(
        "snapshot",
        help="Print a neutral device-independent snapshot.",
    )
    snapshot_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print snapshot as JSON (default).",
    )
    snapshot_parser.add_argument(
        "--store",
        dest="store_snapshot",
        action="store_true",
        default=False,
        help="Persist the snapshot to the sqlite store without changing stdout.",
    )
    snapshot_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path to use with --store (default: config path).",
    )

    turns_parser = subparsers.add_parser(
        "turns",
        help="Print neutral public turns derived from the current snapshot.",
    )
    turns_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print turns as JSON (default).",
    )

    pending_parser = subparsers.add_parser(
        "pending",
        help="Print neutral public pending interactions derived from the current snapshot.",
    )
    pending_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print pending interactions as JSON (default).",
    )

    command_parser = subparsers.add_parser(
        "command",
        help="Read a JSON command request from stdin and print a JSON result.",
    )
    command_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print result as JSON (default).",
    )
    command_parser.add_argument(
        "--db-path",
        dest="db_path",
        default=None,
        help="SQLite database path for command receipts (default: config path).",
    )

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Print read-only Herdr diagnostics.",
    )
    doctor_parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        default=True,
        help="Print diagnostics as JSON (default).",
    )

    return parser


def _load_worker_bindings(config: Config) -> list[WorkerBinding]:
    if config.db_path is None:
        return []
    return list_worker_bindings(
        config.db_path,
        config.host_id,
        backend=_HERDR_BACKEND,
    )


def _fetch_state_with_bindings(
    config: Config,
    stored_bindings: list[WorkerBinding],
) -> tuple[list[Any], list[Any], list[WorkerBinding]]:
    try:
        result = fetch_herdr_state(
            config,
            stored_bindings=stored_bindings,
            include_bindings=True,
        )
    except TypeError:
        spaces, workers = fetch_herdr_state(config)
        return spaces, workers, bindings_from_workers(config, workers)

    if len(result) == 3:
        spaces, workers, bindings = result
        return spaces, workers, bindings
    spaces, workers = result
    return spaces, workers, bindings_from_workers(config, workers)


def _legacy_backend_health(spaces: list[Any], workers: list[Any]) -> list[BackendHealth]:
    return [
        herdr_backend_health(
            "healthy_non_empty" if spaces or workers else "unknown",
            spaces=spaces,
            workers=workers,
        )
    ]


def _fetch_snapshot_observation_with_bindings(
    config: Config,
    stored_bindings: list[WorkerBinding],
) -> tuple[list[Any], list[Any], list[WorkerBinding], list[BackendHealth]]:
    if fetch_herdr_state is not _DEFAULT_FETCH_HERDR_STATE:
        spaces, workers, bindings = _fetch_state_with_bindings(config, stored_bindings)
        return spaces, workers, bindings, _legacy_backend_health(spaces, workers)
    try:
        observation = fetch_herdr_snapshot_observation(
            config,
            stored_bindings=stored_bindings,
        )
    except TypeError:
        spaces, workers, bindings = _fetch_state_with_bindings(config, stored_bindings)
        return spaces, workers, bindings, _legacy_backend_health(spaces, workers)

    spaces = list(getattr(observation, "spaces", []) or [])
    workers = list(getattr(observation, "workers", []) or [])
    bindings = list(getattr(observation, "bindings", []) or [])
    backend_health = list(getattr(observation, "backend_health", []) or [])
    if not backend_health:
        backend_health = _legacy_backend_health(spaces, workers)
    return spaces, workers, bindings, backend_health


def _fetch_command_observation_with_bindings(
    config: Config,
    stored_bindings: list[WorkerBinding],
) -> Any:
    try:
        observation = fetch_herdr_command_observation(config, stored_bindings=stored_bindings)
    except TypeError:
        observation = fetch_herdr_command_observation(config)
    bindings = getattr(observation, "bindings", None)
    if bindings is None:
        object.__setattr__(observation, "bindings", bindings_from_workers(config, observation.workers))
    elif not bindings:
        object.__setattr__(observation, "bindings", bindings_from_workers(config, observation.workers))
    backend_health = getattr(observation, "backend_health", None)
    if backend_health is None or not backend_health:
        object.__setattr__(
            observation,
            "backend_health",
            [
                herdr_backend_health(
                    getattr(observation, "outcome", "unknown"),
                    message=getattr(observation, "message", "") or None,
                    spaces=getattr(observation, "spaces", []) or [],
                    workers=getattr(observation, "workers", []) or [],
                )
            ],
        )
    return observation


def _herdr_health_from_items(items: list[BackendHealth]) -> BackendHealth:
    for item in items:
        if getattr(item, "name", "") == _HERDR_BACKEND:
            return item
    return herdr_backend_health("unknown")


def _observation_health(observation: Any) -> BackendHealth:
    health = getattr(observation, "health", None)
    if isinstance(health, BackendHealth):
        return health
    items = getattr(observation, "backend_health", None)
    if items:
        return _herdr_health_from_items(list(items))
    return herdr_backend_health(
        getattr(observation, "outcome", "unknown"),
        message=getattr(observation, "message", "") or None,
        spaces=getattr(observation, "spaces", []) or [],
        workers=getattr(observation, "workers", []) or [],
    )


def _health_observed_at(backend_health: list[BackendHealth]) -> str:
    health = _herdr_health_from_items(backend_health)
    return health.observed_at or utc_timestamp()


def _current_public_snapshot(config: Config) -> Any:
    spaces, workers, _bindings, backend_health = _fetch_snapshot_observation_with_bindings(
        config,
        [],
    )
    return project_from_observations(
        config,
        spaces=spaces,
        workers=workers,
        backend_health=backend_health,
    )


def _persist_binding_observation(
    config: Config,
    bindings: list[WorkerBinding],
    *,
    observed_at: str,
    workers_present: bool,
    authoritative: bool = True,
) -> list[WorkerBinding]:
    bindings = separate_duplicate_worker_bindings(bindings)
    if config.db_path is None:
        return bindings
    if bindings:
        upsert_worker_bindings(config.db_path, bindings)
    if authoritative and (bindings or not workers_present):
        expire_stale_worker_bindings(
            config.db_path,
            config.host_id,
            backend=_HERDR_BACKEND,
            current_private_fingerprints=[binding.private_fingerprint for binding in bindings],
            now=observed_at,
        )
    return bindings


def cmd_snapshot(
    config: Config,
    *,
    json_output: bool = True,
    store_snapshot: bool = False,
) -> int:
    """Build and print a neutral snapshot."""
    stored_bindings = _load_worker_bindings(config) if store_snapshot else []
    spaces, workers, bindings, backend_health = _fetch_snapshot_observation_with_bindings(
        config,
        stored_bindings,
    )
    snapshot = project_from_observations(
        config,
        spaces=spaces,
        workers=workers,
        backend_health=backend_health,
    )

    if store_snapshot:
        from .store.sqlite import save_snapshot

        if config.db_path is None:
            raise RuntimeError("snapshot persistence requires a db path")
        save_snapshot(config.db_path, snapshot)
        bindings = _persist_binding_observation(
            config,
            bindings,
            observed_at=snapshot.updated_at,
            workers_present=bool(workers),
            authoritative=_herdr_health_from_items(backend_health).status == "healthy",
        )

    if json_output:
        print(snapshot.to_json(indent=2))
    else:
        # Non-JSON output is out of scope; reject cleanly.
        print("error: only --json output is supported", file=sys.stderr)
        return 2

    return 0


def cmd_turns(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Build and print neutral public turns."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    snapshot = _current_public_snapshot(config)
    print(payload_to_json(turns_payload_from_snapshot(snapshot), indent=2))
    return 0


def cmd_pending(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Build and print neutral public pending interactions."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    snapshot = _current_public_snapshot(config)
    print(payload_to_json(pending_payload_from_snapshot(snapshot), indent=2))
    return 0


def cmd_doctor(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Run read-only backend diagnostics and print a JSON result."""
    if not json_output:
        print("error: only --json output is supported", file=sys.stderr)
        return 2
    payload = diagnose_herdr(config)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("status") == "ok" else 1


def _envelope_from_receipt(request: Any, receipt: dict[str, Any]) -> CommandEnvelope:
    if receipt is None:
        return None
    if receipt["payload_fingerprint"] != request.payload_fingerprint():
        return CommandEnvelope.error(
            request,
            error_value(
                STATUS_DUPLICATE_REQUEST,
                "request_id reused with a different payload",
            ),
        )
    if receipt["uncertain"]:
        return CommandEnvelope.error(
            request,
            error_value(
                STATUS_REQUEST_STATE_UNCERTAIN,
                "previous request state is uncertain; not retrying mutation",
            ),
        )
    cached = CommandEnvelope.from_dict(json.loads(receipt["result_json"]))
    return cached


def _reserve_command_receipt(config: Config, request: Any) -> CommandEnvelope | None:
    """Reserve a mutating command key, or return an existing receipt envelope."""
    from .core.commands import CommandRequest

    if not isinstance(request, CommandRequest):
        return None
    if request.action != "send_instruction" or request.dry_run or not request.request_id:
        return None
    if config.db_path is None:
        return None
    pending = CommandEnvelope.error(
        request,
        error_value(
            STATUS_REQUEST_STATE_UNCERTAIN,
            "request is pending backend mutation",
        ),
    )
    reservation = reserve_command_receipt(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id,
        action=request.action,
        payload_fingerprint=request.payload_fingerprint(),
        pending_result_json=envelope_to_receipt_json(pending),
        status=STATUS_PENDING,
    )
    if reservation["reserved"]:
        return None
    return _envelope_from_receipt(request, reservation["receipt"])


def _save_command_receipt(config: Config, request: Any, envelope: CommandEnvelope) -> None:
    from .core.commands import CommandRequest

    if not isinstance(request, CommandRequest):
        return
    if config.db_path is None:
        return
    if request.action != "send_instruction" or request.dry_run or not request.request_id:
        return
    uncertain = envelope.status in {
        STATUS_REQUEST_STATE_UNCERTAIN,
        STATUS_BACKEND_UNAVAILABLE,
    }
    save_command_receipt(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id,
        action=request.action,
        payload_fingerprint=request.payload_fingerprint(),
        status=envelope.status,
        result_json=envelope_to_receipt_json(envelope),
        uncertain=uncertain,
    )


def _command_observation_error(request: Any, observation: Any) -> CommandEnvelope | None:
    from .core.commands import CommandRequest

    if not isinstance(request, CommandRequest):
        return None
    if request.action != "send_instruction" or request.dry_run:
        return None
    health = _observation_health(observation)
    if getattr(observation, "healthy", False) and health.status == "healthy":
        return None
    message = health.message or getattr(observation, "message", "")
    if health.status == "unavailable" or getattr(observation, "status", "") == "unavailable":
        return CommandEnvelope.error(
            request,
            error_value(
                STATUS_BACKEND_UNAVAILABLE,
                message or "Herdr backend is unavailable",
            ),
        )
    return CommandEnvelope.error(
        request,
        error_value(
            STATUS_REQUEST_STATE_UNCERTAIN,
            message or "Herdr observation state is uncertain",
        ),
    )


def cmd_command(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Read a JSON command request from stdin and print a JSON result envelope."""
    payload = sys.stdin.read()
    request, parse_error = parse_command_request(payload)
    if parse_error is not None:
        envelope = CommandEnvelope.error(None, parse_error or error_value("invalid_request", "unknown parse error"))
        if request is not None:
            envelope = CommandEnvelope.error(request, parse_error)
        print(envelope.to_json(indent=2))
        return 1

    validation_error = validate_request(request)
    if validation_error is not None:
        envelope = CommandEnvelope.error(request, validation_error)
        print(envelope.to_json(indent=2))
        return 1

    if request.action == "noop":
        envelope = execute_command(
            request,
            CommandContext(host_id=config.host_id, workers=[]),
        )
        print(envelope.to_json(indent=2))
        return 0 if envelope.ok else 1

    receipt_envelope = _reserve_command_receipt(config, request)
    if receipt_envelope is not None:
        print(receipt_envelope.to_json(indent=2))
        return 0 if receipt_envelope.ok else 1

    stored_bindings: list[WorkerBinding] = []
    if request.action == "send_instruction" and not request.dry_run:
        stored_bindings = _load_worker_bindings(config)
        observation = _fetch_command_observation_with_bindings(config, stored_bindings)
        observation_error = _command_observation_error(request, observation)
        if observation_error is not None:
            _save_command_receipt(config, request, observation_error)
            print(observation_error.to_json(indent=2))
            return 0 if observation_error.ok else 1
        backend_health = list(getattr(observation, "backend_health", []) or [])
        spaces, workers = observation.spaces, observation.workers
        current_bindings = list(getattr(observation, "bindings", []) or [])
        current_bindings = _persist_binding_observation(
            config,
            current_bindings,
            observed_at=current_bindings[0].observed_at if current_bindings else _health_observed_at(backend_health),
            workers_present=bool(workers),
            authoritative=_observation_health(observation).status == "healthy",
        )
        stored_after_refresh = _load_worker_bindings(config)
        workers = rehydrate_workers_from_bindings(
            workers,
            current_bindings,
            stored_after_refresh or stored_bindings,
        )
    else:
        stored_bindings = _load_worker_bindings(config)
        spaces, workers, current_bindings, backend_health = _fetch_snapshot_observation_with_bindings(
            config,
            stored_bindings,
        )
        workers = rehydrate_workers_from_bindings(
            workers,
            current_bindings,
            stored_bindings,
        )
    snapshot = project_from_observations(
        config,
        spaces=spaces,
        workers=workers,
        backend_health=backend_health,
    )

    def backend_sender(target: dict[str, Any], instruction: dict[str, Any]) -> CommandEnvelope:
        return herdr_send_instruction(config, target, instruction)

    context = CommandContext(
        host_id=config.host_id,
        workers=workers,
        snapshot=snapshot,
        backend_sender=backend_sender,
    )

    envelope = execute_command(request, context)
    _save_command_receipt(config, request, envelope)

    print(envelope.to_json(indent=2))
    return 0 if envelope.ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    config = load_config(
        host_id=args.host_id,
        herdr_bin=args.herdr_bin,
        db_path=getattr(args, "db_path", None),
        herdr_timeout_seconds=args.herdr_timeout_seconds,
    )

    if args.command == "snapshot":
        return cmd_snapshot(
            config,
            json_output=args.json_output,
            store_snapshot=args.store_snapshot,
        )

    if args.command == "turns":
        return cmd_turns(config, json_output=args.json_output)

    if args.command == "pending":
        return cmd_pending(config, json_output=args.json_output)

    if args.command == "command":
        return cmd_command(config, json_output=args.json_output)

    if args.command == "doctor":
        return cmd_doctor(config, json_output=args.json_output)

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
