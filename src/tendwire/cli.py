"""Tendwire command-line interface.

Console script entry point: tendwire = tendwire.cli:main
Module entry point: python -m tendwire.cli snapshot --json
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .backends.herdr_cli import fetch_herdr_state
from .backends.herdr_command import send_instruction as herdr_send_instruction
from .config import Config, load_config
from .core.actions import CommandContext, execute_command
from .core.commands import (
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_DUPLICATE_REQUEST,
    STATUS_REQUEST_STATE_UNCERTAIN,
    CommandEnvelope,
    error_value,
    parse_command_request,
)
from .core.projector import project_from_observations
from .store.sqlite import (
    envelope_to_receipt_json,
    get_command_receipt,
    save_command_receipt,
)


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

    return parser


def cmd_snapshot(
    config: Config,
    *,
    json_output: bool = True,
    store_snapshot: bool = False,
) -> int:
    """Build and print a neutral snapshot."""
    spaces, workers = fetch_herdr_state(config)
    snapshot = project_from_observations(
        config,
        spaces=spaces,
        workers=workers,
    )

    if store_snapshot:
        from .store.sqlite import save_snapshot

        if config.db_path is None:
            raise RuntimeError("snapshot persistence requires a db path")
        save_snapshot(config.db_path, snapshot)

    if json_output:
        print(snapshot.to_json(indent=2))
    else:
        # Non-JSON output is out of scope; reject cleanly.
        print("error: only --json output is supported", file=sys.stderr)
        return 2

    return 0


def _check_command_receipt(config: Config, request: Any) -> CommandEnvelope | None:
    """Return a cached/uncertain/duplicate envelope, or None if no receipt applies."""
    from .core.commands import CommandRequest

    if not isinstance(request, CommandRequest):
        return None
    if request.action != "send_instruction" or request.dry_run or not request.request_id:
        return None
    if config.db_path is None:
        return None
    receipt = get_command_receipt(
        config.db_path,
        config.host_id,
        request.request_id,
        request.action,
    )
    if receipt is None:
        return None
    if receipt["uncertain"]:
        return CommandEnvelope.error(
            request,
            error_value(
                STATUS_REQUEST_STATE_UNCERTAIN,
                "previous request state is uncertain; not retrying mutation",
            ),
        )
    if receipt["payload_fingerprint"] == request.payload_fingerprint():
        cached = CommandEnvelope.from_dict(json.loads(receipt["result_json"]))
        return cached
    return CommandEnvelope.error(
        request,
        error_value(
            STATUS_DUPLICATE_REQUEST,
            "request_id reused with a different payload",
        ),
    )


def _save_command_receipt(config: Config, request: Any, envelope: CommandEnvelope) -> None:
    from .core.commands import CommandRequest

    if not isinstance(request, CommandRequest):
        return
    if config.db_path is None:
        return
    if request.action != "send_instruction" or request.dry_run or not request.request_id:
        return
    uncertain = envelope.status in {
        STATUS_BACKEND_FAILED,
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


def cmd_command(
    config: Config,
    *,
    json_output: bool = True,
) -> int:
    """Read a JSON command request from stdin and print a JSON result envelope."""
    payload = sys.stdin.read()
    request, parse_error = parse_command_request(payload)
    if request is None:
        envelope = CommandEnvelope.error(None, parse_error or error_value("invalid_request", "unknown parse error"))
        print(envelope.to_json(indent=2))
        return 1

    receipt_envelope = _check_command_receipt(config, request)
    if receipt_envelope is not None:
        print(receipt_envelope.to_json(indent=2))
        return 0 if receipt_envelope.ok else 1

    spaces, workers = fetch_herdr_state(config)
    snapshot = project_from_observations(
        config,
        spaces=spaces,
        workers=workers,
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
    )

    if args.command == "snapshot":
        return cmd_snapshot(
            config,
            json_output=args.json_output,
            store_snapshot=args.store_snapshot,
        )

    if args.command == "command":
        return cmd_command(config, json_output=args.json_output)

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
