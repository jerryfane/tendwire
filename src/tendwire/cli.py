"""Tendwire command-line interface.

Console script entry point: tendwire = tendwire.cli:main
Module entry point: python -m tendwire.cli snapshot --json
"""

from __future__ import annotations

import argparse
import sys

from .backends.herdr_cli import fetch_herdr_state
from .config import Config, load_config
from .core.projector import project_from_observations


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

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
