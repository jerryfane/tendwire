#!/usr/bin/env python3
"""Run read-only Tendwire commands from a Herdr-managed source checkout."""

from __future__ import annotations

import json
import sys
from pathlib import Path


_COMMANDS = {
    "doctor": ["doctor", "--json"],
    "snapshot": ["snapshot", "--json"],
    "turns": ["turns", "--schema-version", "2", "--json"],
    "pending": ["pending", "--json"],
}


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1 or args[0] not in _COMMANDS:
        print("usage: herdr_plugin.py <doctor|snapshot|turns|pending>", file=sys.stderr)
        return 2
    if sys.version_info < (3, 13):
        print(
            json.dumps(
                {
                    "ok": False,
                    "status": "unsupported_python",
                    "required": "Python 3.13 or newer",
                },
                sort_keys=True,
            )
        )
        return 1

    project_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(project_root / "src"))
    from tendwire.cli import main as tendwire_main

    return int(tendwire_main(_COMMANDS[args[0]]) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
