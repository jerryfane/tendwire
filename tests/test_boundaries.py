"""Boundary tests: core modules must not load connector/runtime code."""

from __future__ import annotations

import subprocess
import sys


_CORE_MODULE_NAMES = (
    "tendwire.core.models",
    "tendwire.core.projector",
    "tendwire.core.attention",
    "tendwire.core.commands",
    "tendwire.core.actions",
)

_FORBIDDEN_PREFIXES = (
    "telegram",
    "herdres",
    "tendwire.backends",
    "tendwire.store",
    "tendwire.connectors",
    "tendwire.routing",
    "tendwire.delivery",
)
_FORBIDDEN_EXACT = {"subprocess"}


def _loaded_modules_after_import(module_name: str) -> set[str]:
    """Return sys.modules keys after importing a single module in isolation."""
    import os

    code = f"""
import sys
before = set(sys.modules.keys())
import {module_name}
after = set(sys.modules.keys())
for name in sorted(after - before):
    print(name)
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), "..", "src")
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"Failed to inspect imports for {module_name}: {result.stderr}"
        )
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def test_core_modules_do_not_load_connector_or_process_modules() -> None:
    for module_name in _CORE_MODULE_NAMES:
        loaded = _loaded_modules_after_import(module_name)
        for name in loaded:
            lower = name.lower()
            if name in _FORBIDDEN_EXACT or lower.startswith(_FORBIDDEN_PREFIXES):
                raise AssertionError(
                    f"{module_name} transitively loads forbidden module {name}"
                )
