"""Boundary tests: core modules must not import Telegram/Herdres/backend code."""

from __future__ import annotations

import subprocess
import sys


_CORE_MODULE_NAMES = (
    "tendwire.core.models",
    "tendwire.core.projector",
    "tendwire.core.attention",
)

_FORBIDDEN_PREFIXES = ("telegram", "herdres")
_FORBIDDEN_TENDWIRE_MODULES = ("tendwire.backends.herdr_cli", "tendwire.store.sqlite")


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


def test_core_modules_do_not_load_telegram_or_herdres() -> None:
    for module_name in _CORE_MODULE_NAMES:
        loaded = _loaded_modules_after_import(module_name)
        for name in loaded:
            lower = name.lower()
            if lower.startswith(_FORBIDDEN_PREFIXES):
                raise AssertionError(
                    f"{module_name} transitively loads forbidden module {name}"
                )


def test_core_modules_do_not_load_backend_or_store_connectors() -> None:
    for module_name in _CORE_MODULE_NAMES:
        loaded = _loaded_modules_after_import(module_name)
        for forbidden in _FORBIDDEN_TENDWIRE_MODULES:
            if forbidden in loaded:
                raise AssertionError(
                    f"{module_name} transitively loads connector module {forbidden}"
                )
