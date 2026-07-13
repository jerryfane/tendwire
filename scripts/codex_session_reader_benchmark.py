#!/usr/bin/env python3
"""Deterministic synthetic benchmark for the private Codex session reader.

Run from a source checkout with ``PYTHONPATH=src``. The benchmark creates a
private memory-backed 20,000-file Codex fixture, prints one compact aggregate
JSON object, and never reports generated session identities or paths. Timing
ceilings are broad documented-host evidence gates; bounded work is the stable
contract.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from time import perf_counter_ns
from typing import Any
from uuid import UUID

from tendwire.backends import herdr_turns

REPORT_SCHEMA_VERSION = 1
FIXTURE_FILE_COUNT = 20_000
FIXTURE_SPARSE_BYTES = 20 * 1024 * 1024
FIXTURE_DATE = "2026-07-03"
TARGET_ORDINAL = 10_000
COLD_LOOKUP_BUDGET_NS = 30_000_000_000
WARM_LOOKUP_BUDGET_NS = 1_000_000_000
COLD_PARSE_BUDGET_NS = 2_000_000_000
INCREMENTAL_POLL_BUDGET_NS = 1_000_000_000
UNCHANGED_POLL_BUDGET_NS = 1_000_000_000
_UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.ASCII,
)


class _ArgumentError(Exception):
    pass


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise _ArgumentError(message)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _event(kind: str, turn_id: str, **extra: Any) -> dict[str, Any]:
    return {
        "type": "event_msg",
        "payload": {"type": kind, "turn_id": turn_id, **extra},
    }


def _message(
    turn_id: str,
    role: str,
    text: str,
    *,
    phase: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "type": "message",
        "role": role,
        "content": [{"type": "output_text", "text": text}],
        "internal_chat_message_metadata_passthrough": {"turn_id": turn_id},
    }
    if phase is not None:
        payload["phase"] = phase
    return {"type": "response_item", "payload": payload}


def _jsonl(*records: Mapping[str, Any]) -> bytes:
    return b"".join(
        _canonical_json(record).encode("utf-8") + b"\n" for record in records
    )


def _rollout_name(session_id: str) -> str:
    return f"rollout-{FIXTURE_DATE}T00-00-00-{session_id}.jsonl"


def _create_private_file(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    os.close(descriptor)


def _create_fixture(home: Path) -> tuple[Path, str, str, str, bytes, bytes]:
    sessions = home / "sessions"
    year = sessions / "2026"
    month = year / "07"
    day = month / "03"
    for directory in (home, sessions, year, month, day):
        directory.mkdir(mode=0o700)

    target_id = str(UUID(int=TARGET_ORDINAL))
    for ordinal in range(1, FIXTURE_FILE_COUNT + 1):
        session_id = str(UUID(int=ordinal))
        _create_private_file(day / _rollout_name(session_id))

    turn_id = "synthetic-benchmark-turn"
    user_text = "synthetic benchmark prompt"
    stream_text = "synthetic benchmark incremental output"
    tail = _jsonl(
        _event("task_started", turn_id),
        _message(turn_id, "user", user_text),
    )
    append = _jsonl(
        _message(turn_id, "assistant", stream_text, phase="commentary")
    )
    target = day / _rollout_name(target_id)
    with target.open("r+b", buffering=0) as handle:
        handle.seek(FIXTURE_SPARSE_BYTES)
        handle.write(b"\n")
        handle.write(tail)
    return target, target_id, turn_id, user_text, stream_text, append


def _reset_codex_state() -> None:
    with herdr_turns._CODEX_PATH_CACHE_LOCK:
        herdr_turns._CODEX_PATH_CACHE.clear()
        herdr_turns._CODEX_INDEX_GENERATION = None
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        herdr_turns._CODEX_SESSION_CACHE.clear()
        herdr_turns._CODEX_SESSION_CACHE_LIVE_KEYS = None
        herdr_turns._CODEX_SESSION_CACHE_BINDING_GENERATIONS = {}
        herdr_turns._CODEX_SESSION_CACHE_BINDING_FINGERPRINTS = {}


def _timed(call: Any) -> tuple[Any, int]:
    started = perf_counter_ns()
    value = call()
    return value, perf_counter_ns() - started


def _contains_private_value(value: Any, forbidden: tuple[str, ...]) -> bool:
    if isinstance(value, Mapping):
        return any(
            _contains_private_value(key, forbidden)
            or _contains_private_value(item, forbidden)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_private_value(item, forbidden) for item in value)
    if not isinstance(value, str):
        return False
    return bool(_UUID_PATTERN.search(value)) or any(
        private and private in value for private in forbidden
    )


def _command_text() -> str:
    return "PYTHONPATH=src python3 scripts/codex_session_reader_benchmark.py --json"


def _benchmark() -> dict[str, Any]:
    benchmark_started = perf_counter_ns()
    load_average = tuple(round(value, 2) for value in os.getloadavg())
    temporary_path: Path | None = None
    index_observations: list[int] = []
    read_observations: list[int] = []
    prior_home = os.environ.get("CODEX_HOME")
    prior_index_observer = herdr_turns._CODEX_INDEX_BUILD_OBSERVER
    prior_read_observer = herdr_turns._CODEX_ISOLATED_READ_OBSERVER
    report: dict[str, Any] | None = None
    forbidden: tuple[str, ...] = ()

    try:
        with tempfile.TemporaryDirectory(
            prefix="tendwire-codex-reader-benchmark-",
            dir="/dev/shm",
        ) as raw_root:
            temporary_path = Path(raw_root)
            root_private = temporary_path.stat().st_mode & 0o777 == 0o700
            home = temporary_path / "codex-home"
            (
                target,
                target_id,
                turn_id,
                user_text,
                stream_text,
                append,
            ) = _create_fixture(home)
            forbidden = (
                raw_root,
                os.fspath(home),
                os.fspath(target),
                target_id,
                turn_id,
                user_text,
                stream_text,
                target.name,
            )
            logical_file_bytes = target.stat().st_size

            os.environ["CODEX_HOME"] = os.fspath(home)
            _reset_codex_state()
            herdr_turns._CODEX_INDEX_BUILD_OBSERVER = index_observations.append
            herdr_turns._CODEX_ISOLATED_READ_OBSERVER = read_observations.append

            wildcard_result, wildcard_ns = _timed(
                lambda: herdr_turns._find_codex_session_file("*")
            )
            builds_after_wildcard = len(index_observations)

            cold_result, cold_lookup_ns = _timed(
                lambda: herdr_turns._find_codex_session_file(target_id)
            )
            builds_after_cold_lookup = len(index_observations)
            warm_result, warm_lookup_ns = _timed(
                lambda: herdr_turns._find_codex_session_file(target_id)
            )
            builds_after_warm_lookup = len(index_observations)

            with herdr_turns._CODEX_PATH_CACHE_LOCK:
                index = herdr_turns._CODEX_INDEX_GENERATION
                if index is None:
                    raise RuntimeError("index_generation_missing")
                indexed_sessions = len(index.entries)
                retained_index_bytes = index.retained_bytes
                index_overflowed = index.overflowed
                generation_visited = index.visited

            cold_content, cold_parse_ns = _timed(
                lambda: herdr_turns._read_codex_session_turn(target_id)
            )
            with target.open("ab", buffering=0) as handle:
                handle.write(append)
            incremental_content, incremental_poll_ns = _timed(
                lambda: herdr_turns._read_codex_session_turn(target_id)
            )
            unchanged_content, unchanged_poll_ns = _timed(
                lambda: herdr_turns._read_codex_session_turn(target_id)
            )

            if len(read_observations) != 3:
                raise RuntimeError("read_observation_count_mismatch")
            cold_parse_bytes, incremental_poll_bytes, unchanged_poll_bytes = (
                read_observations
            )
            builds_after_all_reads = len(index_observations)
            observed_index_visits = sum(index_observations)

            checks = {
                "append_sized_second_poll": incremental_poll_bytes == len(append),
                "cold_lookup_budget_met": cold_lookup_ns <= COLD_LOOKUP_BUDGET_NS,
                "cold_parse_budget_met": cold_parse_ns <= COLD_PARSE_BUDGET_NS,
                "cold_parse_bounded": cold_parse_bytes
                <= herdr_turns._CODEX_RESYNC_INITIAL_BYTES,
                "exact_resolution": cold_result == target.resolve()
                and warm_result == target.resolve(),
                "fixture_file_count_exact": indexed_sessions == FIXTURE_FILE_COUNT,
                "incremental_content_observed": isinstance(incremental_content, Mapping)
                and incremental_content.get("assistant_stream_text") == stream_text,
                "incremental_poll_budget_met": incremental_poll_ns
                <= INCREMENTAL_POLL_BUDGET_NS,
                "index_build_bounded": observed_index_visits
                <= herdr_turns._CODEX_INDEX_MAX_VISITS,
                "index_generation_complete": not index_overflowed,
                "one_index_build": builds_after_cold_lookup == 1
                and builds_after_all_reads == 1,
                "private_temporary_directory": root_private,
                "sparse_large_fixture": logical_file_bytes > FIXTURE_SPARSE_BYTES,
                "unchanged_poll_budget_met": unchanged_poll_ns
                <= UNCHANGED_POLL_BUDGET_NS,
                "unchanged_poll_no_read": unchanged_content is None
                and unchanged_poll_bytes == 0,
                "warm_lookup_budget_met": warm_lookup_ns <= WARM_LOOKUP_BUDGET_NS,
                "warm_lookup_no_walk": builds_after_warm_lookup
                == builds_after_cold_lookup,
                "wildcard_no_match": wildcard_result is None,
                "wildcard_no_walk": builds_after_wildcard == 0,
                "cold_content_observed": isinstance(cold_content, Mapping)
                and cold_content.get("user_text") == user_text,
            }

            report = {
                "schema_version": REPORT_SCHEMA_VERSION,
                "ok": False,
                "status": "validating",
                "command": _command_text(),
                "parameters": {
                    "fixture_files": FIXTURE_FILE_COUNT,
                    "sparse_prefix_bytes": FIXTURE_SPARSE_BYTES,
                },
                "environment": {
                    "architecture": platform.machine(),
                    "fixture_storage": "memory_backed_tmpfs",
                    "load_average_1m_5m_15m": list(load_average),
                    "logical_cpus": os.cpu_count(),
                    "operating_system": platform.system(),
                    "platform": platform.platform(),
                    "platform_release": platform.release(),
                    "python_version": platform.python_version(),
                    "source_checkout_pythonpath": "src",
                    "timer": "perf_counter_ns",
                },
                "fixture": {
                    "file_count": FIXTURE_FILE_COUNT,
                    "logical_session_file_bytes_before_append": logical_file_bytes,
                    "tail_bytes": logical_file_bytes - FIXTURE_SPARSE_BYTES - 1,
                    "append_bytes": len(append),
                },
                "latency_ns": {
                    "wildcard_probe": wildcard_ns,
                    "cold_lookup": {
                        "elapsed_ns": cold_lookup_ns,
                        "documented_host_budget_ns": COLD_LOOKUP_BUDGET_NS,
                        "documented_host_budget_met": checks[
                            "cold_lookup_budget_met"
                        ],
                    },
                    "warm_lookup": {
                        "elapsed_ns": warm_lookup_ns,
                        "documented_host_budget_ns": WARM_LOOKUP_BUDGET_NS,
                        "documented_host_budget_met": checks[
                            "warm_lookup_budget_met"
                        ],
                    },
                    "cold_parse": {
                        "elapsed_ns": cold_parse_ns,
                        "documented_host_budget_ns": COLD_PARSE_BUDGET_NS,
                        "documented_host_budget_met": checks[
                            "cold_parse_budget_met"
                        ],
                    },
                    "incremental_poll": {
                        "elapsed_ns": incremental_poll_ns,
                        "documented_host_budget_ns": INCREMENTAL_POLL_BUDGET_NS,
                        "documented_host_budget_met": checks[
                            "incremental_poll_budget_met"
                        ],
                    },
                    "unchanged_poll": {
                        "elapsed_ns": unchanged_poll_ns,
                        "documented_host_budget_ns": UNCHANGED_POLL_BUDGET_NS,
                        "documented_host_budget_met": checks[
                            "unchanged_poll_budget_met"
                        ],
                    },
                },
                "bounded_work": {
                    "index_builds": builds_after_all_reads,
                    "filesystem_entries_visited": observed_index_visits,
                    "filesystem_entry_visit_bound": herdr_turns._CODEX_INDEX_MAX_VISITS,
                    "generation_entries_visited": generation_visited,
                    "indexed_sessions": indexed_sessions,
                    "retained_index_bytes": retained_index_bytes,
                    "retained_index_byte_bound": herdr_turns._CODEX_INDEX_MAX_BYTES,
                    "wildcard_index_builds": builds_after_wildcard,
                    "warm_lookup_additional_index_builds": builds_after_warm_lookup
                    - builds_after_cold_lookup,
                    "cold_parse_bytes_read": cold_parse_bytes,
                    "cold_parse_byte_bound": herdr_turns._CODEX_RESYNC_INITIAL_BYTES,
                    "incremental_poll_bytes_read": incremental_poll_bytes,
                    "incremental_append_bytes": len(append),
                    "unchanged_poll_bytes_read": unchanged_poll_bytes,
                },
                "checks": checks,
            }
    finally:
        herdr_turns._CODEX_INDEX_BUILD_OBSERVER = prior_index_observer
        herdr_turns._CODEX_ISOLATED_READ_OBSERVER = prior_read_observer
        _reset_codex_state()
        if prior_home is None:
            os.environ.pop("CODEX_HOME", None)
        else:
            os.environ["CODEX_HOME"] = prior_home

    if report is None:
        raise RuntimeError("report_not_created")
    report["checks"]["temporary_artifacts_removed"] = bool(
        temporary_path is not None and not temporary_path.exists()
    )
    if _contains_private_value(report, forbidden):
        raise RuntimeError("privacy_scan_failed")
    report["checks"]["privacy_scan_passed"] = True
    failed = sorted(
        name for name, passed in report["checks"].items() if passed is not True
    )
    if failed:
        raise RuntimeError("benchmark_invariants_failed")
    report["ok"] = True
    report["status"] = "completed"
    report["wall_time_ns"] = perf_counter_ns() - benchmark_started
    return report


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(
        add_help=False,
        description="Run the deterministic synthetic Codex session-reader benchmark.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the aggregate report as one compact JSON object.",
    )
    return parser


def main() -> int:
    try:
        args = _parser().parse_args()
    except _ArgumentError:
        print(
            _canonical_json(
                {
                    "schema_version": REPORT_SCHEMA_VERSION,
                    "ok": False,
                    "status": "invalid_arguments",
                }
            )
        )
        return 2
    if not args.json:
        print(
            _canonical_json(
                {
                    "schema_version": REPORT_SCHEMA_VERSION,
                    "ok": False,
                    "status": "invalid_arguments",
                }
            )
        )
        return 2
    try:
        report = _benchmark()
    except Exception as exc:
        print(
            _canonical_json(
                {
                    "schema_version": REPORT_SCHEMA_VERSION,
                    "ok": False,
                    "status": "benchmark_failed",
                    "error_type": type(exc).__name__,
                }
            )
        )
        return 1
    print(_canonical_json(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
