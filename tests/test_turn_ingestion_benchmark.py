from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "turn_ingestion_benchmark.py"


def _load_driver() -> ModuleType:
    spec = importlib.util.spec_from_file_location("turn_ingestion_benchmark", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _invoke(*arguments: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    return subprocess.run(
        [sys.executable, str(SCRIPT), *arguments],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        env=environment,
        check=False,
        timeout=30,
    )


def _single_object(completed: subprocess.CompletedProcess[str]) -> dict[str, object]:
    assert completed.stderr == ""
    lines = completed.stdout.splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert isinstance(payload, dict)
    assert lines[0] == json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return payload


def test_pending_validator_requires_fixed_durable_health_shape() -> None:
    driver = _load_driver()
    result = {
        "schema_version": 1,
        "host_id": "benchmark-validator-host",
        "pending_interactions": [],
        "backend_health": [],
        "pending_health": {
            "status": "healthy",
            "counts": {"fresh": 0, "stale": 0, "total": 0},
        },
    }
    result["content_fingerprint"] = (
        driver.recompute_pending_content_fingerprint(result)
    )
    valid = {"ok": True, "result": result}

    driver._validate_pending(valid, 2)
    for invalid in (
        {**valid, "ok": False},
        {
            "ok": True,
            "result": {
                **valid["result"],
                "pending_health": {
                    "status": "store_unavailable",
                    "counts": {"fresh": 0, "stale": 0, "total": 0},
                },
            },
        },
        {
            "ok": True,
            "result": {
                **valid["result"],
                "pending_health": {
                    "status": "healthy",
                    "counts": {"fresh": 1, "stale": 0, "total": 0},
                },
            },
        },
        {
            "ok": True,
            "result": {
                **valid["result"],
                "content_fingerprint": "0" * 24,
            },
        },
    ):
        with pytest.raises(RuntimeError, match="pending_list_contract_failed"):
            driver._validate_pending(invalid, 2)


@pytest.mark.skipif(
    not sys.platform.startswith("linux") or not Path("/dev/shm").is_dir(),
    reason="benchmark contract requires Linux tmpfs and Unix sockets",
)
def test_tiny_run_measures_production_pending_without_source_or_turn_reads() -> None:
    completed = _invoke(
        "--workers",
        "2",
        "--blocked-workers",
        "2",
        "--blocked-seconds",
        "0.1",
        "--warmups",
        "0",
        "--samples",
        "1",
        "--json",
    )

    assert completed.returncode == 0
    report = _single_object(completed)
    assert report["ok"] is True
    assert report["status"] == "completed"
    assert report["latency_ns"]["pending_list"]["samples"] == 1
    assert (
        report["latency_ns"]["pending_list"]["documented_host_budget_ns"]
        == 350_000_000
    )
    assert report["latency_ns"]["pending_list"]["documented_host_budget_met"] is True
    assert report["checks"]["production_pending_handler_measured"] is True
    assert report["checks"]["production_event_callback_bound"] is True
    assert report["checks"]["pending_list_started_no_turn_reads"] is True
    assert report["checks"]["pending_list_started_no_source_reads"] is True
    assert report["checks"]["pending_list_store_rows_unchanged"] is True
    assert report["checks"]["cached_requests_started_no_source_reads"] is True
    assert report["checks"]["independent_pending_discovered"] is True
    assert report["checks"]["independent_pending_cleared"] is True
    assert report["checks"]["independent_pending_zero_turn_calls"] is True
    assert (
        report["checks"]["independent_pending_discovery_fingerprint_changed"]
        is True
    )
    assert (
        report["checks"]["independent_pending_unchanged_fingerprint_stable"]
        is True
    )
    assert report["checks"]["independent_pending_clear_fingerprint_changed"] is True
    assert report["checks"]["independent_pending_clear_restored_baseline"] is True
    assert report["checks"]["no_duplicate_pending_rows"] is True
    assert report["checks"]["independent_pending_health_coherent"] is True
    assert (
        report["checks"]["independent_pending_rows_coherent_after_clear"] is True
    )
    assert report["ingestion"]["turn_list_calls_during_pending_measurement"] == 0
    assert report["ingestion"]["independent_turn_list_calls"] == 0
    assert report["ingestion"]["independent_prompt_count"] == 2
    assert report["ingestion"]["independent_clear_count"] == 0
    assert (
        report["transport"]["method_dispatches"]["pending.list"]
        == 1 + report["ingestion"]["independent_pending_polls"]
    )
