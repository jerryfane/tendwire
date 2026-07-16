"""Semantic connector answers for current backend-owned Claude decisions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tendwire.backends.herdr_decision import calibrate_decision_steps
from tendwire.backends.herdr_turns import _pending_observation_from_turn
from tendwire.command_submission import submit_command
from tendwire.core.commands import (
    STATUS_ACCEPTED,
    STATUS_DECISION_NOT_PENDING,
    STATUS_INVALID_SELECTION,
    STATUS_UNKNOWN_WORKER,
    STATUS_UNSUPPORTED_DECISION,
)
from tendwire.core.models import Worker
from tendwire.store.sqlite import (
    apply_backend_pending_observation,
    pending_payload_from_store,
)

from tests.test_command_submission import _binding, _config, _factory, _seed


def _decision_turn(
    *,
    prompt: str = "Choose a database",
    kind: str = "AskUserQuestion",
    multi_select: bool = False,
    question_count: int = 1,
) -> dict[str, Any]:
    return {
        "pending_decision": {
            "decision_id": "private-tool-use",
            "kind": kind,
            "question": prompt,
            "options": [
                {"id": "postgres", "label": "Postgres"},
                {"id": "sqlite", "label": "SQLite"},
                {"id": "duckdb", "label": "DuckDB"},
                {"id": "mysql", "label": "MySQL"},
            ],
            "multi_select": multi_select,
            "question_count": question_count,
        }
    }


def _seed_pending_decision(
    tmp_path: Path,
    *,
    turn: dict[str, Any] | None = None,
) -> tuple[Any, Worker, str]:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    binding = _binding(
        worker,
        private_fingerprint="decision-binding-private",
        turn_target_value="decision-pane-private",
    )
    _seed(config, [worker], [binding])
    assert config.db_path is not None
    observation = _pending_observation_from_turn(turn or _decision_turn())
    assert apply_backend_pending_observation(
        config.db_path,
        config.host_id,
        worker.id,
        observation,
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    payload = pending_payload_from_store(config.db_path, config.host_id)
    row = next(item for item in payload["pending_interactions"] if item["worker_id"] == worker.id)
    return config, worker, row["meta"]["decision"]["decision_ref"]


def _answer_request(
    decision_ref: str,
    *,
    request_id: str = "decision-request-1",
    worker_id: str = "w-1",
    selection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "answer_decision",
        "request_id": request_id,
        "target": {"worker_id": worker_id},
        "params": {
            "decision_ref": decision_ref,
            "selection": selection or {"option_refs": ["2"]},
        },
    }


def test_pending_payload_carries_stable_structured_decision_and_rotates_ref(
    tmp_path: Path,
) -> None:
    config, worker, first_ref = _seed_pending_decision(tmp_path)
    assert config.db_path is not None
    first = pending_payload_from_store(config.db_path, config.host_id)
    first_row = next(item for item in first["pending_interactions"] if item["worker_id"] == worker.id)
    assert first_row["meta"]["decision"] == {
        "decision_ref": first_ref,
        "kind": "single",
        "prompt": "Choose a database",
        "options": [
            {"ref": "1", "label": "Postgres"},
            {"ref": "2", "label": "SQLite"},
            {"ref": "3", "label": "DuckDB"},
            {"ref": "4", "label": "MySQL"},
        ],
        "multi_select": False,
        "question_count": 1,
    }

    binding = _binding(
        worker,
        private_fingerprint="decision-binding-private",
        turn_target_value="decision-pane-private",
    )
    changed = _pending_observation_from_turn(
        _decision_turn(prompt="Choose a durable database")
    )
    assert apply_backend_pending_observation(
        config.db_path,
        config.host_id,
        worker.id,
        changed,
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    second = pending_payload_from_store(config.db_path, config.host_id)
    second_row = next(item for item in second["pending_interactions"] if item["worker_id"] == worker.id)
    assert second_row["meta"]["decision"]["decision_ref"] != first_ref


def test_answer_decision_stale_ref_fails_before_pane_io(tmp_path: Path) -> None:
    config, _worker, _decision_ref = _seed_pending_decision(tmp_path)
    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request("decision-stale"),
        socket_client_factory=_factory(calls),
    )
    assert result.ok is False
    assert result.status == STATUS_DECISION_NOT_PENDING
    assert calls == []


def test_answer_decision_unknown_worker_fails_before_pane_io(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request("decision-any", worker_id="worker-missing"),
        socket_client_factory=_factory(calls),
    )
    assert result.ok is False
    assert result.status == STATUS_UNKNOWN_WORKER
    assert calls == []


@pytest.mark.parametrize(
    "selection",
    [
        {"option_refs": ["9"]},
        {"option_refs": ["1", "2"]},
        {"option_refs": ["2", "2"]},
        {"text": ""},
        {"option_refs": ["1"], "text": "both"},
    ],
)
def test_answer_decision_invalid_selection_fails_before_pane_io(
    tmp_path: Path,
    selection: dict[str, Any],
) -> None:
    config, _worker, decision_ref = _seed_pending_decision(tmp_path)
    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request(decision_ref, selection=selection),
        socket_client_factory=_factory(calls),
    )
    assert result.ok is False
    assert result.status == STATUS_INVALID_SELECTION
    assert calls == []


def test_answer_decision_refuses_multi_question_before_pane_io(tmp_path: Path) -> None:
    config, _worker, decision_ref = _seed_pending_decision(
        tmp_path,
        turn=_decision_turn(question_count=2),
    )
    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request(decision_ref),
        socket_client_factory=_factory(calls),
    )
    assert result.ok is False
    assert result.status == STATUS_UNSUPPORTED_DECISION
    assert calls == []


def test_answer_decision_rejects_plan_write_in_before_pane_io(tmp_path: Path) -> None:
    config, _worker, decision_ref = _seed_pending_decision(
        tmp_path,
        turn=_decision_turn(kind="ExitPlanMode"),
    )
    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request(decision_ref, selection={"text": "Revise this plan"}),
        socket_client_factory=_factory(calls),
    )
    assert result.ok is False
    assert result.status == STATUS_INVALID_SELECTION
    assert calls == []


def test_decision_calibration_steps_cover_single_plan_write_in_and_multi() -> None:
    single = calibrate_decision_steps(
        kind="single", option_count=4, option_refs=("2",)
    )
    assert [(item.operation, item.keys, item.text) for item in single] == [
        ("keys", ("2", "Enter"), None)
    ]

    plan = calibrate_decision_steps(
        kind="plan", option_count=2, option_refs=("1",)
    )
    assert [(item.operation, item.keys, item.text) for item in plan] == [
        ("keys", ("1", "Enter"), None)
    ]

    write_in = calibrate_decision_steps(
        kind="single", option_count=4, text="Use another database"
    )
    assert [(item.operation, item.keys, item.text) for item in write_in] == [
        ("keys", ("5",), None),
        ("input", ("Enter",), "Use another database"),
    ]

    multi = calibrate_decision_steps(
        kind="multi", option_count=4, option_refs=("3", "1")
    )
    assert [(item.operation, item.keys, item.text) for item in multi] == [
        ("keys", ("Enter",), None),
        ("keys", ("Down", "Down", "Enter"), None),
        ("keys", ("Down", "Down", "Enter"), None),
    ]


def test_answer_decision_request_id_replay_does_not_resend_keys(tmp_path: Path) -> None:
    config, _worker, decision_ref = _seed_pending_decision(tmp_path)
    calls: list[dict[str, Any]] = []
    request = _answer_request(decision_ref)

    first = submit_command(config, request, socket_client_factory=_factory(calls))
    replay = submit_command(config, request, socket_client_factory=_factory(calls))

    assert first.ok is True
    assert first.status == STATUS_ACCEPTED
    assert replay.to_dict() == first.to_dict()
    assert calls == [
        {
            "method": "pane.send_keys",
            "params": {
                "pane_id": "decision-pane-private",
                "keys": ["2", "Enter"],
            },
        }
    ]
