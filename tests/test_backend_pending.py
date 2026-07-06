"""Backend-provided pending prompts: a REAL pane prompt (question + choices, captured by the herdres
pending hook through the turn adapter) flows adapter -> herdr_turns -> backend_pending -> pending.list,
superseding the worker's synthetic attention-derived row."""
from __future__ import annotations

from pathlib import Path

from tendwire.backends.herdr_turns import _backend_pending_from_turn, _pop_backend_pending
from tendwire.store.sqlite import init_store, list_backend_pending, merge_backend_pending


def _decision_turn() -> dict:
    return {
        "available": True,
        "complete": False,
        "awaiting_input": True,
        "user_text": "which db?",
        "pending_decision": {
            "decision_id": "toolu_123",
            "prompt": "Which database should we use?",
            "mode": "buttons",
            "options": [
                {"id": "1", "label": "Postgres", "send_text": "Postgres"},
                {"id": "2", "label": "SQLite", "send_text": "SQLite"},
                {"id": "custom", "label": "Tell me differently", "send_text": ""},
            ],
        },
    }


def test_extract_pending_decision():
    pending = _backend_pending_from_turn(_decision_turn())
    assert pending["question"] == "Which database should we use?"
    assert pending["kind"] == "question"
    assert [c["label"] for c in pending["choices"]] == ["Postgres", "SQLite", "Tell me differently"]
    assert pending["choices"][0]["value"] == "Postgres"
    assert pending["meta"]["decision_id"] == "toolu_123"


def test_extract_plan_approval_kind():
    turn = {"pending_decision": {"decision_id": "t", "prompt": "Approve this plan?",
                                 "options": [{"id": "approve", "label": "Approve", "send_text": "1"},
                                             {"id": "revise", "label": "Revise", "send_text": ""}]}}
    assert _backend_pending_from_turn(turn)["kind"] == "approval"


def test_extract_none_without_pending():
    assert _backend_pending_from_turn({"complete": True, "assistant_final_text": "done"}) is None


def test_pop_backend_pending_splits():
    content, pending = _pop_backend_pending({"user_text": "hi", "_backend_pending": {"question": "q"}})
    assert content == {"user_text": "hi"} and pending == {"question": "q"}
    content, pending = _pop_backend_pending({"_backend_pending": {"question": "q"}})
    assert content is None and pending == {"question": "q"}


def test_merge_and_prune_backend_pending(tmp_path: Path):
    db = tmp_path / "p.db"
    init_store(db)
    pending = {"question": "Q?", "kind": "question", "choices": [], "meta": {"source": "backend"}}
    assert merge_backend_pending(db, "h", "w1", pending) is True
    assert merge_backend_pending(db, "h", "w1", pending) is False       # unchanged -> no write
    assert list_backend_pending(db, "h") == {"w1": pending}
    assert merge_backend_pending(db, "h", "w1", None) is True           # answered -> pruned
    assert list_backend_pending(db, "h") == {}
