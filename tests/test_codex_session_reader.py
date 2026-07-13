"""Deterministic contracts for the private Codex session resolver and reader."""

from __future__ import annotations
from dataclasses import replace

import json
import os
from pathlib import Path
from uuid import UUID
from types import SimpleNamespace

import pytest

from tendwire.backends import herdr_turns


SESSION_A = "019f2307-092b-7810-8323-418d7c55bd26"
SESSION_B = "019f2307-092b-7810-8323-418d7c55bd27"
SESSION_C = "11111111-1111-4111-8111-111111111111"


def _event(kind: str, turn_id: str, **extra):
    return {
        "type": "event_msg",
        "payload": {"type": kind, "turn_id": turn_id, **extra},
    }


def _message(turn_id: str, role: str, text: str, *, phase: str | None = None):
    payload = {
        "type": "message",
        "role": role,
        "content": [{"type": "output_text", "text": text}],
        "internal_chat_message_metadata_passthrough": {"turn_id": turn_id},
    }
    if phase is not None:
        payload["phase"] = phase
    return {"type": "response_item", "payload": payload}


def _jsonl(*records, terminate: bool = True) -> bytes:
    body = b"\n".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        for record in records
    )
    return body + (b"\n" if terminate and records else b"")


def _session_path(home: Path, session_id: str = SESSION_A, date: str = "2026-07-03") -> Path:
    year, month, day = date.split("-")
    return (
        home
        / "sessions"
        / year
        / month
        / day
        / f"rollout-{date}T00-00-00-{session_id}.jsonl"
    )


def _write_session(
    home: Path,
    records: tuple[dict, ...] | list[dict],
    *,
    session_id: str = SESSION_A,
    date: str = "2026-07-03",
    terminate: bool = True,
) -> Path:
    path = _session_path(home, session_id, date)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_jsonl(*records, terminate=terminate))
    return path


def _reset_codex() -> None:
    with herdr_turns._CODEX_PATH_CACHE_LOCK:
        herdr_turns._CODEX_PATH_CACHE.clear()
        herdr_turns._CODEX_INDEX_GENERATION = None
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        herdr_turns._CODEX_SESSION_CACHE.clear()
        herdr_turns._CODEX_SESSION_CACHE_LIVE_KEYS = None
        herdr_turns._CODEX_SESSION_CACHE_BINDING_GENERATIONS = {}
        herdr_turns._CODEX_SESSION_CACHE_BINDING_FINGERPRINTS = {}


@pytest.fixture(autouse=True)
def _isolated_codex_state(monkeypatch):
    _reset_codex()
    monkeypatch.setattr(herdr_turns, "_CODEX_INDEX_BUILD_OBSERVER", None)
    monkeypatch.setattr(herdr_turns, "_CODEX_ISOLATED_READ_OBSERVER", None)
    yield
    _reset_codex()


def test_canonical_uuid_rejects_adversarial_spellings_before_filesystem_work(
    tmp_path: Path,
    monkeypatch,
) -> None:
    invalid = [
        "",
        "*",
        "?",
        "[abc]",
        ".",
        "..",
        "/",
        "\\",
        f"../{SESSION_A}",
        f"{SESSION_A}/x",
        f"prefix-{SESSION_A}",
        f"{SESSION_A}-suffix",
        f" {SESSION_A}",
        f"{SESSION_A} ",
        SESSION_A.upper(),
        SESSION_A.replace("-", ""),
        "{" + SESSION_A + "}",
        "urn:uuid:" + SESSION_A,
        SESSION_A.replace("-", "‐"),
        SESSION_A.replace("d", "ԁ"),
        "00000000-0000-0000-0000-000000000000",
        "x" * 10_000,
    ]
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "must-not-touch"))
    monkeypatch.setattr(
        herdr_turns,
        "_build_codex_index",
        lambda *_args: (_ for _ in ()).throw(AssertionError("invalid identity walked")),
    )
    for value in invalid:
        assert herdr_turns._canonical_codex_session_id(value) is None
        assert herdr_turns._find_codex_session_file(value) is None
    assert herdr_turns._canonical_codex_session_id(SESSION_A) == SESSION_A
    assert herdr_turns._canonical_codex_session_id(SESSION_C) == SESSION_C


def test_invalid_identity_is_rejected_before_socket_or_process(monkeypatch) -> None:
    monkeypatch.setattr(
        herdr_turns.socket,
        "socketpair",
        lambda: (_ for _ in ()).throw(AssertionError("socket created")),
    )
    assert (
        herdr_turns._read_file_turn_isolated(
            "codex_session_id",
            "*",
            timeout_seconds=1,
        )
        is None
    )


@pytest.mark.parametrize(
    ("parts", "name", "expected"),
    [
        (("2026", "07", "03"), f"rollout-2026-07-03T00-00-00-{SESSION_A}.jsonl", SESSION_A),
        (("2024", "02", "29"), f"rollout-2024-02-29T23-59-59-{SESSION_C}.jsonl", SESSION_C),
        (("2026", "07", "04"), f"rollout-2026-07-03T00-00-00-{SESSION_A}.jsonl", None),
        (("2026", "02", "30"), f"rollout-2026-02-30T00-00-00-{SESSION_A}.jsonl", None),
        (("2026", "07", "03"), f"rollout-2026-07-03T24-00-00-{SESSION_A}.jsonl", None),
        (("2026", "07", "03"), f"rollout-2026-07-03T00-00-00-{SESSION_A}.JSONL", None),
        (("2026", "07", "03"), f"rollout-2026-07-03T00-00-00.1-{SESSION_A}.jsonl", None),
        (("2026", "07", "03"), f"copy-rollout-2026-07-03T00-00-00-{SESSION_A}.jsonl", None),
        (("2026", "07", "03"), f"rollout-2026-07-03T00-00-00-{SESSION_A}.jsonl.zst", None),
    ],
)
def test_exact_rollout_filename_and_date_grammar(parts, name, expected) -> None:
    assert herdr_turns._codex_rollout_identity(parts, name) == expected


def test_resolver_selects_only_exact_regular_and_rejects_decoys_and_symlinks(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    target = _write_session(home, [_event("task_started", "turn-a")])
    decoy = _write_session(
        home,
        [_event("task_started", "turn-b")],
        session_id=SESSION_B,
    )
    os.utime(decoy, (2_000_000_000, 2_000_000_000))
    target.parent.joinpath(f"prefix-{SESSION_A}.jsonl").write_bytes(b"decoy\n")
    target.parent.joinpath(
        f"rollout-2026-07-03T00-00-00-{SESSION_A}.jsonl.zst"
    ).write_bytes(b"compressed")
    outside = tmp_path / "outside.jsonl"
    outside.write_bytes(b"outside")
    target.parent.joinpath(
        f"rollout-2026-07-03T01-00-00-{SESSION_C}.jsonl"
    ).symlink_to(outside)
    monkeypatch.setenv("CODEX_HOME", str(home))

    assert herdr_turns._find_codex_session_file(SESSION_A) == target.resolve()
    assert herdr_turns._find_codex_session_file(SESSION_B) == decoy.resolve()
    assert herdr_turns._find_codex_session_file(SESSION_C) is None


def test_duplicate_exact_identity_is_ambiguous_independent_of_mtime(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    first = _write_session(home, [], date="2026-07-03")
    second = _write_session(home, [], date="2026-07-04")
    os.utime(first, (2_000_000_000, 2_000_000_000))
    os.utime(second, (1, 1))
    monkeypatch.setenv("CODEX_HOME", str(home))

    resolution = herdr_turns._resolve_codex_session(SESSION_A)
    assert resolution is not None
    assert resolution.status == "ambiguous"
    assert herdr_turns._find_codex_session_file(SESSION_A) is None


def test_complete_20k_index_build_is_bounded_and_deterministic(monkeypatch) -> None:
    root = Path("/virtual/sessions")

    class Entry:
        def __init__(self, path: str, name: str, kind: str):
            self.path = path
            self.name = name
            self.kind = kind

        def is_dir(self, *, follow_symlinks: bool) -> bool:
            assert follow_symlinks is False
            return self.kind == "dir"

        def is_file(self, *, follow_symlinks: bool) -> bool:
            assert follow_symlinks is False
            return self.kind == "file"

    year = Entry(f"{root}/2026", "2026", "dir")
    month = Entry(f"{year.path}/07", "07", "dir")
    day = Entry(f"{month.path}/03", "03", "dir")
    files = []
    for ordinal in range(1, 20_001):
        session_id = str(UUID(int=ordinal))
        name = f"rollout-2026-07-03T00-00-00-{session_id}.jsonl"
        files.append(Entry(f"{day.path}/{name}", name, "file"))
    tree = {
        str(root): [year],
        year.path: [month],
        month.path: [day],
        day.path: files,
    }
    monkeypatch.setattr(herdr_turns.os, "scandir", lambda path: tree[os.fspath(path)])
    monkeypatch.setattr(herdr_turns, "_codex_root_signature", lambda _root: (1, 2, 3, 4))

    generation = herdr_turns._build_codex_index(root)
    assert generation.overflowed is False
    assert len(generation.entries) == 20_000
    assert generation.entries[str(UUID(int=1))][0].endswith("000000000001.jsonl")
    assert generation.entries[str(UUID(int=20_000))][0].endswith("000000004e20.jsonl")
    assert generation.visited == 20_003
    assert generation.retained_bytes <= herdr_turns._CODEX_INDEX_MAX_BYTES


def test_index_iterator_stops_at_first_overflow_sentinel(monkeypatch) -> None:
    produced = 0
    closed = 0

    class Entry:
        def __init__(self, ordinal: int):
            self.name = f"entry-{ordinal}"
            self.path = f"/virtual/{self.name}"

    class UnboundedDirectory:
        def __iter__(self):
            return self

        def __next__(self):
            nonlocal produced
            produced += 1
            return Entry(produced)

        def close(self):
            nonlocal closed
            closed += 1

    observed = []
    monkeypatch.setattr(herdr_turns, "_CODEX_INDEX_MAX_VISITS", 5)
    monkeypatch.setattr(
        herdr_turns,
        "_codex_root_signature",
        lambda _root: (1, 2, 3, 4),
    )
    monkeypatch.setattr(
        herdr_turns.os,
        "scandir",
        lambda _path: UnboundedDirectory(),
    )
    monkeypatch.setattr(
        herdr_turns,
        "_CODEX_INDEX_BUILD_OBSERVER",
        observed.append,
    )

    generation = herdr_turns._build_codex_index(Path("/virtual"))

    assert generation.overflowed is True
    assert generation.entries == {}
    assert generation.visited == 6
    assert produced == 6
    assert closed == 1
    assert observed == [6]


def test_warm_positive_and_negative_lookup_use_one_index_build(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    target = _write_session(home, [])
    monkeypatch.setenv("CODEX_HOME", str(home))
    visits = []
    monkeypatch.setattr(herdr_turns, "_CODEX_INDEX_BUILD_OBSERVER", visits.append)
    clock = [100.0]
    monkeypatch.setattr(herdr_turns.time, "monotonic", lambda: clock[0])

    assert herdr_turns._find_codex_session_file(SESSION_A) == target.resolve()
    assert herdr_turns._find_codex_session_file(SESSION_A) == target.resolve()
    assert herdr_turns._find_codex_session_file(SESSION_B) is None
    assert herdr_turns._find_codex_session_file(SESSION_B) is None
    assert len(visits) == 1
    clock[0] += herdr_turns._CODEX_NEGATIVE_TTL_SECONDS + 0.5
    assert herdr_turns._find_codex_session_file(SESSION_B) is None
    assert herdr_turns._find_codex_session_file(SESSION_A) == target.resolve()
    assert len(visits) == 1
    clock[0] += herdr_turns._CODEX_POSITIVE_TTL_SECONDS
    assert herdr_turns._find_codex_session_file(SESSION_A) == target.resolve()
    assert len(visits) == 2


def test_pure_interpreter_preserves_turn_id_precedence_and_stream_window() -> None:
    work = herdr_turns._CodexWorkState(
        resolver_generation=1,
        root="/root",
        root_file_id=(9, 9),
        session_id=SESSION_A,
        canonical_path=f"/root/2026/07/03/rollout-2026-07-03T00-00-00-{SESSION_A}.jsonl",
        file_id=(1, 2),
        observed_size=0,
        mtime_ns=0,
        ctime_ns=0,
    )
    start = herdr_turns._codex_record_event(_event("task_started", "active"))
    herdr_turns._apply_codex_event(work, start, herdr_turns._CodexRecordSpan(0, 1))
    for index, text in enumerate(("one", "two", "three", "four", "two", "five"), 1):
        event = herdr_turns._codex_record_event(
            _message("active", "assistant", text, phase="commentary")
        )
        herdr_turns._apply_codex_event(
            work,
            event,
            herdr_turns._CodexRecordSpan(index * 10, index * 10 + 5),
        )
    assert [text for _span, text in work.stream_items] == ["three", "four", "two", "five"]
    direct = {
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "user",
            "turn_id": "direct",
            "content": [{"text": "x"}],
            "internal_chat_message_metadata_passthrough": {"turn_id": "metadata"},
        },
    }
    assert herdr_turns._codex_record_event(direct).turn_id == "direct"


def test_partial_record_is_invisible_until_lf_and_commits_once(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    turn_id = "turn-partial"
    base = [_event("task_started", turn_id), _message(turn_id, "user", "prompt")]
    path = _write_session(home, base)
    completion = _jsonl(
        _event("task_complete", turn_id, last_agent_message="final once"),
        terminate=False,
    )
    split = len(completion) // 2
    with path.open("ab") as handle:
        handle.write(completion[:split])
    monkeypatch.setenv("CODEX_HOME", str(home))
    observed = []
    monkeypatch.setattr(herdr_turns, "_CODEX_ISOLATED_READ_OBSERVER", observed.append)

    first = herdr_turns._read_codex_session_turn(SESSION_A)
    assert first["user_text"] == "prompt"
    assert first["assistant_final_text"] is None
    cache_key = (str((home / "sessions").resolve()), SESSION_A)
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        first_state = herdr_turns._CODEX_SESSION_CACHE[cache_key]
    committed = first_state.committed_offset
    assert first_state.partial_record == completion[:split]

    with path.open("ab") as handle:
        handle.write(completion[split:])
    assert herdr_turns._read_codex_session_turn(SESSION_A) is None
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        second_state = herdr_turns._CODEX_SESSION_CACHE[cache_key]
    assert second_state.committed_offset == committed
    assert second_state.partial_record == completion

    with path.open("ab") as handle:
        handle.write(b"\n")
    final = herdr_turns._read_codex_session_turn(SESSION_A)
    assert final["assistant_final_text"] == "final once"
    assert final["complete"] is True
    assert herdr_turns._read_codex_session_turn(SESSION_A) is None
    assert observed[-1] == 0


def test_sparse_cold_read_and_warm_append_are_bounded(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    path = _session_path(home)
    path.parent.mkdir(parents=True)
    turn_id = "turn-sparse"
    tail = _jsonl(
        _event("task_started", turn_id),
        _message(turn_id, "user", "sparse prompt"),
    )
    with path.open("wb") as handle:
        handle.seek(20 * 1024 * 1024)
        handle.write(b"\n")
        handle.write(tail)
    monkeypatch.setenv("CODEX_HOME", str(home))
    observed = []
    monkeypatch.setattr(herdr_turns, "_CODEX_ISOLATED_READ_OBSERVER", observed.append)

    first = herdr_turns._read_codex_session_turn(SESSION_A)
    assert first["user_text"] == "sparse prompt"
    assert observed[-1] <= herdr_turns._CODEX_RESYNC_INITIAL_BYTES
    append = _jsonl(_message(turn_id, "assistant", "working", phase="commentary"))
    with path.open("ab") as handle:
        handle.write(append)
    second = herdr_turns._read_codex_session_turn(SESSION_A)
    assert second["assistant_stream_text"] == "working"
    assert observed[-1] == len(append)
    assert herdr_turns._read_codex_session_turn(SESSION_A) is None
    assert observed[-1] == 0


def test_malformed_and_oversized_records_block_without_checkpoint_advance(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    path = _write_session(
        home,
        [_event("task_started", "blocked"), _message("blocked", "user", "safe")],
    )
    monkeypatch.setenv("CODEX_HOME", str(home))
    first = herdr_turns._read_codex_session_turn(SESSION_A)
    assert first["user_text"] == "safe"
    cache_key = (str((home / "sessions").resolve()), SESSION_A)
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        before = herdr_turns._serialize_codex_state(
            herdr_turns._CODEX_SESSION_CACHE[cache_key]
        )
    with path.open("ab") as handle:
        handle.write(b"{not-json}\n")
    with pytest.raises(ValueError, match="invalid Codex record"):
        herdr_turns._read_codex_session_turn(SESSION_A)
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        assert herdr_turns._serialize_codex_state(
            herdr_turns._CODEX_SESSION_CACHE[cache_key]
        ) == before

    path.write_bytes(_jsonl(_event("task_started", "oversize")) + b"x" * 65)
    monkeypatch.setattr(herdr_turns, "_CODEX_RECORD_MAX_BYTES", 64)
    _reset_codex()
    with pytest.raises(ValueError, match="oversized Codex record"):
        herdr_turns._read_codex_session_turn(SESSION_A)


def test_truncate_and_inode_replacement_recover_latest_exact_turn(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    old_prompt = "old prompt " + ("x" * 1024)
    old = [_event("task_started", "old"), _message("old", "user", old_prompt)]
    path = _write_session(home, old)
    monkeypatch.setenv("CODEX_HOME", str(home))
    assert herdr_turns._read_codex_session_turn(SESSION_A)["source_turn_id"] == "old"

    new_prompt = "new prompt"
    new = [_event("task_started", "new"), _message("new", "user", new_prompt)]
    path.write_bytes(_jsonl(*new))
    os.utime(path, ns=(1_800_000_000_000_000_000, 1_800_000_000_000_000_000))
    truncated = herdr_turns._read_codex_session_turn(SESSION_A)
    assert truncated["source_turn_id"] == "new"
    assert truncated["user_text"] == new_prompt

    replacement = path.with_name("replacement.tmp")
    newest = [
        _event("task_started", "replacement"),
        _message("replacement", "user", "replacement prompt"),
    ]
    replacement.write_bytes(_jsonl(*newest))
    replacement.replace(path)
    replaced = herdr_turns._read_codex_session_turn(SESSION_A)
    assert replaced["source_turn_id"] == "replacement"
    assert replaced["user_text"] == "replacement prompt"


def test_huge_admitted_prompt_and_final_are_exact_and_not_cached(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    prompt = "π" * 300_000
    final = "終" * 300_000
    turn_id = "huge"
    _write_session(
        home,
        [
            _event("task_started", turn_id),
            _message(turn_id, "user", prompt),
            _event("task_complete", turn_id, last_agent_message=final),
        ],
    )
    monkeypatch.setenv("CODEX_HOME", str(home))

    content = herdr_turns._read_codex_session_turn(SESSION_A)
    assert content["user_text"] == prompt
    assert content["assistant_final_text"] == final
    cache_key = (str((home / "sessions").resolve()), SESSION_A)
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        serialized = herdr_turns._serialize_codex_state(
            herdr_turns._CODEX_SESSION_CACHE[cache_key]
        )
    private_json = json.dumps(serialized, separators=(",", ":"))
    assert prompt[:100] not in private_json
    assert final[:100] not in private_json
    assert serialized["stream_spans"] == []


def test_parser_lru_enforces_count_and_weight_with_mru_touch(monkeypatch) -> None:
    monkeypatch.setattr(herdr_turns, "_CODEX_SESSION_CACHE_CAPACITY", 3)
    monkeypatch.setattr(herdr_turns, "_CODEX_SESSION_CACHE_MAX_BYTES", 64 * 1024)

    def state(session_id: str, ordinal: int):
        return herdr_turns._CodexSessionState(
            resolver_generation=1,
            root="/root",
            root_file_id=(9, 9),
            session_id=session_id,
            canonical_path=f"/root/2026/07/03/rollout-2026-07-03T00-00-00-{session_id}.jsonl",
            file_id=(1, ordinal),
            observed_size=ordinal,
            mtime_ns=ordinal,
            ctime_ns=ordinal,
            committed_offset=ordinal,
            partial_record=b"",
            active_turn_id="",
            last_content_turn_id="",
            turn_open=False,
            final_seen=False,
            complete=False,
            stream_spans=(),
        )

    ids = [str(UUID(int=index)) for index in range(1, 5)]
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        for index, session_id in enumerate(ids[:3], 1):
            key = ("/root", session_id)
            assert herdr_turns._codex_cache_store_locked(key, state(session_id, index))
        herdr_turns._codex_cache_get_locked(("/root", ids[0]))
        herdr_turns._codex_cache_store_locked(("/root", ids[3]), state(ids[3], 4))
        assert [key[1] for key in herdr_turns._CODEX_SESSION_CACHE] == [
            ids[2],
            ids[0],
            ids[3],
        ]
        assert herdr_turns._codex_cache_weight_locked() <= 64 * 1024


def test_state_deserializer_rejects_bodies_overlap_and_wrong_rollout_path() -> None:
    state = herdr_turns._CodexSessionState(
        resolver_generation=1,
        root="/root",
        root_file_id=(9, 9),
        session_id=SESSION_A,
        canonical_path=f"/root/2026/07/03/rollout-2026-07-03T00-00-00-{SESSION_A}.jsonl",
        file_id=(1, 2),
        observed_size=100,
        mtime_ns=1,
        ctime_ns=1,
        committed_offset=100,
        partial_record=b"",
        active_turn_id="turn",
        last_content_turn_id="turn",
        turn_open=True,
        final_seen=False,
        complete=False,
        stream_spans=(herdr_turns._CodexRecordSpan(10, 20),),
    )
    serialized = herdr_turns._serialize_codex_state(state)
    assert herdr_turns._deserialize_codex_state(serialized) == state
    body = dict(serialized)
    body["user_text"] = "forbidden"
    with pytest.raises(ValueError, match="invalid Codex parser state"):
        herdr_turns._deserialize_codex_state(body)
    overlap = dict(serialized)
    overlap["stream_spans"] = [[10, 20], [20, 30]]
    with pytest.raises(ValueError, match="overlapping"):
        herdr_turns._deserialize_codex_state(overlap)
    wrong = dict(serialized)
    wrong["canonical_path"] = f"/root/2026/07/04/rollout-2026-07-03T00-00-00-{SESSION_A}.jsonl"
    with pytest.raises(ValueError, match="invalid Codex rollout path"):
        herdr_turns._deserialize_codex_state(wrong)


def test_partial_completion_is_transactional_at_every_byte_split(
    tmp_path: Path,
    monkeypatch,
) -> None:
    turn_id = "all-splits"
    completion = _jsonl(
        _event("task_complete", turn_id, last_agent_message="split final"),
        terminate=False,
    )
    for split in range(len(completion) + 1):
        _reset_codex()
        home = tmp_path / f"split-{split}"
        path = _write_session(
            home,
            [_event("task_started", turn_id), _message(turn_id, "user", "split prompt")],
        )
        base_size = path.stat().st_size
        with path.open("ab") as handle:
            handle.write(completion[:split])
        monkeypatch.setenv("CODEX_HOME", str(home))
        first = herdr_turns._read_codex_session_turn(SESSION_A)
        assert first["assistant_final_text"] is None
        cache_key = (str((home / "sessions").resolve()), SESSION_A)
        with herdr_turns._CODEX_SESSION_CACHE_LOCK:
            state = herdr_turns._CODEX_SESSION_CACHE[cache_key]
        assert state.committed_offset == base_size
        assert state.partial_record == completion[:split]
        if split < len(completion):
            with path.open("ab") as handle:
                handle.write(completion[split:])
            assert herdr_turns._read_codex_session_turn(SESSION_A) is None
        with path.open("ab") as handle:
            handle.write(b"\n")
        completed = herdr_turns._read_codex_session_turn(SESSION_A)
        assert completed["assistant_final_text"] == "split final"
        assert herdr_turns._read_codex_session_turn(SESSION_A) is None


def test_path_result_lru_has_deterministic_mru_and_capacity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    session_ids = [str(UUID(int=index)) for index in range(101, 105)]
    for session_id in session_ids:
        _write_session(home, [], session_id=session_id)
    monkeypatch.setenv("CODEX_HOME", str(home))
    monkeypatch.setattr(herdr_turns, "_CODEX_PATH_CACHE_CAPACITY", 3)
    for session_id in session_ids[:3]:
        assert herdr_turns._resolve_codex_session(session_id).status == "found"
    assert herdr_turns._resolve_codex_session(session_ids[0]).status == "found"
    assert herdr_turns._resolve_codex_session(session_ids[3]).status == "found"
    with herdr_turns._CODEX_PATH_CACHE_LOCK:
        assert [key[1] for key in herdr_turns._CODEX_PATH_CACHE] == [
            session_ids[2],
            session_ids[0],
            session_ids[3],
        ]
        assert (
            herdr_turns._codex_path_cache_weight_locked()
            <= herdr_turns._CODEX_PATH_CACHE_MAX_BYTES
        )


def test_rotation_duplicate_and_parser_cold_start_are_fail_closed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    path = _write_session(
        home,
        [_event("task_started", "rotation"), _message("rotation", "user", "rotating")],
    )
    monkeypatch.setenv("CODEX_HOME", str(home))
    assert herdr_turns._read_codex_session_turn(SESSION_A)["user_text"] == "rotating"

    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        herdr_turns._CODEX_SESSION_CACHE.clear()
    cold = herdr_turns._read_codex_session_turn(SESSION_A)
    assert cold["source_turn_id"] == "rotation"
    with herdr_turns._CODEX_PATH_CACHE_LOCK:
        herdr_turns._CODEX_PATH_CACHE.clear()
        herdr_turns._CODEX_INDEX_GENERATION = None
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        herdr_turns._CODEX_SESSION_CACHE.clear()
    fully_cold = herdr_turns._read_codex_session_turn(SESSION_A)
    assert fully_cold["user_text"] == "rotating"

    duplicate = _session_path(home, SESSION_A, "2026-07-04")
    duplicate.parent.mkdir(parents=True)
    duplicate.write_bytes(path.read_bytes())
    with herdr_turns._CODEX_PATH_CACHE_LOCK:
        herdr_turns._CODEX_PATH_CACHE.clear()
        herdr_turns._CODEX_INDEX_GENERATION = None
    assert herdr_turns._find_codex_session_file(SESSION_A) is None
    assert herdr_turns._resolve_codex_session(SESSION_A).status == "ambiguous"


def test_long_warm_turn_advances_beyond_cold_resync_horizon(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    turn_id = "long-warm"
    path = _write_session(
        home,
        [_event("task_started", turn_id), _message(turn_id, "user", "long prompt")],
    )
    monkeypatch.setenv("CODEX_HOME", str(home))
    monkeypatch.setattr(herdr_turns, "_CODEX_RESYNC_MAX_BYTES", 1024)
    observed = []
    monkeypatch.setattr(herdr_turns, "_CODEX_ISOLATED_READ_OBSERVER", observed.append)
    first = herdr_turns._read_codex_session_turn(SESSION_A)
    assert first["user_text"] == "long prompt"

    newest = []
    for index in range(12):
        text = f"progress-{index}-" + ("x" * 220)
        newest.append(text)
        append = _jsonl(_message(turn_id, "assistant", text, phase="commentary"))
        with path.open("ab") as handle:
            handle.write(append)
        current = herdr_turns._read_codex_session_turn(SESSION_A)
        assert current["source_turn_id"] == turn_id
        assert current["complete"] is False
        assert current["assistant_stream_text"].split("\n\n") == newest[-4:]
        assert observed[-1] <= len(append) + 4 * herdr_turns._CODEX_RECORD_MAX_BYTES
    assert path.stat().st_size > herdr_turns._CODEX_RESYNC_MAX_BYTES

    final_record = _jsonl(
        _event("task_complete", turn_id, last_agent_message="long exact final")
    )
    with path.open("ab") as handle:
        handle.write(final_record)
    final = herdr_turns._read_codex_session_turn(SESSION_A)
    assert final["assistant_final_text"] == "long exact final"
    assert final["assistant_stream_text"] is None
    assert final["complete"] is True


def test_codex_cache_cas_is_monotone_and_does_not_resurrect_eviction() -> None:
    cache_key = ("/root", SESSION_A)
    prior = herdr_turns._CodexSessionState(
        resolver_generation=1,
        root="/root",
        root_file_id=(9, 9),
        session_id=SESSION_A,
        canonical_path=f"/root/2026/07/03/rollout-2026-07-03T00-00-00-{SESSION_A}.jsonl",
        file_id=(1, 2),
        observed_size=10,
        mtime_ns=1,
        ctime_ns=1,
        committed_offset=10,
        partial_record=b"",
        active_turn_id="turn",
        last_content_turn_id="turn",
        turn_open=True,
        final_seen=False,
        complete=False,
        stream_spans=(),
    )
    newer = replace(
        prior,
        observed_size=30,
        mtime_ns=3,
        ctime_ns=3,
        committed_offset=30,
    )
    older = replace(
        prior,
        observed_size=20,
        mtime_ns=2,
        ctime_ns=2,
        committed_offset=20,
    )
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        herdr_turns._CODEX_SESSION_CACHE[cache_key] = prior
    prior_value = herdr_turns._serialize_codex_state(prior)
    assert herdr_turns._publish_codex_cache_state(
        cache_key,
        prior_value,
        newer,
        {"source_turn_id": "turn"},
        None,
    ) == {"source_turn_id": "turn"}
    assert herdr_turns._publish_codex_cache_state(
        cache_key,
        prior_value,
        older,
        {"source_turn_id": "turn"},
        None,
    ) is None
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        assert herdr_turns._CODEX_SESSION_CACHE[cache_key].committed_offset == 30
        herdr_turns._CODEX_SESSION_CACHE.clear()
    assert herdr_turns._publish_codex_cache_state(
        cache_key,
        prior_value,
        newer,
        {"source_turn_id": "turn"},
        None,
    ) is None


def test_symlinked_sessions_root_is_rejected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    outside_home = tmp_path / "outside"
    target = _write_session(outside_home, [_event("task_started", "outside")])
    configured = tmp_path / "configured"
    configured.mkdir()
    (configured / "sessions").symlink_to(outside_home / "sessions", target_is_directory=True)
    monkeypatch.setenv("CODEX_HOME", str(configured))

    assert target.is_file()
    assert herdr_turns._find_codex_session_file(SESSION_A) is None


def test_internal_turn_remains_suppressed_when_later_commentary_arrives(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    turn_id = "internal-turn"
    path = _write_session(
        home,
        [
            _event("task_started", turn_id),
            _message(
                turn_id,
                "user",
                "Acme job\n\nTemplate: security-review\nTemplate instructions:",
            ),
        ],
    )
    monkeypatch.setenv("CODEX_HOME", str(home))
    assert herdr_turns._read_codex_session_turn(SESSION_A) is None
    cache_key = (str((home / "sessions").resolve()), SESSION_A)
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        assert herdr_turns._CODEX_SESSION_CACHE[cache_key].internal_turn is True

    with path.open("ab") as handle:
        handle.write(
            _jsonl(
                _message(
                    turn_id,
                    "assistant",
                    "ordinary-looking internal progress",
                    phase="commentary",
                )
            )
        )
    assert herdr_turns._read_codex_session_turn(SESSION_A) is None
    with herdr_turns._CODEX_SESSION_CACHE_LOCK:
        state = herdr_turns._CODEX_SESSION_CACHE[cache_key]
    assert state.internal_turn is True
    assert state.stream_spans == ()


def test_empty_task_complete_preserves_existing_open_turn_semantics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    turn_id = "empty-complete"
    _write_session(
        home,
        [
            _event("task_started", turn_id),
            _message(turn_id, "user", "still open by existing contract"),
            _event("task_complete", turn_id),
        ],
    )
    monkeypatch.setenv("CODEX_HOME", str(home))

    content = herdr_turns._read_codex_session_turn(SESSION_A)
    assert content["user_text"] == "still open by existing contract"
    assert content["assistant_final_text"] is None
    assert content["complete"] is False
    assert content["has_open_turn"] is True


def test_descriptor_relative_open_rejects_ancestor_symlink_swap(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    path = _write_session(
        home,
        [_event("task_started", "inside"), _message("inside", "user", "inside")],
    )
    monkeypatch.setenv("CODEX_HOME", str(home))
    resolution = herdr_turns._resolve_codex_session(SESSION_A)
    assert resolution is not None and resolution.status == "found"

    outside_day = tmp_path / "outside" / "2026" / "07" / "03"
    outside_day.mkdir(parents=True)
    outside_file = outside_day / path.name
    outside_file.write_bytes(
        _jsonl(
            _event("task_started", "outside"),
            _message("outside", "user", "outside sentinel"),
        )
    )
    original_open = herdr_turns.os.open
    swapped = False

    def racing_open(path_value, flags, mode=0o777, *, dir_fd=None):
        nonlocal swapped
        if path_value == path.name and dir_fd is not None and not swapped:
            swapped = True
            saved = path.parent.with_name("03-saved")
            path.parent.rename(saved)
            path.parent.symlink_to(outside_day, target_is_directory=True)
        return original_open(path_value, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(herdr_turns.os, "open", racing_open)
    with pytest.raises(herdr_turns._TurnReadFailed):
        herdr_turns._open_verified_codex_file(resolution)
    assert swapped is True
    assert outside_file.read_bytes().endswith(b"\n")


def test_sessions_root_swap_during_resolution_is_rejected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "configured"
    _write_session(home, [_event("task_started", "inside")])
    outside = tmp_path / "outside"
    _write_session(outside, [_event("task_started", "outside")])
    monkeypatch.setenv("CODEX_HOME", str(home))
    lexical_root = home / "sessions"
    original_resolve = Path.resolve
    swapped = False

    def racing_resolve(path_value, *args, **kwargs):
        nonlocal swapped
        if path_value == lexical_root and not swapped:
            swapped = True
            lexical_root.rename(home / "sessions-saved")
            lexical_root.symlink_to(outside / "sessions", target_is_directory=True)
        return original_resolve(path_value, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", racing_resolve)
    assert herdr_turns._find_codex_session_file(SESSION_A) is None
    assert swapped is True


def test_incremental_poll_limit_rejects_before_reading() -> None:
    prior = herdr_turns._CodexSessionState(
        resolver_generation=1,
        root="/root",
        root_file_id=(1, 1),
        session_id=SESSION_A,
        canonical_path=f"/root/2026/07/03/rollout-2026-07-03T00-00-00-{SESSION_A}.jsonl",
        file_id=(2, 2),
        observed_size=10,
        mtime_ns=1,
        ctime_ns=1,
        committed_offset=10,
        partial_record=b"",
        active_turn_id="turn",
        last_content_turn_id="turn",
        turn_open=True,
        final_seen=False,
        complete=False,
        stream_spans=(),
    )
    opened = SimpleNamespace(
        st_dev=2,
        st_ino=2,
        st_size=10 + herdr_turns._CODEX_POLL_MAX_BYTES + 1,
        st_mtime_ns=2,
        st_ctime_ns=2,
    )
    with pytest.raises(ValueError, match="poll byte limit"):
        herdr_turns._read_codex_incremental(-1, prior, opened)


def test_ipc_frame_bound_covers_maximum_compact_state_and_visible_turn() -> None:
    assert herdr_turns._CODEX_IPC_FRAME_MAX_BYTES >= (
        herdr_turns._CODEX_STATE_IPC_MAX_BYTES
        + (1 + herdr_turns._MAX_CODEX_STREAM_MESSAGES)
        * herdr_turns._CODEX_RECORD_MAX_BYTES
    )


def test_positive_snapshot_discovers_duplicate_by_named_refresh_bound(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    original = _write_session(home, [], date="2026-07-03")
    monkeypatch.setenv("CODEX_HOME", str(home))
    clock = [500.0]
    monkeypatch.setattr(herdr_turns.time, "monotonic", lambda: clock[0])
    visits = []
    monkeypatch.setattr(herdr_turns, "_CODEX_INDEX_BUILD_OBSERVER", visits.append)

    assert herdr_turns._find_codex_session_file(SESSION_A) == original.resolve()
    duplicate = _write_session(home, [], date="2026-07-04")
    assert duplicate.is_file()
    assert herdr_turns._find_codex_session_file(SESSION_A) == original.resolve()
    assert len(visits) == 1

    clock[0] += herdr_turns._CODEX_POSITIVE_TTL_SECONDS + 0.001
    assert herdr_turns._find_codex_session_file(SESSION_A) is None
    assert herdr_turns._resolve_codex_session(SESSION_A).status == "ambiguous"
    assert len(visits) == 2


def test_root_replacement_invalidates_found_and_nonfound_even_with_same_rollout_inode(
    tmp_path: Path,
    monkeypatch,
) -> None:
    home = tmp_path / "codex"
    original_path = _write_session(
        home,
        [_event("task_started", "root-one")],
    )
    monkeypatch.setenv("CODEX_HOME", str(home))
    builds = []
    monkeypatch.setattr(
        herdr_turns,
        "_CODEX_INDEX_BUILD_OBSERVER",
        builds.append,
    )

    first = herdr_turns._resolve_codex_session(SESSION_A)
    assert first is not None and first.status == "found"
    assert herdr_turns._resolve_codex_session(SESSION_B).status == "missing"
    assert len(builds) == 1
    original_file_id = first.file_id
    original_root_id = first.root_file_id

    old_root = home / "sessions-old"
    (home / "sessions").rename(old_root)
    replacement_path = _session_path(home)
    replacement_path.parent.mkdir(parents=True)
    os.link(
        old_root / replacement_path.relative_to(home / "sessions"),
        replacement_path,
    )
    assert (
        replacement_path.stat().st_dev,
        replacement_path.stat().st_ino,
    ) == original_file_id

    refreshed_found = herdr_turns._resolve_codex_session(SESSION_A)
    assert refreshed_found is not None
    assert refreshed_found.status == "found"
    assert refreshed_found.file_id == original_file_id
    assert refreshed_found.root_file_id != original_root_id
    assert refreshed_found.generation != first.generation
    assert len(builds) == 2
    refreshed_missing = herdr_turns._resolve_codex_session(SESSION_B)
    assert refreshed_missing is not None
    assert refreshed_missing.status == "missing"
    assert refreshed_missing.root_file_id == refreshed_found.root_file_id
    assert len(builds) == 2
