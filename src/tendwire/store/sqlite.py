"""Local-first sqlite persistence for canonical Tendwire snapshots.

The CLI snapshot path works without requiring a live store. This module is
provided for optional persistence and is kept intentionally stdlib-only.
"""

from __future__ import annotations

import fcntl
import json
import os
import secrets
import sqlite3
import stat
import time
import weakref
import threading
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qsl, quote, urlsplit

from ..config import DEFAULT_PENDING_STALE_GRACE_SECONDS
from ..local_state import (
    EntryIdentity,
    EntryType,
    LocalStateError,
    LocalStateErrorCode,
    PermissionState,
    _SQLiteFamilyMemberSnapshot,
    _snapshot_sqlite_family_at,
    canonical_path_from_fd,
    cleanup_private_sqlite_replacement_at,
    create_private_file_at,
    entry_identity,
    identity_matches,
    inspect_private_file_at,
    local_state_error,
    open_resolved_parent,
    prepare_private_sqlite_replacement_at,
    prepare_resolved_private_sqlite_parent,
    prepare_sqlite_family_at,
    private_file_creation_umask,
    publish_private_sqlite_replacement_at,
    release_private_sqlite_replacement_at,
    sqlite_parent_available_bytes_at,
    validate_owned_directory_stat,
    validate_owned_regular_stat,
    verify_created_private_sqlite_replacement_at,
    verify_entry_identity,
)
from ..core.commands import CommandEnvelope
from ..core.models import (
    FINGERPRINT_HEX_CHARS,
    SCHEMA_VERSION,
    Snapshot,
    WorkerBinding,
    normalize_severity,
    separate_duplicate_worker_bindings,
    sanitize_canonical_turn_text,
    sanitize_public_mapping,
    sanitize_public_value,
    stable_fingerprint,
    utc_timestamp,
)
from ..core.turns import (
    InteractionChoice,
    PendingInteraction,
    PendingObservation,
    PendingObservedChoice,
    Turn,
    TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
    TURN_CONTENT_PREVIEW_MAX_CHARS,
    TURN_LIST_CURSOR_TTL_SECONDS,
    TURN_LIST_DEFAULT_LIMIT,
    TURN_LIST_MAX_LIMIT,
    TURN_LIST_SCHEMA_VERSION,
    TURN_SCHEMA_VERSION,
    TURN_TEXT_MAX_CHARS,
    ContentCursorPosition,
    TurnListCursorPosition,
    content_cursor,
    content_revision,
    content_segment_id,
    decode_content_cursor,
    decode_turn_list_cursor,
    decode_turn_since_token,
    is_internal_automation_turn_payload,
    pending_from_snapshot,
    pending_payload_from_snapshot,
    project_persisted_turn_content,
    project_turn_content,
    recompute_pending_content_fingerprint,
    segment_canonical_text,
    turn_list_cursor,
    turn_since_token,
    turns_from_snapshot,
    turns_payload_from_snapshot,
)


FINGERPRINT_HEX_LENGTH = FINGERPRINT_HEX_CHARS
STORE_SCHEMA_VERSION = 10
ATTENTION_LIFECYCLE_OPEN = "open"
ATTENTION_LIFECYCLE_RESOLVED = "resolved"
ATTENTION_RESOLVED_REASON_GONE = "gone"
ATTENTION_RESOLVED_REASON_SUPERSEDED = "superseded"
ATTENTION_OUTBOX_CONNECTOR = "attention"
BACKEND_PENDING_CLAIM_LEASE_SECONDS = 30.0
ATTENTION_MISSING_REQUIRED = 2
ATTENTION_MISSING_GRACE_SECONDS = 120
_ATTENTION_SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}


class StoreSchemaError(RuntimeError):
    """Raised when a store schema cannot be opened safely."""

    def __init__(self, status: str) -> None:
        self.status = str(status)
        super().__init__(self.status)


@dataclass(frozen=True)
class Migration:
    """One exact, transaction-external schema transition."""

    from_version: int
    to_version: int
    apply: Callable[[sqlite3.Connection], None]



@dataclass(frozen=True)
class BackendPendingChoiceClaim:
    status: Literal[
        "claimed",
        "validated",
        "not_found",
        "stale",
        "changed",
        "unknown_choice",
        "already_claimed",
    ]
    claim_token: str | None = None
    worker_id: str | None = None
    worker_fingerprint: str | None = None
    binding_private_fingerprint: str | None = None
    turn_target_value: str | None = None
    picker_ordinal: int | None = None


@dataclass(frozen=True)
class BackendPendingChoiceSend:
    status: Literal[
        "started",
        "not_found",
        "stale",
        "changed",
        "binding_changed",
        "already_started",
    ]
    worker_id: str | None = None
    worker_fingerprint: str | None = None
    binding_private_fingerprint: str | None = None
    turn_target_value: str | None = None
    picker_ordinal: int | None = None


@dataclass(frozen=True)
class SnapshotRetentionPolicy:
    """Database-wide snapshot history bounds."""

    retention_days: int = 14
    retention_count: int = 4096
    batch_size: int = 100

    def __post_init__(self) -> None:
        for name in ("retention_days", "retention_count", "batch_size"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True)
class CompactionOptions:
    """Policy and explicit offline authority for one compaction."""

    dry_run: bool
    acknowledge_offline: bool = False
    backup_path: Path | None = None
    snapshot_retention_days: int = 14
    snapshot_retention_count: int = 4096
    batch_size: int = 100

    def __post_init__(self) -> None:
        if not isinstance(self.dry_run, bool):
            raise ValueError("dry_run must be a boolean")
        if not isinstance(self.acknowledge_offline, bool):
            raise ValueError("acknowledge_offline must be a boolean")
        if self.backup_path is not None:
            object.__setattr__(self, "backup_path", Path(self.backup_path))


@dataclass(frozen=True)
class SnapshotObservationContext:
    authority: Literal["none", "positive", "complete"] = "none"
    observed_at: str | None = None

@dataclass(frozen=True)
class TurnRefreshApplyResult:
    """Atomic turn and backend-pending persistence outcome."""

    updated: int
    pending_changed: bool
    stale_binding: bool = False
    cancelled: bool = False


_UNSET = object()


@dataclass
class TurnContentWorkCounters:
    """Deterministic canonical bytes and SQL work observed by list/page operations."""

    list_sql_queries: int = 0
    list_descriptor_rows: int = 0
    list_preview_chars_examined: int = 0
    list_inline_chars_examined: int = 0
    page_sql_queries: int = 0
    page_blob_reads: int = 0
    page_bytes_examined: int = 0
    page_chars_examined: int = 0
    max_response_utf8_bytes: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "list_sql_queries": self.list_sql_queries,
            "list_descriptor_rows": self.list_descriptor_rows,
            "list_preview_chars_examined": self.list_preview_chars_examined,
            "list_inline_chars_examined": self.list_inline_chars_examined,
            "page_sql_queries": self.page_sql_queries,
            "page_blob_reads": self.page_blob_reads,
            "page_bytes_examined": self.page_bytes_examined,
            "page_chars_examined": self.page_chars_examined,
            "max_response_utf8_bytes": self.max_response_utf8_bytes,
        }


def _record_response_size(
    counters: TurnContentWorkCounters | None,
    payload: Mapping[str, Any],
) -> None:
    if counters is not None:
        counters.max_response_utf8_bytes = max(
            counters.max_response_utf8_bytes,
            len(_canonical_json(payload).encode("utf-8")),
        )

CREATE_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL
);
"""

CREATE_LEGACY_SNAPSHOT_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_snapshots_host_id ON snapshots(host_id)",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON snapshots(created_at)",
    (
        "CREATE INDEX IF NOT EXISTS idx_snapshots_content_fingerprint "
        "ON snapshots(content_fingerprint)"
    ),
)

CREATE_SNAPSHOT_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_snapshots_host_newest "
        "ON snapshots(host_id, id DESC)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_snapshots_created_host_id "
        "ON snapshots(created_at, host_id, id)"
    ),
)
# Leading "~" sorts after every canonical timestamp under SQLite BINARY
# collation, so raw indexed age comparisons never delete unknown history.
_SNAPSHOT_CREATED_AT_QUARANTINE = "~invalid-snapshot-created-at"
_LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE = (
    "9999-12-31T23:59:59.999999+00:00"
)

CREATE_STORE_MAINTENANCE_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS store_maintenance_state (
    scope TEXT PRIMARY KEY CHECK (scope = 'automatic'),
    last_started_at TEXT,
    last_completed_at TEXT,
    last_status TEXT NOT NULL DEFAULT 'never'
        CHECK (last_status IN ('never', 'ok', 'failed')),
    last_examined INTEGER NOT NULL DEFAULT 0 CHECK (last_examined >= 0),
    last_deleted INTEGER NOT NULL DEFAULT 0 CHECK (last_deleted >= 0),
    last_examined_id INTEGER
);
"""

INSERT_STORE_MAINTENANCE_STATE = """
INSERT INTO store_maintenance_state (
    scope, last_started_at, last_completed_at, last_status,
    last_examined, last_deleted, last_examined_id
) VALUES ('automatic', NULL, NULL, 'never', 0, 0, NULL)
ON CONFLICT(scope) DO NOTHING
"""

CREATE_COMMAND_RECEIPTS_TABLE = """
CREATE TABLE IF NOT EXISTS command_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    action TEXT NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    uncertain INTEGER NOT NULL DEFAULT 0
);
"""

CREATE_COMMAND_RECEIPT_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_command_receipts_host_request_action "
    "ON command_receipts(host_id, request_id, action)",
)
CREATE_COMMAND_RECEIPT_UNIQUE_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_command_receipts_host_request_action "
    "ON command_receipts(host_id, request_id, action)"
)

CREATE_WORKER_BINDINGS_TABLE = """
CREATE TABLE IF NOT EXISTS worker_bindings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT NOT NULL,
    backend TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target_value TEXT NOT NULL,
    turn_target_kind TEXT,
    turn_target_value TEXT,
    sendable INTEGER NOT NULL DEFAULT 0,
    reason TEXT,
    observed_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    private_fingerprint TEXT NOT NULL
);
"""

CREATE_WORKER_BINDING_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_worker_id "
    "ON worker_bindings(host_id, worker_id)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_worker_fingerprint "
    "ON worker_bindings(host_id, worker_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_private_fingerprint "
    "ON worker_bindings(host_id, backend, private_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_backend_target "
    "ON worker_bindings(host_id, backend, target_kind, target_value)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_expires_at "
    "ON worker_bindings(host_id, expires_at)",
)
CREATE_WORKER_BINDING_UNIQUE_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_worker_bindings_host_backend_private "
    "ON worker_bindings(host_id, backend, private_fingerprint)"
)

CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL DEFAULT '',
    aggregate_id TEXT NOT NULL DEFAULT '',
    observed_at TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
"""

CREATE_SPACES_TABLE = """
CREATE TABLE IF NOT EXISTS spaces (
    host_id TEXT NOT NULL,
    space_id TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT,
    fingerprint TEXT NOT NULL,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, space_id)
);
"""

CREATE_WORKERS_TABLE = """
CREATE TABLE IF NOT EXISTS workers (
    host_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT NOT NULL,
    space_id TEXT,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    last_seen_at TEXT,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, worker_id)
);
"""

CREATE_TURNS_TABLE = """
CREATE TABLE IF NOT EXISTS turns (
    host_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT,
    space_id TEXT,
    status TEXT NOT NULL,
    kind TEXT NOT NULL,
    updated_at TEXT,
    fingerprint TEXT NOT NULL,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    list_sequence INTEGER NOT NULL CHECK (list_sequence > 0),
    PRIMARY KEY (host_id, turn_id)
);
"""

CREATE_TURN_LIST_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS turn_list_state (
    scope TEXT PRIMARY KEY CHECK (scope = 'turn-list'),
    store_epoch TEXT NOT NULL
);
"""

CREATE_TURN_LIST_HOSTS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_list_hosts (
    host_id TEXT PRIMARY KEY,
    next_sequence INTEGER NOT NULL CHECK (next_sequence > 0),
    traversal_generation INTEGER NOT NULL CHECK (traversal_generation > 0)
);
"""

CREATE_TURN_LIST_INDEXES = (
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_turns_host_list_sequence "
        "ON turns(host_id, list_sequence)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turns_host_worker_list_sequence "
        "ON turns(host_id, worker_id, list_sequence DESC, turn_id)"
    ),
)

CREATE_TURN_CONTENT_REVISIONS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_content_revisions (
    host_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    content_revision TEXT NOT NULL,
    user_text TEXT,
    assistant_final_text TEXT,
    user_state TEXT NOT NULL
        CHECK (user_state IN ('absent', 'complete', 'known_incomplete')),
    final_state TEXT NOT NULL
        CHECK (final_state IN ('absent', 'complete', 'known_incomplete')),
    user_char_length INTEGER NOT NULL CHECK (user_char_length >= 0),
    user_byte_length INTEGER NOT NULL CHECK (user_byte_length >= 0),
    final_char_length INTEGER NOT NULL CHECK (final_char_length >= 0),
    final_byte_length INTEGER NOT NULL CHECK (final_byte_length >= 0),
    user_page_count INTEGER NOT NULL CHECK (user_page_count >= 0),
    final_page_count INTEGER NOT NULL CHECK (final_page_count >= 0),
    is_current INTEGER NOT NULL CHECK (is_current IN (0, 1)),
    created_at TEXT NOT NULL,
    superseded_at TEXT,
    PRIMARY KEY (host_id, turn_id, content_revision),
    FOREIGN KEY (host_id, turn_id)
        REFERENCES turns(host_id, turn_id) ON DELETE RESTRICT
);
"""

CREATE_TURN_CONTENT_PAGE_BOUNDARIES_TABLE = """
CREATE TABLE IF NOT EXISTS turn_content_page_boundaries (
    host_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    content_revision TEXT NOT NULL,
    field TEXT NOT NULL
        CHECK (field IN ('user_text', 'assistant_final_text')),
    page_index INTEGER NOT NULL CHECK (page_index >= 0),
    start_char INTEGER NOT NULL CHECK (start_char >= 0),
    start_byte INTEGER NOT NULL CHECK (start_byte >= 0),
    PRIMARY KEY (
        host_id,
        turn_id,
        content_revision,
        field,
        page_index
    ),
    UNIQUE (
        host_id,
        turn_id,
        content_revision,
        field,
        start_char
    ),
    UNIQUE (
        host_id,
        turn_id,
        content_revision,
        field,
        start_byte
    ),
    FOREIGN KEY (host_id, turn_id, content_revision)
        REFERENCES turn_content_revisions(host_id, turn_id, content_revision)
        ON DELETE CASCADE
);
"""

CREATE_TURN_CONTENT_REVISION_INDEXES = (
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_turn_content_current "
        "ON turn_content_revisions(host_id, turn_id) WHERE is_current = 1"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_content_cleanup "
        "ON turn_content_revisions(host_id, is_current, superseded_at)"
    ),
)

CREATE_TURN_PRESENTATION_PLANS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_presentation_plans (
    id INTEGER PRIMARY KEY,
    host_id TEXT NOT NULL,
    name TEXT NOT NULL,
    plan_token TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    content_revision TEXT NOT NULL,
    presentation_version TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
    part_count INTEGER NOT NULL CHECK (part_count > 0),
    state TEXT NOT NULL
        CHECK (state IN (
            'preparing',
            'waiting_predecessor',
            'active',
            'completed',
            'superseded',
            'failed'
        )),
    replaces_plan_token TEXT,
    recovers_plan_token TEXT,
    created_at TEXT NOT NULL,
    activated_at TEXT,
    completed_at TEXT,
    UNIQUE (host_id, name, plan_token),
    UNIQUE (
        host_id,
        name,
        turn_id,
        content_revision,
        presentation_version,
        generation
    ),
    FOREIGN KEY (host_id, turn_id, content_revision)
        REFERENCES turn_content_revisions(host_id, turn_id, content_revision)
        ON DELETE RESTRICT
);
"""

CREATE_TURN_PRESENTATION_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_presentation_jobs (
    id INTEGER PRIMARY KEY,
    plan_id INTEGER NOT NULL,
    sequence_index INTEGER NOT NULL CHECK (sequence_index >= 0),
    operation TEXT NOT NULL CHECK (operation IN ('upsert', 'retire')),
    part_ordinal INTEGER NOT NULL CHECK (part_ordinal >= 0),
    spans_json TEXT NOT NULL,
    outbox_id INTEGER UNIQUE,
    created_at TEXT NOT NULL,
    UNIQUE (plan_id, sequence_index),
    UNIQUE (plan_id, operation, part_ordinal),
    FOREIGN KEY (plan_id) REFERENCES turn_presentation_plans(id) ON DELETE CASCADE,
    FOREIGN KEY (outbox_id) REFERENCES connector_outbox(id) ON DELETE RESTRICT
);
"""

CREATE_TURN_PRESENTATION_RECOVERIES_TABLE = """
CREATE TABLE IF NOT EXISTS turn_presentation_recoveries (
    id INTEGER PRIMARY KEY,
    host_id TEXT NOT NULL,
    name TEXT NOT NULL,
    request_id TEXT NOT NULL,
    failed_plan_id INTEGER NOT NULL,
    recovered_plan_id INTEGER NOT NULL,
    failed_plan_token TEXT NOT NULL,
    recovered_plan_token TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 2),
    source_job_count INTEGER NOT NULL CHECK (source_job_count > 0),
    delivered_prefix_count INTEGER NOT NULL CHECK (delivered_prefix_count >= 0),
    fresh_job_count INTEGER NOT NULL CHECK (fresh_job_count > 0),
    retained_failed_job_count INTEGER NOT NULL CHECK (retained_failed_job_count > 0),
    prior_attempt_count INTEGER NOT NULL CHECK (prior_attempt_count > 0),
    outcome TEXT NOT NULL CHECK (outcome = 'recovered'),
    created_at TEXT NOT NULL,
    UNIQUE (host_id, name, request_id),
    UNIQUE (failed_plan_id),
    FOREIGN KEY (failed_plan_id)
        REFERENCES turn_presentation_plans(id) ON DELETE RESTRICT,
    FOREIGN KEY (recovered_plan_id)
        REFERENCES turn_presentation_plans(id) ON DELETE RESTRICT
);
"""

CREATE_TURN_PRESENTATION_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_presentation_jobs_plan_sequence "
        "ON turn_presentation_jobs(plan_id, sequence_index)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_presentation_jobs_outbox "
        "ON turn_presentation_jobs(outbox_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_presentation_recoveries_recovered "
        "ON turn_presentation_recoveries(recovered_plan_id)"
    ),
)

CREATE_PENDING_INTERACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS pending_interactions (
    host_id TEXT NOT NULL,
    pending_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT,
    space_id TEXT,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT,
    fingerprint TEXT NOT NULL,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, pending_id)
);
"""

CREATE_ATTENTION_ITEMS_TABLE = """
CREATE TABLE IF NOT EXISTS attention_items (
    host_id TEXT NOT NULL,
    attention_id TEXT NOT NULL,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT,
    fingerprint TEXT NOT NULL,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    first_seen_at TEXT NOT NULL DEFAULT '',
    last_seen_at TEXT NOT NULL DEFAULT '',
    last_changed_at TEXT NOT NULL DEFAULT '',
    resolved_at TEXT,
    lifecycle_status TEXT NOT NULL DEFAULT 'open',
    resolved_reason TEXT,
    signal_count INTEGER NOT NULL DEFAULT 1,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, attention_id)
);
"""

CREATE_ATTENTION_LIFECYCLES_TABLE = """
CREATE TABLE IF NOT EXISTS attention_lifecycles (
    host_id TEXT NOT NULL,
    family_key TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    lifecycle_status TEXT NOT NULL CHECK (lifecycle_status IN ('open','resolved')),
    current_attention_id TEXT,
    first_seen_at TEXT NOT NULL,
    last_positive_at TEXT NOT NULL,
    first_missing_at TEXT,
    missing_observation_count INTEGER NOT NULL DEFAULT 0 CHECK (missing_observation_count >= 0),
    last_accepted_at TEXT NOT NULL,
    last_observation_key TEXT NOT NULL,
    max_notified_severity_rank INTEGER NOT NULL DEFAULT -1,
    PRIMARY KEY (host_id, family_key),
    CHECK (
        (lifecycle_status = 'open' AND current_attention_id IS NOT NULL)
        OR (lifecycle_status = 'resolved' AND current_attention_id IS NULL)
    ),
    CHECK (
        (missing_observation_count = 0 AND first_missing_at IS NULL)
        OR (missing_observation_count > 0 AND first_missing_at IS NOT NULL)
    )
);
"""

CREATE_ATTENTION_LIFECYCLE_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_lifecycles_host_status "
        "ON attention_lifecycles(host_id, lifecycle_status)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_lifecycles_host_current "
        "ON attention_lifecycles(host_id, current_attention_id)"
    ),
)

CREATE_COMMANDS_TABLE = """
CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    action TEXT NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    dry_run INTEGER NOT NULL DEFAULT 0,
    uncertain INTEGER NOT NULL DEFAULT 0,
    request_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    reserved_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL
);
"""

CREATE_CONNECTOR_OUTBOX_TABLE = """
CREATE TABLE IF NOT EXISTS connector_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    connector TEXT NOT NULL,
    delivery_key TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    private_state_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    next_attempt_at TEXT
);
"""

CREATE_CONNECTOR_DELIVERIES_TABLE = """
CREATE TABLE IF NOT EXISTS connector_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    outbox_id INTEGER,
    host_id TEXT NOT NULL,
    connector TEXT NOT NULL,
    delivery_key TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    response_json TEXT NOT NULL DEFAULT '{}',
    private_state_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    FOREIGN KEY (outbox_id) REFERENCES connector_outbox(id) ON DELETE SET NULL
);
"""

CREATE_BACKEND_HEALTH_TABLE = """
CREATE TABLE IF NOT EXISTS backend_health (
    host_id TEXT NOT NULL,
    backend_name TEXT NOT NULL,
    status TEXT NOT NULL,
    outcome TEXT NOT NULL,
    observed_at TEXT,
    snapshot_content_fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, backend_name)
);
"""


CREATE_LEGACY_BACKEND_PENDING_TABLE = """
CREATE TABLE IF NOT EXISTS backend_pending (
    host_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    PRIMARY KEY (host_id, worker_id)
);
"""


CREATE_BACKEND_PENDING_CLAIMS_TABLE = """
CREATE TABLE IF NOT EXISTS backend_pending_claims (
    host_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    claim_token TEXT NOT NULL UNIQUE,
    revision_digest TEXT NOT NULL,
    choice_id TEXT NOT NULL,
    picker_ordinal INTEGER NOT NULL CHECK (picker_ordinal >= 1),
    worker_fingerprint TEXT NOT NULL,
    binding_private_fingerprint TEXT NOT NULL,
    turn_target_value TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('claimed', 'send_started')),
    claimed_at TEXT NOT NULL,
    send_started_at TEXT,
    PRIMARY KEY (host_id, worker_id)
);
"""
CREATE_PR6_TABLES = (
    CREATE_EVENTS_TABLE,
    CREATE_SPACES_TABLE,
    CREATE_WORKERS_TABLE,
    CREATE_TURNS_TABLE,
    CREATE_PENDING_INTERACTIONS_TABLE,
    CREATE_ATTENTION_ITEMS_TABLE,
    CREATE_COMMANDS_TABLE,
    CREATE_CONNECTOR_OUTBOX_TABLE,
    CREATE_CONNECTOR_DELIVERIES_TABLE,
    CREATE_BACKEND_HEALTH_TABLE,
)

CREATE_PR6_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_events_host_observed_at ON events(host_id, observed_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_host_type ON events(host_id, event_type)",
    (
        "CREATE INDEX IF NOT EXISTS idx_events_host_aggregate "
        "ON events(host_id, aggregate_type, aggregate_id)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_spaces_host_status ON spaces(host_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_workers_host_status ON workers(host_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_workers_host_space ON workers(host_id, space_id)",
    "CREATE INDEX IF NOT EXISTS idx_turns_host_worker ON turns(host_id, worker_id)",
    "CREATE INDEX IF NOT EXISTS idx_turns_host_status ON turns(host_id, status)",
    (
        "CREATE INDEX IF NOT EXISTS idx_pending_interactions_host_worker "
        "ON pending_interactions(host_id, worker_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_pending_interactions_host_status "
        "ON pending_interactions(host_id, status)"
    ),
    CREATE_LEGACY_BACKEND_PENDING_TABLE,
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_source "
        "ON attention_items(host_id, source)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_status "
        "ON attention_items(host_id, status)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_lifecycle_status "
        "ON attention_items(host_id, lifecycle_status)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_last_seen "
        "ON attention_items(host_id, last_seen_at)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_fingerprint "
        "ON attention_items(host_id, fingerprint)"
    ),
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_commands_host_request_action "
        "ON commands(host_id, request_id, action)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_commands_host_status ON commands(host_id, status)",
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_connector_outbox_host_connector_key "
        "ON connector_outbox(host_id, connector, delivery_key)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_connector_outbox_status ON connector_outbox(status)",
    (
        "CREATE INDEX IF NOT EXISTS idx_connector_deliveries_outbox "
        "ON connector_deliveries(outbox_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_connector_deliveries_host_connector "
        "ON connector_deliveries(host_id, connector, delivery_key)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_backend_health_host_status ON backend_health(host_id, status)",
)


_SQLITE_FAMILY_SUFFIXES = ("", "-wal", "-shm", "-journal")


def _is_memory_db(db_path: Path | str) -> bool:
    raw = str(db_path)
    if raw == ":memory:":
        return True
    if not raw.startswith("file:"):
        return False
    try:
        query = urlsplit(raw).query
    except ValueError:
        return False
    modes = [
        value
        for name, value in parse_qsl(query, keep_blank_values=True)
        if name == "mode"
    ]
    return modes == ["memory"]


def _has_duplicate_sqlite_uri_mode(db_path: Path | str) -> bool:
    raw = str(db_path)
    if not raw.startswith("file:"):
        return False
    try:
        query = urlsplit(raw).query
    except ValueError:
        return False
    return sum(
        1
        for name, _value in parse_qsl(query, keep_blank_values=True)
        if name == "mode"
    ) > 1


def _validate_parent_fd(parent_fd: int, *, private: bool) -> None:
    try:
        current = os.fstat(parent_fd)
    except OSError:
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
    validate_owned_directory_stat(current)
    forbidden = ~0o700 if private else stat.S_IWGRP | stat.S_IWOTH
    if stat.S_IMODE(current.st_mode) & forbidden:
        raise local_state_error(LocalStateErrorCode.INSECURE_MODE) from None


def _bare_relative_parent(db_path: Path | str) -> bool:
    try:
        raw = os.fspath(db_path)
        return isinstance(raw, str) and not raw.startswith(os.sep) and Path(raw).parent == Path(".")
    except (TypeError, ValueError):
        return False


def _open_filesystem_db(
    db_path: Path | str,
    *,
    prepare: bool,
    retain_parent_shared_lock: bool,
) -> tuple[int, str]:
    shared_lock_held = False
    if prepare and not _bare_relative_parent(db_path):
        parent_fd, leaf, _result = prepare_resolved_private_sqlite_parent(
            db_path,
            retain_parent_shared_lock=retain_parent_shared_lock,
        )
        shared_lock_held = retain_parent_shared_lock
    else:
        parent_fd, leaf = open_resolved_parent(db_path)
        try:
            _validate_parent_fd(parent_fd, private=not prepare)
        except Exception:
            os.close(parent_fd)
            raise
    try:
        if retain_parent_shared_lock and not shared_lock_held:
            try:
                fcntl.flock(parent_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except (BlockingIOError, OSError):
                raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
        if prepare:
            prepare_sqlite_family_at(
                parent_fd,
                leaf,
                retain_parent_shared_lock=retain_parent_shared_lock,
            )
        _validate_sqlite_family_at(parent_fd, leaf)
        return parent_fd, leaf
    except Exception:
        os.close(parent_fd)
        raise


def _validate_sqlite_family_at(parent_fd: int, leaf: str) -> None:
    inspected = _snapshot_sqlite_family_at(
        parent_fd,
        leaf,
        require_main=True,
    )
    for result in inspected:
        if result.state is PermissionState.REPAIR_REQUIRED:
            raise local_state_error(LocalStateErrorCode.INSECURE_MODE)


def _sqlite_store_exists(db_path: Path | str) -> bool:
    if _is_memory_db(db_path):
        return False
    try:
        parent_fd, leaf = open_resolved_parent(db_path)
    except LocalStateError as exc:
        if exc.code is LocalStateErrorCode.MISSING_ENTRY:
            return False
        raise
    try:
        inspected = _snapshot_sqlite_family_at(
            parent_fd,
            leaf,
            require_main=False,
        )
        return inspected[0].state is not PermissionState.ABSENT
    finally:
        os.close(parent_fd)


def _apply_connection_pragmas(conn: sqlite3.Connection, db_path: Path | str) -> None:
    """Apply cheap, connection-local safety settings."""
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")


def _configure_persistent_database_conn(conn: sqlite3.Connection) -> None:
    """Negotiate persistent WAL once, only while initializing or migrating."""
    database_row = conn.execute("PRAGMA database_list").fetchone()
    if database_row is None or not str(database_row[2] or ""):
        return
    mode_row = conn.execute("PRAGMA journal_mode=WAL").fetchone()
    if mode_row is None or str(mode_row[0]).lower() != "wal":
        raise StoreSchemaError("wal_unavailable")


@dataclass(frozen=True)
class _SchemaConnectionAuthority:
    parent_fd: int | None
    parent_identity: EntryIdentity | None
    leaf: str | None
    selected_identity: EntryIdentity | None
    retains_parent_shared_lock: bool


_SCHEMA_CONNECTION_AUTHORITIES: dict[
    int, tuple[weakref.ReferenceType[Any], _SchemaConnectionAuthority]
] = {}
_SCHEMA_CONNECTION_AUTHORITIES_LOCK = threading.RLock()
_SCHEMA_PARENT_SHARED_LOCK_RECOVERY_ATTEMPTS = 3


def _parent_directory_identity(parent_fd: int) -> EntryIdentity:
    try:
        return entry_identity(os.fstat(parent_fd))
    except OSError:
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None


def _close_schema_authority(authority: _SchemaConnectionAuthority) -> None:
    if authority.parent_fd is not None:
        try:
            os.close(authority.parent_fd)
        except OSError:
            pass


def _release_abandoned_schema_connection_authority(
    connection_id: int,
    reference: weakref.ReferenceType[Any],
) -> None:
    authority: _SchemaConnectionAuthority | None = None
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        registered = _SCHEMA_CONNECTION_AUTHORITIES.get(connection_id)
        if registered is None or registered[0] is not reference:
            return
        _SCHEMA_CONNECTION_AUTHORITIES.pop(connection_id)
        authority = registered[1]
    _close_schema_authority(authority)


def _register_schema_connection_authority(
    conn: sqlite3.Connection,
    *,
    parent_fd: int | None = None,
    leaf: str | None = None,
    selected_identity: EntryIdentity | None = None,
    retains_parent_shared_lock: bool = False,
) -> None:
    parent_identity = (
        _parent_directory_identity(parent_fd) if parent_fd is not None else None
    )
    connection_id = id(conn)

    def release(reference: weakref.ReferenceType[Any]) -> None:
        _release_abandoned_schema_connection_authority(connection_id, reference)

    authority = _SchemaConnectionAuthority(
        parent_fd=parent_fd,
        parent_identity=parent_identity,
        leaf=leaf,
        selected_identity=selected_identity,
        retains_parent_shared_lock=retains_parent_shared_lock,
    )
    stale_authority: _SchemaConnectionAuthority | None = None
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        registered = _SCHEMA_CONNECTION_AUTHORITIES.get(connection_id)
        if registered is not None:
            existing = registered[0]()
            if existing is conn:
                return
            if existing is not None:
                raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)
            _SCHEMA_CONNECTION_AUTHORITIES.pop(connection_id)
            stale_authority = registered[1]
        _SCHEMA_CONNECTION_AUTHORITIES[connection_id] = (
            weakref.ref(conn, release),
            authority,
        )
    if stale_authority is not None:
        _close_schema_authority(stale_authority)


def _schema_connection_authority(
    conn: sqlite3.Connection,
) -> _SchemaConnectionAuthority:
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        registered = _SCHEMA_CONNECTION_AUTHORITIES.get(id(conn))
        if registered is None or registered[0]() is not conn:
            raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)
        return registered[1]


def _release_schema_connection_authority(
    conn: sqlite3.Connection,
) -> _SchemaConnectionAuthority | None:
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        registered = _SCHEMA_CONNECTION_AUTHORITIES.get(id(conn))
        if registered is None or registered[0]() is not conn:
            return None
        _SCHEMA_CONNECTION_AUTHORITIES.pop(id(conn))
        return registered[1]


def _has_other_shared_parent_schema_authority(
    conn: sqlite3.Connection,
    authority: _SchemaConnectionAuthority,
) -> bool:
    if authority.parent_identity is None:
        return False
    stale_authorities: list[_SchemaConnectionAuthority] = []
    has_other = False
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        for connection_id, (reference, candidate) in tuple(
            _SCHEMA_CONNECTION_AUTHORITIES.items()
        ):
            registered_conn = reference()
            if registered_conn is None:
                registered = _SCHEMA_CONNECTION_AUTHORITIES.get(connection_id)
                if (
                    registered is not None
                    and registered[0] is reference
                    and registered[1] is candidate
                ):
                    _SCHEMA_CONNECTION_AUTHORITIES.pop(connection_id)
                    stale_authorities.append(candidate)
                continue
            if registered_conn is conn:
                continue
            if (
                candidate.retains_parent_shared_lock
                and candidate.parent_identity == authority.parent_identity
            ):
                has_other = True
                break
    for stale_authority in stale_authorities:
        _close_schema_authority(stale_authority)
    return has_other


def _restore_schema_parent_lock(authority: _SchemaConnectionAuthority) -> bool:
    parent_fd = authority.parent_fd
    if parent_fd is None:
        return False
    if not authority.retains_parent_shared_lock:
        try:
            fcntl.flock(parent_fd, fcntl.LOCK_UN)
        except (BlockingIOError, OSError):
            raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
        return False
    retried = False
    for _attempt in range(_SCHEMA_PARENT_SHARED_LOCK_RECOVERY_ATTEMPTS):
        try:
            fcntl.flock(parent_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            retried = True
            continue
        return retried
    raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)


def _fail_closed_schema_connection(conn: sqlite3.Connection) -> None:
    try:
        conn.close()
    except Exception:
        authority = _release_schema_connection_authority(conn)
        if authority is not None:
            _close_schema_authority(authority)


class _ClosingConnection(sqlite3.Connection):
    """Connection that owns its registered schema authority until close."""

    def close(self) -> None:
        super().close()
        authority = _release_schema_connection_authority(self)
        if authority is not None:
            _close_schema_authority(authority)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        try:
            return super().__exit__(exc_type, exc, tb)
        finally:
            self.close()


@contextmanager
def _filesystem_schema_mutation_authority(
    conn: sqlite3.Connection,
) -> Iterator[None]:
    """Hold exclusive authority for one non-current filesystem schema branch."""

    authority = _schema_connection_authority(conn)
    parent_fd = authority.parent_fd
    leaf = authority.leaf
    selected_identity = authority.selected_identity
    if parent_fd is None or leaf is None or selected_identity is None:
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        if _has_other_shared_parent_schema_authority(conn, authority):
            raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)
        try:
            fcntl.flock(parent_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            try:
                restored_after_contention = _restore_schema_parent_lock(authority)
            except LocalStateError:
                _fail_closed_schema_connection(conn)
            else:
                if restored_after_contention:
                    _fail_closed_schema_connection(conn)
            raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
    try:
        _validate_parent_fd(parent_fd, private=True)
        _verify_expected_identity_at(parent_fd, leaf, selected_identity)
        _validate_sqlite_family_at(parent_fd, leaf)
        try:
            yield
        finally:
            prepare_sqlite_family_at(
                parent_fd,
                leaf,
                retain_parent_shared_lock=authority.retains_parent_shared_lock,
                _parent_exclusive_lock_held=True,
                _expected_main_identity=selected_identity,
            )
            _validate_parent_fd(parent_fd, private=True)
            _verify_expected_identity_at(parent_fd, leaf, selected_identity)
            _validate_sqlite_family_at(parent_fd, leaf)
            _verify_expected_identity_at(parent_fd, leaf, selected_identity)
    finally:
        try:
            restored_after_contention = _restore_schema_parent_lock(authority)
        except LocalStateError:
            _fail_closed_schema_connection(conn)
            raise
        if restored_after_contention:
            _fail_closed_schema_connection(conn)
            raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)


def _verify_expected_identity_at(
    parent_fd: int,
    leaf: str,
    expected_identity: EntryIdentity,
) -> os.stat_result:
    return verify_entry_identity(
        parent_fd,
        leaf,
        expected_identity,
        expected_type=EntryType.REGULAR_FILE,
    )


def _connect(
    db_path: Path | str,
    *,
    isolation_level: str | None = "",
    prepare: bool = False,
    read_only: bool = False,
    _store_lock_held: bool = False,
    _expected_db_identity: EntryIdentity | None = None,
) -> sqlite3.Connection:
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        return _connect_unlocked(
            db_path,
            isolation_level=isolation_level,
            prepare=prepare,
            read_only=read_only,
            _store_lock_held=_store_lock_held,
            _expected_db_identity=_expected_db_identity,
        )


def _connect_unlocked(
    db_path: Path | str,
    *,
    isolation_level: str | None = "",
    prepare: bool = False,
    read_only: bool = False,
    _store_lock_held: bool = False,
    _expected_db_identity: EntryIdentity | None = None,
) -> sqlite3.Connection:
    if prepare and read_only:
        raise ValueError("read-only connections cannot prepare store state")
    raw_db_path = str(db_path)
    if _has_duplicate_sqlite_uri_mode(raw_db_path):
        raise ValueError("sqlite URI must have at most one mode parameter")
    memory_db = _is_memory_db(raw_db_path)
    if memory_db and _expected_db_identity is not None:
        raise ValueError("memory databases do not have filesystem identities")
    parent_fd: int | None = None
    leaf: str | None = None
    selected_identity: EntryIdentity | None = None
    if memory_db:
        connect_target = raw_db_path
        connect_uri = raw_db_path.startswith("file:")
    else:
        try:
            parent_fd, leaf = _open_filesystem_db(
                db_path,
                prepare=prepare,
                retain_parent_shared_lock=not _store_lock_held,
            )
        except LocalStateError as exc:
            if (
                _expected_db_identity is not None
                and exc.code is LocalStateErrorCode.MISSING_ENTRY
            ):
                raise local_state_error(LocalStateErrorCode.ENTRY_CHANGED) from None
            raise
        try:
            selected_family = _snapshot_sqlite_family_at(
                parent_fd,
                leaf,
                require_main=True,
            )
            selected = selected_family[0]
            if selected.identity is None:
                raise local_state_error(LocalStateErrorCode.ENTRY_CHANGED)
            selected_identity = selected.identity
            if (
                _expected_db_identity is not None
                and selected_identity != _expected_db_identity
            ):
                raise local_state_error(LocalStateErrorCode.ENTRY_CHANGED)
            canonical_path = canonical_path_from_fd(parent_fd, leaf)
            mode = "ro" if read_only else "rw"
            immutable_query = ""
            if read_only:
                wal = selected_family[1]
                shm = selected_family[2]
                settled = (
                    wal.state is PermissionState.ABSENT
                    or wal.size == 0
                )
                if settled:
                    immutable_query = "&immutable=1"
                elif shm.state is PermissionState.ABSENT:
                    raise local_state_error(
                        LocalStateErrorCode.OPERATION_FAILED
                    )
            connect_target = (
                f"file:{quote(canonical_path, safe='/')}?mode={mode}"
                f"{immutable_query}"
            )
        except LocalStateError as exc:
            os.close(parent_fd)
            if (
                _expected_db_identity is not None
                and exc.code is LocalStateErrorCode.MISSING_ENTRY
            ):
                raise local_state_error(LocalStateErrorCode.ENTRY_CHANGED) from None
            raise
        except Exception:
            os.close(parent_fd)
            raise
        connect_uri = True
    try:
        with private_file_creation_umask():
            conn = sqlite3.connect(
                connect_target,
                timeout=30.0,
                isolation_level=isolation_level,
                factory=_ClosingConnection,
                uri=connect_uri,
            )
    except Exception:
        if parent_fd is not None:
            os.close(parent_fd)
        raise
    if not isinstance(conn, _ClosingConnection):
        try:
            conn.close()
        finally:
            if parent_fd is not None:
                os.close(parent_fd)
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
    if parent_fd is not None and leaf is not None and selected_identity is not None:
        try:
            # Catch path substitution across sqlite3.connect before any pragma
            # can mutate a database other than the securely resolved one.
            canonical_path_from_fd(parent_fd, leaf)
            _verify_expected_identity_at(parent_fd, leaf, selected_identity)
        except Exception:
            conn.close()
            os.close(parent_fd)
            raise
    if parent_fd is not None:
        assert leaf is not None
        assert selected_identity is not None
        _register_schema_connection_authority(
            conn,
            parent_fd=parent_fd,
            leaf=leaf,
            selected_identity=selected_identity,
            retains_parent_shared_lock=not _store_lock_held,
        )
        parent_fd = None
    else:
        assert memory_db
        _register_schema_connection_authority(conn)
    try:
        with private_file_creation_umask():
            _apply_connection_pragmas(conn, db_path)
            if leaf is not None:
                authority = _schema_connection_authority(conn)
                assert authority.parent_fd is not None
                assert authority.selected_identity is not None
                _verify_expected_identity_at(
                    authority.parent_fd,
                    leaf,
                    authority.selected_identity,
                )
                if not read_only:
                    # Activate new sidecars only on normal store connections.
                    conn.execute("PRAGMA user_version").fetchone()
                _validate_sqlite_family_at(authority.parent_fd, leaf)
                _verify_expected_identity_at(
                    authority.parent_fd,
                    leaf,
                    authority.selected_identity,
                )
        return conn
    except Exception:
        conn.close()
        raise


def _canonical_json(data: Any) -> str:
    """Serialize private or pre-sanitized data without silently dropping fields."""
    return json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

_CONNECTOR_LEASE_STATUS = "leased"
_CONNECTOR_POLLABLE_STATUSES = frozenset({"queued", "deferred", "retry"})
_CONNECTOR_TERMINAL_OUTBOX_STATUS = "delivered"
_CONNECTOR_EXHAUSTED_OUTBOX_STATUS = "dead_letter"
_CONNECTOR_SUPERSEDED_OUTBOX_STATUS = "superseded"
_CONNECTOR_PUBLIC_OUTBOX_STATUSES = frozenset(
    {
        _CONNECTOR_LEASE_STATUS,
        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
        _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
        *_CONNECTOR_POLLABLE_STATUSES,
    }
)
_CONNECTOR_REF_PREFIX = "twref1."


def _json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return dict(raw)
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _connector_datetime(value: str) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _connector_iso(value: str | datetime) -> str:
    parsed = value if isinstance(value, datetime) else _connector_datetime(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


def _connector_now(value: str | None = None) -> str:
    return _connector_iso(value or utc_timestamp())


def _connector_add_seconds(now: str, seconds: int) -> str:
    return _connector_iso(_connector_datetime(now) + timedelta(seconds=max(0, int(seconds))))


def _utc_cutoff(*, retention_days: int, now: str | None = None) -> str:
    current = _connector_datetime(now or utc_timestamp())
    cutoff = current - timedelta(days=max(1, int(retention_days)))
    return _connector_iso(cutoff)


def _connector_public_ref() -> str:
    return f"{_CONNECTOR_REF_PREFIX}{secrets.token_hex(32)}"



def _connector_public_reason(value: Any) -> str:
    clean = sanitize_public_mapping(
        {"reason": str(value or "").strip()},
        backend_neutral=True,
    ).get("reason")
    return clean if isinstance(clean, str) else ""


def _store_public_label(value: Any, *, allowed: Collection[str] | None = None) -> str:
    lowered = str(value or "").strip().lower().replace("-", "_")
    label = "".join(
        char if char.isalnum() or char in {"_", "."} else "_"
        for char in lowered
    )
    label = "_".join(part for part in label.split("_") if part).strip("._")[:64]
    if not label or (allowed is not None and label not in allowed):
        return "unknown"
    clean = sanitize_public_value(label, backend_neutral=True)
    return clean if isinstance(clean, str) and clean == label else "unknown"


def _store_public_text(
    value: Any,
    *,
    default: str = "",
    free_text: bool = False,
) -> str:
    text = str(value or "").strip()
    if free_text:
        clean = sanitize_public_mapping(
            {"reason": text},
            backend_neutral=True,
        ).get("reason")
    else:
        clean = sanitize_public_value(text, backend_neutral=True)
    return clean if isinstance(clean, str) and clean else default






def _connector_private_with_lease(
    raw: Any,
    *,
    delivery_id: int | None,
    attempt: int,
    lease_token: str,
    lease_expires_at: str,
    public_ref: str,
) -> str:
    state = _json_object(raw)
    state["current_delivery_id"] = delivery_id
    state["current_attempt"] = int(attempt)
    state["lease_token"] = str(lease_token)
    state["lease_expires_at"] = str(lease_expires_at)
    state["public_ref"] = str(public_ref)
    return _canonical_json(state)


def _connector_private_clear_current(raw: Any) -> str:
    state = _json_object(raw)
    for key in ("current_delivery_id", "current_attempt", "lease_token", "lease_expires_at", "public_ref"):
        state.pop(key, None)
    return _canonical_json(state)


def _connector_response(
    *,
    ok: bool,
    status: str,
    host_id: str,
    name: str,
    ref: str | None = None,
    key: str | None = None,
    attempt: int | None = None,
    available_at: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "ok": bool(ok),
        "status": str(status),
        "host_id": str(host_id),
        "name": str(name),
    }
    if ref is not None:
        payload["ref"] = str(ref)
    if key is not None:
        payload["key"] = str(key)
    if attempt is not None:
        payload["attempt"] = int(attempt)
    if available_at is not None:
        payload["available_at"] = str(available_at)
    return sanitize_public_value(payload)


def _connector_error_response(
    *,
    status: str,
    host_id: str,
    name: str,
    ref: str | None = None,
) -> dict[str, Any]:
    payload = _connector_response(ok=False, status=status, host_id=host_id, name=name, ref=ref)
    payload["error"] = {
        "code": str(status),
        "message": "reference is not valid for the requested operation",
    }
    return sanitize_public_value(payload)


def _connector_reclaim_expired_leases_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str | None,
    now: str,
) -> int:
    clauses = ["d.status = ?"]
    params: list[Any] = [_CONNECTOR_LEASE_STATUS]
    if host_id:
        clauses.append("d.host_id = ?")
        params.append(str(host_id))
    if name:
        clauses.append("d.connector = ?")
        params.append(str(name))
    rows = conn.execute(
        f"""
        SELECT
            d.id,
            d.outbox_id,
            d.private_state_json,
            o.status,
            o.private_state_json
        FROM connector_deliveries d
        LEFT JOIN connector_outbox o ON o.id = d.outbox_id
        WHERE {" AND ".join(clauses)}
        """,
        params,
    ).fetchall()
    reclaimed = 0
    now_dt = _connector_datetime(now)
    for delivery_id, outbox_id, delivery_private, outbox_status, outbox_private in rows:
        state = _json_object(delivery_private)
        lease_expires_at = state.get("lease_expires_at")
        if not lease_expires_at or _connector_datetime(str(lease_expires_at)) > now_dt:
            continue
        conn.execute(
            """
            UPDATE connector_deliveries
            SET status = ?, response_json = ?, delivered_at = ?
            WHERE id = ? AND status = ?
            """,
            (
                "expired",
                _canonical_json(
                    sanitize_public_mapping({"schema_version": 1, "status": "expired"})
                ),
                now,
                int(delivery_id),
                _CONNECTOR_LEASE_STATUS,
            ),
        )
        outbox_state = _json_object(outbox_private)
        current_delivery_id = outbox_state.get("current_delivery_id")
        if int(outbox_id or 0) > 0 and (
            current_delivery_id is None or int(current_delivery_id or 0) == int(delivery_id)
        ) and str(outbox_status or "") == _CONNECTOR_LEASE_STATUS:
            terminal_after_lease = bool(outbox_state.get("terminal_after_lease"))
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = ?, next_attempt_at = NULL, updated_at = ?,
                    private_state_json = ?
                WHERE id = ? AND status = ?
                """,
                (
                    (
                        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
                        if terminal_after_lease
                        else "queued"
                    ),
                    now,
                    _connector_private_clear_current(outbox_private),
                    int(outbox_id),
                    _CONNECTOR_LEASE_STATUS,
                ),
            )
            terminal_status = (
                _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
                if terminal_after_lease
                else "queued"
            )
            _update_presentation_plan_after_outbox_conn(
                conn,
                outbox_id=int(outbox_id),
                outbox_status=terminal_status,
                now=now,
            )
        reclaimed += 1
    return reclaimed


def _connector_exhaust_retryable_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str | None = None,
    max_attempts: int,
    now: str,
    dry_run: bool = False,
) -> int:
    clauses = [
        "host_id = ?",
        "status IN ('queued', 'deferred', 'retry')",
        """
        (
            SELECT COALESCE(MAX(d.attempt), 0)
            FROM connector_deliveries d
            WHERE d.outbox_id = connector_outbox.id
        ) >= ?
        """,
    ]
    params: list[Any] = [str(host_id), max(1, int(max_attempts))]
    if name is not None:
        clauses.insert(1, "connector = ?")
        params.insert(1, str(name))
    where_sql = " AND ".join(clauses)
    if dry_run:
        row = conn.execute(
            f"SELECT COUNT(*) FROM connector_outbox WHERE {where_sql}",
            params,
        ).fetchone()
        return int(row[0] or 0)

    cursor = conn.execute(
        f"""
        UPDATE connector_outbox
        SET status = ?,
            next_attempt_at = NULL,
            updated_at = ?,
            private_state_json = ?
        WHERE {where_sql}
        """,
        [
            _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
            now,
            "{}",
            *params,
        ],
    )
    _mark_exhausted_presentation_plans_conn(
        conn,
        host_id=str(host_id),
        name=str(name) if name is not None else None,
        now=str(now),
    )
    return int(cursor.rowcount or 0)


def reclaim_expired_connector_leases(
    db_path: Path,
    host_id: str,
    name: str | None = None,
    *,
    now: str | None = None,
) -> dict[str, Any]:
    """Expire stale connector leases and return their outbox rows to polling."""
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "name": str(name or ""),
            "reclaimed": 0,
        })
    current_time = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            reclaimed = _connector_reclaim_expired_leases_conn(
                conn,
                host_id=str(host_id),
                name=str(name) if name is not None else None,
                now=current_time,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "name": str(name or ""),
        "reclaimed": int(reclaimed),
    })


def exhaust_connector_retries(
    db_path: Path,
    host_id: str,
    *,
    max_attempts: int,
    now: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Move host-scoped retryable outbox rows beyond max attempts to a neutral terminal state."""
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "updated": 0,
        })
    current_time = _connector_now(now)
    attempt_limit = max(1, int(max_attempts))
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            if dry_run:
                conn.execute("SAVEPOINT dry_run_exhaust_connector_retries")
                try:
                    _connector_reclaim_expired_leases_conn(
                        conn,
                        host_id=str(host_id),
                        name=None,
                        now=current_time,
                    )
                    updated = _connector_exhaust_retryable_conn(
                        conn,
                        host_id=str(host_id),
                        max_attempts=attempt_limit,
                        now=current_time,
                    )
                finally:
                    conn.execute("ROLLBACK TO dry_run_exhaust_connector_retries")
                    conn.execute("RELEASE dry_run_exhaust_connector_retries")
            else:
                _connector_reclaim_expired_leases_conn(
                    conn,
                    host_id=str(host_id),
                    name=None,
                    now=current_time,
                )
                updated = _connector_exhaust_retryable_conn(
                    conn,
                    host_id=str(host_id),
                    max_attempts=attempt_limit,
                    now=current_time,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "max_attempts": attempt_limit,
        "updated": int(updated),
    })

_TURN_FINAL_NAME = "turn-final"
_PRESENTATION_SCHEMA_VERSION = 1
_PRESENTATION_MAX_PARTS = 10_000
_PRESENTATION_MAX_SPANS_PER_PART = 64
_PRESENTATION_SEQUENCE_WIDTH = 6
_PRESENTATION_FIELDS = ("user_text", "assistant_final_text")
_PRESENTATION_FIELD_RANK = {
    field: index for index, field in enumerate(_PRESENTATION_FIELDS)
}
_PRESENTATION_TOKEN_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
)
_PRESENTATION_LABEL_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
)


def _valid_presentation_opaque(value: Any, prefix: str) -> bool:
    if not isinstance(value, str) or not value.startswith(prefix):
        return False
    body = value[len(prefix) :]
    return bool(body) and all(char in _PRESENTATION_TOKEN_CHARS for char in body)


def _valid_presentation_label(value: Any, *, prefix: str | None = None) -> bool:
    if not isinstance(value, str) or not value or len(value) > 128:
        return False
    if prefix is not None and not value.startswith(prefix):
        return False
    if any(char not in _PRESENTATION_LABEL_CHARS for char in value):
        return False
    return sanitize_public_value(value, backend_neutral=True) == value




def _presentation_plan_token(
    *,
    host_id: str,
    name: str,
    turn_id: str,
    content_revision_value: str,
    presentation_version: str,
    part_count: int,
) -> str:
    digest = stable_fingerprint(
        {
            "domain": "tendwire.connector.prepare.v1",
            "host_id": str(host_id),
            "name": str(name),
            "turn_id": str(turn_id),
            "content_revision": str(content_revision_value),
            "presentation_version": str(presentation_version),
            "part_count": int(part_count),
        },
        length=64,
    )
    return f"twplan1.{digest}"


def _presentation_recovery_token(
    *,
    host_id: str,
    name: str,
    failed_plan_token: str,
    request_id: str,
    generation: int,
) -> str:
    digest = stable_fingerprint(
        {
            "domain": "tendwire.connector.prepare.recover.v1",
            "host_id": str(host_id),
            "name": str(name),
            "failed_plan_token": str(failed_plan_token),
            "request_id": str(request_id),
            "generation": int(generation),
        },
        length=64,
    )
    return f"twplan1.{digest}"


def _restore_presentation_tokens(
    sanitized: dict[str, Any],
    original: Mapping[str, Any],
) -> dict[str, Any]:
    for key in (
        "plan_token",
        "replaces_plan_token",
        "recovers_plan_token",
        "failed_plan_token",
        "recovered_plan_token",
    ):
        value = original.get(key)
        if value is None and key in original:
            sanitized[key] = None
        elif (
            isinstance(value, str)
            and value.startswith("twplan1.")
            and value[8:]
            and all(char.isalnum() or char in "-_" for char in value[8:])
        ):
            sanitized[key] = value
    return sanitized


def _presentation_response(payload: Mapping[str, Any]) -> dict[str, Any]:
    return _restore_presentation_tokens(
        dict(sanitize_public_value(dict(payload))),
        payload,
    )


def _presentation_error(
    status: str,
    *,
    host_id: str,
    name: str,
    plan_token: str | None = None,
    failed_plan_token: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": _PRESENTATION_SCHEMA_VERSION,
        "ok": False,
        "status": str(status),
        "host_id": str(host_id),
        "name": str(name),
        "error": {
            "code": str(status),
            "message": "presentation plan request could not be applied",
        },
    }
    if plan_token is not None:
        payload["plan_token"] = str(plan_token)
    if failed_plan_token is not None:
        payload["failed_plan_token"] = str(failed_plan_token)
    return _presentation_response(payload)


def _current_presentation_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
) -> tuple[Any | None, str | None]:
    row = conn.execute(
        """
        SELECT
            user_state,
            final_state,
            user_char_length,
            final_char_length,
            is_current
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND content_revision = ?
        """,
        (str(host_id), str(turn_id), str(content_revision_value)),
    ).fetchone()
    if row is None:
        return None, "content_revision_not_found"
    if int(row[4] or 0) != 1:
        return row, "revision_conflict"
    states = (str(row[0]), str(row[1]))
    if "known_incomplete" in states or "complete" not in states:
        return row, "content_known_incomplete"
    return row, None


def _presentation_plan_row_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    plan_token: str,
) -> Any | None:
    return conn.execute(
        """
        SELECT
            id,
            turn_id,
            content_revision,
            presentation_version,
            part_count,
            state,
            replaces_plan_token,
            generation,
            recovers_plan_token
        FROM turn_presentation_plans
        WHERE host_id = ? AND name = ? AND plan_token = ?
        """,
        (str(host_id), str(name), str(plan_token)),
    ).fetchone()


def _presentation_accepted_parts_conn(
    conn: sqlite3.Connection,
    plan_id: int,
) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM turn_presentation_jobs
        WHERE plan_id = ? AND operation = 'upsert'
        """,
        (int(plan_id),),
    ).fetchone()
    return int(row[0] or 0)


def prepare_connector_plan_begin(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    turn_id: str,
    content_revision: str,
    presentation_version: str,
    part_count: int,
    now: str | None = None,
) -> dict[str, Any]:
    """Idempotently begin one bounded range-only presentation plan."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or isinstance(part_count, bool)
        or not isinstance(part_count, int)
        or part_count < 1
        or part_count > _PRESENTATION_MAX_PARTS
        or not _valid_presentation_label(turn_id, prefix="turn-")
        or not _valid_presentation_opaque(content_revision, "twrev1.")
        or not _valid_presentation_label(presentation_version)
    ):
        return _presentation_error("invalid_params", host_id=host_id, name=name)
    count = part_count
    token = _presentation_plan_token(
        host_id=str(host_id),
        name=str(name),
        turn_id=str(turn_id),
        content_revision_value=str(content_revision),
        presentation_version=str(presentation_version),
        part_count=count,
    )
    created_at = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _, revision_error = _current_presentation_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(turn_id),
                content_revision_value=str(content_revision),
            )
            if revision_error is not None:
                conn.rollback()
                return _presentation_error(
                    revision_error,
                    host_id=host_id,
                    name=name,
                )
            existing = conn.execute(
                """
                SELECT
                    id,
                    plan_token,
                    part_count,
                    state,
                    turn_id,
                    content_revision,
                    presentation_version
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND content_revision = ?
                  AND presentation_version = ?
                  AND generation = 1
                """,
                (
                    str(host_id),
                    str(name),
                    str(turn_id),
                    str(content_revision),
                    str(presentation_version),
                ),
            ).fetchone()
            if existing is not None:
                if (
                    int(existing[2]) != count
                    or str(existing[1]) != token
                    or str(existing[4]) != str(turn_id)
                    or str(existing[5]) != str(content_revision)
                    or str(existing[6]) != str(presentation_version)
                ):
                    conn.rollback()
                    return _presentation_error(
                        "plan_conflict",
                        host_id=host_id,
                        name=name,
                    )
                accepted = _presentation_accepted_parts_conn(conn, int(existing[0]))
                conn.commit()
                return _presentation_response(
                    {
                        "schema_version": _PRESENTATION_SCHEMA_VERSION,
                        "ok": True,
                        "status": "ok",
                        "host_id": str(host_id),
                        "name": str(name),
                        "plan_token": token,
                        "state": str(existing[3]),
                        "part_count": count,
                        "accepted_parts": accepted,
                        "generation": 1,
                    }
                )
            cursor = conn.execute(
                """
                INSERT INTO turn_presentation_plans (
                    host_id,
                    name,
                    plan_token,
                    turn_id,
                    content_revision,
                    presentation_version,
                    generation,
                    part_count,
                    state,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, 'preparing', ?)
                """,
                (
                    str(host_id),
                    str(name),
                    token,
                    str(turn_id),
                    str(content_revision),
                    str(presentation_version),
                    count,
                    created_at,
                ),
            )
            plan_id = int(cursor.lastrowid)
            conn.commit()
            return _presentation_response(
                {
                    "schema_version": _PRESENTATION_SCHEMA_VERSION,
                    "ok": True,
                    "status": "ok",
                    "host_id": str(host_id),
                    "name": str(name),
                    "plan_token": token,
                    "state": "preparing",
                    "part_count": count,
                    "accepted_parts": 0,
                    "generation": 1,
                }
            )
        except Exception:
            conn.rollback()
            raise


def _validate_presentation_spans(
    spans: Iterable[Mapping[str, Any]],
    *,
    revision_row: Any,
) -> list[dict[str, Any]] | None:
    normalized: list[dict[str, Any]] = []
    prior_rank = -1
    prior_end_by_field: dict[str, int] = {}
    states = {
        "user_text": str(revision_row[0]),
        "assistant_final_text": str(revision_row[1]),
    }
    lengths = {
        "user_text": int(revision_row[2] or 0),
        "assistant_final_text": int(revision_row[3] or 0),
    }
    for raw in spans:
        if not isinstance(raw, Mapping):
            return None
        field = str(raw.get("field") or "")
        start = raw.get("start_char")
        end = raw.get("end_char")
        if (
            field not in _PRESENTATION_FIELD_RANK
            or states[field] != "complete"
            or isinstance(start, bool)
            or isinstance(end, bool)
            or not isinstance(start, int)
            or not isinstance(end, int)
            or start < 0
            or end <= start
            or end > lengths[field]
        ):
            return None
        rank = _PRESENTATION_FIELD_RANK[field]
        if rank < prior_rank or start < prior_end_by_field.get(field, 0):
            return None
        prior_rank = rank
        prior_end_by_field[field] = end
        normalized.append(
            {
                "field": field,
                "start_char": int(start),
                "end_char": int(end),
            }
        )
    if not normalized or len(normalized) > _PRESENTATION_MAX_SPANS_PER_PART:
        return None
    return normalized


def prepare_connector_plan_part(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    plan_token: str,
    ordinal: int,
    spans: Iterable[Mapping[str, Any]],
    now: str | None = None,
) -> dict[str, Any]:
    """Idempotently stage one ordinal's bounded canonical coordinate ranges."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
            plan_token=plan_token,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or not _valid_presentation_opaque(plan_token, "twplan1.")
        or isinstance(ordinal, bool)
        or not isinstance(ordinal, int)
        or not isinstance(spans, list | tuple)
        or not spans
        or len(spans) > _PRESENTATION_MAX_SPANS_PER_PART
    ):
        return _presentation_error(
            "invalid_params",
            host_id=host_id,
            name=name,
        )
    created_at = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            plan = _presentation_plan_row_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                plan_token=str(plan_token),
            )
            if plan is None:
                conn.rollback()
                return _presentation_error(
                    "plan_not_found",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            plan_id = int(plan[0])
            part_count = int(plan[4])
            if ordinal < 0 or ordinal >= part_count:
                conn.rollback()
                return _presentation_error(
                    "invalid_params",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            revision_row = conn.execute(
                """
                SELECT
                    user_state,
                    final_state,
                    user_char_length,
                    final_char_length,
                    is_current
                FROM turn_content_revisions
                WHERE host_id = ? AND turn_id = ? AND content_revision = ?
                """,
                (str(host_id), str(plan[1]), str(plan[2])),
            ).fetchone()
            if revision_row is None:
                conn.rollback()
                return _presentation_error(
                    "content_revision_not_found",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            normalized = _validate_presentation_spans(spans, revision_row=revision_row)
            if normalized is None:
                conn.rollback()
                return _presentation_error(
                    "invalid_params",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            encoded = _canonical_json(normalized)
            existing = conn.execute(
                """
                SELECT spans_json
                FROM turn_presentation_jobs
                WHERE plan_id = ? AND operation = 'upsert' AND part_ordinal = ?
                """,
                (plan_id, int(ordinal)),
            ).fetchone()
            if existing is not None:
                if str(existing[0]) != encoded:
                    conn.rollback()
                    return _presentation_error(
                        "plan_conflict",
                        host_id=host_id,
                        name=name,
                        plan_token=plan_token,
                    )
            elif str(plan[5]) != "preparing":
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            else:
                conn.execute(
                    """
                    INSERT INTO turn_presentation_jobs (
                        plan_id,
                        sequence_index,
                        operation,
                        part_ordinal,
                        spans_json,
                        created_at
                    ) VALUES (?, ?, 'upsert', ?, ?, ?)
                    """,
                    (
                        plan_id,
                        int(ordinal),
                        int(ordinal),
                        encoded,
                        created_at,
                    ),
                )
            accepted = _presentation_accepted_parts_conn(conn, plan_id)
            conn.commit()
            return _presentation_response(
                {
                    "schema_version": _PRESENTATION_SCHEMA_VERSION,
                    "ok": True,
                    "status": "ok",
                    "host_id": str(host_id),
                    "name": str(name),
                    "plan_token": str(plan_token),
                    "ordinal": int(ordinal),
                    "accepted_parts": accepted,
                }
            )
        except Exception:
            conn.rollback()
            raise


def _presentation_exact_coverage(
    staged_rows: Iterable[Any],
    *,
    revision_row: Any,
) -> bool:
    states = {
        "user_text": str(revision_row[0]),
        "assistant_final_text": str(revision_row[1]),
    }
    lengths = {
        "user_text": int(revision_row[2] or 0),
        "assistant_final_text": int(revision_row[3] or 0),
    }
    cursors: dict[str, int] = {}
    last_rank = -1
    for row in staged_rows:
        spans = json.loads(str(row[2]))
        for span in spans:
            field = str(span["field"])
            start = int(span["start_char"])
            end = int(span["end_char"])
            rank = _PRESENTATION_FIELD_RANK[field]
            if rank < last_rank:
                return False
            if rank > last_rank and last_rank >= 0:
                prior_field = _PRESENTATION_FIELDS[last_rank]
                if cursors.get(prior_field, 0) != lengths[prior_field]:
                    return False
            if states[field] != "complete" or start != cursors.get(field, 0):
                return False
            cursors[field] = end
            last_rank = rank
    return bool(cursors) and all(
        end == lengths[field] for field, end in cursors.items()
    )


def _materialize_connector_plan_job_conn(
    conn: sqlite3.Connection,
    *,
    plan_id: int,
    job_id: int,
    host_id: str,
    name: str,
    delivery_key: str,
    payload: Mapping[str, Any],
    created_at: str,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id,
            connector,
            delivery_key,
            status,
            payload_json,
            private_state_json,
            created_at,
            updated_at,
            next_attempt_at
        ) VALUES (?, ?, ?, 'queued', ?, '{}', ?, ?, NULL)
        """,
        (
            str(host_id),
            str(name),
            str(delivery_key),
            _canonical_json(dict(payload)),
            str(created_at),
            str(created_at),
        ),
    )
    outbox_id = int(cursor.lastrowid)
    conn.execute(
        """
        UPDATE turn_presentation_jobs
        SET outbox_id = ?
        WHERE id = ? AND plan_id = ? AND outbox_id IS NULL
        """,
        (outbox_id, int(job_id), int(plan_id)),
    )
    return outbox_id


def _mark_obsolete_presentation_plans_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    turn_id: str,
    keep_plan_id: int,
    now: str,
) -> bool:
    obsolete = conn.execute(
        """
        SELECT id
        FROM turn_presentation_plans
        WHERE host_id = ?
          AND name = ?
          AND turn_id = ?
          AND id != ?
          AND state IN ('preparing', 'waiting_predecessor', 'active', 'completed')
        """,
        (str(host_id), str(name), str(turn_id), int(keep_plan_id)),
    ).fetchall()
    obsolete_ids = [int(row[0]) for row in obsolete]
    if not obsolete_ids:
        return False
    placeholders = ",".join("?" for _ in obsolete_ids)
    conn.execute(
        f"""
        UPDATE turn_presentation_plans
        SET state = 'superseded'
        WHERE id IN ({placeholders})
        """,
        obsolete_ids,
    )
    conn.execute(
        f"""
        UPDATE connector_outbox
        SET status = 'superseded',
            next_attempt_at = NULL,
            updated_at = ?
        WHERE id IN (
            SELECT outbox_id
            FROM turn_presentation_jobs
            WHERE plan_id IN ({placeholders}) AND outbox_id IS NOT NULL
        )
          AND status IN ('queued', 'retry', 'deferred')
        """,
        (str(now), *obsolete_ids),
    )
    leased = conn.execute(
        f"""
        SELECT outbox.id, outbox.private_state_json
        FROM connector_outbox AS outbox
        JOIN turn_presentation_jobs AS jobs ON jobs.outbox_id = outbox.id
        WHERE jobs.plan_id IN ({placeholders}) AND outbox.status = 'leased'
        """,
        obsolete_ids,
    ).fetchall()
    for outbox_id, private_state_json in leased:
        state = _json_object(private_state_json)
        state["terminal_after_lease"] = True
        conn.execute(
            "UPDATE connector_outbox SET private_state_json = ? WHERE id = ?",
            (_canonical_json(state), int(outbox_id)),
        )
    return bool(leased)


def _activate_waiting_presentation_plans_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    now: str,
) -> int:
    cursor = conn.execute(
        """
        UPDATE turn_presentation_plans AS waiting
        SET state = 'active', activated_at = COALESCE(activated_at, ?)
        WHERE waiting.host_id = ?
          AND waiting.name = ?
          AND waiting.state = 'waiting_predecessor'
          AND NOT EXISTS (
              SELECT 1
              FROM turn_presentation_plans AS older
              JOIN turn_presentation_jobs AS jobs ON jobs.plan_id = older.id
              JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
              WHERE older.host_id = waiting.host_id
                AND older.name = waiting.name
                AND older.turn_id = waiting.turn_id
                AND older.id != waiting.id
                AND outbox.status = 'leased'
          )
        """,
        (str(now), str(host_id), str(name)),
    )
    return int(cursor.rowcount or 0)


def _update_presentation_plan_after_outbox_conn(
    conn: sqlite3.Connection,
    *,
    outbox_id: int,
    outbox_status: str,
    now: str,
) -> None:
    plan = conn.execute(
        """
        SELECT plans.id, plans.host_id, plans.name, plans.state
        FROM turn_presentation_jobs AS jobs
        JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
        WHERE jobs.outbox_id = ?
        """,
        (int(outbox_id),),
    ).fetchone()
    if plan is not None:
        plan_id = int(plan[0])
        if outbox_status == _CONNECTOR_EXHAUSTED_OUTBOX_STATUS and str(plan[3]) in {
            "active",
            "waiting_predecessor",
        }:
            conn.execute(
                """
                UPDATE turn_presentation_plans
                SET state = 'failed'
                WHERE id = ?
                """,
                (plan_id,),
            )
        elif outbox_status == _CONNECTOR_TERMINAL_OUTBOX_STATUS and str(plan[3]) == "active":
            remaining = conn.execute(
                """
                SELECT COUNT(*)
                FROM turn_presentation_jobs AS jobs
                JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
                WHERE jobs.plan_id = ? AND outbox.status != 'delivered'
                """,
                (plan_id,),
            ).fetchone()
            if int(remaining[0] or 0) == 0:
                conn.execute(
                    """
                    UPDATE turn_presentation_plans
                    SET state = 'completed', completed_at = COALESCE(completed_at, ?)
                    WHERE id = ? AND state = 'active'
                    """,
                    (str(now), plan_id),
                )
        _activate_waiting_presentation_plans_conn(
            conn,
            host_id=str(plan[1]),
            name=str(plan[2]),
            now=str(now),
        )


def _mark_exhausted_presentation_plans_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str | None,
    now: str,
) -> None:
    params: list[Any] = [str(host_id)]
    connector_clause = ""
    if name is not None:
        connector_clause = "AND plans.name = ?"
        params.append(str(name))
    conn.execute(
        f"""
        UPDATE turn_presentation_plans AS plans
        SET state = 'failed'
        WHERE plans.host_id = ?
          {connector_clause}
          AND plans.state IN ('active', 'waiting_predecessor')
          AND EXISTS (
              SELECT 1
              FROM turn_presentation_jobs AS jobs
              JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
              WHERE jobs.plan_id = plans.id AND outbox.status = 'dead_letter'
          )
        """,
        params,
    )


def prepare_connector_plan_commit(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    plan_token: str,
    now: str | None = None,
) -> dict[str, Any]:
    """Atomically validate exact coverage and materialize one plan's ordered jobs."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
            plan_token=plan_token,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or not _valid_presentation_opaque(plan_token, "twplan1.")
    ):
        return _presentation_error(
            "invalid_params",
            host_id=host_id,
            name=name,
        )
    current_time = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            plan = _presentation_plan_row_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                plan_token=str(plan_token),
            )
            if plan is None:
                conn.rollback()
                return _presentation_error(
                    "plan_not_found",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            plan_id = int(plan[0])
            plan_state = str(plan[5])
            job_count_row = conn.execute(
                """
                SELECT COUNT(*)
                FROM turn_presentation_jobs
                WHERE plan_id = ? AND outbox_id IS NOT NULL
                """,
                (plan_id,),
            ).fetchone()
            materialized_job_count = int(job_count_row[0] or 0)
            if materialized_job_count > 0:
                conn.commit()
                return _presentation_response(
                    {
                        "schema_version": _PRESENTATION_SCHEMA_VERSION,
                        "ok": True,
                        "status": "ok",
                        "host_id": str(host_id),
                        "name": str(name),
                        "plan_token": str(plan_token),
                        "state": plan_state,
                        "job_count": materialized_job_count,
                        "generation": int(plan[7]),
                    }
                )
            if plan_state != "preparing":
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            revision_row, revision_error = _current_presentation_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(plan[1]),
                content_revision_value=str(plan[2]),
            )
            if revision_error is not None or revision_row is None:
                conn.rollback()
                return _presentation_error(
                    revision_error or "revision_conflict",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            staged = conn.execute(
                """
                SELECT id, part_ordinal, spans_json
                FROM turn_presentation_jobs
                WHERE plan_id = ? AND operation = 'upsert'
                ORDER BY part_ordinal
                """,
                (plan_id,),
            ).fetchall()
            expected_count = int(plan[4])
            if (
                len(staged) != expected_count
                or [int(row[1]) for row in staged] != list(range(expected_count))
                or not _presentation_exact_coverage(staged, revision_row=revision_row)
            ):
                conn.rollback()
                return _presentation_error(
                    "plan_incomplete",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            predecessor = conn.execute(
                """
                SELECT id, plan_token
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND id != ?
                  AND activated_at IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(host_id), str(name), str(plan[1]), plan_id),
            ).fetchone()
            replaces_token = str(predecessor[1]) if predecessor is not None else None
            completed_baseline = conn.execute(
                """
                SELECT COALESCE(MAX(id), 0)
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND id != ?
                  AND completed_at IS NOT NULL
                """,
                (str(host_id), str(name), str(plan[1]), plan_id),
            ).fetchone()
            baseline_id = int(completed_baseline[0] or 0)
            footprint = conn.execute(
                """
                SELECT COALESCE(MAX(part_count), 0)
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND id != ?
                  AND activated_at IS NOT NULL
                  AND id >= ?
                """,
                (
                    str(host_id),
                    str(name),
                    str(plan[1]),
                    plan_id,
                    baseline_id,
                ),
            ).fetchone()
            prior_part_count = int(footprint[0] or 0)
            common = {
                "schema_version": _PRESENTATION_SCHEMA_VERSION,
                "plan_token": str(plan_token),
                "content_revision": str(plan[2]),
                "presentation_version": str(plan[3]),
                "part_count": expected_count,
                "replaces_plan_token": replaces_token,
            }
            for job_id, part_ordinal, spans_json in staged:
                sequence = int(part_ordinal)
                payload = {
                    **common,
                    "operation": "upsert",
                    "sequence_index": sequence,
                    "part_ordinal": int(part_ordinal),
                    "spans": json.loads(str(spans_json)),
                }
                _materialize_connector_plan_job_conn(
                    conn,
                    plan_id=plan_id,
                    job_id=int(job_id),
                    host_id=str(host_id),
                    name=str(name),
                    delivery_key=(
                        f"{name}:{plan_token}:"
                        f"{sequence:0{_PRESENTATION_SEQUENCE_WIDTH}d}"
                    ),
                    payload=payload,
                    created_at=current_time,
                )
            sequence = expected_count
            for old_ordinal in range(prior_part_count - 1, expected_count - 1, -1):
                cursor = conn.execute(
                    """
                    INSERT INTO turn_presentation_jobs (
                        plan_id,
                        sequence_index,
                        operation,
                        part_ordinal,
                        spans_json,
                        created_at
                    ) VALUES (?, ?, 'retire', ?, '[]', ?)
                    """,
                    (plan_id, sequence, int(old_ordinal), current_time),
                )
                payload = {
                    **common,
                    "operation": "retire",
                    "sequence_index": sequence,
                    "part_ordinal": int(old_ordinal),
                    "spans": [],
                }
                _materialize_connector_plan_job_conn(
                    conn,
                    plan_id=plan_id,
                    job_id=int(cursor.lastrowid),
                    host_id=str(host_id),
                    name=str(name),
                    delivery_key=(
                        f"{name}:{plan_token}:"
                        f"{sequence:0{_PRESENTATION_SEQUENCE_WIDTH}d}"
                    ),
                    payload=payload,
                    created_at=current_time,
                )
                sequence += 1
            leased_predecessor = _mark_obsolete_presentation_plans_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                turn_id=str(plan[1]),
                keep_plan_id=plan_id,
                now=current_time,
            )
            next_state = "waiting_predecessor" if leased_predecessor else "active"
            conn.execute(
                """
                UPDATE turn_presentation_plans
                SET state = ?,
                    replaces_plan_token = ?,
                    activated_at = CASE WHEN ? = 'active' THEN ? ELSE NULL END
                WHERE id = ? AND state = 'preparing'
                """,
                (
                    next_state,
                    replaces_token,
                    next_state,
                    current_time,
                    plan_id,
                ),
            )
            conn.commit()
            return _presentation_response(
                {
                    "schema_version": _PRESENTATION_SCHEMA_VERSION,
                    "ok": True,
                    "status": "ok",
                    "host_id": str(host_id),
                    "name": str(name),
                    "plan_token": str(plan_token),
                    "state": next_state,
                    "job_count": sequence,
                    "generation": int(plan[7]),
                }
            )
        except Exception:
            conn.rollback()
            raise



def _presentation_recovery_result(
    *,
    failed_plan_token: str,
    plan_token: str,
    generation: int,
    content_revision: str,
    acknowledged_prefix_count: int,
    executable_job_count: int,
    retained_failed_job_count: int,
    prior_attempt_count: int,
    idempotent_replay: bool,
) -> dict[str, Any]:
    return _presentation_response(
        {
            "schema_version": _PRESENTATION_SCHEMA_VERSION,
            "ok": True,
            "status": "recovered",
            "failed_plan_token": str(failed_plan_token),
            "plan_token": str(plan_token),
            "generation": int(generation),
            "content_revision": str(content_revision),
            "state": "active",
            "acknowledged_prefix_count": int(acknowledged_prefix_count),
            "executable_job_count": int(executable_job_count),
            "retained_failed_job_count": int(retained_failed_job_count),
            "prior_attempt_count": int(prior_attempt_count),
            "idempotent_replay": bool(idempotent_replay),
        }
    )


def prepare_connector_plan_recover(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    failed_plan_token: str,
    request_id: str,
    now: str | None = None,
) -> dict[str, Any]:
    """Explicitly replace one failed immutable plan with its unfinished suffix."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
            failed_plan_token=failed_plan_token,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or not _valid_presentation_opaque(failed_plan_token, "twplan1.")
        or not _valid_presentation_label(request_id)
    ):
        return _presentation_error(
            "invalid_params",
            host_id=host_id,
            name=name,
            failed_plan_token=failed_plan_token,
        )
    current_time = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            prior_request = conn.execute(
                """
                SELECT
                    audit.failed_plan_token,
                    audit.recovered_plan_token,
                    audit.generation,
                    plans.content_revision,
                    audit.delivered_prefix_count,
                    audit.fresh_job_count,
                    audit.retained_failed_job_count,
                    audit.prior_attempt_count
                FROM turn_presentation_recoveries AS audit
                JOIN turn_presentation_plans AS plans
                  ON plans.id = audit.recovered_plan_id
                WHERE audit.host_id = ? AND audit.name = ? AND audit.request_id = ?
                """,
                (str(host_id), str(name), str(request_id)),
            ).fetchone()
            if prior_request is not None:
                if str(prior_request[0]) != str(failed_plan_token):
                    conn.rollback()
                    return _presentation_error(
                        "request_conflict",
                        host_id=host_id,
                        name=name,
                        failed_plan_token=failed_plan_token,
                    )
                conn.commit()
                return _presentation_recovery_result(
                    failed_plan_token=str(prior_request[0]),
                    plan_token=str(prior_request[1]),
                    generation=int(prior_request[2]),
                    content_revision=str(prior_request[3]),
                    acknowledged_prefix_count=int(prior_request[4]),
                    executable_job_count=int(prior_request[5]),
                    retained_failed_job_count=int(prior_request[6]),
                    prior_attempt_count=int(prior_request[7]),
                    idempotent_replay=True,
                )
            failed = conn.execute(
                """
                SELECT
                    id,
                    turn_id,
                    content_revision,
                    presentation_version,
                    generation,
                    part_count,
                    state
                FROM turn_presentation_plans
                WHERE host_id = ? AND name = ? AND plan_token = ?
                """,
                (str(host_id), str(name), str(failed_plan_token)),
            ).fetchone()
            if failed is None:
                conn.rollback()
                return _presentation_error(
                    "plan_not_found",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            if str(failed[6]) != "failed":
                conn.rollback()
                return _presentation_error(
                    "plan_not_failed",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            _, revision_error = _current_presentation_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(failed[1]),
                content_revision_value=str(failed[2]),
            )
            if revision_error is not None:
                conn.rollback()
                return _presentation_error(
                    revision_error,
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            latest_generation = conn.execute(
                """
                SELECT MAX(generation)
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND content_revision = ?
                  AND presentation_version = ?
                """,
                (
                    str(host_id),
                    str(name),
                    str(failed[1]),
                    str(failed[2]),
                    str(failed[3]),
                ),
            ).fetchone()
            if int(latest_generation[0] or 0) != int(failed[4]):
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            inherited_audit = conn.execute(
                """
                SELECT
                    delivered_prefix_count,
                    retained_failed_job_count,
                    prior_attempt_count
                FROM turn_presentation_recoveries
                WHERE recovered_plan_id = ?
                """,
                (int(failed[0]),),
            ).fetchone()
            inherited_prefix_count = (
                int(inherited_audit[0]) if inherited_audit is not None else 0
            )
            inherited_failed_count = (
                int(inherited_audit[1]) if inherited_audit is not None else 0
            )
            inherited_attempt_count = (
                int(inherited_audit[2]) if inherited_audit is not None else 0
            )
            source_jobs = conn.execute(
                """
                SELECT
                    jobs.sequence_index,
                    jobs.operation,
                    jobs.part_ordinal,
                    jobs.spans_json,
                    outbox.delivery_key,
                    outbox.status,
                    outbox.payload_json
                FROM turn_presentation_jobs AS jobs
                JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
                WHERE jobs.plan_id = ?
                ORDER BY jobs.sequence_index
                """,
                (int(failed[0]),),
            ).fetchall()
            if (
                not source_jobs
                or [int(row[0]) for row in source_jobs]
                != list(
                    range(
                        inherited_prefix_count,
                        inherited_prefix_count + len(source_jobs),
                    )
                )
            ):
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            local_acknowledged_count = 0
            for source_job in source_jobs:
                if str(source_job[5]) != "delivered":
                    break
                local_acknowledged_count += 1
            acknowledged_prefix_count = (
                inherited_prefix_count + local_acknowledged_count
            )
            suffix = source_jobs[local_acknowledged_count:]
            retained_failed_job_count = inherited_failed_count + sum(
                str(row[5]) == "dead_letter" for row in suffix
            )
            if (
                not suffix
                or retained_failed_job_count <= inherited_failed_count
                or any(str(row[5]) in {"delivered", "leased"} for row in suffix)
            ):
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            current_attempt_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM connector_deliveries AS deliveries
                    JOIN turn_presentation_jobs AS jobs
                      ON jobs.outbox_id = deliveries.outbox_id
                    WHERE jobs.plan_id = ?
                    """,
                    (int(failed[0]),),
                ).fetchone()[0]
                or 0
            )
            prior_attempt_count = inherited_attempt_count + current_attempt_count
            if current_attempt_count < 1:
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            generation = int(failed[4]) + 1
            recovered_token = _presentation_recovery_token(
                host_id=str(host_id),
                name=str(name),
                failed_plan_token=str(failed_plan_token),
                request_id=str(request_id),
                generation=generation,
            )
            plan_cursor = conn.execute(
                """
                INSERT INTO turn_presentation_plans (
                    host_id,
                    name,
                    plan_token,
                    turn_id,
                    content_revision,
                    presentation_version,
                    generation,
                    part_count,
                    state,
                    replaces_plan_token,
                    recovers_plan_token,
                    created_at,
                    activated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)
                """,
                (
                    str(host_id),
                    str(name),
                    recovered_token,
                    str(failed[1]),
                    str(failed[2]),
                    str(failed[3]),
                    generation,
                    int(failed[5]),
                    str(failed_plan_token),
                    str(failed_plan_token),
                    current_time,
                    current_time,
                ),
            )
            recovered_plan_id = int(plan_cursor.lastrowid)
            common = {
                "schema_version": _PRESENTATION_SCHEMA_VERSION,
                "plan_token": recovered_token,
                "content_revision": str(failed[2]),
                "presentation_version": str(failed[3]),
                "part_count": int(failed[5]),
                "replaces_plan_token": str(failed_plan_token),
            }
            if local_acknowledged_count:
                common["predecessor_job_key"] = str(
                    source_jobs[local_acknowledged_count - 1][4]
                )
            elif inherited_prefix_count:
                inherited_payload = _json_object(source_jobs[0][6])
                predecessor_job_key = str(
                    inherited_payload.get("predecessor_job_key") or ""
                )
                if not predecessor_job_key:
                    conn.rollback()
                    return _presentation_error(
                        "plan_conflict",
                        host_id=host_id,
                        name=name,
                        failed_plan_token=failed_plan_token,
                    )
                common["predecessor_job_key"] = predecessor_job_key
            for (
                sequence_index,
                operation,
                part_ordinal,
                spans_json,
                _key,
                _status,
                _payload_json,
            ) in suffix:
                job_cursor = conn.execute(
                    """
                    INSERT INTO turn_presentation_jobs (
                        plan_id,
                        sequence_index,
                        operation,
                        part_ordinal,
                        spans_json,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        recovered_plan_id,
                        int(sequence_index),
                        str(operation),
                        int(part_ordinal),
                        str(spans_json),
                        current_time,
                    ),
                )
                payload = {
                    **common,
                    "operation": str(operation),
                    "sequence_index": int(sequence_index),
                    "part_ordinal": int(part_ordinal),
                    "spans": json.loads(str(spans_json)),
                }
                _materialize_connector_plan_job_conn(
                    conn,
                    plan_id=recovered_plan_id,
                    job_id=int(job_cursor.lastrowid),
                    host_id=str(host_id),
                    name=str(name),
                    delivery_key=(
                        f"{name}:{recovered_token}:"
                        f"{int(sequence_index):0{_PRESENTATION_SEQUENCE_WIDTH}d}"
                    ),
                    payload=payload,
                    created_at=current_time,
                )
            conn.execute(
                """
                INSERT INTO turn_presentation_recoveries (
                    host_id,
                    name,
                    request_id,
                    failed_plan_id,
                    recovered_plan_id,
                    failed_plan_token,
                    recovered_plan_token,
                    generation,
                    source_job_count,
                    delivered_prefix_count,
                    fresh_job_count,
                    retained_failed_job_count,
                    prior_attempt_count,
                    outcome,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'recovered', ?)
                """,
                (
                    str(host_id),
                    str(name),
                    str(request_id),
                    int(failed[0]),
                    recovered_plan_id,
                    str(failed_plan_token),
                    recovered_token,
                    generation,
                    len(source_jobs),
                    acknowledged_prefix_count,
                    len(suffix),
                    retained_failed_job_count,
                    prior_attempt_count,
                    current_time,
                ),
            )
            conn.commit()
            return _presentation_recovery_result(
                failed_plan_token=str(failed_plan_token),
                plan_token=recovered_token,
                generation=generation,
                content_revision=str(failed[2]),
                acknowledged_prefix_count=acknowledged_prefix_count,
                executable_job_count=len(suffix),
                retained_failed_job_count=retained_failed_job_count,
                prior_attempt_count=prior_attempt_count,
                idempotent_replay=False,
            )
        except Exception:
            conn.rollback()
            raise


def poll_connector_outbox(
    db_path: Path,
    host_id: str,
    name: str,
    *,
    limit: int = 1,
    lease_seconds: int = 60,
    max_attempts: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Atomically lease due connector outbox rows for one neutral queue name."""
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "name": str(name),
            "items": [],
        })
    current_time = _connector_now(now)
    lease_expires_at = _connector_add_seconds(current_time, max(1, int(lease_seconds)))
    row_limit = max(1, min(int(limit), 100))
    items: list[dict[str, Any]] = []
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _connector_reclaim_expired_leases_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                now=current_time,
            )
            if max_attempts is not None:
                _connector_exhaust_retryable_conn(
                    conn,
                    host_id=str(host_id),
                    name=str(name),
                    max_attempts=max_attempts,
                    now=current_time,
                )
                _mark_exhausted_presentation_plans_conn(
                    conn,
                    host_id=str(host_id),
                    name=str(name),
                    now=current_time,
                )
            rows = conn.execute(
                """
                SELECT
                    outbox.id,
                    outbox.delivery_key,
                    outbox.payload_json,
                    outbox.private_state_json
                FROM connector_outbox AS outbox
                WHERE outbox.host_id = ?
                  AND outbox.connector = ?
                  AND outbox.status IN ('queued', 'deferred', 'retry')
                  AND (
                      outbox.next_attempt_at IS NULL
                      OR outbox.next_attempt_at = ''
                      OR outbox.next_attempt_at <= ?
                  )
                  AND (
                      NOT EXISTS (
                          SELECT 1
                          FROM turn_presentation_jobs AS linked
                          WHERE linked.outbox_id = outbox.id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM turn_presentation_jobs AS current_job
                          JOIN turn_presentation_plans AS current_plan
                            ON current_plan.id = current_job.plan_id
                          WHERE current_job.outbox_id = outbox.id
                            AND current_plan.state = 'active'
                            AND NOT EXISTS (
                                SELECT 1
                                FROM turn_presentation_jobs AS predecessor
                                JOIN connector_outbox AS predecessor_outbox
                                  ON predecessor_outbox.id = predecessor.outbox_id
                                WHERE predecessor.plan_id = current_job.plan_id
                                  AND predecessor.sequence_index
                                      < current_job.sequence_index
                                  AND predecessor_outbox.status != 'delivered'
                            )
                      )
                  )
                ORDER BY outbox.id
                LIMIT ?
                """,
                (str(host_id), str(name), current_time, row_limit),
            ).fetchall()
            for row in rows:
                outbox_id = int(row[0])
                attempt_row = conn.execute(
                    """
                    SELECT COALESCE(MAX(attempt), 0)
                    FROM connector_deliveries
                    WHERE outbox_id = ?
                    """,
                    (outbox_id,),
                ).fetchone()
                attempt = int(attempt_row[0] or 0) + 1
                lease_token = secrets.token_urlsafe(24)
                public_ref = _connector_public_ref()
                cursor = conn.execute(
                    """
                    INSERT INTO connector_deliveries (
                        outbox_id,
                        host_id,
                        connector,
                        delivery_key,
                        attempt,
                        status,
                        response_json,
                        private_state_json,
                        created_at,
                        delivered_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        outbox_id,
                        str(host_id),
                        str(name),
                        str(row[1]),
                        attempt,
                        _CONNECTOR_LEASE_STATUS,
                        _canonical_json(sanitize_public_mapping({})),
                        _connector_private_with_lease(
                            {},
                            delivery_id=None,
                            attempt=attempt,
                            lease_token=lease_token,
                            lease_expires_at=lease_expires_at,
                            public_ref=public_ref,
                        ),
                        current_time,
                        None,
                    ),
                )
                delivery_id = int(cursor.lastrowid)
                conn.execute(
                    """
                    UPDATE connector_deliveries
                    SET private_state_json = ?
                    WHERE id = ?
                    """,
                    (
                        _connector_private_with_lease(
                            {},
                            delivery_id=delivery_id,
                            attempt=attempt,
                            lease_token=lease_token,
                            lease_expires_at=lease_expires_at,
                            public_ref=public_ref,
                        ),
                        delivery_id,
                    ),
                )
                conn.execute(
                    """
                    UPDATE connector_outbox
                    SET status = ?, updated_at = ?, private_state_json = ?
                    WHERE id = ? AND status IN ('queued', 'deferred', 'retry')
                    """,
                    (
                        _CONNECTOR_LEASE_STATUS,
                        current_time,
                        _connector_private_with_lease(
                            row[3],
                            delivery_id=delivery_id,
                            attempt=attempt,
                            lease_token=lease_token,
                            lease_expires_at=lease_expires_at,
                            public_ref=public_ref,
                        ),
                        outbox_id,
                    ),
                )
                items.append(
                    {
                        "outbox_id": outbox_id,
                        "delivery_id": delivery_id,
                        "host_id": str(host_id),
                        "name": str(name),
                        "key": str(row[1]),
                        "attempt": attempt,
                        "lease_token": lease_token,
                        "leased_until": lease_expires_at,
                        "ref": public_ref,
                        "available_at": current_time,
                        "payload": _restore_presentation_tokens(
                            sanitize_public_mapping(
                                _json_object(row[2]),
                                backend_neutral=True,
                            ),
                            _json_object(row[2]),
                        ),
                    }
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    result = dict(sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "name": str(name),
        "items": items,
    }))
    sanitized_items = result.get("items")
    if isinstance(sanitized_items, list):
        for sanitized_item, original_item in zip(sanitized_items, items, strict=True):
            if not isinstance(sanitized_item, dict):
                continue
            sanitized_payload = sanitized_item.get("payload")
            original_payload = original_item.get("payload")
            if isinstance(sanitized_payload, dict) and isinstance(original_payload, Mapping):
                _restore_presentation_tokens(sanitized_payload, original_payload)
    return result


def _connector_validate_live_ref_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    ref: str,
    now: str,
) -> tuple[Any | None, str | None]:
    rows = conn.execute(
        """
        SELECT
            d.id,
            d.outbox_id,
            d.host_id,
            d.connector,
            d.delivery_key,
            d.attempt,
            d.status,
            d.private_state_json,
            o.status,
            o.private_state_json
        FROM connector_deliveries d
        LEFT JOIN connector_outbox o ON o.id = d.outbox_id
        WHERE d.host_id = ? AND d.connector = ? AND d.status = ?
        ORDER BY d.id DESC
        """,
        (str(host_id), str(name), _CONNECTOR_LEASE_STATUS),
    ).fetchall()
    for row in rows:
        delivery_state = _json_object(row[7])
        if str(delivery_state.get("public_ref") or "") != str(ref):
            continue
        if str(row[6] or "") != _CONNECTOR_LEASE_STATUS:
            return row, "stale_ref"
        outbox_state = _json_object(row[9])
        if int(outbox_state.get("current_delivery_id") or 0) != int(row[0]):
            return row, "stale_ref"
        if str(row[8] or "") != _CONNECTOR_LEASE_STATUS:
            return row, "stale_ref"
        lease_expires_at = str(delivery_state.get("lease_expires_at") or "")
        if not lease_expires_at or _connector_datetime(lease_expires_at) <= _connector_datetime(now):
            return row, "expired_ref"
        return row, None
    return None, "invalid_ref"


def _connector_update_ref(
    db_path: Path,
    *,
    action: str,
    host_id: str,
    name: str,
    ref: str,
    response: Mapping[str, Any] | None = None,
    reason: str | None = None,
    available_at: str | None = None,
    delay_seconds: int | None = None,
    max_attempts: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    if not _sqlite_store_exists(db_path):
        return _connector_error_response(status="store_unavailable", host_id=host_id, name=name, ref=ref)
    current_time = _connector_now(now)
    sanitized_response = sanitize_public_mapping(response or {}, backend_neutral=True)
    sanitized_reason = _connector_public_reason(reason)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _connector_reclaim_expired_leases_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                now=current_time,
            )
            row, error = _connector_validate_live_ref_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                ref=str(ref),
                now=current_time,
            )
            if error is not None or row is None:
                conn.rollback()
                return _connector_error_response(status=error or "invalid_ref", host_id=host_id, name=name, ref=ref)

            delivery_id = int(row[0])
            outbox_id = int(row[1] or 0)
            delivery_key = str(row[4])
            attempt = int(row[5] or 0)
            if action == "ack":
                response_json = _canonical_json(
                    sanitize_public_value({
                        "schema_version": 1,
                        "status": "acknowledged",
                        "response": dict(sanitized_response),
                    })
                )
                conn.execute(
                    """
                    UPDATE connector_deliveries
                    SET status = ?, response_json = ?, delivered_at = ?
                    WHERE id = ?
                    """,
                    ("delivered", response_json, current_time, int(delivery_id)),
                )
                conn.execute(
                    """
                    UPDATE connector_outbox
                    SET status = ?, next_attempt_at = NULL, updated_at = ?, private_state_json = ?
                    WHERE id = ?
                    """,
                    (
                        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
                        current_time,
                        _connector_private_clear_current(row[9]),
                        int(outbox_id),
                    ),
                )
                migration_group = str(
                    _json_object(row[9]).get("migration_group") or ""
                )
                if migration_group:
                    conn.execute(
                        """
                        UPDATE connector_outbox
                        SET status = ?, next_attempt_at = NULL, updated_at = ?
                        WHERE id != ? AND status IN ('queued', 'retry', 'deferred')
                          AND json_extract(private_state_json, '$.migration_group') = ?
                        """,
                        (
                            _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
                            current_time,
                            outbox_id,
                            migration_group,
                        ),
                    )
                    leased_siblings = conn.execute(
                        """
                        SELECT id, private_state_json
                        FROM connector_outbox
                        WHERE id != ? AND status = 'leased'
                          AND json_extract(
                              private_state_json, '$.migration_group'
                          ) = ?
                        """,
                        (outbox_id, migration_group),
                    ).fetchall()
                    for sibling_id, sibling_private in leased_siblings:
                        conn.execute(
                            """
                            UPDATE connector_outbox
                            SET private_state_json = ?
                            WHERE id = ? AND status = 'leased'
                            """,
                            (
                                _migration_private_state(
                                    sibling_private,
                                    group=migration_group,
                                    canonical=False,
                                    terminal_after_lease=True,
                                ),
                                int(sibling_id),
                            ),
                        )
                _update_presentation_plan_after_outbox_conn(
                    conn,
                    outbox_id=outbox_id,
                    outbox_status=_CONNECTOR_TERMINAL_OUTBOX_STATUS,
                    now=current_time,
                )
                conn.commit()
                return _connector_response(
                    ok=True,
                    status="acknowledged",
                    host_id=host_id,
                    name=name,
                    ref=ref,
                    key=delivery_key,
                    attempt=attempt,
                )

            if available_at is None:
                available_at = _connector_add_seconds(
                    current_time,
                    60 if delay_seconds is None else int(delay_seconds),
                )
            else:
                available_at = _connector_iso(available_at)
            attempt_limit = max(1, int(max_attempts)) if max_attempts is not None else None
            exhausted = action == "fail" and attempt_limit is not None and attempt >= attempt_limit
            result_status = "attempts_exhausted" if exhausted else ("retry_scheduled" if action == "fail" else "deferred")
            delivery_status = "failed" if action == "fail" else "deferred"
            outbox_status = (
                _CONNECTOR_EXHAUSTED_OUTBOX_STATUS
                if exhausted
                else ("retry" if action == "fail" else "deferred")
            )
            terminal_after_lease = bool(
                _json_object(row[9]).get("terminal_after_lease")
            )
            if terminal_after_lease:
                result_status = "superseded"
                outbox_status = _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
            response_json = _canonical_json(
                sanitize_public_value({
                    "schema_version": 1,
                    "status": result_status,
                    "reason": sanitized_reason,
                    "available_at": None if terminal_after_lease else available_at,
                    "response": dict(sanitized_response),
                })
            )
            conn.execute(
                """
                UPDATE connector_deliveries
                SET status = ?, response_json = ?, delivered_at = ?
                WHERE id = ?
                """,
                (delivery_status, response_json, current_time, int(delivery_id)),
            )
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = ?, next_attempt_at = ?, updated_at = ?, private_state_json = ?
                WHERE id = ?
                """,
                (
                    outbox_status,
                    None if exhausted or terminal_after_lease else available_at,
                    current_time,
                    _connector_private_clear_current(row[9]),
                    int(outbox_id),
                ),
            )
            _update_presentation_plan_after_outbox_conn(
                conn,
                outbox_id=outbox_id,
                outbox_status=outbox_status,
                now=current_time,
            )
            conn.commit()
            return _connector_response(
                ok=True,
                status=result_status,
                host_id=host_id,
                name=name,
                ref=ref,
                key=delivery_key,
                attempt=attempt,
                available_at=None if exhausted or terminal_after_lease else available_at,
            )
        except Exception:
            conn.rollback()
            raise


def ack_connector_delivery(
    db_path: Path,
    *,
    host_id: str,
    name: str,
    ref: str,
    response: Mapping[str, Any] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Acknowledge a live connector lease and make the outbox item terminal."""
    return _connector_update_ref(
        db_path,
        action="ack",
        host_id=host_id,
        name=name,
        ref=ref,
        response=response,
        now=now,
    )


def fail_connector_delivery(
    db_path: Path,
    *,
    host_id: str,
    name: str,
    ref: str,
    reason: str | None = None,
    response: Mapping[str, Any] | None = None,
    available_at: str | None = None,
    delay_seconds: int | None = None,
    max_attempts: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Record a connector failure and schedule the outbox item for retry."""
    return _connector_update_ref(
        db_path,
        action="fail",
        host_id=host_id,
        name=name,
        ref=ref,
        reason=reason,
        response=response,
        available_at=available_at,
        delay_seconds=delay_seconds,
        max_attempts=max_attempts,
        now=now,
    )


def defer_connector_delivery(
    db_path: Path,
    *,
    host_id: str,
    name: str,
    ref: str,
    reason: str | None = None,
    response: Mapping[str, Any] | None = None,
    available_at: str | None = None,
    delay_seconds: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Record a connector deferral and make the outbox item available later."""
    return _connector_update_ref(
        db_path,
        action="defer",
        host_id=host_id,
        name=name,
        ref=ref,
        reason=reason,
        response=response,
        available_at=available_at,
        delay_seconds=delay_seconds,
        now=now,
    )


def _snapshot_dict(snapshot: Snapshot) -> dict[str, Any]:
    if hasattr(snapshot, "to_dict"):
        data = snapshot.to_dict()
    else:
        data = json.loads(snapshot.to_json())
    return dict(data)


def _sort_observations(value: Any) -> Any:
    if not isinstance(value, list):
        return value
    if not all(isinstance(item, Mapping) for item in value):
        return value
    return sorted(
        (dict(item) for item in value),
        key=lambda item: (
            str(item.get("id") or item.get("fingerprint") or ""),
            str(item.get("fingerprint") or ""),
            _canonical_json(item),
        ),
    )


def _strip_content_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_content_volatile(item)
            for key, item in value.items()
            if str(key).lower() not in {"updated_at", "observed_at", "content_fingerprint"}
        }
    if isinstance(value, list | tuple):
        return [_strip_content_volatile(item) for item in value]
    return value


def _fingerprint_input(data: Mapping[str, Any]) -> dict[str, Any]:
    fingerprint_data = dict(_strip_content_volatile(data))
    for collection in ("spaces", "workers", "attention"):
        if collection in fingerprint_data:
            fingerprint_data[collection] = _sort_observations(
                fingerprint_data[collection]
            )
    return fingerprint_data


def _content_fingerprint(data: Mapping[str, Any]) -> str:
    raw = data.get("content_fingerprint")
    if isinstance(raw, str) and raw:
        return raw
    return stable_fingerprint(
        _fingerprint_input(data),
        length=FINGERPRINT_HEX_LENGTH,
    )


def _command_receipt_from_row(row: Any) -> dict[str, Any]:
    return {
        "host_id": row[0],
        "request_id": row[1],
        "action": row[2],
        "payload_fingerprint": row[3],
        "status": row[4],
        "result_json": row[5],
        "created_at": row[6],
        "completed_at": row[7],
        "uncertain": bool(row[8]),
    }


def _worker_binding_from_row(row: Any) -> WorkerBinding:
    return WorkerBinding(
        host_id=row[0],
        worker_id=row[1],
        worker_fingerprint=row[2],
        backend=row[3],
        target_kind=row[4],
        target_value=row[5],
        turn_target_kind=row[6],
        turn_target_value=row[7],
        sendable=bool(row[8]),
        reason=row[9],
        observed_at=row[10],
        expires_at=row[11],
        private_fingerprint=row[12],
    )


def _dedupe_command_receipts(conn: sqlite3.Connection) -> None:
    """Keep the latest legacy receipt per logical key using bounded batches."""
    while True:
        delete_ids = [
            int(row[0])
            for row in conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY host_id, request_id, action
                            ORDER BY
                                COALESCE(completed_at, created_at) DESC,
                                created_at DESC,
                                id DESC
                        ) AS receipt_rank
                    FROM command_receipts
                )
                SELECT id
                FROM ranked
                WHERE receipt_rank > 1
                ORDER BY id
                LIMIT 500
                """
            ).fetchall()
        ]
        if not delete_ids:
            return
        placeholders = ",".join("?" for _ in delete_ids)
        conn.execute(
            f"DELETE FROM command_receipts WHERE id IN ({placeholders})",
            delete_ids,
        )


def _ensure_command_receipt_unique_index(conn: sqlite3.Connection) -> None:
    for row in conn.execute("PRAGMA index_list(command_receipts)").fetchall():
        index_name = str(row[1])
        is_unique = int(row[2]) == 1
        if index_name == "ux_command_receipts_host_request_action" and not is_unique:
            conn.execute("DROP INDEX ux_command_receipts_host_request_action")
            break
    conn.execute(CREATE_COMMAND_RECEIPT_UNIQUE_INDEX)


def _latest_command_receipt_row(
    conn: sqlite3.Connection,
    host_id: str,
    request_id: str,
    action: str,
) -> Any:
    return conn.execute(
        """
        SELECT
            host_id,
            request_id,
            action,
            payload_fingerprint,
            status,
            result_json,
            created_at,
            completed_at,
            uncertain
        FROM command_receipts
        WHERE host_id = ? AND request_id = ? AND action = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (str(host_id), str(request_id), str(action)),
    ).fetchone()


def _snapshot_payload(data: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    payload_data = sanitize_public_mapping(data)
    payload_data.setdefault("schema_version", SCHEMA_VERSION)
    fingerprint = _content_fingerprint(payload_data)
    payload_data["content_fingerprint"] = fingerprint
    return payload_data, fingerprint


def _table_columns(conn: sqlite3.Connection, table: str = "snapshots") -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]) for row in rows}


def _ensure_columns(
    conn: sqlite3.Connection,
    table: str,
    columns: Mapping[str, str],
) -> None:
    existing = _table_columns(conn, table)
    for column, definition in columns.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _backfill_content_fingerprints(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT id, payload
        FROM snapshots
        WHERE content_fingerprint IS NULL OR content_fingerprint = ''
        """
    ).fetchall()
    for row_id, payload in rows:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            fingerprint = _content_fingerprint({"payload": payload})
            conn.execute(
                "UPDATE snapshots SET content_fingerprint = ? WHERE id = ?",
                (fingerprint, row_id),
            )
            continue
        if not isinstance(data, Mapping):
            fingerprint = _content_fingerprint({"payload": data})
            conn.execute(
                "UPDATE snapshots SET content_fingerprint = ? WHERE id = ?",
                (fingerprint, row_id),
            )
            continue
        payload_data, fingerprint = _snapshot_payload(
            Snapshot.from_dict(data).to_dict()
        )
        conn.execute(
            """
            UPDATE snapshots
            SET content_fingerprint = ?, payload = ?
            WHERE id = ?
            """,
            (fingerprint, _canonical_json(payload_data), row_id),
        )


def _ensure_command_receipt_columns(conn: sqlite3.Connection) -> None:
    _ensure_columns(
        conn,
        "command_receipts",
        {
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "request_id": "TEXT NOT NULL DEFAULT ''",
            "action": "TEXT NOT NULL DEFAULT ''",
            "payload_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT ''",
            "result_json": "TEXT NOT NULL DEFAULT '{}'",
            "created_at": "TEXT NOT NULL DEFAULT ''",
            "completed_at": "TEXT",
            "uncertain": "INTEGER NOT NULL DEFAULT 0",
        },
    )


def _ensure_worker_binding_columns(conn: sqlite3.Connection) -> None:
    _ensure_columns(
        conn,
        "worker_bindings",
        {
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "worker_id": "TEXT NOT NULL DEFAULT ''",
            "worker_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "backend": "TEXT NOT NULL DEFAULT ''",
            "target_kind": "TEXT NOT NULL DEFAULT ''",
            "target_value": "TEXT NOT NULL DEFAULT ''",
            "turn_target_kind": "TEXT",
            "turn_target_value": "TEXT",
            "sendable": "INTEGER NOT NULL DEFAULT 0",
            "reason": "TEXT",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "expires_at": "TEXT NOT NULL DEFAULT '9999-12-31T23:59:59+00:00'",
            "private_fingerprint": "TEXT NOT NULL DEFAULT ''",
        },
    )


def _ensure_pr6_columns(conn: sqlite3.Connection) -> None:
    _ensure_columns(
        conn,
        "events",
        {
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "event_type": "TEXT NOT NULL DEFAULT ''",
            "aggregate_type": "TEXT NOT NULL DEFAULT ''",
            "aggregate_id": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "spaces",
        {
            "name": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "updated_at": "TEXT",
            "fingerprint": "TEXT NOT NULL DEFAULT ''",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "workers",
        {
            "worker_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "space_id": "TEXT",
            "name": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "last_seen_at": "TEXT",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "turns",
        {
            "worker_id": "TEXT NOT NULL DEFAULT ''",
            "worker_fingerprint": "TEXT",
            "space_id": "TEXT",
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "kind": "TEXT NOT NULL DEFAULT 'unknown'",
            "updated_at": "TEXT",
            "fingerprint": "TEXT NOT NULL DEFAULT ''",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "pending_interactions",
        {
            "worker_id": "TEXT NOT NULL DEFAULT ''",
            "worker_fingerprint": "TEXT",
            "space_id": "TEXT",
            "kind": "TEXT NOT NULL DEFAULT 'unknown'",
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "updated_at": "TEXT",
            "fingerprint": "TEXT NOT NULL DEFAULT ''",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "attention_items",
        {
            "source": "TEXT NOT NULL DEFAULT ''",
            "kind": "TEXT NOT NULL DEFAULT 'unknown'",
            "severity": "TEXT NOT NULL DEFAULT 'info'",
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "updated_at": "TEXT",
            "fingerprint": "TEXT NOT NULL DEFAULT ''",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "first_seen_at": "TEXT NOT NULL DEFAULT ''",
            "last_seen_at": "TEXT NOT NULL DEFAULT ''",
            "last_changed_at": "TEXT NOT NULL DEFAULT ''",
            "resolved_at": "TEXT",
            "lifecycle_status": "TEXT NOT NULL DEFAULT 'open'",
            "resolved_reason": "TEXT",
            "signal_count": "INTEGER NOT NULL DEFAULT 1",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "commands",
        {
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "request_id": "TEXT NOT NULL DEFAULT ''",
            "action": "TEXT NOT NULL DEFAULT ''",
            "payload_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT ''",
            "dry_run": "INTEGER NOT NULL DEFAULT 0",
            "uncertain": "INTEGER NOT NULL DEFAULT 0",
            "request_json": "TEXT NOT NULL DEFAULT '{}'",
            "result_json": "TEXT NOT NULL DEFAULT '{}'",
            "created_at": "TEXT NOT NULL DEFAULT ''",
            "reserved_at": "TEXT",
            "completed_at": "TEXT",
            "updated_at": "TEXT NOT NULL DEFAULT ''",
        },
    )
    _ensure_columns(
        conn,
        "connector_outbox",
        {
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "connector": "TEXT NOT NULL DEFAULT ''",
            "delivery_key": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
            "private_state_json": "TEXT NOT NULL DEFAULT '{}'",
            "created_at": "TEXT NOT NULL DEFAULT ''",
            "updated_at": "TEXT NOT NULL DEFAULT ''",
            "next_attempt_at": "TEXT",
        },
    )
    _ensure_columns(
        conn,
        "connector_deliveries",
        {
            "outbox_id": "INTEGER",
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "connector": "TEXT NOT NULL DEFAULT ''",
            "delivery_key": "TEXT NOT NULL DEFAULT ''",
            "attempt": "INTEGER NOT NULL DEFAULT 0",
            "status": "TEXT NOT NULL DEFAULT ''",
            "response_json": "TEXT NOT NULL DEFAULT '{}'",
            "private_state_json": "TEXT NOT NULL DEFAULT '{}'",
            "created_at": "TEXT NOT NULL DEFAULT ''",
            "delivered_at": "TEXT",
        },
    )
    _ensure_columns(
        conn,
        "backend_health",
        {
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "outcome": "TEXT NOT NULL DEFAULT 'unknown'",
            "observed_at": "TEXT",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )


def _append_event_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    event_type: str,
    payload: Mapping[str, Any],
    aggregate_type: str = "",
    aggregate_id: str = "",
    observed_at: str | None = None,
    content_fingerprint: str | None = None,
) -> int:
    payload_json = _canonical_json(payload)
    fingerprint = content_fingerprint or stable_fingerprint(
        {"event_type": event_type, "payload": payload}
    )
    cursor = conn.execute(
        """
        INSERT INTO events (
            host_id,
            event_type,
            aggregate_type,
            aggregate_id,
            observed_at,
            content_fingerprint,
            payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(host_id),
            str(event_type),
            str(aggregate_type),
            str(aggregate_id),
            observed_at or utc_timestamp(),
            str(fingerprint),
            payload_json,
        ),
    )
    return int(cursor.lastrowid)


def append_event(
    db_path: Path,
    host_id: str,
    event_type: str,
    payload: Mapping[str, Any],
    *,
    aggregate_type: str = "",
    aggregate_id: str = "",
    observed_at: str | None = None,
    content_fingerprint: str | None = None,
) -> int:
    """Append a private store event and return its row id."""
    with _connect(db_path, prepare=True) as conn:
        _ensure_schema(conn)
        return _append_event_conn(
            conn,
            host_id=host_id,
            event_type=event_type,
            payload=payload,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            observed_at=observed_at,
            content_fingerprint=content_fingerprint,
        )


def _prune_host_projection(
    conn: sqlite3.Connection,
    table: str,
    key_column: str,
    host_id: str,
    keep_ids: Iterable[str],
) -> None:
    ids = sorted({str(value) for value in keep_ids})
    if ids:
        placeholders = ",".join("?" for _ in ids)
        conn.execute(
            f"DELETE FROM {table} WHERE host_id = ? AND {key_column} NOT IN ({placeholders})",
            [str(host_id), *ids],
        )
    else:
        conn.execute(f"DELETE FROM {table} WHERE host_id = ?", (str(host_id),))


def _turn_payload_is_prune_protected(payload_json: Any) -> bool:
    """Rows tied to a command or a concrete backend turn outlive snapshot rewrites."""
    try:
        payload = json.loads(str(payload_json or "{}"))
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, Mapping):
        return False
    return bool(
        str(payload.get("origin_command_id") or "").strip()
        or str(payload.get("source_turn_id") or "").strip()
    )


def _delete_turn_if_unreferenced_conn(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: str,
) -> bool:
    """Delete one historical turn and invalidate active list traversals."""
    protected = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions AS revisions
        WHERE revisions.host_id = ? AND revisions.turn_id = ?
          AND (
              EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans AS plans
                  WHERE plans.host_id = revisions.host_id
                    AND plans.turn_id = revisions.turn_id
                    AND plans.content_revision = revisions.content_revision
              )
              OR EXISTS (
                  SELECT 1
                  FROM connector_outbox AS outbox
                  WHERE outbox.host_id = revisions.host_id
                    AND outbox.connector = ?
                    AND json_valid(outbox.payload_json)
                    AND json_extract(
                        outbox.payload_json,
                        '$.content_revision'
                    ) = revisions.content_revision
              )
          )
        LIMIT 1
        """,
        (str(host_id), str(turn_id), _TURN_FINAL_NAME),
    ).fetchone()
    if protected is not None:
        return False
    _ensure_turn_list_host_state_conn(conn, host_id)
    conn.execute(
        "DELETE FROM turn_content_revisions WHERE host_id = ? AND turn_id = ?",
        (str(host_id), str(turn_id)),
    )
    cursor = conn.execute(
        "DELETE FROM turns WHERE host_id = ? AND turn_id = ?",
        (str(host_id), str(turn_id)),
    )
    deleted = cursor.rowcount > 0
    if deleted:
        _increment_turn_list_generation_conn(conn, host_id)
    return deleted


def _prune_turn_projection(
    conn: sqlite3.Connection,
    host_id: str,
    keep_ids: Iterable[str],
) -> None:
    ids = sorted({str(value) for value in keep_ids})
    if ids:
        placeholders = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"""
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = ? AND turn_id NOT IN ({placeholders})
            """,
            [str(host_id), *ids],
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = ?
            """,
            (str(host_id),),
        ).fetchall()
    for turn_id, payload_json in rows:
        if _turn_payload_is_prune_protected(payload_json):
            continue
        _delete_turn_if_unreferenced_conn(conn, str(host_id), str(turn_id))


def _attention_id_from_item(item: Mapping[str, Any]) -> str:
    return str(item.get("id") or item.get("fingerprint") or "unknown")


def _attention_lifecycle_payload(
    item: Mapping[str, Any],
    *,
    attention_id: str,
    observed_at: str,
    first_seen_at: str,
    last_seen_at: str,
    last_changed_at: str,
    lifecycle_status: str,
    signal_count: int,
    resolved_at: str | None = None,
    resolved_reason: str | None = None,
) -> dict[str, Any]:
    payload = dict(item)
    payload.setdefault("id", attention_id)
    payload.setdefault("source", "")
    payload.setdefault("kind", "unknown")
    payload.setdefault("severity", "info")
    payload.setdefault("status", "unknown")
    payload.setdefault("fingerprint", "")
    payload["observed_at"] = observed_at
    payload["first_seen_at"] = first_seen_at
    payload["last_seen_at"] = last_seen_at
    payload["last_changed_at"] = last_changed_at
    payload["lifecycle_status"] = lifecycle_status
    payload["resolved_at"] = resolved_at
    if resolved_reason is not None:
        payload["resolved_reason"] = resolved_reason
    payload["signal_count"] = max(1, int(signal_count))
    return sanitize_public_value(payload)


def _attention_severity_rank(value: Any) -> int:
    return _ATTENTION_SEVERITY_RANK.get(normalize_severity(value), 0)


def _strict_utc_timestamp(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return None
    try:
        return parsed.astimezone(timezone.utc).isoformat()
    except (OverflowError, ValueError):
        return None


def _legacy_snapshot_created_at_is_authoritative(
    created_at: Any,
    payload: Any,
) -> bool:
    """Distinguish a real year-9999 observation from the former sentinel."""
    canonical_created_at = _strict_utc_timestamp(created_at)
    canonical_payload_at = _strict_utc_timestamp(
        _json_object(payload).get("updated_at")
    )
    return (
        canonical_created_at
        == _LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE
        and canonical_payload_at == canonical_created_at
    )


def _attention_family_key(host_id: str, item: Mapping[str, Any]) -> str:
    source = _store_public_text(item.get("source"), default="unknown")
    kind = _store_public_label(item.get("kind"))
    return stable_fingerprint(
        {
            "domain": "tendwire.attention.lifecycle-family.v1",
            "host_id": str(host_id),
            "source": source,
            "kind": kind,
        }
    )


def _attention_observation_key(
    *,
    host_id: str,
    authority: str,
    observed_at: str,
    content_fingerprint: str,
) -> str:
    return stable_fingerprint(
        {
            "domain": "tendwire.attention.observation.v1",
            "host_id": str(host_id),
            "authority": str(authority),
            "observed_at": observed_at,
            "snapshot_content_fingerprint": str(content_fingerprint),
        }
    )


@dataclass(frozen=True)
class _AttentionLifecycleState:
    host_id: str
    family_key: str
    generation: int
    lifecycle_status: str
    current_attention_id: str | None
    first_seen_at: str
    last_positive_at: str
    first_missing_at: str | None
    missing_observation_count: int
    last_accepted_at: str
    last_observation_key: str
    max_notified_severity_rank: int


@dataclass(frozen=True)
class _AttentionObservation:
    host_id: str
    family_key: str
    authority: str
    observed_at: str
    observation_key: str
    signal: Mapping[str, Any] | None


@dataclass(frozen=True)
class _AttentionTransition:
    action: str
    next_state: _AttentionLifecycleState | None
    upsert_signal: Mapping[str, Any] | None = None
    superseded_attention_id: str | None = None
    resolve_attention_id: str | None = None
    delivery: tuple[str, str] | None = None


def _plan_attention_transition(
    state: _AttentionLifecycleState | None,
    observation: _AttentionObservation,
) -> _AttentionTransition:
    signal = observation.signal
    if state is not None:
        if observation.observed_at < state.last_accepted_at:
            return _AttentionTransition("no-op", state)
        if observation.observed_at == state.last_accepted_at:
            return _AttentionTransition("no-op", state)

    if signal is not None:
        attention_id = _attention_id_from_item(signal)
        severity = normalize_severity(signal.get("severity"))
        severity_rank = _attention_severity_rank(severity)
        if state is None:
            next_state = _AttentionLifecycleState(
                host_id=observation.host_id,
                family_key=observation.family_key,
                generation=1,
                lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
                current_attention_id=attention_id,
                first_seen_at=observation.observed_at,
                last_positive_at=observation.observed_at,
                first_missing_at=None,
                missing_observation_count=0,
                last_accepted_at=observation.observed_at,
                last_observation_key=observation.observation_key,
                max_notified_severity_rank=severity_rank,
            )
            return _AttentionTransition(
                "open",
                next_state,
                upsert_signal=signal,
                delivery=("attention_created", "initial"),
            )

        if state.lifecycle_status == ATTENTION_LIFECYCLE_RESOLVED:
            next_state = _AttentionLifecycleState(
                host_id=state.host_id,
                family_key=state.family_key,
                generation=state.generation + 1,
                lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
                current_attention_id=attention_id,
                first_seen_at=observation.observed_at,
                last_positive_at=observation.observed_at,
                first_missing_at=None,
                missing_observation_count=0,
                last_accepted_at=observation.observed_at,
                last_observation_key=observation.observation_key,
                max_notified_severity_rank=severity_rank,
            )
            return _AttentionTransition(
                "open",
                next_state,
                upsert_signal=signal,
                delivery=("attention_created", "initial"),
            )

        escalated = severity_rank > state.max_notified_severity_rank
        next_state = _AttentionLifecycleState(
            host_id=state.host_id,
            family_key=state.family_key,
            generation=state.generation,
            lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
            current_attention_id=attention_id,
            first_seen_at=state.first_seen_at,
            last_positive_at=observation.observed_at,
            first_missing_at=None,
            missing_observation_count=0,
            last_accepted_at=observation.observed_at,
            last_observation_key=observation.observation_key,
            max_notified_severity_rank=max(
                state.max_notified_severity_rank, severity_rank
            ),
        )
        return _AttentionTransition(
            "escalate" if escalated else "update",
            next_state,
            upsert_signal=signal,
            superseded_attention_id=(
                state.current_attention_id
                if state.current_attention_id != attention_id
                else None
            ),
            delivery=(
                ("attention_escalated", f"severity:{severity}")
                if escalated
                else None
            ),
        )

    if state is None or state.lifecycle_status != ATTENTION_LIFECYCLE_OPEN:
        return _AttentionTransition("no-op", state)
    if observation.authority != "complete":
        return _AttentionTransition("no-op", state)

    first_missing_at = state.first_missing_at or observation.observed_at
    missing_count = state.missing_observation_count + 1
    elapsed = (
        datetime.fromisoformat(observation.observed_at)
        - datetime.fromisoformat(first_missing_at)
    ).total_seconds()
    resolves = (
        missing_count >= ATTENTION_MISSING_REQUIRED
        and elapsed >= ATTENTION_MISSING_GRACE_SECONDS
    )
    next_state = _AttentionLifecycleState(
        host_id=state.host_id,
        family_key=state.family_key,
        generation=state.generation,
        lifecycle_status=(
            ATTENTION_LIFECYCLE_RESOLVED
            if resolves
            else ATTENTION_LIFECYCLE_OPEN
        ),
        current_attention_id=None if resolves else state.current_attention_id,
        first_seen_at=state.first_seen_at,
        last_positive_at=state.last_positive_at,
        first_missing_at=first_missing_at,
        missing_observation_count=missing_count,
        last_accepted_at=observation.observed_at,
        last_observation_key=observation.observation_key,
        max_notified_severity_rank=state.max_notified_severity_rank,
    )
    return _AttentionTransition(
        "resolve" if resolves else (
            "start-missing" if state.first_missing_at is None else "advance-missing"
        ),
        next_state,
        resolve_attention_id=state.current_attention_id if resolves else None,
    )


def _enqueue_attention_lifecycle_job_conn(
    conn: sqlite3.Connection,
    *,
    state: _AttentionLifecycleState,
    event_type: str,
    stage: str,
    attention_payload: Mapping[str, Any],
    transition_at: str,
) -> None:
    transition_key = stable_fingerprint(
        {
            "domain": "tendwire.attention.transition.v1",
            "host_id": state.host_id,
            "family_key": state.family_key,
            "generation": state.generation,
            "event_type": event_type,
            "stage": stage,
        }
    )
    delivery_key = f"attention:{event_type}:{transition_key}"
    payload = sanitize_public_value(
        {
            "schema_version": 1,
            "event_type": event_type,
            "host_id": state.host_id,
            "attention": dict(attention_payload),
            "transition_at": transition_at,
        }
    )
    conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id, connector, delivery_key, status, payload_json,
            private_state_json, created_at, updated_at, next_attempt_at
        ) VALUES (?, ?, ?, 'queued', ?, '{}', ?, ?, NULL)
        ON CONFLICT(host_id, connector, delivery_key) DO NOTHING
        """,
        (
            state.host_id,
            ATTENTION_OUTBOX_CONNECTOR,
            delivery_key,
            _canonical_json(payload),
            transition_at,
            transition_at,
        ),
    )


def _upsert_attention_projection_conn(
    conn: sqlite3.Connection,
    *,
    state: _AttentionLifecycleState,
    item: Mapping[str, Any],
    content_fingerprint: str,
    observed_at: str,
    prior_signal_count: int = 0,
) -> dict[str, Any]:
    item = sanitize_public_mapping(item)
    attention_id = _attention_id_from_item(item)
    source = _store_public_text(item.get("source"), default="unknown")
    kind = _store_public_label(item.get("kind"))
    severity = normalize_severity(item.get("severity"))
    signal_status = str(item.get("status") or "unknown")
    fingerprint = str(item.get("fingerprint") or "")
    existing = conn.execute(
        """
        SELECT fingerprint, lifecycle_status, signal_count, last_changed_at,
               severity, status
        FROM attention_items
        WHERE host_id = ? AND attention_id = ?
        """,
        (state.host_id, attention_id),
    ).fetchone()
    signal_count = max(
        max(0, int(prior_signal_count)),
        0 if existing is None else max(0, int(existing[2] or 0)),
    ) + 1
    changed = (
        existing is None
        or str(existing[0] or "") != fingerprint
        or str(existing[1] or "") != ATTENTION_LIFECYCLE_OPEN
        or normalize_severity(existing[4]) != severity
        or str(existing[5] or "") != signal_status
    )
    last_changed_at = (
        observed_at if changed else str(existing[3] or observed_at)
    )
    conn.execute(
        """
        INSERT INTO attention_items (
            host_id, attention_id, source, kind, severity, status, updated_at,
            fingerprint, snapshot_content_fingerprint, observed_at,
            first_seen_at, last_seen_at, last_changed_at, resolved_at,
            lifecycle_status, resolved_reason, signal_count, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'open', NULL, ?, ?)
        ON CONFLICT(host_id, attention_id) DO UPDATE SET
            source = excluded.source,
            kind = excluded.kind,
            severity = excluded.severity,
            status = excluded.status,
            updated_at = excluded.updated_at,
            fingerprint = excluded.fingerprint,
            snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
            observed_at = excluded.observed_at,
            first_seen_at = excluded.first_seen_at,
            last_seen_at = excluded.last_seen_at,
            last_changed_at = excluded.last_changed_at,
            resolved_at = NULL,
            lifecycle_status = 'open',
            resolved_reason = NULL,
            signal_count = excluded.signal_count,
            payload_json = excluded.payload_json
        """,
        (
            state.host_id,
            attention_id,
            source,
            kind,
            severity,
            signal_status,
            item.get("updated_at"),
            fingerprint,
            str(content_fingerprint),
            observed_at,
            state.first_seen_at,
            observed_at,
            last_changed_at,
            signal_count,
            _canonical_json(dict(item)),
        ),
    )
    return _attention_lifecycle_payload(
        item,
        attention_id=attention_id,
        observed_at=observed_at,
        first_seen_at=state.first_seen_at,
        last_seen_at=observed_at,
        last_changed_at=last_changed_at,
        lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
        signal_count=signal_count,
    )


def _attention_state_from_row(row: Any) -> _AttentionLifecycleState:
    return _AttentionLifecycleState(
        host_id=str(row[0]),
        family_key=str(row[1]),
        generation=int(row[2]),
        lifecycle_status=str(row[3]),
        current_attention_id=str(row[4]) if row[4] is not None else None,
        first_seen_at=str(row[5]),
        last_positive_at=str(row[6]),
        first_missing_at=str(row[7]) if row[7] is not None else None,
        missing_observation_count=int(row[8]),
        last_accepted_at=str(row[9]),
        last_observation_key=str(row[10]),
        max_notified_severity_rank=int(row[11]),
    )


def _apply_attention_observation_conn(
    conn: sqlite3.Connection,
    *,
    snapshot: Snapshot,
    payload_data: Mapping[str, Any],
    content_fingerprint: str,
    observation: SnapshotObservationContext,
) -> None:
    authority = (
        observation.authority
        if observation.authority in {"none", "positive", "complete"}
        else "none"
    )
    if authority == "none":
        return
    observed_at = _strict_utc_timestamp(observation.observed_at)
    if observed_at is None:
        return
    host_id = str(snapshot.host_id)
    observation_key = _attention_observation_key(
        host_id=host_id,
        authority=authority,
        observed_at=observed_at,
        content_fingerprint=content_fingerprint,
    )

    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for raw_item in payload_data.get("attention", []):
        if not isinstance(raw_item, Mapping):
            continue
        item = sanitize_public_mapping(raw_item)
        family_key = _attention_family_key(host_id, item)
        grouped.setdefault(family_key, []).append(item)

    def candidate_rank(item: Mapping[str, Any]) -> tuple[int, str, str]:
        updated_at = _strict_utc_timestamp(item.get("updated_at")) or ""
        return (
            _attention_severity_rank(item.get("severity")),
            updated_at,
            "".join(chr(0x10FFFF - ord(ch)) for ch in _attention_id_from_item(item)),
        )

    selected = {
        family_key: max(items, key=candidate_rank)
        for family_key, items in grouped.items()
    }
    rows = conn.execute(
        """
        SELECT host_id, family_key, generation, lifecycle_status,
               current_attention_id, first_seen_at, last_positive_at,
               first_missing_at, missing_observation_count, last_accepted_at,
               last_observation_key, max_notified_severity_rank
        FROM attention_lifecycles
        WHERE host_id = ?
        """,
        (host_id,),
    ).fetchall()
    states = {
        state.family_key: state
        for state in (_attention_state_from_row(row) for row in rows)
    }
    family_keys = set(selected)
    if authority == "complete":
        family_keys.update(
            key
            for key, state in states.items()
            if state.lifecycle_status == ATTENTION_LIFECYCLE_OPEN
        )

    for family_key in sorted(family_keys):
        state = states.get(family_key)
        signal = selected.get(family_key)
        transition = _plan_attention_transition(
            state,
            _AttentionObservation(
                host_id=host_id,
                family_key=family_key,
                authority=authority,
                observed_at=observed_at,
                observation_key=observation_key,
                signal=signal,
            ),
        )
        next_state = transition.next_state
        if transition.action == "no-op" or next_state is None:
            continue
        if state is None:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO attention_lifecycles (
                    host_id, family_key, generation, lifecycle_status,
                    current_attention_id, first_seen_at, last_positive_at,
                    first_missing_at, missing_observation_count, last_accepted_at,
                    last_observation_key, max_notified_severity_rank
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    next_state.host_id,
                    next_state.family_key,
                    next_state.generation,
                    next_state.lifecycle_status,
                    next_state.current_attention_id,
                    next_state.first_seen_at,
                    next_state.last_positive_at,
                    next_state.first_missing_at,
                    next_state.missing_observation_count,
                    next_state.last_accepted_at,
                    next_state.last_observation_key,
                    next_state.max_notified_severity_rank,
                ),
            )
        else:
            cursor = conn.execute(
                """
                UPDATE attention_lifecycles
                SET generation = ?, lifecycle_status = ?,
                    current_attention_id = ?, first_seen_at = ?,
                    last_positive_at = ?, first_missing_at = ?,
                    missing_observation_count = ?, last_accepted_at = ?,
                    last_observation_key = ?, max_notified_severity_rank = ?
                WHERE host_id = ? AND family_key = ? AND generation = ?
                  AND lifecycle_status = ? AND last_accepted_at < ?
                """,
                (
                    next_state.generation,
                    next_state.lifecycle_status,
                    next_state.current_attention_id,
                    next_state.first_seen_at,
                    next_state.last_positive_at,
                    next_state.first_missing_at,
                    next_state.missing_observation_count,
                    next_state.last_accepted_at,
                    next_state.last_observation_key,
                    next_state.max_notified_severity_rank,
                    state.host_id,
                    state.family_key,
                    state.generation,
                    state.lifecycle_status,
                    observed_at,
                ),
            )
        if int(cursor.rowcount or 0) != 1:
            continue

        prior_signal_count = 0
        if transition.superseded_attention_id is not None:
            prior_row = conn.execute(
                """
                SELECT signal_count FROM attention_items
                WHERE host_id = ? AND attention_id = ?
                """,
                (host_id, transition.superseded_attention_id),
            ).fetchone()
            prior_signal_count = int(prior_row[0] or 0) if prior_row else 0
        if transition.superseded_attention_id is not None:
            conn.execute(
                """
                UPDATE attention_items
                SET lifecycle_status = 'resolved', resolved_at = ?,
                    resolved_reason = ?, last_changed_at = ?
                WHERE host_id = ? AND attention_id = ? AND lifecycle_status = 'open'
                """,
                (
                    observed_at,
                    ATTENTION_RESOLVED_REASON_SUPERSEDED,
                    observed_at,
                    host_id,
                    transition.superseded_attention_id,
                ),
            )
        public_payload: dict[str, Any] | None = None
        if transition.upsert_signal is not None:
            public_payload = _upsert_attention_projection_conn(
                conn,
                state=next_state,
                item=transition.upsert_signal,
                content_fingerprint=content_fingerprint,
                observed_at=observed_at,
                prior_signal_count=prior_signal_count,
            )
        if transition.resolve_attention_id is not None:
            conn.execute(
                """
                UPDATE attention_items
                SET lifecycle_status = 'resolved', resolved_at = ?,
                    resolved_reason = ?, last_changed_at = ?,
                    snapshot_content_fingerprint = ?
                WHERE host_id = ? AND attention_id = ? AND lifecycle_status = 'open'
                """,
                (
                    observed_at,
                    ATTENTION_RESOLVED_REASON_GONE,
                    observed_at,
                    str(content_fingerprint),
                    host_id,
                    transition.resolve_attention_id,
                ),
            )
        if transition.delivery is not None and public_payload is not None:
            _enqueue_attention_lifecycle_job_conn(
                conn,
                state=next_state,
                event_type=transition.delivery[0],
                stage=transition.delivery[1],
                attention_payload=public_payload,
                transition_at=observed_at,
            )
        states[family_key] = next_state


def _append_snapshot_saved_event_conn(
    conn: sqlite3.Connection,
    snapshot: Snapshot,
    *,
    snapshot_id: int,
    content_fingerprint: str,
    private_snapshot_data: Mapping[str, Any],
) -> None:
    _append_event_conn(
        conn,
        host_id=str(snapshot.host_id),
        event_type="snapshot.saved",
        aggregate_type="snapshot",
        aggregate_id=str(content_fingerprint),
        observed_at=str(snapshot.updated_at),
        content_fingerprint=str(content_fingerprint),
        payload={
            "snapshot_id": int(snapshot_id),
            "content_fingerprint": str(content_fingerprint),
            "snapshot": dict(private_snapshot_data),
        },
    )


def _ensure_turn_list_host_state_conn(
    conn: sqlite3.Connection,
    host_id: str,
) -> None:
    conn.execute(
        """
        INSERT INTO turn_list_hosts (
            host_id,
            next_sequence,
            traversal_generation
        )
        SELECT ?, COALESCE(MAX(list_sequence), 0) + 1, 1
        FROM turns
        WHERE host_id = ?
        ON CONFLICT(host_id) DO NOTHING
        """,
        (str(host_id), str(host_id)),
    )


def _turn_list_host_state_conn(
    conn: sqlite3.Connection,
    host_id: str,
) -> tuple[int, int]:
    row = conn.execute(
        """
        SELECT next_sequence, traversal_generation
        FROM turn_list_hosts
        WHERE host_id = ?
        """,
        (str(host_id),),
    ).fetchone()
    if row is not None:
        return max(0, int(row[0]) - 1), int(row[1])
    row = conn.execute(
        "SELECT COALESCE(MAX(list_sequence), 0) FROM turns WHERE host_id = ?",
        (str(host_id),),
    ).fetchone()
    return int(row[0] if row is not None else 0), 1


def _turn_list_sequence_conn(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: str,
) -> int:
    """Return an existing immutable sequence or consume one durable host counter."""
    row = conn.execute(
        """
        SELECT list_sequence
        FROM turns
        WHERE host_id = ? AND turn_id = ?
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    if row is not None:
        return int(row[0])
    _ensure_turn_list_host_state_conn(conn, host_id)
    row = conn.execute(
        "SELECT next_sequence FROM turn_list_hosts WHERE host_id = ?",
        (str(host_id),),
    ).fetchone()
    if row is None:
        raise StoreSchemaError("turn_list_host_state_unavailable")
    sequence = int(row[0])
    conn.execute(
        """
        UPDATE turn_list_hosts
        SET next_sequence = next_sequence + 1
        WHERE host_id = ?
        """,
        (str(host_id),),
    )
    return sequence


def _increment_turn_list_generation_conn(
    conn: sqlite3.Connection,
    host_id: str,
) -> None:
    _ensure_turn_list_host_state_conn(conn, host_id)
    conn.execute(
        """
        UPDATE turn_list_hosts
        SET traversal_generation = traversal_generation + 1
        WHERE host_id = ?
        """,
        (str(host_id),),
    )


def _refresh_snapshot_projections_conn(
    conn: sqlite3.Connection,
    snapshot: Snapshot,
    payload_data: Mapping[str, Any],
    *,
    content_fingerprint: str,
) -> None:
    payload_data = sanitize_public_mapping(payload_data)
    host_id = str(snapshot.host_id)
    observed_at = str(snapshot.updated_at)

    space_ids: set[str] = set()
    for item in payload_data.get("spaces", []):
        if not isinstance(item, Mapping):
            continue
        space_id = str(item.get("id") or "unknown")
        space_ids.add(space_id)
        conn.execute(
            """
            INSERT INTO spaces (
                host_id,
                space_id,
                name,
                status,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, space_id) DO UPDATE SET
                name = excluded.name,
                status = excluded.status,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                space_id,
                str(item.get("name") or space_id),
                str(item.get("status") or "unknown"),
                item.get("updated_at"),
                str(item.get("fingerprint") or ""),
                str(content_fingerprint),
                observed_at,
                _canonical_json(dict(item)),
            ),
        )
    _prune_host_projection(conn, "spaces", "space_id", host_id, space_ids)

    worker_ids: set[str] = set()
    for item in payload_data.get("workers", []):
        if not isinstance(item, Mapping):
            continue
        worker_id = str(item.get("id") or "unknown")
        worker_ids.add(worker_id)
        conn.execute(
            """
            INSERT INTO workers (
                host_id,
                worker_id,
                worker_fingerprint,
                space_id,
                name,
                status,
                last_seen_at,
                snapshot_content_fingerprint,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, worker_id) DO UPDATE SET
                worker_fingerprint = excluded.worker_fingerprint,
                space_id = excluded.space_id,
                name = excluded.name,
                status = excluded.status,
                last_seen_at = excluded.last_seen_at,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                worker_id,
                str(item.get("fingerprint") or ""),
                item.get("space_id"),
                str(item.get("name") or worker_id),
                str(item.get("status") or "unknown"),
                item.get("last_seen_at"),
                str(content_fingerprint),
                observed_at,
                _canonical_json(dict(item)),
            ),
        )
    _prune_host_projection(conn, "workers", "worker_id", host_id, worker_ids)


    turn_ids: set[str] = set()
    for turn in turns_from_snapshot(snapshot):
        item = sanitize_public_mapping(turn.to_dict())
        turn_id = str(item.get("id") or "unknown")
        turn_ids.add(turn_id)
        list_sequence = _turn_list_sequence_conn(conn, host_id, turn_id)
        conn.execute(
            """
            INSERT INTO turns (
                host_id,
                turn_id,
                worker_id,
                worker_fingerprint,
                space_id,
                status,
                kind,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json,
                list_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, turn_id) DO UPDATE SET
                worker_id = excluded.worker_id,
                worker_fingerprint = excluded.worker_fingerprint,
                space_id = excluded.space_id,
                status = excluded.status,
                kind = excluded.kind,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                turn_id,
                str(item.get("worker_id") or ""),
                item.get("worker_fingerprint"),
                item.get("space_id"),
                str(item.get("status") or "unknown"),
                str(item.get("kind") or "unknown"),
                item.get("updated_at"),
                str(item.get("fingerprint") or ""),
                str(content_fingerprint),
                observed_at,
                _canonical_json(item),
                list_sequence,
            ),
        )
        _ensure_payload_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=turn_id,
            payload=item,
            observed_at=str(observed_at) if observed_at else None,
        )
    _prune_turn_projection(conn, host_id, turn_ids)

    pending_ids: set[str] = set()
    for pending in pending_from_snapshot(snapshot):
        item = sanitize_public_mapping(pending.to_dict())
        pending_id = str(item.get("id") or "unknown")
        pending_ids.add(pending_id)
        conn.execute(
            """
            INSERT INTO pending_interactions (
                host_id,
                pending_id,
                worker_id,
                worker_fingerprint,
                space_id,
                kind,
                status,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, pending_id) DO UPDATE SET
                worker_id = excluded.worker_id,
                worker_fingerprint = excluded.worker_fingerprint,
                space_id = excluded.space_id,
                kind = excluded.kind,
                status = excluded.status,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                pending_id,
                str(item.get("worker_id") or ""),
                item.get("worker_fingerprint"),
                item.get("space_id"),
                str(item.get("kind") or "unknown"),
                str(item.get("status") or "unknown"),
                item.get("updated_at"),
                str(item.get("fingerprint") or ""),
                str(content_fingerprint),
                observed_at,
                _canonical_json(item),
            ),
        )
    _prune_host_projection(
        conn,
        "pending_interactions",
        "pending_id",
        host_id,
        pending_ids,
    )

    backend_names: set[str] = set()
    for item in payload_data.get("backend_health", []):
        if not isinstance(item, Mapping):
            continue
        backend_name = str(item.get("name") or "unknown")
        backend_names.add(backend_name)
        conn.execute(
            """
            INSERT INTO backend_health (
                host_id,
                backend_name,
                status,
                outcome,
                observed_at,
                snapshot_content_fingerprint,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, backend_name) DO UPDATE SET
                status = excluded.status,
                outcome = excluded.outcome,
                observed_at = excluded.observed_at,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                backend_name,
                str(item.get("status") or "unknown"),
                str(item.get("outcome") or "unknown"),
                item.get("observed_at"),
                str(content_fingerprint),
                _canonical_json(dict(item)),
            ),
        )
    _prune_host_projection(
        conn,
        "backend_health",
        "backend_name",
        host_id,
        backend_names,
    )


def _upsert_command_audit(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    request_id: str,
    action: str,
    payload_fingerprint: str,
    status: str,
    result_json: str,
    created_at: str | None = None,
    reserved_at: str | None = None,
    completed_at: str | None = None,
    uncertain: bool = False,
    dry_run: bool = False,
    request_json: str = "{}",
    updated_at: str | None = None,
) -> None:
    if not str(request_id):
        return
    now = utc_timestamp()
    created = created_at or now
    updated = updated_at or now
    conn.execute(
        """
        INSERT INTO commands (
            host_id,
            request_id,
            action,
            payload_fingerprint,
            status,
            dry_run,
            uncertain,
            request_json,
            result_json,
            created_at,
            reserved_at,
            completed_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(host_id, request_id, action) DO UPDATE SET
            payload_fingerprint = excluded.payload_fingerprint,
            status = excluded.status,
            uncertain = excluded.uncertain,
            result_json = excluded.result_json,
            completed_at = excluded.completed_at,
            updated_at = excluded.updated_at
        """,
        (
            str(host_id),
            str(request_id),
            str(action),
            str(payload_fingerprint),
            str(status),
            int(dry_run),
            int(uncertain),
            str(request_json),
            str(result_json),
            created,
            reserved_at,
            completed_at,
            updated,
        ),
    )


def _command_audit_exists(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    request_id: str,
    action: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM commands
        WHERE host_id = ? AND request_id = ? AND action = ?
        LIMIT 1
        """,
        (str(host_id), str(request_id), str(action)),
    ).fetchone()
    return row is not None


def _upsert_command_audit_from_receipt_row(
    conn: sqlite3.Connection,
    row: Any,
) -> None:
    if _command_audit_exists(
        conn,
        host_id=str(row[0]),
        request_id=str(row[1]),
        action=str(row[2]),
    ):
        return
    created_at = str(row[6] or utc_timestamp())
    completed_at = row[7]
    _upsert_command_audit(
        conn,
        host_id=str(row[0]),
        request_id=str(row[1]),
        action=str(row[2]),
        payload_fingerprint=str(row[3]),
        status=str(row[4]),
        result_json=str(row[5]),
        created_at=created_at,
        reserved_at=created_at,
        completed_at=completed_at,
        uncertain=bool(row[8]),
        updated_at=str(completed_at or created_at),
    )


def find_recent_matching_command_submission(
    db_path: Path,
    host_id: str,
    *,
    action: str,
    worker_id: str,
    worker_fingerprint: str = "",
    instruction_text: str,
    since: str,
    exclude_request_id: str = "",
) -> dict[str, Any] | None:
    """Return a recent same-worker/same-text accepted command, if one exists."""
    if not _sqlite_store_exists(db_path) or not str(worker_id).strip() or not str(instruction_text):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT request_id, status, request_json, created_at, updated_at
            FROM commands
            WHERE host_id = ?
              AND action = ?
              AND request_id != ?
              AND status = 'accepted'
              AND updated_at >= ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 200
            """,
            (str(host_id), str(action), str(exclude_request_id), str(since)),
        ).fetchall()
    for row in rows:
        try:
            request = json.loads(str(row[2] or "{}"))
        except json.JSONDecodeError:
            continue
        if not isinstance(request, dict):
            continue
        target = request.get("target")
        instruction = request.get("instruction")
        if not isinstance(target, dict) or not isinstance(instruction, dict):
            continue
        if str(target.get("worker_id") or "").strip() != str(worker_id).strip():
            continue
        previous_fingerprint = str(target.get("worker_fingerprint") or "").strip()
        current_fingerprint = str(worker_fingerprint or "").strip()
        if previous_fingerprint and current_fingerprint and previous_fingerprint != current_fingerprint:
            continue
        if instruction.get("text") != instruction_text:
            continue
        return sanitize_public_value({
            "request_id": str(row[0] or ""),
            "status": str(row[1] or ""),
            "created_at": str(row[3] or ""),
            "updated_at": str(row[4] or ""),
        })
    return None


def _backfill_command_audit(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT
            host_id,
            request_id,
            action,
            payload_fingerprint,
            status,
            result_json,
            created_at,
            completed_at,
            uncertain
        FROM command_receipts
        WHERE request_id != ''
        ORDER BY id
        """
    ).fetchall()
    for row in rows:
        _upsert_command_audit_from_receipt_row(conn, row)


def _backfill_legacy_attention_columns(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE attention_items
        SET
            first_seen_at = CASE
                WHEN first_seen_at IS NULL OR first_seen_at = ''
                THEN COALESCE(NULLIF(observed_at, ''), updated_at, '')
                ELSE first_seen_at
            END,
            last_seen_at = CASE
                WHEN last_seen_at IS NULL OR last_seen_at = ''
                THEN COALESCE(NULLIF(observed_at, ''), updated_at, '')
                ELSE last_seen_at
            END,
            last_changed_at = CASE
                WHEN last_changed_at IS NULL OR last_changed_at = ''
                THEN COALESCE(NULLIF(observed_at, ''), updated_at, '')
                ELSE last_changed_at
            END,
            lifecycle_status = CASE
                WHEN lifecycle_status IS NULL OR lifecycle_status = ''
                THEN 'open'
                ELSE lifecycle_status
            END,
            signal_count = CASE
                WHEN signal_count IS NULL OR signal_count < 1
                THEN 1
                ELSE signal_count
            END
        """
    )


def _migration_private_state(
    raw: Any,
    *,
    group: str,
    canonical: bool,
    terminal_after_lease: bool = False,
) -> str:
    state = _json_object(raw)
    state["migration_group"] = group
    state["migration_canonical"] = bool(canonical)
    if terminal_after_lease:
        state["terminal_after_lease"] = True
    else:
        state.pop("terminal_after_lease", None)
    return _canonical_json(state)


def _legacy_attention_job_identity(
    row_host_id: str,
    payload_json: Any,
) -> tuple[str, str, str, str, str, str | None] | None:
    payload = sanitize_public_mapping(_json_object(payload_json), backend_neutral=True)
    if str(payload.get("host_id") or "") != str(row_host_id):
        return None
    event_type = str(payload.get("event_type") or "")
    if event_type not in {"attention_created", "attention_escalated"}:
        return None
    attention = payload.get("attention")
    if not isinstance(attention, Mapping):
        return None
    source = _store_public_text(attention.get("source"), default="")
    kind = _store_public_label(attention.get("kind"))
    if not source or kind == "unknown":
        return None
    family_key = _attention_family_key(str(row_host_id), attention)
    stage = (
        "initial"
        if event_type == "attention_created"
        else f"severity:{normalize_severity(attention.get('severity'))}"
    )
    return (
        str(row_host_id),
        family_key,
        event_type,
        stage,
        _attention_id_from_item(attention),
        _strict_utc_timestamp(payload.get("transition_at")),
    )


def _migrate_v4_attention_rows_conn(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS temp.attention_v5_rows")
    conn.execute(
        """
        CREATE TEMP TABLE attention_v5_rows (
            host_id TEXT NOT NULL,
            family_key TEXT NOT NULL,
            attention_id TEXT NOT NULL,
            is_open INTEGER NOT NULL,
            positive_at TEXT,
            changed_at TEXT,
            first_seen_at TEXT,
            severity_rank INTEGER NOT NULL,
            signal_count INTEGER NOT NULL
        )
        """
    )
    cursor = conn.execute(
        """
        SELECT host_id, attention_id, source, kind, severity, lifecycle_status,
               updated_at, observed_at, first_seen_at, last_seen_at,
               last_changed_at, signal_count
        FROM attention_items
        ORDER BY host_id, attention_id
        """
    )
    while True:
        batch = cursor.fetchmany(500)
        if not batch:
            break
        values: list[tuple[Any, ...]] = []
        for row in batch:
            host_id = str(row[0])
            item = {"source": row[2], "kind": row[3]}
            positive_at = (
                _strict_utc_timestamp(row[9])
                or _strict_utc_timestamp(row[7])
                or _strict_utc_timestamp(row[6])
            )
            values.append(
                (
                    host_id,
                    _attention_family_key(host_id, item),
                    str(row[1]),
                    int(str(row[5] or "open") == ATTENTION_LIFECYCLE_OPEN),
                    positive_at,
                    _strict_utc_timestamp(row[10]),
                    _strict_utc_timestamp(row[8]),
                    _attention_severity_rank(row[4]),
                    max(1, int(row[11] or 1)),
                )
            )
        conn.executemany(
            """
            INSERT INTO attention_v5_rows (
                host_id, family_key, attention_id, is_open, positive_at,
                changed_at, first_seen_at, severity_rank, signal_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )

    family_cursor = conn.execute(
        """
        SELECT host_id, family_key
        FROM attention_v5_rows
        GROUP BY host_id, family_key
        ORDER BY host_id, family_key
        """
    )
    while True:
        families = family_cursor.fetchmany(500)
        if not families:
            break
        for host_id_raw, family_key_raw in families:
            host_id = str(host_id_raw)
            family_key = str(family_key_raw)
            candidates = conn.execute(
                """
                SELECT attention_id, is_open, positive_at, changed_at,
                       first_seen_at, severity_rank, signal_count
                FROM attention_v5_rows
                WHERE host_id = ? AND family_key = ?
                ORDER BY attention_id
                """,
                (host_id, family_key),
            )
            winner: tuple[Any, ...] | None = None
            earliest_first: str | None = None
            latest_positive: str | None = None
            latest_progress: str | None = None
            total_signals = 0
            max_severity = -1
            while True:
                candidate_batch = candidates.fetchmany(500)
                if not candidate_batch:
                    break
                for candidate in candidate_batch:
                    total_signals += max(1, int(candidate[6] or 1))
                    max_severity = max(max_severity, int(candidate[5]))
                    if candidate[4] and (
                        earliest_first is None or str(candidate[4]) < earliest_first
                    ):
                        earliest_first = str(candidate[4])
                    if candidate[2] and (
                        latest_positive is None or str(candidate[2]) > latest_positive
                    ):
                        latest_positive = str(candidate[2])
                    progress_at = max(
                        str(candidate[2] or ""),
                        str(candidate[3] or ""),
                    )
                    if progress_at and (
                        latest_progress is None or progress_at > latest_progress
                    ):
                        latest_progress = progress_at
                    rank = (
                        progress_at,
                        int(candidate[1]),
                        str(candidate[2] or ""),
                        str(candidate[3] or ""),
                        int(candidate[5]),
                        int(candidate[6]),
                        "".join(
                            chr(0x10FFFF - ord(ch)) for ch in str(candidate[0])
                        ),
                    )
                    if winner is None or rank > winner[0]:
                        winner = (rank, *candidate)
            if winner is None or latest_positive is None:
                continue
            winner_attention_id = str(winner[1])
            is_open = bool(winner[2])
            first_seen_at = earliest_first or latest_positive
            # The lifecycle watermark (last_accepted_at) must be the newest
            # lifecycle progress — max(latest positive, latest change/resolve) —
            # not merely the latest positive. A resolved episode whose resolution
            # (t10) is newer than its last positive (t0) would otherwise seed the
            # watermark at t0, letting a delayed positive at t5 (< the authoritative
            # resolution) pass the observation guard and spuriously reopen
            # generation 2 with a fresh notification. last_positive_at stays the
            # actual latest positive; the observation key is anchored to the
            # accepted progress so replaying the authoritative resolution is a no-op.
            accepted_progress = latest_progress or latest_positive
            observation_key = stable_fingerprint(
                {
                    "domain": "tendwire.attention.observation.v1",
                    "host_id": host_id,
                    "authority": "migration",
                    "observed_at": accepted_progress,
                    "snapshot_content_fingerprint": family_key,
                }
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO attention_lifecycles (
                    host_id, family_key, generation, lifecycle_status,
                    current_attention_id, first_seen_at, last_positive_at,
                    first_missing_at, missing_observation_count, last_accepted_at,
                    last_observation_key, max_notified_severity_rank
                ) VALUES (?, ?, 1, ?, ?, ?, ?, NULL, 0, ?, ?, ?)
                """,
                (
                    host_id,
                    family_key,
                    (
                        ATTENTION_LIFECYCLE_OPEN
                        if is_open
                        else ATTENTION_LIFECYCLE_RESOLVED
                    ),
                    winner_attention_id if is_open else None,
                    first_seen_at,
                    latest_positive,
                    accepted_progress,
                    observation_key,
                    max_severity,
                ),
            )
            if is_open:
                conn.execute(
                    """
                    UPDATE attention_items
                    SET first_seen_at = ?, last_seen_at = ?,
                        signal_count = ?, lifecycle_status = 'open',
                        resolved_at = NULL, resolved_reason = NULL
                    WHERE host_id = ? AND attention_id = ?
                    """,
                    (
                        first_seen_at,
                        latest_positive,
                        total_signals,
                        host_id,
                        winner_attention_id,
                    ),
                )
                conn.execute(
                    """
                    UPDATE attention_items
                    SET lifecycle_status = 'resolved',
                        resolved_at = COALESCE(NULLIF(resolved_at, ''), ?),
                        resolved_reason = ?,
                        last_changed_at = ?
                    WHERE host_id = ? AND lifecycle_status = 'open'
                      AND attention_id != ?
                      AND attention_id IN (
                          SELECT attention_id FROM attention_v5_rows
                          WHERE host_id = ? AND family_key = ? AND is_open = 1
                      )
                    """,
                    (
                        latest_positive,
                        ATTENTION_RESOLVED_REASON_SUPERSEDED,
                        latest_positive,
                        host_id,
                        winner_attention_id,
                        host_id,
                        family_key,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE attention_items
                    SET lifecycle_status = 'resolved',
                        resolved_at = COALESCE(NULLIF(resolved_at, ''), ?),
                        resolved_reason = CASE
                            WHEN attention_id = ? THEN COALESCE(resolved_reason, 'gone')
                            ELSE ?
                        END,
                        last_changed_at = ?
                    WHERE host_id = ? AND lifecycle_status = 'open'
                      AND attention_id IN (
                          SELECT attention_id FROM attention_v5_rows
                          WHERE host_id = ? AND family_key = ? AND is_open = 1
                      )
                    """,
                    (
                        latest_positive,
                        winner_attention_id,
                        ATTENTION_RESOLVED_REASON_SUPERSEDED,
                        latest_positive,
                        host_id,
                        host_id,
                        family_key,
                    ),
                )
    conn.execute("DROP TABLE temp.attention_v5_rows")


def _migrate_v4_attention_outbox_conn(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS temp.attention_v5_jobs")
    conn.execute(
        """
        CREATE TEMP TABLE attention_v5_jobs (
            outbox_id INTEGER PRIMARY KEY,
            host_id TEXT NOT NULL,
            family_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            stage TEXT NOT NULL,
            attention_id TEXT NOT NULL,
            transition_at TEXT,
            group_key TEXT NOT NULL
        )
        """
    )
    cursor = conn.execute(
        """
        SELECT id, host_id, payload_json
        FROM connector_outbox
        WHERE connector = ?
        ORDER BY id
        """,
        (ATTENTION_OUTBOX_CONNECTOR,),
    )
    while True:
        batch = cursor.fetchmany(500)
        if not batch:
            break
        values: list[tuple[Any, ...]] = []
        for outbox_id, host_id, payload_json in batch:
            identity = _legacy_attention_job_identity(str(host_id), payload_json)
            if identity is None:
                continue
            (
                identity_host,
                family_key,
                event_type,
                stage,
                attention_id,
                transition_at,
            ) = identity
            group_key = stable_fingerprint(
                {
                    "domain": "tendwire.attention.migration-group.v1",
                    "host_id": identity_host,
                    "family_key": family_key,
                    "generation": 1,
                    "event_type": event_type,
                    "stage": stage,
                }
            )
            values.append(
                (
                    int(outbox_id),
                    identity_host,
                    family_key,
                    event_type,
                    stage,
                    attention_id,
                    transition_at,
                    group_key,
                )
            )
            if event_type == "attention_escalated":
                severity = stage.removeprefix("severity:")
                conn.execute(
                    """
                    UPDATE attention_lifecycles
                    SET max_notified_severity_rank =
                        MAX(max_notified_severity_rank, ?)
                    WHERE host_id = ? AND family_key = ?
                    """,
                    (
                        _attention_severity_rank(severity),
                        identity_host,
                        family_key,
                    ),
                )
        conn.executemany(
            """
            INSERT INTO attention_v5_jobs (
                outbox_id, host_id, family_key, event_type, stage,
                attention_id, transition_at, group_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )

    group_cursor = conn.execute(
        """
        SELECT host_id, family_key, event_type, stage, group_key
        FROM attention_v5_jobs
        GROUP BY host_id, family_key, event_type, stage, group_key
        ORDER BY group_key
        """
    )
    while True:
        groups = group_cursor.fetchmany(500)
        if not groups:
            break
        for host_id, family_key, event_type, stage, group_key in groups:
            candidate_rows = conn.execute(
                """
                SELECT o.id, o.status, o.payload_json, o.private_state_json,
                       j.attention_id, j.transition_at
                FROM connector_outbox o
                JOIN attention_v5_jobs j ON j.outbox_id = o.id
                WHERE j.group_key = ?
                  AND o.status IN ('queued', 'retry', 'deferred', 'leased')
                ORDER BY o.id
                """,
                (group_key,),
            ).fetchall()
            if not candidate_rows:
                continue
            lifecycle_row = conn.execute(
                """
                SELECT l.lifecycle_status, l.current_attention_id,
                       l.first_seen_at, l.last_positive_at, l.last_accepted_at,
                       i.last_changed_at, i.last_seen_at, i.observed_at,
                       i.updated_at
                FROM attention_lifecycles l
                LEFT JOIN attention_items i
                  ON i.host_id = l.host_id
                 AND i.attention_id = l.current_attention_id
                WHERE l.host_id = ? AND l.family_key = ?
                """,
                (host_id, family_key),
            ).fetchone()
            lifecycle_open = (
                lifecycle_row is not None
                and str(lifecycle_row[0]) == ATTENTION_LIFECYCLE_OPEN
                and lifecycle_row[1] is not None
            )
            current_attention_id = (
                str(lifecycle_row[1]) if lifecycle_open else ""
            )
            current_anchor_candidates = (
                [
                    canonical
                    for canonical in (
                        _strict_utc_timestamp(value)
                        # Include the lifecycle's persisted last_accepted_at
                        # (index 4) alongside the attention_items timestamps so
                        # the current-episode anchor reflects the authoritative
                        # accepted-progress watermark even when the row's own
                        # timestamps are skewed (e.g. a delayed positive).
                        for value in lifecycle_row[4:9]
                    )
                    if canonical is not None
                ]
                if lifecycle_open
                else []
            )
            current_episode_anchor = (
                max(current_anchor_candidates)
                if current_anchor_candidates
                else ""
            )
            terminalized_current_episode = False
            if lifecycle_open:
                terminal_rows = conn.execute(
                    """
                    SELECT j.attention_id, j.transition_at
                    FROM connector_outbox o
                    JOIN attention_v5_jobs j ON j.outbox_id = o.id
                    WHERE j.group_key = ?
                      AND o.status IN ('delivered', 'dead_letter')
                    """,
                    (group_key,),
                ).fetchall()
                terminalized_current_episode = bool(current_episode_anchor) and any(
                    str(terminal_attention_id) == current_attention_id
                    and bool(terminal_transition_at)
                    and str(terminal_transition_at) >= current_episode_anchor
                    for terminal_attention_id, terminal_transition_at in terminal_rows
                )

            leased_rows = [
                row
                for row in candidate_rows
                if str(row[1]) == _CONNECTOR_LEASE_STATUS
                and conn.execute(
                    """
                    SELECT 1 FROM connector_deliveries
                    WHERE outbox_id = ? AND status = 'leased'
                    LIMIT 1
                    """,
                    (int(row[0]),),
                ).fetchone()
                is not None
            ]
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = ?, next_attempt_at = NULL
                WHERE id IN (
                    SELECT j.outbox_id FROM attention_v5_jobs j
                    WHERE j.group_key = ?
                ) AND (
                    status IN ('queued', 'retry', 'deferred')
                    OR (
                        status = 'leased'
                        AND id NOT IN (
                            SELECT d.outbox_id FROM connector_deliveries d
                            WHERE d.status = 'leased' AND d.outbox_id IS NOT NULL
                        )
                    )
                )
                """,
                (_CONNECTOR_SUPERSEDED_OUTBOX_STATUS, group_key),
            )

            def active_rank(row: Any) -> tuple[int, str, int, int]:
                return (
                    int(str(row[4]) == current_attention_id),
                    str(row[5] or ""),
                    int(str(row[1]) == _CONNECTOR_LEASE_STATUS),
                    -int(row[0]),
                )

            if not lifecycle_open or terminalized_current_episode:
                for leased_row in leased_rows:
                    conn.execute(
                        """
                        UPDATE connector_outbox
                        SET private_state_json = ?
                        WHERE id = ? AND status = 'leased'
                        """,
                        (
                            _migration_private_state(
                                leased_row[3],
                                group=str(group_key),
                                canonical=False,
                                terminal_after_lease=True,
                            ),
                            int(leased_row[0]),
                        ),
                    )
                continue

            pollable_candidates = [
                row
                for row in candidate_rows
                if str(row[1]) in _CONNECTOR_POLLABLE_STATUSES
            ]
            active_candidates = [*pollable_candidates, *leased_rows]
            if not active_candidates:
                continue
            selected = max(active_candidates, key=active_rank)
            selected_id = int(selected[0])
            selected_is_lease = (
                str(selected[1]) == _CONNECTOR_LEASE_STATUS
                and any(int(row[0]) == selected_id for row in leased_rows)
            )
            for leased_row in leased_rows:
                leased_id = int(leased_row[0])
                conn.execute(
                    """
                    UPDATE connector_outbox
                    SET private_state_json = ?
                    WHERE id = ? AND status = 'leased'
                    """,
                    (
                        _migration_private_state(
                            leased_row[3],
                            group=str(group_key),
                            canonical=selected_is_lease and leased_id == selected_id,
                            terminal_after_lease=(
                                not selected_is_lease or leased_id != selected_id
                            ),
                        ),
                        leased_id,
                    ),
                )
            if selected_is_lease:
                continue
            transition_key = stable_fingerprint(
                {
                    "domain": "tendwire.attention.transition.v1",
                    "host_id": str(host_id),
                    "family_key": str(family_key),
                    "generation": 1,
                    "event_type": str(event_type),
                    "stage": str(stage),
                }
            )
            canonical_key = f"attention:{event_type}:{transition_key}"
            payload = sanitize_public_mapping(
                _json_object(selected[2]), backend_neutral=True
            )
            transition_at = str(selected[5] or "")
            if not transition_at:
                transition_at = (
                    str(lifecycle_row[4])
                    if lifecycle_row is not None
                    else "1970-01-01T00:00:00+00:00"
                )
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, NULL)
                ON CONFLICT(host_id, connector, delivery_key) DO NOTHING
                """,
                (
                    str(host_id),
                    ATTENTION_OUTBOX_CONNECTOR,
                    canonical_key,
                    _canonical_json(payload),
                    _migration_private_state(
                        {},
                        group=str(group_key),
                        canonical=True,
                    ),
                    transition_at,
                    transition_at,
                ),
            )
    conn.execute("DROP TABLE temp.attention_v5_jobs")


def _migrate_v0_to_v1_conn(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_SNAPSHOTS_TABLE)
    if "content_fingerprint" not in _table_columns(conn):
        conn.execute(
            "ALTER TABLE snapshots ADD COLUMN "
            "content_fingerprint TEXT NOT NULL DEFAULT ''"
        )
    _backfill_content_fingerprints(conn)
    for statement in CREATE_LEGACY_SNAPSHOT_INDEXES:
        conn.execute(statement)


def _migrate_v1_to_v2_conn(conn: sqlite3.Connection) -> None:
    _migrate_v0_to_v1_conn(conn)
    conn.execute(CREATE_COMMAND_RECEIPTS_TABLE)
    _ensure_command_receipt_columns(conn)
    _dedupe_command_receipts(conn)
    for statement in CREATE_COMMAND_RECEIPT_INDEXES:
        conn.execute(statement)
    _ensure_command_receipt_unique_index(conn)
    conn.execute(CREATE_WORKER_BINDINGS_TABLE)
    _ensure_worker_binding_columns(conn)
    for statement in CREATE_WORKER_BINDING_INDEXES:
        conn.execute(statement)
    conn.execute(CREATE_WORKER_BINDING_UNIQUE_INDEX)


def _migrate_v2_to_v3_conn(conn: sqlite3.Connection) -> None:
    _migrate_v1_to_v2_conn(conn)
    for statement in CREATE_PR6_TABLES:
        conn.execute(statement)
    _ensure_pr6_columns(conn)
    for statement in CREATE_PR6_INDEXES:
        conn.execute(statement)
    _backfill_command_audit(conn)


def _migrate_v3_to_v4_conn(conn: sqlite3.Connection) -> None:
    _migrate_v2_to_v3_conn(conn)
    _backfill_legacy_attention_columns(conn)


def _migrate_v4_to_v5_conn(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_ATTENTION_LIFECYCLES_TABLE)
    for statement in CREATE_ATTENTION_LIFECYCLE_INDEXES:
        conn.execute(statement)
    _migrate_v4_attention_rows_conn(conn)
    _migrate_v4_attention_outbox_conn(conn)


_LEGACY_TRUNCATION_MARKER = "\n[truncated]"


def _legacy_canonical_field(value: Any) -> tuple[str | None, str]:
    text = sanitize_canonical_turn_text(value)
    if text is None or text == "":
        return None, "absent"
    state = "known_incomplete" if text.endswith(_LEGACY_TRUNCATION_MARKER) else "complete"
    return text, state


def _insert_turn_content_page_boundaries_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    field: str,
    segments: Iterable[Any],
) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO turn_content_page_boundaries (
            host_id,
            turn_id,
            content_revision,
            field,
            page_index,
            start_char,
            start_byte
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                str(host_id),
                str(turn_id),
                str(content_revision_value),
                str(field),
                int(segment.index),
                int(segment.start_char),
                int(segment.start_byte),
            )
            for segment in segments
        ),
    )


def _insert_turn_content_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    user_text: str | None,
    assistant_final_text: str | None,
    user_state: str,
    final_state: str,
    created_at: str,
    is_current: bool = True,
) -> str:
    revision = content_revision(
        str(turn_id),
        user_text,
        assistant_final_text,
        user_state,
        final_state,
    )
    user_segments = segment_canonical_text(user_text or "") if user_state == "complete" else ()
    final_segments = (
        segment_canonical_text(assistant_final_text or "")
        if final_state == "complete"
        else ()
    )
    conn.execute(
        """
        INSERT INTO turn_content_revisions (
            host_id, turn_id, content_revision, user_text, assistant_final_text,
            user_state, final_state, user_char_length, user_byte_length,
            final_char_length, final_byte_length, user_page_count,
            final_page_count, is_current, created_at, superseded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT(host_id, turn_id, content_revision) DO NOTHING
        """,
        (
            str(host_id),
            str(turn_id),
            revision,
            user_text,
            assistant_final_text,
            user_state,
            final_state,
            len(user_text or ""),
            len((user_text or "").encode("utf-8")),
            len(assistant_final_text or ""),
            len((assistant_final_text or "").encode("utf-8")),
            len(user_segments),
            len(final_segments),
            int(bool(is_current)),
            str(created_at),
        ),
    )
    _insert_turn_content_page_boundaries_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        content_revision_value=revision,
        field="user_text",
        segments=user_segments,
    )
    _insert_turn_content_page_boundaries_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        content_revision_value=revision,
        field="assistant_final_text",
        segments=final_segments,
    )
    return revision


def _backfill_legacy_turn_content_conn(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT host_id, turn_id, observed_at, payload_json
        FROM turns
        ORDER BY host_id, turn_id
        """
    ).fetchall()
    for host_id, turn_id, observed_at, payload_json in rows:
        try:
            payload = json.loads(str(payload_json or "{}"))
        except (TypeError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        user_text, user_state = _legacy_canonical_field(payload.get("user_text"))
        final_text, final_state = _legacy_canonical_field(
            payload.get("assistant_final_text")
        )
        if user_state != "absent" or final_state != "absent":
            _insert_turn_content_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(turn_id),
                user_text=user_text,
                assistant_final_text=final_text,
                user_state=user_state,
                final_state=final_state,
                created_at=str(observed_at or "1970-01-01T00:00:00+00:00"),
            )
        for key in (
            "user_text",
            "assistant_final_text",
            "user_preview",
            "assistant_final_preview",
            "content",
        ):
            payload.pop(key, None)
        encoded = _canonical_json(payload)
        conn.execute(
            """
            UPDATE turns
            SET payload_json = ?, fingerprint = ?
            WHERE host_id = ? AND turn_id = ?
            """,
            (
                encoded,
                stable_fingerprint(payload),
                str(host_id),
                str(turn_id),
            ),
        )


def _ensure_payload_turn_content_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    payload: Mapping[str, Any],
    observed_at: str | None,
) -> bool:
    current = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND is_current = 1
        LIMIT 1
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    if current is not None:
        return False
    user_text = sanitize_canonical_turn_text(payload.get("user_text"))
    final_text = sanitize_canonical_turn_text(
        payload.get("assistant_final_text")
    )
    if user_text == "":
        user_text = None
    if final_text == "":
        final_text = None
    user_state = "complete" if user_text else "absent"
    final_state = "complete" if final_text else "absent"
    if user_state == "absent" and final_state == "absent":
        return _ensure_absent_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            observed_at=observed_at,
        )
    _insert_turn_content_revision_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        user_text=user_text,
        assistant_final_text=final_text,
        user_state=user_state,
        final_state=final_state,
        created_at=str(observed_at or "1970-01-01T00:00:00+00:00"),
    )
    return True


def _ensure_absent_turn_content_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    observed_at: str | None,
) -> bool:
    current = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND is_current = 1
        LIMIT 1
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    if current is not None:
        return False
    revision = _insert_turn_content_revision_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        user_text=None,
        assistant_final_text=None,
        user_state="absent",
        final_state="absent",
        created_at=str(observed_at or "1970-01-01T00:00:00+00:00"),
    )
    cursor = conn.execute(
        """
        UPDATE turn_content_revisions
        SET is_current = 1, superseded_at = NULL
        WHERE host_id = ?
          AND turn_id = ?
          AND content_revision = ?
          AND NOT EXISTS (
              SELECT 1
              FROM turn_content_revisions AS current_revision
              WHERE current_revision.host_id = ?
                AND current_revision.turn_id = ?
                AND current_revision.is_current = 1
          )
        """,
        (
            str(host_id),
            str(turn_id),
            revision,
            str(host_id),
            str(turn_id),
        ),
    )
    return bool(cursor.rowcount)


def _backfill_missing_turn_content_revisions_conn(
    conn: sqlite3.Connection,
) -> int:
    """Give every stored turn one stable authoritative v2 content descriptor."""
    repaired = 0
    cursor = conn.execute(
        """
        SELECT turns.host_id, turns.turn_id, turns.observed_at
        FROM turns
        WHERE NOT EXISTS (
            SELECT 1
            FROM turn_content_revisions AS revisions
            WHERE revisions.host_id = turns.host_id
              AND revisions.turn_id = turns.turn_id
              AND revisions.is_current = 1
        )
        ORDER BY turns.host_id, turns.turn_id
        """
    )
    while True:
        rows = cursor.fetchmany(500)
        if not rows:
            return repaired
        for host_id, turn_id, observed_at in rows:
            if _ensure_absent_turn_content_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(turn_id),
                observed_at=str(observed_at) if observed_at else None,
            ):
                repaired += 1


def _rebuild_v6_presentation_plans_conn(conn: sqlite3.Connection) -> None:
    """Rebuild the two bounded plan tables with generation-aware v7 keys."""
    conn.execute(
        """
        CREATE TABLE turn_presentation_plans_v7 (
            id INTEGER PRIMARY KEY,
            host_id TEXT NOT NULL,
            name TEXT NOT NULL,
            plan_token TEXT NOT NULL,
            turn_id TEXT NOT NULL,
            content_revision TEXT NOT NULL,
            presentation_version TEXT NOT NULL,
            generation INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
            part_count INTEGER NOT NULL CHECK (part_count > 0),
            state TEXT NOT NULL
                CHECK (state IN (
                    'preparing',
                    'waiting_predecessor',
                    'active',
                    'completed',
                    'superseded',
                    'failed'
                )),
            replaces_plan_token TEXT,
            recovers_plan_token TEXT,
            created_at TEXT NOT NULL,
            activated_at TEXT,
            completed_at TEXT,
            UNIQUE (host_id, name, plan_token),
            UNIQUE (
                host_id,
                name,
                turn_id,
                content_revision,
                presentation_version,
                generation
            ),
            FOREIGN KEY (host_id, turn_id, content_revision)
                REFERENCES turn_content_revisions(host_id, turn_id, content_revision)
                ON DELETE RESTRICT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO turn_presentation_plans_v7 (
            id, host_id, name, plan_token, turn_id, content_revision,
            presentation_version, generation, part_count, state,
            replaces_plan_token, recovers_plan_token, created_at,
            activated_at, completed_at
        )
        SELECT
            id, host_id, name, plan_token, turn_id, content_revision,
            presentation_version, 1, part_count, state,
            replaces_plan_token, NULL, created_at, activated_at, completed_at
        FROM turn_presentation_plans
        ORDER BY id
        """
    )
    conn.execute(
        """
        CREATE TABLE turn_presentation_jobs_v7 (
            id INTEGER PRIMARY KEY,
            plan_id INTEGER NOT NULL,
            sequence_index INTEGER NOT NULL CHECK (sequence_index >= 0),
            operation TEXT NOT NULL CHECK (operation IN ('upsert', 'retire')),
            part_ordinal INTEGER NOT NULL CHECK (part_ordinal >= 0),
            spans_json TEXT NOT NULL,
            outbox_id INTEGER UNIQUE,
            created_at TEXT NOT NULL,
            UNIQUE (plan_id, sequence_index),
            UNIQUE (plan_id, operation, part_ordinal),
            FOREIGN KEY (plan_id)
                REFERENCES turn_presentation_plans_v7(id) ON DELETE CASCADE,
            FOREIGN KEY (outbox_id)
                REFERENCES connector_outbox(id) ON DELETE RESTRICT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO turn_presentation_jobs_v7 (
            id, plan_id, sequence_index, operation, part_ordinal,
            spans_json, outbox_id, created_at
        )
        SELECT
            id, plan_id, sequence_index, operation, part_ordinal,
            spans_json, outbox_id, created_at
        FROM turn_presentation_jobs
        ORDER BY id
        """
    )
    conn.execute("DROP TABLE turn_presentation_jobs")
    conn.execute("DROP TABLE turn_presentation_plans")
    conn.execute(
        "ALTER TABLE turn_presentation_plans_v7 RENAME TO turn_presentation_plans"
    )
    conn.execute(
        "ALTER TABLE turn_presentation_jobs_v7 RENAME TO turn_presentation_jobs"
    )


def _migrate_v6_to_v7_conn(conn: sqlite3.Connection) -> None:
    """Add explicit failed-plan generations and immutable recovery audit."""
    conn.execute(CREATE_TURN_CONTENT_PAGE_BOUNDARIES_TABLE)
    _backfill_missing_turn_content_revisions_conn(conn)
    _backfill_missing_turn_content_page_boundaries_conn(conn)
    plan_columns = {
        str(row[1])
        for row in conn.execute(
            "PRAGMA table_info(turn_presentation_plans)"
        ).fetchall()
    }
    if "generation" not in plan_columns:
        _rebuild_v6_presentation_plans_conn(conn)
    conn.execute(CREATE_TURN_PRESENTATION_RECOVERIES_TABLE)
    for statement in CREATE_TURN_PRESENTATION_INDEXES:
        conn.execute(statement)


def _migrate_v5_to_v6_conn(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_TURN_CONTENT_REVISIONS_TABLE)
    conn.execute(CREATE_TURN_CONTENT_PAGE_BOUNDARIES_TABLE)
    for statement in CREATE_TURN_CONTENT_REVISION_INDEXES:
        conn.execute(statement)
    conn.execute(CREATE_TURN_PRESENTATION_PLANS_TABLE)
    conn.execute(CREATE_TURN_PRESENTATION_JOBS_TABLE)
    conn.execute(CREATE_TURN_PRESENTATION_RECOVERIES_TABLE)
    for statement in CREATE_TURN_PRESENTATION_INDEXES:
        conn.execute(statement)
    _backfill_legacy_turn_content_conn(conn)
    _backfill_missing_turn_content_revisions_conn(conn)


def _normalize_snapshot_created_at_v8_conn(
    conn: sqlite3.Connection,
) -> None:
    """Canonicalize legacy ordering keys before the v8 age index is built."""
    last_id = 0
    while True:
        rows = conn.execute(
            """
            SELECT id, created_at, payload
            FROM snapshots
            WHERE id > ?
            ORDER BY id
            LIMIT 500
            """,
            (last_id,),
        ).fetchall()
        if not rows:
            return
        updates: list[tuple[str, int]] = []
        for row_id, raw_created_at, raw_payload in rows:
            raw_created_at_text = str(raw_created_at)
            canonical = _strict_utc_timestamp(raw_created_at_text)
            if (
                canonical == _LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE
                and not _legacy_snapshot_created_at_is_authoritative(
                    raw_created_at,
                    raw_payload,
                )
            ):
                canonical = None
            canonical = canonical or _SNAPSHOT_CREATED_AT_QUARANTINE
            if str(raw_created_at) != canonical:
                updates.append((canonical, int(row_id)))
            last_id = int(row_id)
        if updates:
            conn.executemany(
                "UPDATE snapshots SET created_at = ? WHERE id = ?",
                updates,
            )


def _migrate_v7_to_v8_conn(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_STORE_MAINTENANCE_STATE_TABLE)
    conn.execute(INSERT_STORE_MAINTENANCE_STATE)
    _normalize_snapshot_created_at_v8_conn(conn)
    for statement in CREATE_SNAPSHOT_INDEXES:
        conn.execute(statement)
    for index_name in (
        "idx_snapshots_host_id",
        "idx_snapshots_created_at",
        "idx_snapshots_content_fingerprint",
        "idx_snapshots_host_created_id",
    ):
        conn.execute(f"DROP INDEX IF EXISTS {index_name}")


def _ensure_turn_list_state_conn(conn: sqlite3.Connection) -> str:
    conn.execute(CREATE_TURN_LIST_STATE_TABLE)
    row = conn.execute(
        "SELECT store_epoch FROM turn_list_state WHERE scope = 'turn-list'"
    ).fetchone()
    if row is not None and str(row[0]):
        return str(row[0])
    epoch = secrets.token_urlsafe(32)
    conn.execute(
        """
        INSERT INTO turn_list_state (scope, store_epoch)
        VALUES ('turn-list', ?)
        ON CONFLICT(scope) DO NOTHING
        """,
        (epoch,),
    )
    row = conn.execute(
        "SELECT store_epoch FROM turn_list_state WHERE scope = 'turn-list'"
    ).fetchone()
    if row is None or not str(row[0]):
        raise StoreSchemaError("turn_list_state_unavailable")
    return str(row[0])


def _turn_list_store_epoch_conn(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT store_epoch FROM turn_list_state WHERE scope = 'turn-list'"
    ).fetchone()
    if row is None or not str(row[0]):
        raise StoreSchemaError("turn_list_state_unavailable")
    return str(row[0])


def _ensure_turn_list_host_states_conn(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_TURN_LIST_HOSTS_TABLE)
    conn.execute(
        """
        INSERT INTO turn_list_hosts (
            host_id,
            next_sequence,
            traversal_generation
        )
        SELECT host_id, COALESCE(MAX(list_sequence), 0) + 1, 1
        FROM turns
        GROUP BY host_id
        ON CONFLICT(host_id) DO NOTHING
        """
    )


def _migrate_v8_to_v9_conn(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "turns")
    if "list_sequence" not in columns:
        conn.execute(
            "ALTER TABLE turns ADD COLUMN list_sequence "
            "INTEGER NOT NULL DEFAULT 0"
        )
    conn.execute(
        """
        WITH ranked AS (
            SELECT
                host_id,
                turn_id,
                ROW_NUMBER() OVER (
                    PARTITION BY host_id
                    ORDER BY COALESCE(updated_at, observed_at, ''), turn_id
                ) AS assigned_sequence
            FROM turns
        )
        UPDATE turns
        SET list_sequence = (
            SELECT assigned_sequence
            FROM ranked
            WHERE ranked.host_id = turns.host_id
              AND ranked.turn_id = turns.turn_id
        )
        WHERE list_sequence <= 0
        """
    )
    for statement in CREATE_TURN_LIST_INDEXES:
        conn.execute(statement)
    _ensure_turn_list_state_conn(conn)
    _ensure_turn_list_host_states_conn(conn)


def _migrate_v9_to_v10_conn(conn: sqlite3.Connection) -> None:
    """Add explicit pending freshness, private routing, and two-phase claims."""
    conn.execute(CREATE_LEGACY_BACKEND_PENDING_TABLE)
    columns = _table_columns(conn, "backend_pending")
    additions = (
        ("revision_digest", "TEXT NOT NULL DEFAULT ''"),
        ("choice_routes_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("binding_private_fingerprint", "TEXT NOT NULL DEFAULT ''"),
        ("observed_turn_target_value", "TEXT NOT NULL DEFAULT ''"),
        ("observation_state", "TEXT NOT NULL DEFAULT 'open'"),
        ("freshness", "TEXT NOT NULL DEFAULT 'fresh'"),
        ("last_success_at", "TEXT"),
        ("last_failure_at", "TEXT"),
        ("grace_deadline", "TEXT"),
        ("updated_at", "TEXT NOT NULL DEFAULT ''"),
    )
    for name, declaration in additions:
        if name not in columns:
            conn.execute(
                f"ALTER TABLE backend_pending ADD COLUMN {name} {declaration}"
            )
    rows = conn.execute(
        """
        SELECT host_id, worker_id, payload_json, observed_at,
               revision_digest, last_success_at, updated_at
        FROM backend_pending
        """
    ).fetchall()
    for (
        host_id,
        worker_id,
        payload_json,
        observed_at,
        revision_digest,
        last_success_at,
        updated_at,
    ) in rows:
        timestamp = _strict_utc_timestamp(observed_at) or "1970-01-01T00:00:00+00:00"
        digest = str(revision_digest or "") or stable_fingerprint(
            {"legacy_backend_pending": str(payload_json)}
        )
        conn.execute(
            """
            UPDATE backend_pending
            SET revision_digest = ?,
                freshness = 'fresh',
                last_success_at = ?,
                updated_at = ?
            WHERE host_id = ? AND worker_id = ?
            """,
            (
                digest,
                str(last_success_at or timestamp),
                str(updated_at or timestamp),
                str(host_id),
                str(worker_id),
            ),
        )
    conn.execute(CREATE_BACKEND_PENDING_CLAIMS_TABLE)


MIGRATIONS: tuple[Migration, ...] = (
    Migration(0, 1, _migrate_v0_to_v1_conn),
    Migration(1, 2, _migrate_v1_to_v2_conn),
    Migration(2, 3, _migrate_v2_to_v3_conn),
    Migration(3, 4, _migrate_v3_to_v4_conn),
    Migration(4, 5, _migrate_v4_to_v5_conn),
    Migration(5, 6, _migrate_v5_to_v6_conn),
    Migration(6, 7, _migrate_v6_to_v7_conn),
    Migration(7, 8, _migrate_v7_to_v8_conn),
    Migration(8, 9, _migrate_v8_to_v9_conn),
    Migration(9, 10, _migrate_v9_to_v10_conn),
)


def _validate_migration_registry(
    migrations: tuple[Migration, ...] | None = None,
    *,
    target_version: int = STORE_SCHEMA_VERSION,
) -> None:
    registry = MIGRATIONS if migrations is None else migrations
    expected = 0
    for migration in registry:
        if (
            migration.from_version != expected
            or migration.to_version != expected + 1
        ):
            raise RuntimeError("invalid migration registry")
        expected = migration.to_version
    if expected != STORE_SCHEMA_VERSION:
        raise RuntimeError("invalid migration registry target")
    if not 0 <= int(target_version) <= STORE_SCHEMA_VERSION:
        raise RuntimeError("unsupported migration target")


def _create_current_schema_conn(conn: sqlite3.Connection) -> None:
    """Create an empty database directly at the current schema."""
    if conn.in_transaction:
        raise StoreSchemaError("schema_migration_in_transaction")
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(CREATE_SNAPSHOTS_TABLE)
        conn.execute(CREATE_COMMAND_RECEIPTS_TABLE)
        conn.execute(CREATE_WORKER_BINDINGS_TABLE)
        for statement in CREATE_PR6_TABLES:
            conn.execute(statement)
        conn.execute(CREATE_LEGACY_BACKEND_PENDING_TABLE)
        _migrate_v9_to_v10_conn(conn)
        conn.execute(CREATE_ATTENTION_LIFECYCLES_TABLE)
        conn.execute(CREATE_TURN_CONTENT_REVISIONS_TABLE)
        conn.execute(CREATE_TURN_CONTENT_PAGE_BOUNDARIES_TABLE)
        conn.execute(CREATE_TURN_PRESENTATION_PLANS_TABLE)
        conn.execute(CREATE_TURN_PRESENTATION_JOBS_TABLE)
        conn.execute(CREATE_TURN_PRESENTATION_RECOVERIES_TABLE)
        conn.execute(CREATE_STORE_MAINTENANCE_STATE_TABLE)
        conn.execute(CREATE_TURN_LIST_STATE_TABLE)
        conn.execute(CREATE_TURN_LIST_HOSTS_TABLE)
        for statement in CREATE_COMMAND_RECEIPT_INDEXES:
            conn.execute(statement)
        conn.execute(CREATE_COMMAND_RECEIPT_UNIQUE_INDEX)
        for statement in CREATE_WORKER_BINDING_INDEXES:
            conn.execute(statement)
        conn.execute(CREATE_WORKER_BINDING_UNIQUE_INDEX)
        for statement in CREATE_PR6_INDEXES:
            conn.execute(statement)
        for statement in CREATE_TURN_LIST_INDEXES:
            conn.execute(statement)
        for statement in CREATE_ATTENTION_LIFECYCLE_INDEXES:
            conn.execute(statement)
        for statement in CREATE_TURN_CONTENT_REVISION_INDEXES:
            conn.execute(statement)
        for statement in CREATE_TURN_PRESENTATION_INDEXES:
            conn.execute(statement)
        for statement in CREATE_SNAPSHOT_INDEXES:
            conn.execute(statement)
        conn.execute(INSERT_STORE_MAINTENANCE_STATE)
        _ensure_turn_list_state_conn(conn)
        conn.execute(f"PRAGMA user_version = {STORE_SCHEMA_VERSION}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _database_has_application_objects(conn: sqlite3.Connection) -> bool:
    return (
        conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
              AND type IN ('table', 'index', 'view', 'trigger')
            LIMIT 1
            """
        ).fetchone()
        is not None
    )


def _run_migrations(
    conn: sqlite3.Connection,
    *,
    target_version: int = STORE_SCHEMA_VERSION,
) -> None:
    """Run exact ordered transitions with one transaction per version."""
    if conn.in_transaction:
        raise StoreSchemaError("schema_migration_in_transaction")
    _validate_migration_registry(target_version=target_version)
    current = int(conn.execute("PRAGMA user_version").fetchone()[0])
    while current < int(target_version):
        migration = MIGRATIONS[current]
        if migration.from_version != current:
            raise RuntimeError("invalid migration registry dispatch")
        conn.execute("BEGIN IMMEDIATE")
        try:
            migration.apply(conn)
            conn.execute(f"PRAGMA user_version = {migration.to_version}")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        current = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if current != migration.to_version:
            raise StoreSchemaError("schema_version_not_advanced")


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Gate the current schema cheaply, or initialize/migrate older stores."""
    version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if version == STORE_SCHEMA_VERSION:
        return
    if version > STORE_SCHEMA_VERSION:
        raise StoreSchemaError("schema_too_new")
    if not isinstance(conn, _ClosingConnection):
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)
    schema_authority = _schema_connection_authority(conn)
    authority = (
        nullcontext()
        if schema_authority.parent_fd is None
        else _filesystem_schema_mutation_authority(conn)
    )
    with authority:
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version == STORE_SCHEMA_VERSION:
            return
        if version > STORE_SCHEMA_VERSION:
            raise StoreSchemaError("schema_too_new")
        with private_file_creation_umask():
            _configure_persistent_database_conn(conn)
            if version == 0 and not _database_has_application_objects(conn):
                _create_current_schema_conn(conn)
                return
            _run_migrations(conn)


_ensure_schema = ensure_schema


def init_store(db_path: Path) -> None:
    """Initialize or migrate the sqlite store to the current schema."""
    with _connect(db_path, prepare=True) as conn:
        ensure_schema(conn)


def store_status(
    db_path: Path,
    host_id: str,
    *,
    snapshot_retention_days: int = 14,
    snapshot_retention_count: int = 4096,
    maintenance_batch_size: int = 100,
    maintenance_cadence_seconds: int = 3600,
    require_current_schema: bool = False,
) -> dict[str, Any]:
    """Return bounded public-safe host state and database maintenance aggregates."""
    policy = SnapshotRetentionPolicy(
        retention_days=snapshot_retention_days,
        retention_count=snapshot_retention_count,
        batch_size=maintenance_batch_size,
    )
    maintenance_empty = {
        "last_completed_at": None,
        "status": "not_initialized",
        "snapshot_count": 0,
        "snapshot_retention_days": policy.retention_days,
        "snapshot_retention_count": policy.retention_count,
        "maintenance_batch_size": policy.batch_size,
        "maintenance_cadence_seconds": int(maintenance_cadence_seconds),
        "backlog": False,
    }
    def unavailable(status: str) -> dict[str, Any]:
        return dict(sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": status,
            "host_id": str(host_id),
            "counts": {},
            "outbox": {
                "pending": 0,
                "leased": 0,
                "terminal": 0,
                "by_status": {},
            },
            "maintenance": maintenance_empty,
        }))

    if not require_current_schema and not _sqlite_store_exists(db_path):
        return unavailable("store_unavailable")
    tables = (
        "snapshots",
        "events",
        "spaces",
        "workers",
        "turns",
        "pending_interactions",
        "attention_items",
        "commands",
        "command_receipts",
        "backend_health",
    )
    try:
        with _connect(db_path, read_only=require_current_schema) as conn:
            if require_current_schema:
                conn.execute("PRAGMA query_only=ON")
                version = int(conn.execute("PRAGMA user_version").fetchone()[0])
                if version != STORE_SCHEMA_VERSION:
                    return unavailable("schema_not_current")
            else:
                _ensure_schema(conn)
            counts = {
                table: int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE host_id = ?",
                        (str(host_id),),
                    ).fetchone()[0]
                )
                for table in tables
            }
            last_event_row = conn.execute(
                """
                SELECT observed_at
                FROM events
                WHERE host_id = ?
                ORDER BY observed_at DESC, id DESC
                LIMIT 1
                """,
                (str(host_id),),
            ).fetchone()
            last_snapshot_row = conn.execute(
                """
                SELECT created_at
                FROM snapshots
                WHERE host_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(host_id),),
            ).fetchone()
            outbox_rows = conn.execute(
                """
                SELECT status, COUNT(*)
                FROM connector_outbox
                WHERE host_id = ?
                GROUP BY status
                """,
                (str(host_id),),
            ).fetchall()
            maintenance_row = conn.execute(
                """
                SELECT last_completed_at, last_status
                FROM store_maintenance_state
                WHERE scope = 'automatic'
                """
            ).fetchone()
            snapshot_count = int(
                conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            )
            backlog_ids, _ = _snapshot_retention_candidates_conn(
                conn,
                cutoff_at=_utc_cutoff(retention_days=policy.retention_days),
                retention_count=policy.retention_count,
                batch_size=1,
            )
    except (LocalStateError, StoreSchemaError, sqlite3.Error):
        if require_current_schema:
            return unavailable("store_unavailable")
        raise
    by_status: dict[str, int] = {}
    for row in outbox_rows:
        status = _store_public_label(row[0], allowed=_CONNECTOR_PUBLIC_OUTBOX_STATUSES)
        by_status[status] = by_status.get(status, 0) + int(row[1] or 0)
    pending_statuses = _CONNECTOR_POLLABLE_STATUSES
    terminal_statuses = {
        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
        _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
    }
    outbox = {
        "pending": sum(
            count for status, count in by_status.items() if status in pending_statuses
        ),
        "leased": int(by_status.get(_CONNECTOR_LEASE_STATUS, 0)),
        "terminal": sum(
            count for status, count in by_status.items() if status in terminal_statuses
        ),
        "by_status": by_status,
    }
    maintenance = {
        **maintenance_empty,
        "last_completed_at": (
            maintenance_row[0] if maintenance_row is not None else None
        ),
        "status": (
            _store_public_label(
                maintenance_row[1],
                allowed={"never", "ok", "failed"},
            )
            if maintenance_row is not None
            else "not_initialized"
        ),
        "snapshot_count": snapshot_count,
        "backlog": bool(backlog_ids),
    }
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "counts": counts,
        "outbox": outbox,
        "last_event_at": last_event_row[0] if last_event_row is not None else None,
        "last_snapshot_at": last_snapshot_row[0] if last_snapshot_row is not None else None,
        "maintenance": maintenance,
    })


def tail_event_metadata(
    db_path: Path,
    host_id: str,
    *,
    limit: int = 20,
) -> dict[str, Any]:
    """Return bounded event/history metadata without raw payloads."""
    row_limit = max(1, min(int(limit), 100))
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "limit": row_limit,
            "events": [],
        })
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT id, event_type, aggregate_type, observed_at, content_fingerprint
            FROM events
            WHERE host_id = ?
            ORDER BY observed_at DESC, id DESC
            LIMIT ?
            """,
            (str(host_id), row_limit),
        ).fetchall()
    events = [
        {
            "row_id": int(row[0]),
            "event_type": _store_public_label(row[1]),
            "aggregate_type": _store_public_label(row[2]),
            "observed_at": str(row[3] or ""),
            "content_fingerprint": str(row[4] or ""),
        }
        for row in rows
    ]
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "limit": row_limit,
        "events": events,
    })


_TURN_CONTENT_MAINTENANCE_BATCH = 100
_TURN_CONTENT_MAINTENANCE_BATCH_MAX = 1_000
_TURN_CONTENT_TERMINAL_PLAN_STATES = frozenset(
    {"completed", "superseded", "failed"}
)
_TURN_CONTENT_TERMINAL_OUTBOX_STATES = frozenset(
    {
        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
        _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
    }
)


_SNAPSHOT_AGE_CANDIDATE_SQL = """
SELECT candidate.id
FROM snapshots AS candidate INDEXED BY idx_snapshots_created_host_id
WHERE candidate.created_at < :cutoff_at
  AND candidate.id <> (
      SELECT newest.id
      FROM snapshots AS newest INDEXED BY idx_snapshots_host_newest
      WHERE newest.host_id = candidate.host_id
      ORDER BY newest.id DESC
      LIMIT 1
  )
ORDER BY candidate.created_at, candidate.host_id, candidate.id
LIMIT :candidate_limit
"""

_SNAPSHOT_COUNT_CANDIDATE_SQL = """
WITH RECURSIVE
hosts(host_id) AS (
    SELECT MIN(first_host.host_id)
    FROM snapshots AS first_host INDEXED BY idx_snapshots_host_newest
    UNION ALL
    SELECT (
        SELECT MIN(next_host.host_id)
        FROM snapshots AS next_host INDEXED BY idx_snapshots_host_newest
        WHERE next_host.host_id > hosts.host_id
    )
    FROM hosts
    WHERE hosts.host_id IS NOT NULL
),
boundaries(host_id, boundary_id) AS MATERIALIZED (
    SELECT
        hosts.host_id,
        (
            SELECT boundary.id
            FROM snapshots AS boundary INDEXED BY idx_snapshots_host_newest
            WHERE boundary.host_id = hosts.host_id
            ORDER BY boundary.id DESC
            LIMIT 1 OFFSET :retention_offset
        )
    FROM hosts
    WHERE hosts.host_id IS NOT NULL
)
SELECT candidate.id
FROM boundaries
JOIN snapshots AS candidate INDEXED BY idx_snapshots_host_newest
  ON candidate.host_id = boundaries.host_id
 AND candidate.id < boundaries.boundary_id
WHERE boundaries.boundary_id IS NOT NULL
LIMIT :candidate_limit
"""


def _snapshot_retention_candidates_conn(
    conn: sqlite3.Connection,
    *,
    cutoff_at: str,
    retention_count: int,
    batch_size: int,
) -> tuple[list[int], bool]:
    candidate_limit = int(batch_size) + 1
    age_ids = [
        int(row[0])
        for row in conn.execute(
            _SNAPSHOT_AGE_CANDIDATE_SQL,
            {
                "cutoff_at": str(cutoff_at),
                "candidate_limit": candidate_limit,
            },
        ).fetchall()
    ]
    if len(age_ids) == candidate_limit:
        return sorted(set(age_ids))[: int(batch_size)], True
    count_ids = [
        int(row[0])
        for row in conn.execute(
            _SNAPSHOT_COUNT_CANDIDATE_SQL,
            {
                "retention_offset": int(retention_count) - 1,
                "candidate_limit": candidate_limit,
            },
        ).fetchall()
    ]
    merged = sorted(set(age_ids).union(count_ids))
    candidates = merged[: int(batch_size)]
    saturated = (
        len(merged) > int(batch_size)
        or len(count_ids) == candidate_limit
    )
    return candidates, saturated


def _delete_snapshot_candidates_conn(
    conn: sqlite3.Connection,
    candidate_ids: Iterable[int],
) -> int:
    ids = tuple(int(candidate_id) for candidate_id in candidate_ids)
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    return int(
        conn.execute(
            f"DELETE FROM snapshots WHERE id IN ({placeholders})",
            ids,
        ).rowcount
        or 0
    )


def _snapshot_retention_result(
    *,
    ok: bool,
    status: str,
    dry_run: bool,
    policy: SnapshotRetentionPolicy,
    cutoff_at: str,
    examined: int,
    deleted: int,
    eligible: int,
    remaining_candidates: bool,
    latest_hosts_retained: int,
) -> dict[str, Any]:
    return dict(sanitize_public_value({
        "schema_version": 1,
        "ok": bool(ok),
        "status": str(status),
        "scope": "database",
        "dry_run": bool(dry_run),
        "retention_days": policy.retention_days,
        "retention_count": policy.retention_count,
        "cutoff_at": str(cutoff_at),
        "batch_size": policy.batch_size,
        "examined": int(examined),
        "deleted": int(deleted),
        "eligible": int(eligible),
        "remaining_candidates": bool(remaining_candidates),
        "latest_hosts_retained": int(latest_hosts_retained),
    }))


def cleanup_snapshot_retention(
    db_path: Path,
    *,
    retention_days: int,
    retention_count: int,
    batch_size: int = 100,
    now: str | None = None,
    dry_run: bool = False,
    _store_lock_held: bool = False,
    _expected_db_identity: EntryIdentity | None = None,
) -> dict[str, Any]:
    """Apply one bounded database-wide snapshot retention batch."""
    try:
        policy = SnapshotRetentionPolicy(
            retention_days=retention_days,
            retention_count=retention_count,
            batch_size=batch_size,
        )
    except ValueError:
        fallback = SnapshotRetentionPolicy()
        return _snapshot_retention_result(
            ok=False,
            status="invalid_policy",
            dry_run=bool(dry_run),
            policy=fallback,
            cutoff_at=_utc_cutoff(retention_days=fallback.retention_days, now=now),
            examined=0,
            deleted=0,
            eligible=0,
            remaining_candidates=False,
            latest_hosts_retained=0,
        )
    cutoff_at = _utc_cutoff(retention_days=policy.retention_days, now=now)
    if not _sqlite_store_exists(db_path):
        return _snapshot_retention_result(
            ok=False,
            status="store_unavailable",
            dry_run=bool(dry_run),
            policy=policy,
            cutoff_at=cutoff_at,
            examined=0,
            deleted=0,
            eligible=0,
            remaining_candidates=False,
            latest_hosts_retained=0,
        )
    with _connect(
        db_path,
        isolation_level=None,
        _store_lock_held=_store_lock_held,
        _expected_db_identity=_expected_db_identity,
    ) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            candidates, saturated = _snapshot_retention_candidates_conn(
                conn,
                cutoff_at=cutoff_at,
                retention_count=policy.retention_count,
                batch_size=policy.batch_size,
            )
            deleted = (
                0
                if dry_run
                else _delete_snapshot_candidates_conn(conn, candidates)
            )
            if dry_run:
                remaining = saturated
                conn.rollback()
            else:
                remaining_ids, _ = _snapshot_retention_candidates_conn(
                    conn,
                    cutoff_at=cutoff_at,
                    retention_count=policy.retention_count,
                    batch_size=1,
                )
                remaining = bool(remaining_ids)
                conn.commit()
            latest_hosts_retained = int(
                conn.execute(
                    "SELECT COUNT(DISTINCT host_id) FROM snapshots"
                ).fetchone()[0]
            )
        except Exception:
            conn.rollback()
            raise
    return _snapshot_retention_result(
        ok=True,
        status="ok",
        dry_run=bool(dry_run),
        policy=policy,
        cutoff_at=cutoff_at,
        examined=len(candidates),
        deleted=deleted,
        eligible=len(candidates),
        remaining_candidates=remaining,
        latest_hosts_retained=latest_hosts_retained,
    )


_COMPACTION_STATUSES = frozenset(
    {
        "dry_run",
        "completed",
        "invalid_request",
        "store_unavailable",
        "schema_not_current",
        "permissions_failed",
        "offline_required",
        "integrity_failed",
        "insufficient_space",
        "backup_failed",
        "maintenance_failed",
        "checkpoint_failed",
        "replacement_failed",
        "rollback_completed",
        "rollback_failed",
    }
)


class _CompactionAbort(RuntimeError):
    def __init__(self, status: str) -> None:
        if status not in _COMPACTION_STATUSES:
            status = "replacement_failed"
        self.status = status
        super().__init__(status)


def _compaction_report(options: CompactionOptions) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "ok": False,
        "status": "invalid_request",
        "command": "store.compact",
        "scope": "database",
        "dry_run": bool(options.dry_run),
        "maintenance_window_acknowledged": bool(options.acknowledge_offline),
        "permissions": {"ok": False, "outcome": "unsafe"},
        "integrity": {
            "before": "not_run",
            "backup": "not_run",
            "replacement": "not_run",
            "after": "not_run",
        },
        "space": {
            "available_bytes": 0,
            "required_bytes": 0,
            "headroom_ok": False,
        },
        "snapshots": {
            "before": 0,
            "retained": 0,
            "eligible": 0,
            "examined": 0,
            "deleted": 0,
            "remaining": 0,
            "latest_hosts_retained": 0,
        },
        "storage": {
            "before_bytes": 0,
            "estimated_reclaimable_bytes": 0,
            "after_bytes": None,
        },
        "backup": {
            "required": True,
            "created": False,
            "verified": False,
        },
        "checkpoint": {"status": "not_run"},
        "replacement": {"status": "not_run"},
        "rollback": {"status": "not_needed"},
    }


def _public_compaction_report(
    report: dict[str, Any], *, status: str, ok: bool = False
) -> dict[str, Any]:
    report["status"] = status if status in _COMPACTION_STATUSES else "replacement_failed"
    report["ok"] = bool(ok)
    public = dict(sanitize_public_value(report))
    public["command"] = "store.compact"
    return public


def _call_compaction_phase(
    phase_hook: Callable[[str], None] | None,
    phase: str,
    *,
    failure_status: str,
) -> None:
    if phase_hook is None:
        return
    try:
        phase_hook(phase)
    except Exception:
        raise _CompactionAbort(failure_status) from None


def _readonly_sqlite_at(
    parent_fd: int,
    leaf: str,
    *,
    immutable: bool = False,
    expected_identity: EntryIdentity | None = None,
) -> sqlite3.Connection:
    if expected_identity is not None:
        _verify_expected_identity_at(parent_fd, leaf, expected_identity)
    canonical_path = canonical_path_from_fd(parent_fd, leaf)
    immutable_query = "&immutable=1" if immutable else ""
    target = f"file:{quote(canonical_path, safe='/')}?mode=ro{immutable_query}"
    conn = sqlite3.connect(target, timeout=0.0, isolation_level=None, uri=True)
    try:
        if expected_identity is not None:
            _verify_expected_identity_at(parent_fd, leaf, expected_identity)
        conn.execute("PRAGMA busy_timeout=0")
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn
    except Exception:
        conn.close()
        raise


def _writable_sqlite_at(
    parent_fd: int,
    leaf: str,
    *,
    expected_identity: EntryIdentity | None = None,
) -> sqlite3.Connection:
    if expected_identity is not None:
        _verify_expected_identity_at(parent_fd, leaf, expected_identity)
    canonical_path = canonical_path_from_fd(parent_fd, leaf)
    target = f"file:{quote(canonical_path, safe='/')}?mode=rw"
    with private_file_creation_umask():
        conn = sqlite3.connect(target, timeout=0.0, isolation_level=None, uri=True)
    try:
        if expected_identity is not None:
            _verify_expected_identity_at(parent_fd, leaf, expected_identity)
        conn.execute("PRAGMA busy_timeout=0")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn
    except Exception:
        conn.close()
        raise


def _quick_check_ok(conn: sqlite3.Connection) -> bool:
    try:
        rows = conn.execute("PRAGMA quick_check").fetchall()
    except sqlite3.Error:
        return False
    return len(rows) == 1 and str(rows[0][0]).lower() == "ok"


def _foreign_key_check_ok(conn: sqlite3.Connection) -> bool:
    try:
        return conn.execute("PRAGMA foreign_key_check").fetchone() is None
    except sqlite3.Error:
        return False


def _compaction_snapshot_metrics(
    conn: sqlite3.Connection,
    *,
    cutoff_at: str,
    retention_count: int,
) -> tuple[int, int, int, int]:
    row = conn.execute(
        """
        WITH ranked AS (
            SELECT
                host_id,
                created_at,
                payload,
                content_fingerprint,
                ROW_NUMBER() OVER (
                    PARTITION BY host_id
                    ORDER BY id DESC
                ) AS newest_rank
            FROM snapshots
        )
        SELECT
            COUNT(*),
            COALESCE(SUM(
                CASE
                    WHEN newest_rank > 1
                     AND (created_at < ? OR newest_rank > ?)
                    THEN 1
                    ELSE 0
                END
            ), 0),
            COUNT(DISTINCT host_id),
            COALESCE(SUM(
                CASE
                    WHEN newest_rank > 1
                     AND (created_at < ? OR newest_rank > ?)
                    THEN LENGTH(payload)
                       + LENGTH(host_id)
                       + LENGTH(created_at)
                       + LENGTH(content_fingerprint)
                       + 64
                    ELSE 0
                END
            ), 0)
        FROM ranked
        """,
        (
            str(cutoff_at),
            int(retention_count),
            str(cutoff_at),
            int(retention_count),
        ),
    ).fetchone()
    if row is None:
        raise _CompactionAbort("store_unavailable")
    return int(row[0]), int(row[1]), int(row[2]), int(row[3])


def _compaction_page_metrics(conn: sqlite3.Connection) -> tuple[int, int]:
    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
    page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
    freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    live_bytes = max(page_size, max(0, page_count - freelist_count) * page_size)
    reclaimable = max(0, freelist_count * page_size)
    return live_bytes, reclaimable


def _compaction_family_metrics(
    members: tuple[_SQLiteFamilyMemberSnapshot, ...],
) -> tuple[int, int, int, EntryIdentity]:
    if len(members) != len(_SQLITE_FAMILY_SUFFIXES):
        raise _CompactionAbort("permissions_failed")
    main = members[0]
    if main.state is PermissionState.ABSENT:
        raise _CompactionAbort("store_unavailable")
    if any(
        member.state is PermissionState.REPAIR_REQUIRED
        for member in members
    ):
        raise _CompactionAbort("permissions_failed")

    identities: set[EntryIdentity] = set()
    family_bytes = 0
    for member in members:
        if member.state is PermissionState.ABSENT:
            if any(
                value is not None
                for value in (
                    member.mode,
                    member.identity,
                    member.size,
                    member.link_count,
                )
            ):
                raise _CompactionAbort("permissions_failed")
            continue
        if (
            member.mode is None
            or member.identity is None
            or member.size is None
            or member.link_count is None
            or member.link_count != 1
            or member.size < 0
        ):
            raise _CompactionAbort("permissions_failed")
        if member.identity in identities:
            raise _CompactionAbort("permissions_failed")
        identities.add(member.identity)
        family_bytes += member.size

    if (
        main.mode is None
        or main.identity is None
        or main.size is None
    ):
        raise _CompactionAbort("store_unavailable")
    return family_bytes, main.size, main.mode, main.identity


def _sqlite_family_bytes_and_identity(
    parent_fd: int,
    leaf: str,
) -> tuple[int, int, int, EntryIdentity]:
    members = _snapshot_sqlite_family_at(
        parent_fd,
        leaf,
        require_main=False,
    )
    return _compaction_family_metrics(members)


def _compaction_family_permissions_ok(
    members: tuple[_SQLiteFamilyMemberSnapshot, ...],
    *,
    expected_main: EntryIdentity,
) -> bool:
    if len(members) != len(_SQLITE_FAMILY_SUFFIXES):
        return False
    main = members[0]
    if (
        main.state is not PermissionState.PRIVATE
        or main.identity != expected_main
    ):
        return False
    return all(
        member.state in {PermissionState.PRIVATE, PermissionState.ABSENT}
        for member in members
    )


def _create_verified_compaction_backup(
    source_parent_fd: int,
    source_leaf: str,
    backup_parent_fd: int,
    backup_leaf: str,
    *,
    retained_mode: int,
    source_identity: EntryIdentity,
) -> EntryIdentity:
    if inspect_private_file_at(backup_parent_fd, backup_leaf).state is not PermissionState.ABSENT:
        raise _CompactionAbort("invalid_request")
    backup_fd = create_private_file_at(backup_parent_fd, backup_leaf)
    try:
        created = os.fstat(backup_fd)
        validate_owned_regular_stat(created)
        backup_identity = entry_identity(created)
        if backup_identity == source_identity:
            raise _CompactionAbort("invalid_request")
        source_conn = _readonly_sqlite_at(
            source_parent_fd,
            source_leaf,
            expected_identity=source_identity,
        )
        destination = _writable_sqlite_at(
            backup_parent_fd,
            backup_leaf,
            expected_identity=backup_identity,
        )
        try:
            source_conn.backup(destination)
            mode_row = destination.execute("PRAGMA journal_mode=DELETE").fetchone()
            if mode_row is None or str(mode_row[0]).lower() != "delete":
                raise _CompactionAbort("backup_failed")
        finally:
            destination.close()
            source_conn.close()
        current = verify_entry_identity(
            backup_parent_fd,
            backup_leaf,
            backup_identity,
            expected_type=EntryType.REGULAR_FILE,
        )
        if int(current.st_nlink) != 1 or identity_matches(source_identity, current):
            raise _CompactionAbort("backup_failed")
        os.fchmod(backup_fd, retained_mode)
        os.fsync(backup_fd)
        current = os.fstat(backup_fd)
        if stat.S_IMODE(current.st_mode) != retained_mode:
            raise _CompactionAbort("backup_failed")
    except _CompactionAbort:
        raise
    except Exception:
        raise _CompactionAbort("backup_failed") from None
    finally:
        os.close(backup_fd)
    backup_conn = _readonly_sqlite_at(
        backup_parent_fd,
        backup_leaf,
        expected_identity=backup_identity,
    )
    try:
        if not _quick_check_ok(backup_conn):
            raise _CompactionAbort("backup_failed")
    finally:
        backup_conn.close()
    return backup_identity


def _checkpoint_truncate(
    db_path: Path,
    *,
    expected_identity: EntryIdentity,
    store_lock_held: bool = False,
) -> bool:
    try:
        with _connect(
            db_path,
            isolation_level=None,
            _store_lock_held=store_lock_held,
            _expected_db_identity=expected_identity,
        ) as conn:
            conn.execute("PRAGMA busy_timeout=0")
            row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    except (LocalStateError, sqlite3.Error, StoreSchemaError):
        return False
    return (
        row is not None
        and len(row) >= 2
        and int(row[0]) == 0
        and int(row[1]) == 0
    )


def _activate_compacted_wal(
    db_path: Path,
    *,
    expected_identity: EntryIdentity,
    store_lock_held: bool = False,
) -> bool:
    try:
        with _connect(
            db_path,
            isolation_level=None,
            _store_lock_held=store_lock_held,
            _expected_db_identity=expected_identity,
        ) as conn:
            conn.execute("PRAGMA busy_timeout=0")
            with private_file_creation_umask():
                _configure_persistent_database_conn(conn)
    except (LocalStateError, sqlite3.Error, StoreSchemaError):
        return False
    return True


def _vacuum_into_replacement(
    db_path: Path,
    handle: Any,
    phase_hook: Callable[[str], None] | None,
    *,
    source_identity: EntryIdentity,
    store_lock_held: bool = False,
) -> Any:
    released_handle: Any = handle
    try:
        with release_private_sqlite_replacement_at(handle) as (
            released_handle,
            replacement_target,
        ):
            _call_compaction_phase(
                phase_hook,
                "during_replacement",
                failure_status="replacement_failed",
            )
            with _connect(
                db_path,
                isolation_level=None,
                _store_lock_held=store_lock_held,
                _expected_db_identity=source_identity,
            ) as source:
                source.execute("PRAGMA busy_timeout=0")
                source.execute("VACUUM INTO ?", (replacement_target,))
        return verify_created_private_sqlite_replacement_at(released_handle)
    except Exception:
        try:
            created_handle = verify_created_private_sqlite_replacement_at(
                released_handle
            )
            cleanup_private_sqlite_replacement_at(created_handle)
        except LocalStateError:
            pass
        raise _CompactionAbort("replacement_failed") from None


def _copy_backup_into_replacement(
    backup_parent_fd: int,
    backup_leaf: str,
    handle: Any,
    *,
    backup_identity: EntryIdentity,
) -> Any:
    released_handle: Any = handle
    try:
        with release_private_sqlite_replacement_at(handle) as (
            released_handle,
            replacement_target,
        ):
            source = _readonly_sqlite_at(
                backup_parent_fd,
                backup_leaf,
                expected_identity=backup_identity,
            )
            with private_file_creation_umask():
                destination = sqlite3.connect(
                    replacement_target,
                    timeout=0.0,
                    isolation_level=None,
                )
            try:
                source.backup(destination)
                mode_row = destination.execute(
                    "PRAGMA journal_mode=DELETE"
                ).fetchone()
                if mode_row is None or str(mode_row[0]).lower() != "delete":
                    raise _CompactionAbort("rollback_failed")
            finally:
                destination.close()
                source.close()
        return verify_created_private_sqlite_replacement_at(released_handle)
    except Exception:
        try:
            created_handle = verify_created_private_sqlite_replacement_at(
                released_handle
            )
            cleanup_private_sqlite_replacement_at(created_handle)
        except LocalStateError:
            pass
        raise


def _replacement_integrity_ok(
    parent_fd: int,
    replacement_leaf: str,
    *,
    expected_identity: EntryIdentity,
) -> bool:
    conn = _readonly_sqlite_at(
        parent_fd,
        replacement_leaf,
        expected_identity=expected_identity,
    )
    try:
        return _quick_check_ok(conn) and _foreign_key_check_ok(conn)
    finally:
        conn.close()


def _restore_verified_compaction_backup(
    db_path: Path,
    source_parent_fd: int,
    source_leaf: str,
    backup_parent_fd: int,
    backup_leaf: str,
    *,
    backup_identity: EntryIdentity,
    retained_mode: int,
    expected_source_identity: EntryIdentity,
    store_lock_held: bool = False,
) -> bool:
    rollback_handle: Any = None
    published = False
    try:
        backup_stat = verify_entry_identity(
            backup_parent_fd,
            backup_leaf,
            backup_identity,
            expected_type=EntryType.REGULAR_FILE,
        )
        if int(backup_stat.st_nlink) != 1:
            return False
        backup_conn = _readonly_sqlite_at(
            backup_parent_fd,
            backup_leaf,
            expected_identity=backup_identity,
        )
        try:
            if not _quick_check_ok(backup_conn):
                return False
        finally:
            backup_conn.close()

        current_members = _snapshot_sqlite_family_at(
            source_parent_fd,
            source_leaf,
            require_main=True,
        )
        _family_bytes, _main_bytes, _main_mode, current_identity = (
            _compaction_family_metrics(current_members)
        )
        if current_identity != expected_source_identity:
            return False
        if not _checkpoint_truncate(
            db_path,
            expected_identity=expected_source_identity,
            store_lock_held=store_lock_held,
        ):
            return False
        rollback_handle = prepare_private_sqlite_replacement_at(
            source_parent_fd,
            basename=source_leaf,
            retained_mode=retained_mode,
        )
        rollback_handle = _copy_backup_into_replacement(
            backup_parent_fd,
            backup_leaf,
            rollback_handle,
            backup_identity=backup_identity,
        )
        replacement_identity = rollback_handle._replacement_identity
        if replacement_identity is None or not _replacement_integrity_ok(
            source_parent_fd,
            rollback_handle._replacement_name,
            expected_identity=replacement_identity,
        ):
            return False
        restored_identity = publish_private_sqlite_replacement_at(
            rollback_handle,
            expected_source=expected_source_identity,
        )
        published = True
        if not _activate_compacted_wal(
            db_path,
            expected_identity=restored_identity,
            store_lock_held=store_lock_held,
        ):
            return False
        restored = _readonly_sqlite_at(
            source_parent_fd,
            source_leaf,
            expected_identity=restored_identity,
        )
        try:
            restored_ok = (
                _quick_check_ok(restored)
                and _foreign_key_check_ok(restored)
            )
        finally:
            restored.close()
        restored_members = _snapshot_sqlite_family_at(
            source_parent_fd,
            source_leaf,
            require_main=True,
        )
        return restored_ok and _compaction_family_permissions_ok(
            restored_members,
            expected_main=restored_identity,
        )
    except Exception:
        return False
    finally:
        if rollback_handle is not None and not published:
            try:
                cleanup_private_sqlite_replacement_at(rollback_handle)
            except LocalStateError:
                pass


def compact_store(
    db_path: Path,
    *,
    options: CompactionOptions,
    now: str | None = None,
    phase_hook: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Inspect or explicitly compact one current-v9 store while offline."""
    report = _compaction_report(options)
    try:
        policy = SnapshotRetentionPolicy(
            retention_days=options.snapshot_retention_days,
            retention_count=options.snapshot_retention_count,
            batch_size=options.batch_size,
        )
        cutoff_at = _utc_cutoff(
            retention_days=policy.retention_days,
            now=now,
        )
    except (TypeError, ValueError):
        return _public_compaction_report(report, status="invalid_request")
    source_parent_fd = -1
    backup_parent_fd = -1
    backup_leaf = ""
    backup_identity: EntryIdentity | None = None
    replacement_handle: Any = None
    replacement_published = False
    source_mutated = False
    stable_lock_held = False
    retained_mode = 0o600
    authoritative_source_identity: EntryIdentity | None = None
    stage = "preflight"
    try:
        if options.dry_run and (
            options.acknowledge_offline or options.backup_path is not None
        ):
            raise _CompactionAbort("invalid_request")
        if _is_memory_db(db_path):
            raise _CompactionAbort("invalid_request")
        if not options.dry_run and (
            options.acknowledge_offline is not True or options.backup_path is None
        ):
            raise _CompactionAbort("invalid_request")

        source_parent_fd, source_leaf = open_resolved_parent(db_path)
        _validate_parent_fd(source_parent_fd, private=True)
        source_members = _snapshot_sqlite_family_at(
            source_parent_fd,
            source_leaf,
            require_main=False,
        )
        try:
            (
                family_bytes,
                source_bytes,
                retained_mode,
                source_identity,
            ) = _compaction_family_metrics(source_members)
        except _CompactionAbort as exc:
            if exc.status == "permissions_failed":
                report["permissions"]["outcome"] = (
                    "repair_required"
                    if any(
                        member.state is PermissionState.REPAIR_REQUIRED
                        for member in source_members
                    )
                    else "unsafe"
                )
            raise
        if not options.dry_run:
            try:
                fcntl.flock(
                    source_parent_fd,
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except (BlockingIOError, OSError):
                raise _CompactionAbort("offline_required") from None
            stable_lock_held = True
            stage = "offline"
            locked_members = _snapshot_sqlite_family_at(
                source_parent_fd,
                source_leaf,
                require_main=False,
            )
            if locked_members[0].identity != source_identity:
                raise local_state_error(LocalStateErrorCode.ENTRY_CHANGED)
            source_members = locked_members
            try:
                (
                    family_bytes,
                    source_bytes,
                    retained_mode,
                    source_identity,
                ) = _compaction_family_metrics(source_members)
            except _CompactionAbort as exc:
                if exc.status == "permissions_failed":
                    report["permissions"]["outcome"] = (
                        "repair_required"
                        if any(
                            member.state is PermissionState.REPAIR_REQUIRED
                            for member in source_members
                        )
                        else "unsafe"
                    )
                raise
        report["permissions"] = {"ok": True, "outcome": "compliant"}
        authoritative_source_identity = source_identity
        report["storage"]["before_bytes"] = family_bytes

        wal = source_members[1]
        shm = source_members[2]
        preflight_immutable = (
            wal.state is PermissionState.ABSENT
            or wal.size == 0
        )
        if not preflight_immutable and shm.state is PermissionState.ABSENT:
            raise _CompactionAbort(
                "store_unavailable" if options.dry_run else "offline_required"
            )
        source = _readonly_sqlite_at(
            source_parent_fd,
            source_leaf,
            immutable=preflight_immutable,
            expected_identity=source_identity,
        )
        try:
            version_row = source.execute("PRAGMA user_version").fetchone()
            if version_row is None or int(version_row[0]) != STORE_SCHEMA_VERSION:
                raise _CompactionAbort("schema_not_current")
            if not _quick_check_ok(source):
                report["integrity"]["before"] = "failed"
                raise _CompactionAbort("integrity_failed")
            report["integrity"]["before"] = "ok"
            (
                before,
                eligible,
                latest_hosts,
                eligible_logical_bytes,
            ) = _compaction_snapshot_metrics(
                source,
                cutoff_at=cutoff_at,
                retention_count=policy.retention_count,
            )
            live_bytes, reclaimable_bytes = _compaction_page_metrics(source)
        except sqlite3.Error:
            raise _CompactionAbort("store_unavailable") from None
        finally:
            source.close()
        report["snapshots"].update(
            {
                "before": before,
                "retained": before - eligible,
                "eligible": eligible,
                "remaining": eligible,
                "latest_hosts_retained": latest_hosts,
            }
        )
        report["storage"]["estimated_reclaimable_bytes"] = min(
            source_bytes,
            reclaimable_bytes + eligible_logical_bytes,
        )
        required_bytes = family_bytes * 2 + max(source_bytes, live_bytes)
        available_bytes = sqlite_parent_available_bytes_at(source_parent_fd)
        report["space"] = {
            "available_bytes": available_bytes,
            "required_bytes": required_bytes,
            "headroom_ok": available_bytes >= required_bytes,
        }
        if available_bytes < required_bytes and not options.dry_run:
            raise _CompactionAbort("insufficient_space")

        if options.dry_run:
            return _public_compaction_report(report, status="dry_run", ok=True)

        assert options.backup_path is not None
        backup_parent_fd, backup_leaf = open_resolved_parent(options.backup_path)
        _validate_parent_fd(backup_parent_fd, private=True)
        source_parent_stat = os.fstat(source_parent_fd)
        backup_parent_stat = os.fstat(backup_parent_fd)
        if (
            identity_matches(entry_identity(source_parent_stat), backup_parent_stat)
            and source_leaf == backup_leaf
        ):
            raise _CompactionAbort("invalid_request")
        if inspect_private_file_at(
            backup_parent_fd, backup_leaf
        ).state is not PermissionState.ABSENT:
            raise _CompactionAbort("invalid_request")
        backup_available = sqlite_parent_available_bytes_at(backup_parent_fd)
        if (
            source_parent_stat.st_dev != backup_parent_stat.st_dev
            and backup_available < family_bytes
        ):
            report["space"]["available_bytes"] = min(
                int(report["space"]["available_bytes"]),
                backup_available,
            )
            report["space"]["headroom_ok"] = False
            raise _CompactionAbort("insufficient_space")

        stage = "offline"
        try:
            with _connect(
                db_path,
                isolation_level=None,
                _store_lock_held=True,
                _expected_db_identity=source_identity,
            ) as exclusive:
                exclusive.execute("PRAGMA busy_timeout=0")
                exclusive.execute("BEGIN EXCLUSIVE")
                if int(exclusive.execute("PRAGMA user_version").fetchone()[0]) != STORE_SCHEMA_VERSION:
                    exclusive.rollback()
                    raise _CompactionAbort("schema_not_current")
                if not _quick_check_ok(exclusive):
                    report["integrity"]["before"] = "failed"
                    exclusive.rollback()
                    raise _CompactionAbort("integrity_failed")
                exclusive.commit()
        except sqlite3.OperationalError:
            raise _CompactionAbort("offline_required") from None
        _call_compaction_phase(
            phase_hook,
            "after_precheck",
            failure_status="backup_failed",
        )

        stage = "backup"
        _call_compaction_phase(
            phase_hook,
            "before_backup",
            failure_status="backup_failed",
        )
        backup_identity = _create_verified_compaction_backup(
            source_parent_fd,
            source_leaf,
            backup_parent_fd,
            backup_leaf,
            retained_mode=retained_mode,
            source_identity=source_identity,
        )
        report["backup"] = {
            "required": True,
            "created": True,
            "verified": True,
        }
        report["integrity"]["backup"] = "ok"
        _call_compaction_phase(
            phase_hook,
            "after_backup",
            failure_status="maintenance_failed",
        )

        stage = "maintenance"
        while True:
            batch = cleanup_snapshot_retention(
                db_path,
                retention_days=policy.retention_days,
                retention_count=policy.retention_count,
                batch_size=policy.batch_size,
                now=now,
                dry_run=False,
                _store_lock_held=True,
                _expected_db_identity=source_identity,
            )
            if not batch.get("ok"):
                raise _CompactionAbort("maintenance_failed")
            examined = int(batch.get("examined") or 0)
            deleted = int(batch.get("deleted") or 0)
            source_mutated = source_mutated or deleted > 0
            report["snapshots"]["examined"] += examined
            report["snapshots"]["deleted"] += deleted
            if not batch.get("remaining_candidates"):
                break
            if examined <= 0 or deleted <= 0:
                raise _CompactionAbort("maintenance_failed")
        report["snapshots"]["remaining"] = 0
        report["snapshots"]["retained"] = (
            report["snapshots"]["before"] - report["snapshots"]["deleted"]
        )

        stage = "checkpoint"
        source_mutated = True
        if not _checkpoint_truncate(
            db_path,
            store_lock_held=True,
            expected_identity=source_identity,
        ):
            report["checkpoint"]["status"] = "failed"
            raise _CompactionAbort("checkpoint_failed")
        report["checkpoint"]["status"] = "completed"

        stage = "replacement"
        replacement_handle = prepare_private_sqlite_replacement_at(
            source_parent_fd,
            basename=source_leaf,
            retained_mode=retained_mode,
        )
        replacement_handle = _vacuum_into_replacement(
            db_path,
            replacement_handle,
            phase_hook,
            store_lock_held=True,
            source_identity=source_identity,
        )
        report["replacement"]["status"] = "built"
        replacement_identity = replacement_handle._replacement_identity
        if replacement_identity is None or not _replacement_integrity_ok(
            source_parent_fd,
            replacement_handle._replacement_name,
            expected_identity=replacement_identity,
        ):
            report["integrity"]["replacement"] = "failed"
            raise _CompactionAbort("replacement_failed")
        report["integrity"]["replacement"] = "ok"
        _call_compaction_phase(
            phase_hook,
            "after_replacement_check",
            failure_status="replacement_failed",
        )
        _call_compaction_phase(
            phase_hook,
            "before_publish",
            failure_status="replacement_failed",
        )
        published_identity = publish_private_sqlite_replacement_at(
            replacement_handle,
            expected_source=source_identity,
        )
        authoritative_source_identity = published_identity
        replacement_published = True
        report["replacement"]["status"] = "published"
        if not _activate_compacted_wal(
            db_path,
            store_lock_held=True,
            expected_identity=published_identity,
        ):
            raise _CompactionAbort("replacement_failed")
        _call_compaction_phase(
            phase_hook,
            "publication_failed",
            failure_status="replacement_failed",
        )

        stage = "post_publish"
        post = _readonly_sqlite_at(
            source_parent_fd,
            source_leaf,
            expected_identity=published_identity,
        )
        try:
            post_ok = _quick_check_ok(post) and _foreign_key_check_ok(post)
        finally:
            post.close()
        post_members = _snapshot_sqlite_family_at(
            source_parent_fd,
            source_leaf,
            require_main=True,
        )
        post_ok = post_ok and _compaction_family_permissions_ok(
            post_members,
            expected_main=published_identity,
        )
        report["integrity"]["after"] = "ok" if post_ok else "failed"
        if not post_ok:
            raise _CompactionAbort("replacement_failed")
        _call_compaction_phase(
            phase_hook,
            "after_publish_check",
            failure_status="replacement_failed",
        )
        after_family, _after_main, _mode, after_identity = (
            _compaction_family_metrics(post_members)
        )
        if after_identity != published_identity:
            raise _CompactionAbort("replacement_failed")
        report["storage"]["after_bytes"] = after_family
        return _public_compaction_report(report, status="completed", ok=True)
    except _CompactionAbort as exc:
        if replacement_published or source_mutated:
            report["rollback"]["status"] = "failed"
            if (
                backup_identity is not None
                and backup_parent_fd >= 0
                and authoritative_source_identity is not None
                and _restore_verified_compaction_backup(
                    db_path,
                    source_parent_fd,
                    source_leaf,
                    backup_parent_fd,
                    backup_leaf,
                    backup_identity=backup_identity,
                    expected_source_identity=authoritative_source_identity,
                    retained_mode=retained_mode,
                    store_lock_held=stable_lock_held,
                )
            ):
                report["rollback"]["status"] = "completed"
                report["integrity"]["after"] = "ok"
                return _public_compaction_report(
                    report,
                    status="rollback_completed",
                )
            return _public_compaction_report(report, status="rollback_failed")
        if stage == "backup" and report["backup"]["created"]:
            report["integrity"]["backup"] = "failed"
        if stage == "replacement":
            report["replacement"]["status"] = "failed"
        return _public_compaction_report(report, status=exc.status)
    except LocalStateError:
        if replacement_published or source_mutated:
            report["rollback"]["status"] = "failed"
            if (
                backup_identity is not None
                and backup_parent_fd >= 0
                and authoritative_source_identity is not None
                and _restore_verified_compaction_backup(
                    db_path,
                    source_parent_fd,
                    source_leaf,
                    backup_parent_fd,
                    backup_leaf,
                    backup_identity=backup_identity,
                    expected_source_identity=authoritative_source_identity,
                    retained_mode=retained_mode,
                    store_lock_held=stable_lock_held,
                )
            ):
                report["rollback"]["status"] = "completed"
                report["integrity"]["after"] = "ok"
                return _public_compaction_report(
                    report,
                    status="rollback_completed",
                )
            return _public_compaction_report(report, status="rollback_failed")
        status = (
            "permissions_failed"
            if stage in {"preflight", "offline"}
            else "backup_failed"
            if stage == "backup"
            else "checkpoint_failed"
            if stage == "checkpoint"
            else "replacement_failed"
        )
        return _public_compaction_report(report, status=status)
    except Exception:
        if replacement_published or source_mutated:
            report["rollback"]["status"] = "failed"
            if (
                backup_identity is not None
                and backup_parent_fd >= 0
                and authoritative_source_identity is not None
                and _restore_verified_compaction_backup(
                    db_path,
                    source_parent_fd,
                    source_leaf,
                    backup_parent_fd,
                    backup_leaf,
                    backup_identity=backup_identity,
                    expected_source_identity=authoritative_source_identity,
                    retained_mode=retained_mode,
                    store_lock_held=stable_lock_held,
                )
            ):
                report["rollback"]["status"] = "completed"
                report["integrity"]["after"] = "ok"
                return _public_compaction_report(
                    report,
                    status="rollback_completed",
                )
            return _public_compaction_report(report, status="rollback_failed")
        status = {
            "preflight": "store_unavailable",
            "offline": "offline_required",
            "backup": "backup_failed",
            "maintenance": "maintenance_failed",
            "checkpoint": "checkpoint_failed",
            "replacement": "replacement_failed",
        }.get(stage, "replacement_failed")
        return _public_compaction_report(report, status=status)
    finally:
        if replacement_handle is not None and not replacement_published:
            try:
                cleanup_private_sqlite_replacement_at(replacement_handle)
            except LocalStateError:
                pass
        if backup_parent_fd >= 0:
            os.close(backup_parent_fd)
        if source_parent_fd >= 0:
            os.close(source_parent_fd)


def maybe_run_automatic_store_maintenance(
    db_path: Path,
    *,
    policy: SnapshotRetentionPolicy,
    cadence_seconds: int = 3600,
    now: str | None = None,
) -> dict[str, Any]:
    """Run one serialized automatic batch when the persisted cadence is due."""
    if (
        isinstance(cadence_seconds, bool)
        or not isinstance(cadence_seconds, int)
        or cadence_seconds <= 0
    ):
        raise ValueError("cadence_seconds must be a positive integer")
    current_at = _connector_now(now)
    if not _sqlite_store_exists(db_path):
        return dict(sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "due": False,
            "last_completed_at": None,
            "next_due_at": None,
            "snapshot": {
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
            },
            "batch_size": policy.batch_size,
        }))
    cutoff_at = _utc_cutoff(retention_days=policy.retention_days, now=current_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            state = conn.execute(
                """
                SELECT last_completed_at
                FROM store_maintenance_state
                WHERE scope = 'automatic'
                """
            ).fetchone()
            last_completed_at = (
                str(state[0]) if state is not None and state[0] is not None else None
            )
            next_due_at = (
                _connector_add_seconds(last_completed_at, cadence_seconds)
                if last_completed_at is not None
                else None
            )
            due = (
                next_due_at is None
                or _connector_datetime(current_at) >= _connector_datetime(next_due_at)
            )
            if not due:
                conn.rollback()
                return dict(sanitize_public_value({
                    "schema_version": 1,
                    "ok": True,
                    "status": "not_due",
                    "due": False,
                    "last_completed_at": last_completed_at,
                    "next_due_at": next_due_at,
                    "snapshot": {
                        "examined": 0,
                        "deleted": 0,
                        "remaining_candidates": False,
                    },
                    "batch_size": policy.batch_size,
                }))
            candidates, _ = _snapshot_retention_candidates_conn(
                conn,
                cutoff_at=cutoff_at,
                retention_count=policy.retention_count,
                batch_size=policy.batch_size,
            )
            deleted = _delete_snapshot_candidates_conn(conn, candidates)
            remaining_ids, _ = _snapshot_retention_candidates_conn(
                conn,
                cutoff_at=cutoff_at,
                retention_count=policy.retention_count,
                batch_size=1,
            )
            conn.execute(
                """
                UPDATE store_maintenance_state
                SET last_started_at = ?,
                    last_completed_at = ?,
                    last_status = 'ok',
                    last_examined = ?,
                    last_deleted = ?,
                    last_examined_id = ?
                WHERE scope = 'automatic'
                """,
                (
                    current_at,
                    current_at,
                    len(candidates),
                    deleted,
                    max(candidates) if candidates else None,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return dict(sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "due": True,
        "last_completed_at": current_at,
        "next_due_at": _connector_add_seconds(current_at, cadence_seconds),
        "snapshot": {
            "examined": len(candidates),
            "deleted": deleted,
            "remaining_candidates": bool(remaining_ids),
        },
        "batch_size": policy.batch_size,
    }))


def cleanup_event_retention(
    db_path: Path,
    host_id: str,
    *,
    retention_days: int,
    now: str | None = None,
    dry_run: bool = False,
    batch_size: int = 100,
) -> dict[str, Any]:
    """Delete one bounded host-scoped batch from event history."""
    days = max(1, int(retention_days))
    bounded_batch = max(1, min(int(batch_size), 1_000))
    cutoff_at = _utc_cutoff(retention_days=days, now=now)
    base = {
        "schema_version": 1,
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "retention_days": days,
        "cutoff_at": cutoff_at,
        "batch_size": bounded_batch,
    }
    if not _sqlite_store_exists(db_path):
        return dict(sanitize_public_value({
            **base,
            "ok": False,
            "status": "store_unavailable",
            "examined": 0,
            "deleted": 0,
            "remaining_candidates": False,
        }))
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            candidate_pool = [
                int(row[0])
                for row in conn.execute(
                    """
                    SELECT id
                    FROM events
                    WHERE host_id = ? AND observed_at < ?
                    ORDER BY observed_at, id
                    LIMIT ?
                    """,
                    (str(host_id), cutoff_at, bounded_batch + 1),
                ).fetchall()
            ]
            candidate_ids = candidate_pool[:bounded_batch]
            deleted = 0
            if candidate_ids and not dry_run:
                placeholders = ",".join("?" for _ in candidate_ids)
                deleted = int(
                    conn.execute(
                        f"DELETE FROM events WHERE id IN ({placeholders})",
                        candidate_ids,
                    ).rowcount
                    or 0
                )
            if dry_run:
                remaining = len(candidate_pool) > bounded_batch
            else:
                remaining = bool(
                    conn.execute(
                        """
                        SELECT 1
                        FROM events
                        WHERE host_id = ? AND observed_at < ?
                        LIMIT 1
                        """,
                        (str(host_id), cutoff_at),
                    ).fetchone()
                )
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
    return dict(sanitize_public_value({
        **base,
        "ok": True,
        "status": "ok",
        "examined": len(candidate_ids),
        "deleted": len(candidate_ids) if dry_run else deleted,
        "remaining_candidates": remaining,
    }))


def _turn_content_retention_candidates_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    cutoff_at: str,
    batch_size: int,
) -> list[tuple[str, int]]:
    rows = conn.execute(
        """
        SELECT candidate_type, candidate_id
        FROM (
            SELECT
                'plan' AS candidate_type,
                plans.id AS candidate_id,
                CASE
                    WHEN plans.state = 'preparing' THEN plans.created_at
                    ELSE COALESCE(
                        plans.completed_at,
                        plans.activated_at,
                        plans.created_at
                    )
                END AS eligible_at
            FROM turn_presentation_plans AS plans
            WHERE plans.host_id = :host_id
              AND (
                  (
                      plans.state = 'preparing'
                      AND plans.created_at < :cutoff_at
                  )
                  OR (
                      plans.state IN ('completed', 'superseded', 'failed')
                      AND COALESCE(
                          plans.completed_at,
                          plans.activated_at,
                          plans.created_at
                      ) < :cutoff_at
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans AS replacement
                  WHERE replacement.host_id = plans.host_id
                    AND replacement.name = plans.name
                    AND replacement.turn_id = plans.turn_id
                    AND replacement.replaces_plan_token = plans.plan_token
                    AND replacement.state IN (
                        'preparing',
                        'waiting_predecessor',
                        'active'
                    )
              )
              AND (
                  plans.state = 'preparing'
                  OR plans.activated_at IS NULL
                  OR plans.id < COALESCE(
                      (
                          SELECT MAX(completed.id)
                          FROM turn_presentation_plans AS completed
                          WHERE completed.host_id = plans.host_id
                            AND completed.name = plans.name
                            AND completed.turn_id = plans.turn_id
                            AND completed.completed_at IS NOT NULL
                      ),
                      0
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_jobs AS jobs
                  LEFT JOIN connector_outbox AS outbox
                    ON outbox.id = jobs.outbox_id
                  LEFT JOIN connector_deliveries AS deliveries
                    ON deliveries.outbox_id = outbox.id
                  WHERE jobs.plan_id = plans.id
                    AND (
                        (
                            outbox.id IS NOT NULL
                            AND (
                                plans.state = 'preparing'
                                OR outbox.status NOT IN (
                                    'delivered',
                                    'dead_letter',
                                    'superseded'
                                )
                                OR outbox.updated_at IS NULL
                                OR outbox.updated_at >= :cutoff_at
                            )
                        )
                        OR (
                            deliveries.id IS NOT NULL
                            AND (
                                deliveries.status = 'leased'
                                OR COALESCE(
                                    deliveries.delivered_at,
                                    deliveries.created_at
                                ) >= :cutoff_at
                            )
                        )
                    )
              )
            UNION ALL
            SELECT
                'revision' AS candidate_type,
                revisions.rowid AS candidate_id,
                revisions.superseded_at AS eligible_at
            FROM turn_content_revisions AS revisions
            WHERE revisions.host_id = :host_id
              AND revisions.is_current = 0
              AND revisions.superseded_at IS NOT NULL
              AND revisions.superseded_at < :cutoff_at
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans AS plans
                  WHERE plans.host_id = revisions.host_id
                    AND plans.turn_id = revisions.turn_id
                    AND plans.content_revision = revisions.content_revision
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM connector_outbox AS outbox
                  WHERE outbox.host_id = revisions.host_id
                    AND outbox.connector = :turn_final_name
                    AND json_valid(outbox.payload_json)
                    AND json_extract(
                        outbox.payload_json,
                        '$.content_revision'
                    ) = revisions.content_revision
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turns
                  WHERE turns.host_id = revisions.host_id
                    AND turns.turn_id = revisions.turn_id
                    AND json_valid(turns.payload_json)
                    AND (
                        json_extract(
                            turns.payload_json,
                            '$.content_revision'
                        ) = revisions.content_revision
                        OR json_extract(
                            turns.payload_json,
                            '$.content.content_revision'
                        ) = revisions.content_revision
                    )
              )
        )
        ORDER BY eligible_at, candidate_type, candidate_id
        LIMIT :batch_size
        """,
        {
            "host_id": str(host_id),
            "cutoff_at": str(cutoff_at),
            "turn_final_name": _TURN_FINAL_NAME,
            "batch_size": int(batch_size),
        },
    ).fetchall()
    return [(str(row[0]), int(row[1])) for row in rows]


def _terminal_plan_reference_reason_conn(
    conn: sqlite3.Connection,
    *,
    plan: sqlite3.Row | tuple[Any, ...],
    cutoff_at: str,
) -> str | None:
    plan_id = int(plan[0])
    host_id = str(plan[1])
    name = str(plan[2])
    plan_token = str(plan[3])
    turn_id = str(plan[4])
    state = str(plan[5])
    activated_at = plan[7]
    if state == "preparing":
        unexpected_anchor = conn.execute(
            """
            SELECT 1
            FROM turn_presentation_jobs AS jobs
            LEFT JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
            LEFT JOIN connector_deliveries AS deliveries
              ON deliveries.outbox_id = outbox.id
            WHERE jobs.plan_id = ?
              AND (jobs.outbox_id IS NOT NULL OR deliveries.id IS NOT NULL)
            LIMIT 1
            """,
            (plan_id,),
        ).fetchone()
        return "reference" if unexpected_anchor is not None else None
    if state not in _TURN_CONTENT_TERMINAL_PLAN_STATES:
        return "reference"
    live_replacement = conn.execute(
        """
        SELECT 1
        FROM turn_presentation_plans
        WHERE host_id = ? AND name = ? AND turn_id = ?
          AND replaces_plan_token = ?
          AND state IN ('preparing', 'waiting_predecessor', 'active')
        LIMIT 1
        """,
        (host_id, name, turn_id, plan_token),
    ).fetchone()
    if live_replacement is not None:
        return "replacement"
    latest_completed = conn.execute(
        """
        SELECT COALESCE(MAX(id), 0)
        FROM turn_presentation_plans
        WHERE host_id = ? AND name = ? AND turn_id = ?
          AND completed_at IS NOT NULL
        """,
        (host_id, name, turn_id),
    ).fetchone()
    baseline_id = int(latest_completed[0] or 0)
    if activated_at is not None and (baseline_id == 0 or plan_id >= baseline_id):
        return "failed_prefix_or_current_baseline"
    anchors = conn.execute(
        """
        SELECT
            outbox.id,
            outbox.status,
            outbox.updated_at,
            deliveries.id,
            deliveries.status,
            COALESCE(deliveries.delivered_at, deliveries.created_at)
        FROM turn_presentation_jobs AS jobs
        LEFT JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
        LEFT JOIN connector_deliveries AS deliveries
          ON deliveries.outbox_id = outbox.id
        WHERE jobs.plan_id = ?
        """,
        (plan_id,),
    ).fetchall()
    for outbox_id, outbox_status, updated_at, delivery_id, delivery_status, audit_at in anchors:
        if outbox_id is not None and (
            str(outbox_status) not in _TURN_CONTENT_TERMINAL_OUTBOX_STATES
            or not updated_at
            or str(updated_at) >= str(cutoff_at)
        ):
            return "outbox"
        if delivery_id is not None and (
            str(delivery_status) == _CONNECTOR_LEASE_STATUS
            or not audit_at
            or str(audit_at) >= str(cutoff_at)
        ):
            return "delivery"
    return None


def _delete_retained_plan_conn(
    conn: sqlite3.Connection,
    *,
    plan_id: int,
) -> dict[str, int]:
    outbox_ids = [
        int(row[0])
        for row in conn.execute(
            """
            SELECT outbox_id
            FROM turn_presentation_jobs
            WHERE plan_id = ? AND outbox_id IS NOT NULL
            """,
            (int(plan_id),),
        ).fetchall()
    ]
    deliveries_deleted = 0
    outbox_deleted = 0
    if outbox_ids:
        placeholders = ",".join("?" for _ in outbox_ids)
        deliveries_deleted = int(
            conn.execute(
                f"DELETE FROM connector_deliveries WHERE outbox_id IN ({placeholders})",
                outbox_ids,
            ).rowcount
            or 0
        )
    jobs_deleted = int(
        conn.execute(
            "DELETE FROM turn_presentation_jobs WHERE plan_id = ?",
            (int(plan_id),),
        ).rowcount
        or 0
    )
    if outbox_ids:
        placeholders = ",".join("?" for _ in outbox_ids)
        outbox_deleted = int(
            conn.execute(
                f"DELETE FROM connector_outbox WHERE id IN ({placeholders})",
                outbox_ids,
            ).rowcount
            or 0
        )
    plan_deleted = int(
        conn.execute(
            "DELETE FROM turn_presentation_plans WHERE id = ?",
            (int(plan_id),),
        ).rowcount
        or 0
    )
    return {
        "plans": plan_deleted,
        "jobs": jobs_deleted,
        "queue_anchors": outbox_deleted,
        "attempts": deliveries_deleted,
    }


def _delete_superseded_revision_conn(
    conn: sqlite3.Connection,
    *,
    revision_rowid: int,
    host_id: str,
    cutoff_at: str,
) -> bool:
    cursor = conn.execute(
        """
        DELETE FROM turn_content_revisions AS revisions
        WHERE revisions.rowid = ?
          AND revisions.host_id = ?
          AND revisions.is_current = 0
          AND revisions.superseded_at IS NOT NULL
          AND revisions.superseded_at < ?
          AND NOT EXISTS (
              SELECT 1
              FROM turn_presentation_plans AS plans
              WHERE plans.host_id = revisions.host_id
                AND plans.turn_id = revisions.turn_id
                AND plans.content_revision = revisions.content_revision
          )
          AND NOT EXISTS (
              SELECT 1
              FROM connector_outbox AS outbox
              WHERE outbox.host_id = revisions.host_id
                AND outbox.connector = ?
                AND json_valid(outbox.payload_json)
                AND json_extract(
                    outbox.payload_json,
                    '$.content_revision'
                ) = revisions.content_revision
          )
          AND NOT EXISTS (
              SELECT 1
              FROM turns
              WHERE turns.host_id = revisions.host_id
                AND turns.turn_id = revisions.turn_id
                AND json_valid(turns.payload_json)
                AND (
                    json_extract(
                        turns.payload_json,
                        '$.content_revision'
                    ) = revisions.content_revision
                    OR json_extract(
                        turns.payload_json,
                        '$.content.content_revision'
                    ) = revisions.content_revision
                )
          )
        """,
        (
            int(revision_rowid),
            str(host_id),
            str(cutoff_at),
            _TURN_FINAL_NAME,
        ),
    )
    return bool(cursor.rowcount)


def cleanup_turn_content_retention(
    db_path: Path,
    host_id: str,
    *,
    retention_days: int,
    now: str | None = None,
    dry_run: bool = False,
    batch_size: int = _TURN_CONTENT_MAINTENANCE_BATCH,
) -> dict[str, Any]:
    """Remove old presentation anchors, then superseded canonical revisions, in one bounded batch."""
    days = max(1, int(retention_days))
    bounded_batch = max(
        1,
        min(int(batch_size), _TURN_CONTENT_MAINTENANCE_BATCH_MAX),
    )
    cutoff_at = _utc_cutoff(retention_days=days, now=now)
    empty_counts = {
        "plans": 0,
        "jobs": 0,
        "queue_anchors": 0,
        "attempts": 0,
        "revisions": 0,
    }
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "retention_days": days,
            "cutoff_at": cutoff_at,
            "batch_size": bounded_batch,
            "examined": 0,
            "deleted": 0,
            "skipped_reference": 0,
            "deleted_rows": empty_counts,
        })
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        candidates = _turn_content_retention_candidates_conn(
            conn,
            host_id=str(host_id),
            cutoff_at=cutoff_at,
            batch_size=bounded_batch,
        )
    deleted_rows = dict(empty_counts)
    skipped_reference = 0
    deleted = 0
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            for candidate_type, candidate_id in candidates:
                if candidate_type == "plan":
                    plan = conn.execute(
                        """
                        SELECT
                            id, host_id, name, plan_token, turn_id, state,
                            created_at, activated_at, completed_at
                        FROM turn_presentation_plans
                        WHERE id = ? AND host_id = ?
                          AND (
                              (state = 'preparing' AND created_at < ?)
                              OR (
                                  state IN ('completed', 'superseded', 'failed')
                                  AND COALESCE(
                                      completed_at,
                                      activated_at,
                                      created_at
                                  ) < ?
                              )
                          )
                        """,
                        (
                            int(candidate_id),
                            str(host_id),
                            cutoff_at,
                            cutoff_at,
                        ),
                    ).fetchone()
                    if plan is None:
                        skipped_reference += 1
                        continue
                    if _terminal_plan_reference_reason_conn(
                        conn,
                        plan=plan,
                        cutoff_at=cutoff_at,
                    ) is not None:
                        skipped_reference += 1
                        continue
                    plan_counts = _delete_retained_plan_conn(
                        conn,
                        plan_id=int(candidate_id),
                    )
                    if not plan_counts["plans"]:
                        skipped_reference += 1
                        continue
                    deleted += 1
                    for key, count in plan_counts.items():
                        deleted_rows[key] += int(count)
                    continue
                if _delete_superseded_revision_conn(
                    conn,
                    revision_rowid=int(candidate_id),
                    host_id=str(host_id),
                    cutoff_at=cutoff_at,
                ):
                    deleted += 1
                    deleted_rows["revisions"] += 1
                else:
                    skipped_reference += 1
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "retention_days": days,
        "cutoff_at": cutoff_at,
        "stale_preparing_before": cutoff_at,
        "batch_size": bounded_batch,
        "examined": len(candidates),
        "deleted": deleted,
        "skipped_reference": skipped_reference,
        "deleted_rows": deleted_rows,
    })


def run_store_maintenance(
    db_path: Path,
    host_id: str,
    *,
    retention_days: int,
    max_outbox_attempts: int,
    now: str | None = None,
    dry_run: bool = False,
    content_batch_size: int = _TURN_CONTENT_MAINTENANCE_BATCH,
    event_batch_size: int = 100,
    snapshot_retention_days: int = 14,
    snapshot_retention_count: int = 4096,
    snapshot_batch_size: int = 100,
) -> dict[str, Any]:
    """Run one bounded batch for every online store-maintenance class."""
    retention = cleanup_event_retention(
        db_path,
        host_id,
        retention_days=retention_days,
        now=now,
        dry_run=dry_run,
        batch_size=event_batch_size,
    )
    snapshots = cleanup_snapshot_retention(
        db_path,
        retention_days=snapshot_retention_days,
        retention_count=snapshot_retention_count,
        batch_size=snapshot_batch_size,
        now=now,
        dry_run=dry_run,
    )
    outbox = exhaust_connector_retries(
        db_path,
        host_id,
        max_attempts=max_outbox_attempts,
        now=now,
        dry_run=dry_run,
    )
    turn_content = cleanup_turn_content_retention(
        db_path,
        host_id,
        retention_days=retention_days,
        now=now,
        dry_run=dry_run,
        batch_size=content_batch_size,
    )
    ok = (
        bool(retention.get("ok"))
        and bool(snapshots.get("ok"))
        and bool(outbox.get("ok"))
        and bool(turn_content.get("ok"))
    )
    return sanitize_public_value({
        "schema_version": 1,
        "ok": ok,
        "status": "ok" if ok else "store_unavailable",
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "retention": {
            "retention_days": int(retention.get("retention_days") or retention_days),
            "cutoff_at": retention.get("cutoff_at"),
            "batch_size": int(retention.get("batch_size") or event_batch_size),
            "examined": int(retention.get("examined") or 0),
            "deleted": int(retention.get("deleted") or 0),
            "remaining_candidates": bool(
                retention.get("remaining_candidates")
            ),
        },
        "snapshots": {
            "scope": "database",
            "retention_days": int(
                snapshots.get("retention_days") or snapshot_retention_days
            ),
            "retention_count": int(
                snapshots.get("retention_count") or snapshot_retention_count
            ),
            "cutoff_at": snapshots.get("cutoff_at"),
            "batch_size": int(
                snapshots.get("batch_size") or snapshot_batch_size
            ),
            "examined": int(snapshots.get("examined") or 0),
            "deleted": int(snapshots.get("deleted") or 0),
            "eligible": int(snapshots.get("eligible") or 0),
            "remaining_candidates": bool(
                snapshots.get("remaining_candidates")
            ),
            "latest_hosts_retained": int(
                snapshots.get("latest_hosts_retained") or 0
            ),
        },
        "outbox": {
            "max_attempts": int(outbox.get("max_attempts") or max_outbox_attempts),
            "updated": int(outbox.get("updated") or 0),
        },
        "turn_content": {
            "dry_run": bool(turn_content.get("dry_run")),
            "retention_days": int(
                turn_content.get("retention_days") or retention_days
            ),
            "cutoff_at": turn_content.get("cutoff_at"),
            "stale_preparing_before": turn_content.get(
                "stale_preparing_before"
            ),
            "batch_size": int(
                turn_content.get("batch_size") or content_batch_size
            ),
            "examined": int(turn_content.get("examined") or 0),
            "deleted": int(turn_content.get("deleted") or 0),
            "skipped_reference": int(
                turn_content.get("skipped_reference") or 0
            ),
            "deleted_rows": dict(
                turn_content.get("deleted_rows")
                or {
                    "plans": 0,
                    "jobs": 0,
                    "queue_anchors": 0,
                    "attempts": 0,
                    "revisions": 0,
                }
            ),
        },
    })


_TURN_CONTENT_FIELDS = frozenset(
    {
        "user_text",
        "assistant_final_text",
        "assistant_stream_text",
        "model",
        "complete",
        "has_open_turn",
        "source_turn_id",
    }
)

_SOURCE_TURN_HISTORY_LIMIT = 6

_TURN_IDENTITY_SEED_FIELDS = (
    "schema_version",
    "host_id",
    "worker_id",
    "worker_fingerprint",
    "space_id",
    "status",
    "kind",
    "source",
    "origin_command_id",
    "title",
    "summary",
)


def _turn_merge_match_text(value: Any) -> str:
    return "\n".join(" ".join(line.split()) for line in str(value or "").splitlines()).strip()


def _turn_merge_score(payload: Mapping[str, Any], content: Mapping[str, Any]) -> tuple[int, str, str]:
    incoming_user = _turn_merge_match_text(content.get("user_text"))
    existing_user = _turn_merge_match_text(payload.get("user_text"))
    source = str(payload.get("source") or "")
    has_origin = bool(str(payload.get("origin_command_id") or "").strip())
    open_turn = payload.get("has_open_turn") is True or payload.get("complete") is False
    has_existing_content = bool(
        existing_user
        or str(payload.get("assistant_final_text") or "").strip()
        or str(payload.get("assistant_stream_text") or "").strip()
    )
    score = 0
    if incoming_user and existing_user == incoming_user:
        score += 1000
    elif incoming_user and has_origin and existing_user:
        score -= 500
    if has_origin and incoming_user and existing_user == incoming_user:
        score += 250
    elif has_origin:
        score -= 40
    if open_turn:
        score += 80
    if source == "command":
        score += 40 if incoming_user and existing_user == incoming_user else -20
    elif source == "snapshot":
        score += 10
    if not has_existing_content:
        score += 5
    return (
        score,
        str(payload.get("updated_at") or payload.get("observed_at") or ""),
        str(payload.get("id") or payload.get("turn_id") or ""),
    )


def _turn_content_matches_origin(payload: Mapping[str, Any], content: Mapping[str, Any]) -> bool:
    incoming_user = _turn_merge_match_text(content.get("user_text"))
    if not incoming_user:
        return False
    return incoming_user == _turn_merge_match_text(payload.get("user_text"))
def _normalize_backend_pending_payload(
    pending: Mapping[str, Any],
    choice_routes: tuple[tuple[str, int], ...],
) -> tuple[dict[str, Any], str]:
    """Validate and canonicalize one complete public overlay before persistence."""
    clean = sanitize_public_mapping(pending)
    if not isinstance(clean, dict) or not set(clean) <= {
        "question",
        "kind",
        "choices",
        "meta",
    }:
        raise ValueError("invalid backend pending payload")
    question = clean.get("question")
    kind = clean.get("kind")
    choices = clean.get("choices", [])
    meta = clean.get("meta", {"source": "backend"})
    if (
        not isinstance(question, str)
        or not question.strip()
        or not isinstance(kind, str)
        or not kind.strip()
        or not isinstance(choices, list)
        or not isinstance(meta, Mapping)
    ):
        raise ValueError("invalid backend pending payload")
    normalized_choices: list[dict[str, str]] = []
    seen: set[str] = set()
    for choice in choices:
        if not isinstance(choice, Mapping) or set(choice) != {"choice_id", "label"}:
            raise ValueError("invalid backend pending choice")
        choice_id = choice.get("choice_id")
        label = choice.get("label")
        if (
            type(choice_id) is not str
            or not choice_id
            or choice_id in seen
            or type(label) is not str
            or not label.strip()
        ):
            raise ValueError("invalid backend pending choice")
        seen.add(choice_id)
        normalized_choices.append({"choice_id": choice_id, "label": label})
    route_map = dict(choice_routes)
    if set(route_map) != seen or any(
        type(ordinal) is not int or ordinal < 1 for ordinal in route_map.values()
    ):
        raise ValueError("backend pending choices do not match private routes")
    normalized = {
        "question": question,
        "kind": kind,
        "choices": normalized_choices,
        "meta": sanitize_public_mapping(meta),
    }
    return normalized, _canonical_json(route_map)


def _pending_observed_time(observed_at: str | None) -> tuple[str, datetime]:
    value = observed_at or utc_timestamp()
    canonical = _strict_utc_timestamp(value)
    if canonical is None:
        raise ValueError("observed_at must be a UTC timestamp")
    return canonical, datetime.fromisoformat(canonical)


def _apply_backend_pending_observation_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    observation: PendingObservation,
    *,
    observed_at: str,
    stale_grace_seconds: float,
    binding_private_fingerprint: str = "",
    observed_turn_target_value: str = "",
) -> bool:
    """Apply one binding-scoped durable backend-pending transition."""
    current_time, current_dt = _pending_observed_time(observed_at)
    if (
        isinstance(stale_grace_seconds, bool)
        or not isinstance(stale_grace_seconds, (int, float))
        or not float(stale_grace_seconds) > 0
    ):
        raise ValueError("stale_grace_seconds must be positive")
    source_binding = str(binding_private_fingerprint or "")
    source_target = str(observed_turn_target_value or "")
    key = (str(host_id), str(worker_id))
    row = conn.execute(
        """
        SELECT payload_json, revision_digest, choice_routes_json,
               binding_private_fingerprint, observed_turn_target_value,
               observation_state, freshness, grace_deadline, observed_at
        FROM backend_pending
        WHERE host_id = ? AND worker_id = ?
        """,
        key,
    ).fetchone()
    stored_binding = str(row[3]) if row is not None else ""
    stored_target = str(row[4]) if row is not None else ""
    stored_observed_at = (
        _strict_utc_timestamp(row[8]) if row is not None else None
    )
    if (
        stored_observed_at is not None
        and current_dt < datetime.fromisoformat(stored_observed_at)
    ):
        return False

    if observation.kind == "worker_authoritatively_absent":
        if source_binding:
            cursor = conn.execute(
                """
                DELETE FROM backend_pending
                WHERE host_id = ? AND worker_id = ?
                  AND binding_private_fingerprint = ?
                """,
                (*key, source_binding),
            )
        else:
            cursor = conn.execute(
                "DELETE FROM backend_pending WHERE host_id = ? AND worker_id = ?",
                key,
            )
        if cursor.rowcount:
            conn.execute(
                "DELETE FROM backend_pending_claims "
                "WHERE host_id = ? AND worker_id = ?",
                key,
            )
        return bool(cursor.rowcount)

    binding_changed = (
        row is not None
        and source_binding
        and stored_binding
        and source_binding != stored_binding
    )
    if binding_changed:
        return False
    effective_binding = source_binding or stored_binding
    effective_target = source_target or stored_target

    if observation.kind == "read_failed":
        if row is None:
            conn.execute(
                """
                INSERT INTO backend_pending (
                    host_id, worker_id, payload_json, observed_at,
                    revision_digest, choice_routes_json,
                    binding_private_fingerprint, observed_turn_target_value,
                    observation_state, freshness, last_success_at,
                    last_failure_at, grace_deadline, updated_at
                ) VALUES (?, ?, '{}', ?, '', '{}', ?, ?, 'failed', 'stale',
                          NULL, ?, NULL, ?)
                """,
                (
                    *key,
                    current_time,
                    effective_binding,
                    effective_target,
                    current_time,
                    current_time,
                ),
            )
            return True
        state = str(row[5])
        freshness = str(row[6])
        if state == "open":
            deadline = _strict_utc_timestamp(row[7])
            if (
                deadline is not None
                and current_dt >= datetime.fromisoformat(deadline)
            ):
                conn.execute(
                    """
                    UPDATE backend_pending
                    SET observed_at = ?,
                        binding_private_fingerprint = ?,
                        observed_turn_target_value = ?,
                        observation_state = 'failed', freshness = 'stale',
                        last_failure_at = ?, grace_deadline = NULL,
                        updated_at = ?
                    WHERE host_id = ? AND worker_id = ?
                    """,
                    (
                        current_time,
                        effective_binding,
                        effective_target,
                        current_time,
                        current_time,
                        *key,
                    ),
                )
                conn.execute(
                    """
                    DELETE FROM backend_pending_claims
                    WHERE host_id = ? AND worker_id = ? AND state = 'claimed'
                    """,
                    key,
                )
                return True
            if freshness == "stale":
                conn.execute(
                    """
                    UPDATE backend_pending
                    SET observed_at = ?, last_failure_at = ?
                    WHERE host_id = ? AND worker_id = ?
                    """,
                    (current_time, current_time, *key),
                )
                return False
            grace_deadline = (
                current_dt + timedelta(seconds=float(stale_grace_seconds))
            ).isoformat(timespec="microseconds")
            conn.execute(
                """
                UPDATE backend_pending
                SET observed_at = ?, binding_private_fingerprint = ?,
                    observed_turn_target_value = ?, freshness = 'stale',
                    last_failure_at = ?, grace_deadline = ?, updated_at = ?
                WHERE host_id = ? AND worker_id = ?
                """,
                (
                    current_time,
                    effective_binding,
                    effective_target,
                    current_time,
                    grace_deadline,
                    current_time,
                    *key,
                ),
            )
            conn.execute(
                """
                DELETE FROM backend_pending_claims
                WHERE host_id = ? AND worker_id = ? AND state = 'claimed'
                """,
                key,
            )
            return True
        if freshness == "stale":
            conn.execute(
                """
                UPDATE backend_pending
                SET observed_at = ?, last_failure_at = ?
                WHERE host_id = ? AND worker_id = ?
                """,
                (current_time, current_time, *key),
            )
            return False
        conn.execute(
            """
            UPDATE backend_pending
            SET observed_at = ?, binding_private_fingerprint = ?,
                observed_turn_target_value = ?, freshness = 'stale',
                last_failure_at = ?, grace_deadline = NULL, updated_at = ?
            WHERE host_id = ? AND worker_id = ?
            """,
            (
                current_time,
                effective_binding,
                effective_target,
                current_time,
                current_time,
                *key,
            ),
        )
        return True

    if observation.kind == "read_succeeded_invalid_prompt":
        changed = row is None or (
            str(row[5]),
            str(row[6]),
            stored_binding,
            stored_target,
        ) != ("invalid", "stale", effective_binding, effective_target)
        conn.execute(
            """
            INSERT INTO backend_pending (
                host_id, worker_id, payload_json, observed_at, revision_digest,
                choice_routes_json, binding_private_fingerprint,
                observed_turn_target_value, observation_state, freshness,
                last_success_at, last_failure_at, grace_deadline, updated_at
            ) VALUES (?, ?, '{}', ?, '', '{}', ?, ?, 'invalid', 'stale',
                      NULL, ?, NULL, ?)
            ON CONFLICT(host_id, worker_id) DO UPDATE SET
                payload_json = '{}', observed_at = excluded.observed_at,
                revision_digest = '', choice_routes_json = '{}',
                binding_private_fingerprint =
                    excluded.binding_private_fingerprint,
                observed_turn_target_value =
                    excluded.observed_turn_target_value,
                observation_state = 'invalid', freshness = 'stale',
                last_success_at = NULL,
                last_failure_at = excluded.last_failure_at,
                grace_deadline = NULL, updated_at = excluded.updated_at
            """,
            (
                *key,
                current_time,
                effective_binding,
                effective_target,
                current_time,
                current_time,
            ),
        )
        conn.execute(
            "DELETE FROM backend_pending_claims "
            "WHERE host_id = ? AND worker_id = ?",
            key,
        )
        return changed

    if observation.kind == "read_succeeded_no_prompt":
        changed = row is None or (
            str(row[5]),
            str(row[6]),
            stored_binding,
            stored_target,
        ) != ("none", "fresh", effective_binding, effective_target)
        conn.execute(
            """
            INSERT INTO backend_pending (
                host_id, worker_id, payload_json, observed_at, revision_digest,
                choice_routes_json, binding_private_fingerprint,
                observed_turn_target_value, observation_state, freshness,
                last_success_at, last_failure_at, grace_deadline, updated_at
            ) VALUES (?, ?, '{}', ?, '', '{}', ?, ?, 'none', 'fresh',
                      ?, NULL, NULL, ?)
            ON CONFLICT(host_id, worker_id) DO UPDATE SET
                payload_json = '{}', observed_at = excluded.observed_at,
                revision_digest = '', choice_routes_json = '{}',
                binding_private_fingerprint =
                    excluded.binding_private_fingerprint,
                observed_turn_target_value =
                    excluded.observed_turn_target_value,
                observation_state = 'none', freshness = 'fresh',
                last_success_at = excluded.last_success_at,
                last_failure_at = NULL, grace_deadline = NULL,
                updated_at = excluded.updated_at
            """,
            (
                *key,
                current_time,
                effective_binding,
                effective_target,
                current_time,
                current_time,
            ),
        )
        conn.execute(
            "DELETE FROM backend_pending_claims "
            "WHERE host_id = ? AND worker_id = ?",
            key,
        )
        return changed

    revision_digest = stable_fingerprint(
        {
            "decision_revision": str(observation.revision_digest),
            "binding_private_fingerprint": source_binding,
            "observed_turn_target_value": source_target,
        }
    )
    persisted_choices = tuple(
        (
            "choice-"
            + stable_fingerprint(
                {
                    "revision": revision_digest,
                    "ordinal": choice.picker_ordinal,
                    "label": choice.label,
                }
            ),
            choice.label,
            choice.picker_ordinal,
        )
        for choice in observation.choices
    )
    public_payload = {
        "question": observation.question,
        "kind": observation.pending_kind or "question",
        "choices": [
            {"choice_id": choice_id, "label": label}
            for choice_id, label, _ordinal in persisted_choices
        ],
        "meta": {"source": "backend"},
    }
    routes = tuple(
        (choice_id, ordinal)
        for choice_id, _label, ordinal in persisted_choices
    )
    normalized, routes_json = _normalize_backend_pending_payload(
        public_payload,
        routes,
    )
    payload_json = _canonical_json(normalized)
    changed = row is None or (
        str(row[0]),
        str(row[1]),
        str(row[2]),
        stored_binding,
        stored_target,
        str(row[5]),
        str(row[6]),
    ) != (
        payload_json,
        revision_digest,
        routes_json,
        source_binding,
        source_target,
        "open",
        "fresh",
    )
    conn.execute(
        """
        DELETE FROM backend_pending_claims
        WHERE host_id = ? AND worker_id = ?
          AND (
               revision_digest != ?
            OR binding_private_fingerprint != ?
            OR turn_target_value != ?
          )
        """,
        (*key, revision_digest, source_binding, source_target),
    )
    conn.execute(
        """
        INSERT INTO backend_pending (
            host_id, worker_id, payload_json, observed_at, revision_digest,
            choice_routes_json, binding_private_fingerprint,
            observed_turn_target_value, observation_state, freshness,
            last_success_at, last_failure_at, grace_deadline, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', 'fresh', ?, NULL, NULL, ?)
        ON CONFLICT(host_id, worker_id) DO UPDATE SET
            payload_json = excluded.payload_json,
            observed_at = excluded.observed_at,
            revision_digest = excluded.revision_digest,
            choice_routes_json = excluded.choice_routes_json,
            binding_private_fingerprint =
                excluded.binding_private_fingerprint,
            observed_turn_target_value =
                excluded.observed_turn_target_value,
            observation_state = 'open', freshness = 'fresh',
            last_success_at = excluded.last_success_at,
            last_failure_at = NULL, grace_deadline = NULL,
            updated_at = excluded.updated_at
        """,
        (
            *key,
            payload_json,
            current_time,
            revision_digest,
            routes_json,
            source_binding,
            source_target,
            current_time,
            current_time,
        ),
    )
    return changed


def _merge_backend_pending_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    pending: Mapping[str, Any] | None,
    *,
    observed_at: str,
) -> bool:
    """Compatibility entrypoint routed through the explicit transition helper."""
    if pending is None:
        observation = PendingObservation("read_succeeded_no_prompt")
    else:
        clean = sanitize_public_mapping(pending)
        raw_choices = clean.get("choices", []) if isinstance(clean, Mapping) else []
        normalized_choices = [
            InteractionChoice.from_dict(choice)
            for choice in raw_choices
            if isinstance(choice, Mapping)
        ]
        choices = tuple(
            PendingObservedChoice(
                choice_id=choice.choice_id,
                label=choice.label,
                picker_ordinal=ordinal,
            )
            for ordinal, choice in enumerate(normalized_choices, 1)
        )
        question = str(clean.get("question") or clean.get("kind") or "Pending action")
        observation = PendingObservation(
            "open_prompt",
            question=question,
            pending_kind=str(clean.get("kind") or "question"),
            choices=choices,
            revision_digest=stable_fingerprint({"legacy_pending_revision": clean}),
        )
    return _apply_backend_pending_observation_conn(
        conn,
        host_id,
        worker_id,
        observation,
        observed_at=observed_at,
        stale_grace_seconds=DEFAULT_PENDING_STALE_GRACE_SECONDS,
    )


def apply_backend_pending_observation(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    observation: PendingObservation,
    *,
    observed_at: str | None = None,
    stale_grace_seconds: float = DEFAULT_PENDING_STALE_GRACE_SECONDS,
    binding_private_fingerprint: str | None = None,
    observed_turn_target_value: str | None = None,
) -> bool:
    """Apply one explicit observation in a short writer transaction."""
    if not _sqlite_store_exists(db_path):
        return False
    current_time, _ = _pending_observed_time(observed_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            changed = _apply_backend_pending_observation_conn(
                conn,
                host_id,
                worker_id,
                observation,
                observed_at=current_time,
                stale_grace_seconds=stale_grace_seconds,
                binding_private_fingerprint=str(
                    binding_private_fingerprint or ""
                ),
                observed_turn_target_value=str(
                    observed_turn_target_value or ""
                ),
            )
            conn.commit()
            return changed
        except Exception:
            conn.rollback()
            raise


def merge_backend_pending(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    pending: Mapping[str, Any] | None,
) -> bool:
    """Presence-sync one worker's backend-provided pending prompt."""
    if not _sqlite_store_exists(db_path):
        return False
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            changed = _merge_backend_pending_conn(
                conn,
                host_id,
                worker_id,
                pending,
                observed_at=utc_timestamp(),
            )
            conn.commit()
            return changed
        except Exception:
            conn.rollback()
            raise


def list_backend_pending(db_path: Path | str, host_id: str) -> dict[str, dict[str, Any]]:
    """worker_id -> normalized pending dict for every live backend-provided prompt."""
    out: dict[str, dict[str, Any]] = {}
    if not _sqlite_store_exists(db_path):
        return out
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        for worker_id, payload_json in conn.execute(
            "SELECT worker_id, payload_json FROM backend_pending "
            "WHERE host_id = ? AND observation_state = 'open'",
            (host_id,),
        ).fetchall():
            try:
                payload = json.loads(payload_json)
            except (TypeError, ValueError):
                continue
            if isinstance(payload, Mapping):
                out[str(worker_id)] = sanitize_public_mapping(payload)
    return sanitize_public_mapping(out)


def _backend_pending_health_from_rows(
    freshness_values: Iterable[Any],
) -> dict[str, Any]:
    fresh = 0
    stale = 0
    for value in freshness_values:
        if str(value) == "stale":
            stale += 1
        else:
            fresh += 1
    return {
        "status": "degraded" if stale else "healthy",
        "counts": {"fresh": fresh, "stale": stale, "total": fresh + stale},
    }


def backend_pending_health(
    db_path: Path | str,
    host_id: str,
) -> dict[str, Any]:
    """Return only aggregate pending freshness; never transitions state."""
    unavailable = {
        "status": "store_unavailable",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    if not _sqlite_store_exists(db_path):
        return unavailable
    try:
        with _connect(db_path) as conn:
            conn.execute("PRAGMA query_only=ON")
            if (
                int(conn.execute("PRAGMA user_version").fetchone()[0])
                != STORE_SCHEMA_VERSION
            ):
                return unavailable
            rows = conn.execute(
                """
                SELECT freshness
                FROM backend_pending
                WHERE host_id = ?
                  AND (observation_state = 'open' OR freshness = 'stale')
                """,
                (str(host_id),),
            ).fetchall()
    except Exception:
        return unavailable
    return _backend_pending_health_from_rows(row[0] for row in rows)


def _backend_pending_interaction(
    *,
    host_id: str,
    worker_id: str,
    pending: Mapping[str, Any],
    routes_json: Any,
    revision_digest: Any,
    freshness: Any,
    last_success_at: Any,
    grace_deadline: Any,
    updated_at: Any,
    worker: Any,
) -> dict[str, Any]:
    routes = json.loads(routes_json)
    if not isinstance(routes, Mapping) or not all(
        type(key) is str
        and key
        and type(value) is int
        and value >= 1
        for key, value in routes.items()
    ):
        raise ValueError("invalid backend pending routes")
    validation_routes = tuple(
        (str(key), int(value)) for key, value in routes.items()
    )
    if not validation_routes:
        raw_choices = pending.get("choices", [])
        if isinstance(raw_choices, list):
            validation_routes = tuple(
                (str(choice.get("choice_id")), ordinal)
                for ordinal, choice in enumerate(raw_choices, 1)
                if isinstance(choice, Mapping) and choice.get("choice_id")
            )
    normalized, _ = _normalize_backend_pending_payload(
        pending,
        validation_routes,
    )
    if type(revision_digest) is not str or not revision_digest:
        raise ValueError("invalid backend pending revision")
    state = str(freshness)
    if state not in {"fresh", "stale"}:
        raise ValueError("invalid backend pending freshness")
    meta = dict(normalized["meta"])
    meta["freshness"] = state
    interaction = PendingInteraction.from_dict(
        {
            "id": f"pending-{stable_fingerprint({'revision': revision_digest, 'worker_id': worker_id})}",
            "host_id": host_id,
            "worker_id": worker_id,
            "question": normalized["question"],
            "kind": normalized["kind"],
            "choices": normalized["choices"],
            "status": "open",
            "worker_fingerprint": (
                worker.fingerprint if worker is not None else None
            ),
            "space_id": worker.space_id if worker is not None else None,
            "created_at": last_success_at,
            "updated_at": updated_at,
            "expires_at": grace_deadline if state == "stale" else None,
            "meta": meta,
        }
    )
    return interaction.to_dict()


def pending_payload_from_store(
    db_path: Path | str,
    host_id: str,
) -> dict[str, Any]:
    """Project one coherent snapshot plus a strictly validated pending overlay."""
    unavailable = {
        "schema_version": TURN_SCHEMA_VERSION,
        "host_id": str(host_id),
        "ok": False,
        "status": "store_unavailable",
        "pending_interactions": [],
        "backend_health": [],
        "pending_health": {
            "status": "store_unavailable",
            "counts": {"fresh": 0, "stale": 0, "total": 0},
        },
    }
    if not _sqlite_store_exists(db_path):
        return unavailable

    try:
        with _connect(db_path) as conn:
            conn.execute("PRAGMA query_only=ON")
            if (
                int(conn.execute("PRAGMA user_version").fetchone()[0])
                != STORE_SCHEMA_VERSION
            ):
                return unavailable
            conn.execute("BEGIN")
            try:
                snapshot_row = conn.execute(
                    """
                    SELECT payload
                    FROM snapshots
                    WHERE host_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (str(host_id),),
                ).fetchone()
                backend_rows = conn.execute(
                    """
                    SELECT worker_id, payload_json, choice_routes_json,
                           revision_digest, freshness, last_success_at,
                           grace_deadline, updated_at, observation_state
                    FROM backend_pending
                    WHERE host_id = ?
                    ORDER BY worker_id
                    """,
                    (str(host_id),),
                ).fetchall()
            finally:
                conn.rollback()
    except Exception:
        return unavailable

    if snapshot_row is None:
        return unavailable
    raw_snapshot = snapshot_row[0]
    if not isinstance(raw_snapshot, str) or not raw_snapshot:
        return unavailable
    try:
        decoded_snapshot = json.loads(raw_snapshot)
    except (TypeError, ValueError):
        return unavailable
    if not isinstance(decoded_snapshot, Mapping):
        return unavailable
    if decoded_snapshot.get("host_id") != str(host_id):
        return unavailable
    if _strict_utc_timestamp(decoded_snapshot.get("updated_at")) is None:
        return unavailable
    for collection_name in ("spaces", "workers", "attention", "backend_health"):
        collection = decoded_snapshot.get(collection_name, [])
        if not isinstance(collection, list) or not all(
            isinstance(item, Mapping) for item in collection
        ):
            return unavailable
    for collection_name in ("spaces", "workers", "attention"):
        for item in decoded_snapshot.get(collection_name, []):
            if "meta" in item and not isinstance(item.get("meta"), Mapping):
                return unavailable
    for item in decoded_snapshot.get("attention", []):
        actions = item.get("suggested_actions", item.get("actions", []))
        if not isinstance(actions, list) or not all(
            isinstance(action, Mapping) for action in actions
        ):
            return unavailable
        for action in actions:
            if "params" in action and not isinstance(action.get("params"), Mapping):
                return unavailable
    for item in decoded_snapshot.get("backend_health", []):
        if "counts" in item and not isinstance(item.get("counts"), Mapping):
            return unavailable
    try:
        stored_snapshot = Snapshot.from_dict(
            sanitize_public_mapping(decoded_snapshot)
        )
    except Exception:
        return unavailable
    if stored_snapshot.host_id != str(host_id):
        return unavailable

    payload = dict(pending_payload_from_snapshot(stored_snapshot))
    workers = {worker.id: worker for worker in stored_snapshot.workers}
    built: dict[str, dict[str, Any]] = {}
    suppressed_worker_ids: set[str] = set()
    for (
        worker_id,
        payload_json,
        routes_json,
        revision_digest,
        freshness,
        last_success_at,
        grace_deadline,
        updated_at,
        observation_state,
    ) in backend_rows:
        if str(observation_state) == "none":
            suppressed_worker_ids.add(str(worker_id))
            continue
        if str(observation_state) != "open":
            continue
        try:
            pending = json.loads(payload_json)
            if not isinstance(pending, Mapping):
                continue
            built[str(worker_id)] = _backend_pending_interaction(
                host_id=str(host_id),
                worker_id=str(worker_id),
                pending=pending,
                routes_json=routes_json,
                revision_digest=revision_digest,
                freshness=freshness,
                last_success_at=last_success_at,
                grace_deadline=grace_deadline,
                updated_at=updated_at,
                worker=workers.get(str(worker_id)),
            )
        except (TypeError, ValueError):
            continue

    rows = [
        row
        for row in payload.get("pending_interactions", [])
        if row.get("worker_id") not in built
        and row.get("worker_id") not in suppressed_worker_ids
    ]
    rows.extend(built.values())
    rows.sort(
        key=lambda row: (
            str(row.get("id") or ""),
            str(row.get("fingerprint") or ""),
        )
    )
    payload["pending_interactions"] = rows
    payload["pending_health"] = _backend_pending_health_from_rows(
        row[4]
        for row in backend_rows
        if str(row[8]) == "open" or str(row[4]) == "stale"
    )
    payload["content_fingerprint"] = recompute_pending_content_fingerprint(payload)
    return sanitize_public_mapping(payload)


def _backend_pending_claim_context_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    binding_private_fingerprint: str,
    observed_turn_target_value: str,
    *,
    observed_at: str,
) -> tuple[Any, str, str, str] | None:
    if (
        not str(binding_private_fingerprint)
        or not str(observed_turn_target_value)
    ):
        return None
    snapshot_row = conn.execute(
        "SELECT payload FROM snapshots WHERE host_id = ? ORDER BY id DESC LIMIT 1",
        (str(host_id),),
    ).fetchone()
    if snapshot_row is None:
        return None
    try:
        snapshot = Snapshot.from_dict(json.loads(snapshot_row[0]))
    except Exception:
        return None
    worker = next(
        (item for item in snapshot.workers if item.id == str(worker_id)),
        None,
    )
    if worker is None or worker.status in {"closed", "failed", "unknown"}:
        return None
    binding_row = conn.execute(
        """
        SELECT private_fingerprint, worker_fingerprint, turn_target_value
        FROM worker_bindings
        WHERE host_id = ? AND worker_id = ? AND worker_fingerprint = ?
          AND backend = 'herdr' AND turn_target_kind = 'pane_id'
          AND private_fingerprint = ? AND turn_target_value = ?
          AND sendable = 1 AND expires_at > ?
        """,
        (
            str(host_id),
            str(worker_id),
            str(worker.fingerprint),
            str(binding_private_fingerprint),
            str(observed_turn_target_value),
            str(observed_at),
        ),
    ).fetchone()
    if binding_row is None:
        return None
    return (
        worker,
        str(binding_row[0]),
        str(binding_row[1]),
        str(binding_row[2]),
    )


def _backend_pending_claim_expired(
    claimed_at: Any,
    current_time: str,
    lease_seconds: float,
) -> bool:
    if (
        isinstance(lease_seconds, bool)
        or not isinstance(lease_seconds, (int, float))
        or not float(lease_seconds) > 0
    ):
        raise ValueError("claim_lease_seconds must be positive")
    claimed = _strict_utc_timestamp(claimed_at)
    if claimed is None:
        return False
    return datetime.fromisoformat(current_time) >= (
        datetime.fromisoformat(claimed)
        + timedelta(seconds=float(lease_seconds))
    )


def claim_backend_pending_choice(
    db_path: Path | str,
    host_id: str,
    pending_id: str,
    pending_fingerprint: str,
    choice_id: str,
    *,
    claim: bool = True,
    observed_at: str | None = None,
    claim_lease_seconds: float = BACKEND_PENDING_CLAIM_LEASE_SECONDS,
) -> BackendPendingChoiceClaim:
    """Validate, and optionally durably claim, one exact fresh public choice."""
    if not _sqlite_store_exists(db_path):
        return BackendPendingChoiceClaim("not_found")
    current_time, _ = _pending_observed_time(observed_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE" if claim else "BEGIN")
        try:
            candidates = conn.execute(
                """
                SELECT worker_id, payload_json, choice_routes_json,
                       revision_digest, freshness, last_success_at,
                       grace_deadline, updated_at,
                       binding_private_fingerprint,
                       observed_turn_target_value
                FROM backend_pending
                WHERE host_id = ? AND observation_state = 'open'
                ORDER BY worker_id
                """,
                (str(host_id),),
            ).fetchall()
            matched: tuple[Any, ...] | None = None
            interaction: dict[str, Any] | None = None
            context: tuple[Any, str, str, str] | None = None
            for candidate in candidates:
                candidate_context = _backend_pending_claim_context_conn(
                    conn,
                    str(host_id),
                    str(candidate[0]),
                    str(candidate[8]),
                    str(candidate[9]),
                    observed_at=current_time,
                )
                if candidate_context is None:
                    continue
                try:
                    candidate_interaction = _backend_pending_interaction(
                        host_id=str(host_id),
                        worker_id=str(candidate[0]),
                        pending=json.loads(candidate[1]),
                        routes_json=candidate[2],
                        revision_digest=candidate[3],
                        freshness=candidate[4],
                        last_success_at=candidate[5],
                        grace_deadline=candidate[6],
                        updated_at=candidate[7],
                        worker=candidate_context[0],
                    )
                except Exception:
                    continue
                if candidate_interaction["id"] == str(pending_id):
                    matched = candidate
                    interaction = candidate_interaction
                    context = candidate_context
                    break
            if matched is None or interaction is None or context is None:
                conn.rollback()
                return BackendPendingChoiceClaim("not_found")
            if str(matched[4]) != "fresh":
                conn.rollback()
                return BackendPendingChoiceClaim("stale")
            if interaction["fingerprint"] != str(pending_fingerprint):
                conn.rollback()
                return BackendPendingChoiceClaim("changed")
            routes = json.loads(matched[2])
            ordinal = routes.get(str(choice_id)) if isinstance(routes, Mapping) else None
            if type(ordinal) is not int or ordinal < 1:
                conn.rollback()
                return BackendPendingChoiceClaim("unknown_choice")
            existing = conn.execute(
                """
                SELECT state, claimed_at
                FROM backend_pending_claims
                WHERE host_id = ? AND worker_id = ?
                """,
                (str(host_id), str(matched[0])),
            ).fetchone()
            if existing is not None:
                reclaimable = (
                    str(existing[0]) == "claimed"
                    and _backend_pending_claim_expired(
                        existing[1],
                        current_time,
                        claim_lease_seconds,
                    )
                )
                if reclaimable and claim:
                    conn.execute(
                        """
                        DELETE FROM backend_pending_claims
                        WHERE host_id = ? AND worker_id = ? AND state = 'claimed'
                          AND claimed_at = ?
                        """,
                        (str(host_id), str(matched[0]), str(existing[1])),
                    )
                elif not reclaimable:
                    conn.rollback()
                    return BackendPendingChoiceClaim("already_claimed")
            fields = {
                "worker_id": str(matched[0]),
                "worker_fingerprint": context[2],
                "binding_private_fingerprint": context[1],
                "turn_target_value": context[3],
                "picker_ordinal": ordinal,
            }
            if not claim:
                conn.rollback()
                return BackendPendingChoiceClaim("validated", **fields)
            token = secrets.token_urlsafe(32)
            conn.execute(
                """
                INSERT INTO backend_pending_claims (
                    host_id, worker_id, claim_token, revision_digest, choice_id,
                    picker_ordinal, worker_fingerprint, binding_private_fingerprint,
                    turn_target_value, state, claimed_at, send_started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'claimed', ?, NULL)
                """,
                (
                    str(host_id), str(matched[0]), token, str(matched[3]),
                    str(choice_id), ordinal, context[2], context[1], context[3],
                    current_time,
                ),
            )
            conn.commit()
            return BackendPendingChoiceClaim("claimed", claim_token=token, **fields)
        except Exception:
            conn.rollback()
            raise


def start_backend_pending_choice_send(
    db_path: Path | str,
    host_id: str,
    claim_token: str,
    *,
    observed_at: str | None = None,
    claim_lease_seconds: float = BACKEND_PENDING_CLAIM_LEASE_SECONDS,
) -> BackendPendingChoiceSend:
    """CAS a pre-send claim against the current revision and exact binding."""
    if not _sqlite_store_exists(db_path):
        return BackendPendingChoiceSend("not_found")
    current_time, _ = _pending_observed_time(observed_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                """
                SELECT worker_id, revision_digest, choice_id, picker_ordinal,
                       worker_fingerprint, binding_private_fingerprint,
                       turn_target_value, state, claimed_at
                FROM backend_pending_claims
                WHERE host_id = ? AND claim_token = ?
                """,
                (str(host_id), str(claim_token)),
            ).fetchone()
            if row is None:
                conn.rollback()
                return BackendPendingChoiceSend("not_found")
            fields = {
                "worker_id": str(row[0]),
                "worker_fingerprint": str(row[4]),
                "binding_private_fingerprint": str(row[5]),
                "turn_target_value": str(row[6]),
                "picker_ordinal": int(row[3]),
            }
            if (
                str(row[7]) == "claimed"
                and _backend_pending_claim_expired(
                    row[8],
                    current_time,
                    claim_lease_seconds,
                )
            ):
                conn.execute(
                    """
                    DELETE FROM backend_pending_claims
                    WHERE host_id = ? AND claim_token = ? AND state = 'claimed'
                    """,
                    (str(host_id), str(claim_token)),
                )
                conn.commit()
                return BackendPendingChoiceSend("not_found")
            if str(row[7]) == "send_started":
                conn.rollback()
                return BackendPendingChoiceSend("already_started", **fields)
            current = conn.execute(
                """
                SELECT revision_digest, choice_routes_json, freshness,
                       binding_private_fingerprint,
                       observed_turn_target_value
                FROM backend_pending
                WHERE host_id = ? AND worker_id = ? AND observation_state = 'open'
                """,
                (str(host_id), str(row[0])),
            ).fetchone()
            if current is None:
                conn.rollback()
                return BackendPendingChoiceSend("changed")
            if str(current[2]) != "fresh":
                conn.rollback()
                return BackendPendingChoiceSend("stale")
            try:
                routes = json.loads(current[1])
            except (TypeError, ValueError):
                routes = {}
            if (
                str(current[0]) != str(row[1])
                or str(current[3]) != str(row[5])
                or str(current[4]) != str(row[6])
                or not isinstance(routes, Mapping)
                or routes.get(str(row[2])) != int(row[3])
            ):
                conn.rollback()
                return BackendPendingChoiceSend("changed")
            context = _backend_pending_claim_context_conn(
                conn,
                str(host_id),
                str(row[0]),
                str(row[5]),
                str(row[6]),
                observed_at=current_time,
            )
            if context is None or (context[1], context[2], context[3]) != (
                str(row[5]),
                str(row[4]),
                str(row[6]),
            ):
                conn.rollback()
                return BackendPendingChoiceSend("binding_changed")
            cursor = conn.execute(
                """
                UPDATE backend_pending_claims
                SET state = 'send_started', send_started_at = ?
                WHERE host_id = ? AND claim_token = ? AND state = 'claimed'
                """,
                (current_time, str(host_id), str(claim_token)),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return BackendPendingChoiceSend("changed")
            conn.commit()
            return BackendPendingChoiceSend("started", **fields)
        except Exception:
            conn.rollback()
            raise


def abandon_backend_pending_choice_claim(
    db_path: Path | str,
    host_id: str,
    claim_token: str,
) -> bool:
    """Release only a claim that has not crossed the pane-I/O boundary."""
    if not _sqlite_store_exists(db_path):
        return False
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = conn.execute(
                """
                DELETE FROM backend_pending_claims
                WHERE host_id = ? AND claim_token = ? AND state = 'claimed'
                """,
                (str(host_id), str(claim_token)),
            )
            conn.commit()
            return cursor.rowcount == 1
        except Exception:
            conn.rollback()
            raise


def finish_backend_pending_choice_send(
    db_path: Path | str,
    host_id: str,
    claim_token: str,
    *,
    accepted: bool,
    observed_at: str | None = None,
) -> bool:
    """Tombstone an accepted exact revision; retain failed sends as uncertain."""
    if not accepted or not _sqlite_store_exists(db_path):
        return False
    current_time, _ = _pending_observed_time(observed_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                """
                SELECT worker_id, revision_digest,
                       binding_private_fingerprint, turn_target_value
                FROM backend_pending_claims
                WHERE host_id = ? AND claim_token = ? AND state = 'send_started'
                """,
                (str(host_id), str(claim_token)),
            ).fetchone()
            if row is None:
                conn.rollback()
                return False
            updated = conn.execute(
                """
                UPDATE backend_pending
                SET payload_json = '{}',
                    observed_at = ?,
                    revision_digest = '',
                    choice_routes_json = '{}',
                    observation_state = 'none',
                    freshness = 'fresh',
                    last_success_at = ?,
                    last_failure_at = NULL,
                    grace_deadline = NULL,
                    updated_at = ?
                WHERE host_id = ?
                  AND worker_id = ?
                  AND revision_digest = ?
                  AND binding_private_fingerprint = ?
                  AND observed_turn_target_value = ?
                  AND observation_state IN ('open', 'failed')
                """,
                (
                    current_time,
                    current_time,
                    current_time,
                    str(host_id),
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    str(row[3]),
                ),
            )
            deleted = conn.execute(
                """
                DELETE FROM backend_pending_claims
                WHERE host_id = ? AND claim_token = ? AND state = 'send_started'
                """,
                (str(host_id), str(claim_token)),
            )
            conn.commit()
            return updated.rowcount == 1 and deleted.rowcount == 1
        except Exception:
            conn.rollback()
            raise


def prune_backend_pending(
    db_path: Path | str,
    host_id: str,
    live_binding_private_fingerprints: Iterable[str],
    *,
    deadline_monotonic: float | None = None,
    cancelled: Callable[[], bool] | None = None,
    observed_at: str | None = None,
) -> int:
    """Delete state whose exact authoritative pane binding disappeared."""
    if not _sqlite_store_exists(db_path):
        return 0
    live = {
        str(private_fingerprint)
        for private_fingerprint in live_binding_private_fingerprints
        if str(private_fingerprint)
    }
    current_time, _ = _pending_observed_time(observed_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        if not _begin_turn_refresh_transaction(
            conn,
            deadline_monotonic=deadline_monotonic,
            cancelled=cancelled,
        ):
            return 0
        try:
            if _turn_refresh_is_cancelled(
                deadline_monotonic=deadline_monotonic,
                cancelled=cancelled,
            ):
                conn.rollback()
                return 0
            stored = [
                (str(row[0]), str(row[1]), str(row[2]))
                for row in conn.execute(
                    """
                    SELECT worker_id, binding_private_fingerprint,
                           observed_turn_target_value
                    FROM backend_pending
                    WHERE host_id = ?
                    """,
                    (str(host_id),),
                ).fetchall()
            ]
            stale = [
                (worker_id, private_fingerprint, turn_target_value)
                for worker_id, private_fingerprint, turn_target_value in stored
                if private_fingerprint not in live
            ]
            for worker_id, private_fingerprint, turn_target_value in stale:
                _apply_backend_pending_observation_conn(
                    conn,
                    str(host_id),
                    worker_id,
                    PendingObservation("worker_authoritatively_absent"),
                    observed_at=current_time,
                    stale_grace_seconds=DEFAULT_PENDING_STALE_GRACE_SECONDS,
                    binding_private_fingerprint=private_fingerprint,
                    observed_turn_target_value=turn_target_value,
                )
            if _turn_refresh_is_cancelled(
                deadline_monotonic=deadline_monotonic,
                cancelled=cancelled,
            ):
                conn.rollback()
                return 0
            conn.commit()
            return len(stale)
        except Exception:
            conn.rollback()
            raise


def _current_turn_content_rows_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
) -> list[tuple[Any, dict[str, Any], dict[str, Any] | None, str]]:
    rows = conn.execute(
        """
        SELECT
            turns.turn_id,
            turns.payload_json,
            revisions.content_revision,
            revisions.user_text,
            revisions.assistant_final_text,
            revisions.user_state,
            revisions.final_state,
            turns.observed_at
        FROM turns
        LEFT JOIN turn_content_revisions AS revisions
          ON revisions.host_id = turns.host_id
         AND revisions.turn_id = turns.turn_id
         AND revisions.is_current = 1
        WHERE turns.host_id = ? AND turns.worker_id = ?
        """,
        (str(host_id), str(worker_id)),
    ).fetchall()
    decoded: list[tuple[Any, dict[str, Any], dict[str, Any] | None, str]] = []
    for (
        turn_id,
        payload_json,
        revision,
        user_text,
        final_text,
        user_state,
        final_state,
        stored_observed_at,
    ) in rows:
        try:
            loaded = json.loads(str(payload_json or "{}"))
        except (TypeError, json.JSONDecodeError):
            loaded = {}
        payload = sanitize_public_mapping(loaded) if isinstance(loaded, Mapping) else {}
        current = (
            {
                "content_revision": str(revision),
                "user_text": user_text,
                "assistant_final_text": final_text,
                "user_state": str(user_state),
                "final_state": str(final_state),
            }
            if revision is not None
            else None
        )
        decoded.append((turn_id, payload, current, str(stored_observed_at or "")))
    return decoded


def _turn_with_current_content(
    payload: Mapping[str, Any],
    current: Mapping[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(payload)
    if current is not None:
        merged["user_text"] = current.get("user_text")
        merged["assistant_final_text"] = current.get("assistant_final_text")
    return merged


def _source_turn_matches(payload: Mapping[str, Any], incoming_source_turn: str) -> bool:
    stored = str(payload.get("source_turn_id") or "").strip()
    if not stored or not incoming_source_turn:
        return False
    candidate = Turn.from_dict({**dict(payload), "source_turn_id": incoming_source_turn})
    return candidate.source_turn_id == stored


def _merge_canonical_field(
    incoming: str | None,
    current_text: Any,
    current_state: Any,
) -> tuple[str | None, str]:
    if incoming is None or incoming == "":
        state = str(current_state or "absent")
        if state not in {"absent", "complete", "known_incomplete"}:
            state = "absent"
        return (
            str(current_text) if current_text is not None and state != "absent" else None,
            state,
        )
    return incoming, "complete"


def _retain_authoritative_completion(
    metadata: Mapping[str, Any],
    current: Mapping[str, Any] | None,
    existing: Mapping[str, Any],
    *,
    incoming_final: str | None,
    observed_at: str,
) -> dict[str, Any]:
    merged = dict(metadata)
    terminal = (
        existing.get("complete") is True
        or (current is not None and str(current.get("final_state") or "") == "complete")
    )
    completes_now = merged.get("complete") is True or bool(incoming_final)
    if terminal or completes_now:
        merged["complete"] = True
        merged["has_open_turn"] = False
        merged["assistant_stream_text"] = None
        merged["completed_at"] = existing.get("completed_at") or observed_at
    return merged


def _turn_observation_is_newer(incoming: str, stored: str) -> bool:
    if not stored:
        return True
    incoming_timestamp = _strict_utc_timestamp(incoming)
    stored_timestamp = _strict_utc_timestamp(stored)
    if incoming_timestamp is not None and stored_timestamp is not None:
        return _connector_datetime(incoming_timestamp) > _connector_datetime(stored_timestamp)
    return str(incoming) > str(stored)

def _turn_has_authoritative_observation(
    payload: Mapping[str, Any],
    current: Mapping[str, Any] | None,
) -> bool:
    if str(payload.get("source_turn_id") or "").strip():
        return True
    if any(
        payload.get(key) not in (None, "", False)
        for key in ("assistant_stream_text", "complete", "has_open_turn")
    ):
        return True
    return current is not None and any(
        str(current.get(key) or "") != "absent"
        for key in ("user_state", "final_state")
    )


def _replace_current_turn_content_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    current: Mapping[str, Any] | None,
    incoming_user: str | None,
    incoming_final: str | None,
    current_time: str,
) -> bool:
    user_text, user_state = _merge_canonical_field(
        incoming_user,
        current.get("user_text") if current else None,
        current.get("user_state") if current else None,
    )
    final_text, final_state = _merge_canonical_field(
        incoming_final,
        current.get("assistant_final_text") if current else None,
        current.get("final_state") if current else None,
    )
    if user_state == "absent" and final_state == "absent":
        return False
    revision = content_revision(
        str(turn_id),
        user_text,
        final_text,
        user_state,
        final_state,
    )
    if current is not None and str(current.get("content_revision") or "") == revision:
        return False
    conn.execute(
        """
        UPDATE turn_content_revisions
        SET is_current = 0, superseded_at = ?
        WHERE host_id = ? AND turn_id = ? AND is_current = 1
        """,
        (current_time, str(host_id), str(turn_id)),
    )
    existing = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND content_revision = ?
        """,
        (str(host_id), str(turn_id), revision),
    ).fetchone()
    if existing is None:
        _insert_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            user_text=user_text,
            assistant_final_text=final_text,
            user_state=user_state,
            final_state=final_state,
            created_at=current_time,
        )
    else:
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET is_current = 1, superseded_at = NULL
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (str(host_id), str(turn_id), revision),
        )
    return True


def _strip_canonical_turn_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    lightweight = dict(payload)
    for key in (
        "user_text",
        "assistant_final_text",
        "user_preview",
        "assistant_final_preview",
        "content",
    ):
        lightweight.pop(key, None)
    return lightweight


def _merge_turn_content_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    content: Mapping[str, Any],
    *,
    observed_at: str,
) -> int:
    if not any(key in content for key in _TURN_CONTENT_FIELDS):
        return 0
    incoming_user = (
        sanitize_canonical_turn_text(content.get("user_text"))
        if "user_text" in content
        else None
    )
    incoming_final = (
        sanitize_canonical_turn_text(content.get("assistant_final_text"))
        if "assistant_final_text" in content
        else None
    )
    clean_content = sanitize_public_mapping(
        {
            key: content.get(key)
            for key in _TURN_CONTENT_FIELDS
            if key in content and key not in {"user_text", "assistant_final_text"}
        }
    )
    automation_probe = {
        **clean_content,
        "user_text": incoming_user,
        "assistant_final_text": incoming_final,
    }
    if is_internal_automation_turn_payload(automation_probe):
        return 0
    rows = _current_turn_content_rows_conn(conn, host_id, worker_id)
    if not rows:
        return 0
    incoming_source_turn = str(clean_content.get("source_turn_id") or "").strip()
    exact_source_rows = [
        row
        for row in rows
        if incoming_source_turn
        and _source_turn_matches(row[1], incoming_source_turn)
    ]
    scored_rows = [
        (
            turn_id,
            payload,
            current,
            stored_observed_at,
            _turn_with_current_content(payload, current),
        )
        for turn_id, payload, current, stored_observed_at in rows
    ]
    base_turn_id, base_payload, base_current, base_observed_at, base_view = max(
        scored_rows,
        key=lambda row: _turn_merge_score(row[4], automation_probe),
    )
    changed = False
    if exact_source_rows:
        turn_id, payload, current, stored_observed_at = exact_source_rows[0]
        if not _turn_observation_is_newer(observed_at, stored_observed_at):
            return 0
        payload.update(
            _retain_authoritative_completion(
                clean_content,
                current,
                payload,
                incoming_final=incoming_final,
                observed_at=observed_at,
            )
        )
        metadata_changed = _update_turn_row(
            conn,
            host_id,
            turn_id,
            payload,
            observed_at,
        )
        revision_changed = _replace_current_turn_content_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            current=current,
            incoming_user=incoming_user,
            incoming_final=incoming_final,
            current_time=observed_at,
        )
        revision_repaired = _ensure_absent_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            observed_at=observed_at,
        )
        changed = metadata_changed or revision_changed or revision_repaired
    elif incoming_source_turn:
        seed = {
            key: base_payload.get(key)
            for key in _TURN_IDENTITY_SEED_FIELDS
            if base_payload.get(key) is not None
        }
        if seed.get("origin_command_id") and not _turn_content_matches_origin(
            base_view,
            automation_probe,
        ):
            seed.pop("origin_command_id", None)
            if str(seed.get("source") or "") == "command":
                seed["source"] = "snapshot"
        seed.update(
            _retain_authoritative_completion(
                clean_content,
                None,
                {},
                incoming_final=incoming_final,
                observed_at=observed_at,
            )
        )
        item = _strip_canonical_turn_payload(Turn.from_dict(seed).to_dict())
        turn_id = str(item.get("id") or "unknown")
        list_sequence = _turn_list_sequence_conn(conn, host_id, turn_id)
        conn.execute(
            """
            INSERT INTO turns (
                host_id, turn_id, worker_id, worker_fingerprint, space_id,
                status, kind, updated_at, fingerprint,
                snapshot_content_fingerprint, observed_at, payload_json,
                list_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, turn_id) DO UPDATE SET
                status = excluded.status,
                kind = excluded.kind,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                str(host_id),
                turn_id,
                str(item.get("worker_id") or worker_id),
                item.get("worker_fingerprint"),
                item.get("space_id"),
                str(item.get("status") or "unknown"),
                str(item.get("kind") or "unknown"),
                observed_at,
                str(item.get("fingerprint") or ""),
                "",
                observed_at,
                _canonical_json(item),
                list_sequence,
            ),
        )
        _replace_current_turn_content_conn(
            conn,
            host_id=str(host_id),
            turn_id=turn_id,
            current=None,
            incoming_user=incoming_user,
            incoming_final=incoming_final,
            current_time=observed_at,
        )
        _ensure_absent_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            observed_at=observed_at,
        )
        if not str(base_payload.get("source_turn_id") or "").strip():
            base_payload["assistant_stream_text"] = None
            _update_turn_row(
                conn,
                host_id,
                base_turn_id,
                base_payload,
                observed_at,
            )
        _prune_source_turn_history(conn, host_id, worker_id)
        changed = True
    else:
        if (
            _turn_has_authoritative_observation(base_payload, base_current)
            and not _turn_observation_is_newer(observed_at, base_observed_at)
        ):
            return 0
        payload = dict(base_payload)
        payload.update(
            _retain_authoritative_completion(
                clean_content,
                base_current,
                payload,
                incoming_final=incoming_final,
                observed_at=observed_at,
            )
        )
        metadata_changed = _update_turn_row(
            conn,
            host_id,
            base_turn_id,
            payload,
            observed_at,
        )
        revision_changed = _replace_current_turn_content_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(base_turn_id),
            current=base_current,
            incoming_user=incoming_user,
            incoming_final=incoming_final,
            current_time=observed_at,
        )
        revision_repaired = _ensure_absent_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(base_turn_id),
            observed_at=observed_at,
        )
        changed = metadata_changed or revision_changed or revision_repaired
    return int(changed)


def _turn_refresh_binding_matches_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    expected: WorkerBinding,
) -> bool:
    if (
        str(expected.host_id) != str(host_id)
        or str(expected.worker_id) != str(worker_id)
    ):
        return False
    row = conn.execute(
        """
        SELECT
            worker_id,
            worker_fingerprint,
            backend,
            turn_target_kind,
            turn_target_value
        FROM worker_bindings
        WHERE host_id = ?
          AND backend = ?
          AND private_fingerprint = ?
          AND expires_at > ?
        """,
        (
            str(host_id),
            str(expected.backend),
            str(expected.private_fingerprint),
            utc_timestamp(),
        ),
    ).fetchone()
    return row is not None and tuple(row) == (
        str(expected.worker_id),
        str(expected.worker_fingerprint),
        str(expected.backend),
        expected.turn_target_kind,
        expected.turn_target_value,
    )


def _turn_refresh_is_cancelled(
    *,
    deadline_monotonic: float | None,
    cancelled: Callable[[], bool] | None,
) -> bool:
    if cancelled is not None:
        try:
            if cancelled():
                return True
        except Exception:
            return True
    return (
        deadline_monotonic is not None
        and time.monotonic() >= float(deadline_monotonic)
    )


def _begin_turn_refresh_transaction(
    conn: sqlite3.Connection,
    *,
    deadline_monotonic: float | None,
    cancelled: Callable[[], bool] | None,
) -> bool:
    if deadline_monotonic is None and cancelled is None:
        conn.execute("BEGIN IMMEDIATE")
        return True
    conn.execute("PRAGMA busy_timeout=50")
    while True:
        if _turn_refresh_is_cancelled(
            deadline_monotonic=deadline_monotonic,
            cancelled=cancelled,
        ):
            return False
        try:
            conn.execute("BEGIN IMMEDIATE")
            return True
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" not in message and "busy" not in message:
                raise


def apply_turn_refresh(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    content: Mapping[str, Any],
    *,
    backend_pending: Mapping[str, Any] | None | object = _UNSET,
    backend_pending_observation: PendingObservation | object = _UNSET,
    expected_binding: WorkerBinding | None = None,
    deadline_monotonic: float | None = None,
    cancelled: Callable[[], bool] | None = None,
    observed_at: str | None = None,
    pending_stale_grace_seconds: float = DEFAULT_PENDING_STALE_GRACE_SECONDS,
) -> TurnRefreshApplyResult:
    """Atomically apply one binding's turn observation and optional pending state."""
    if not _sqlite_store_exists(db_path):
        return TurnRefreshApplyResult(0, False)
    current_time, _ = _pending_observed_time(observed_at)
    if (
        backend_pending is not _UNSET
        and backend_pending_observation is not _UNSET
    ):
        raise ValueError("provide only one backend pending observation")
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        # Transaction acquisition must precede every merge-base read. Use
        # cancellable short waits for daemon-owned refresh work so a stopped
        # scheduler cannot commit after its deadline.
        if not _begin_turn_refresh_transaction(
            conn,
            deadline_monotonic=deadline_monotonic,
            cancelled=cancelled,
        ):
            return TurnRefreshApplyResult(0, False, False, True)
        try:
            if _turn_refresh_is_cancelled(
                deadline_monotonic=deadline_monotonic,
                cancelled=cancelled,
            ):
                conn.rollback()
                return TurnRefreshApplyResult(0, False, False, True)
            if expected_binding is not None and not _turn_refresh_binding_matches_conn(
                conn,
                str(host_id),
                str(worker_id),
                expected_binding,
            ):
                conn.rollback()
                return TurnRefreshApplyResult(0, False, True)
            updated = _merge_turn_content_conn(
                conn,
                str(host_id),
                str(worker_id),
                content,
                observed_at=current_time,
            )
            if backend_pending_observation is not _UNSET:
                if not isinstance(
                    backend_pending_observation,
                    PendingObservation,
                ):
                    raise TypeError("invalid backend pending observation")
                pending_changed = _apply_backend_pending_observation_conn(
                    conn,
                    str(host_id),
                    str(worker_id),
                    backend_pending_observation,
                    observed_at=current_time,
                    stale_grace_seconds=pending_stale_grace_seconds,
                    binding_private_fingerprint=(
                        str(expected_binding.private_fingerprint)
                        if expected_binding is not None
                        else ""
                    ),
                    observed_turn_target_value=(
                        str(expected_binding.turn_target_value or "")
                        if expected_binding is not None
                        else ""
                    ),
                )
            elif backend_pending is not _UNSET:
                pending_changed = _merge_backend_pending_conn(
                    conn,
                    str(host_id),
                    str(worker_id),
                    backend_pending,
                    observed_at=current_time,
                )
            else:
                pending_changed = False
            if _turn_refresh_is_cancelled(
                deadline_monotonic=deadline_monotonic,
                cancelled=cancelled,
            ):
                conn.rollback()
                return TurnRefreshApplyResult(0, False, False, True)
            conn.commit()
            return TurnRefreshApplyResult(updated, pending_changed)
        except Exception:
            conn.rollback()
            raise


def merge_turn_content(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    content: Mapping[str, Any],
    *,
    observed_at: str | None = None,
) -> int:
    """Compatibility wrapper for the transactional authoritative turn merge."""
    return apply_turn_refresh(
        db_path,
        host_id,
        worker_id,
        content,
        observed_at=observed_at,
    ).updated


def _update_turn_row(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: Any,
    payload: dict[str, Any],
    current_time: str,
) -> bool:
    item = _strip_canonical_turn_payload(Turn.from_dict(payload).to_dict())
    encoded = _canonical_json(item)
    row = conn.execute(
        """
        SELECT status, kind, updated_at, fingerprint, payload_json
        FROM turns
        WHERE host_id = ? AND turn_id = ?
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    values = (
        str(item.get("status") or "unknown"),
        str(item.get("kind") or "unknown"),
        item.get("updated_at") or (row[2] if row is not None else None) or current_time,
        str(item.get("fingerprint") or ""),
        encoded,
    )
    if row is not None and tuple(row) == values:
        return False
    conn.execute(
        """
        UPDATE turns
        SET status = ?,
            kind = ?,
            updated_at = ?,
            fingerprint = ?,
            observed_at = ?,
            payload_json = ?
        WHERE host_id = ? AND turn_id = ?
        """,
        (
            values[0],
            values[1],
            values[2],
            values[3],
            current_time,
            values[4],
            str(host_id),
            str(turn_id),
        ),
    )
    return True


def _prune_source_turn_history(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
) -> None:
    rows = conn.execute(
        """
        SELECT turn_id, payload_json
        FROM turns
        WHERE host_id = ? AND worker_id = ?
        ORDER BY COALESCE(updated_at, observed_at, '') DESC
        """,
        (str(host_id), str(worker_id)),
    ).fetchall()
    kept = 0
    for turn_id, payload_json in rows:
        try:
            payload = json.loads(str(payload_json or "{}"))
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, Mapping) or not str(
            payload.get("source_turn_id") or ""
        ).strip():
            continue
        kept += 1
        if kept <= _SOURCE_TURN_HISTORY_LIMIT:
            continue
        _delete_turn_if_unreferenced_conn(conn, str(host_id), str(turn_id))


def upsert_command_pending_turn(
    db_path: Path,
    host_id: str,
    worker: Any,
    *,
    request_id: str,
    instruction_text: str,
    observed_at: str | None = None,
) -> dict[str, Any] | None:
    """Upsert a public pending turn for an accepted command submission."""
    clean_request_id = str(request_id or "").strip()
    clean_text = str(instruction_text or "").strip()
    if not clean_request_id or not clean_text:
        return None
    current_time = observed_at or utc_timestamp()
    worker_id = str(getattr(worker, "id", "") or "").strip()
    if not worker_id and isinstance(worker, Mapping):
        worker_id = str(worker.get("id") or "").strip()
    if not worker_id:
        return None
    item = sanitize_public_mapping(Turn(
        host_id=str(host_id),
        worker_id=worker_id,
        worker_fingerprint=str(getattr(worker, "fingerprint", "") or ""),
        space_id=getattr(worker, "space_id", None),
        status="active",
        kind="task",
        source="command",
        user_text=clean_text,
        assistant_final_text="",
        assistant_stream_text="",
        complete=False,
        has_open_turn=True,
        started_at=current_time,
        updated_at=current_time,
        origin_command_id=clean_request_id,
    ).to_dict())
    turn_id = str(item.get("id") or "")
    if not turn_id:
        return None
    content_fingerprint = stable_fingerprint(
        {
            "source": "command",
            "host_id": str(host_id),
            "worker_id": worker_id,
            "request_id": clean_request_id,
            "turn_fingerprint": item.get("fingerprint"),
        }
    )
    with _connect(db_path, prepare=True, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            list_sequence = _turn_list_sequence_conn(conn, host_id, turn_id)
            conn.execute(
            """
            INSERT INTO turns (
                host_id,
                turn_id,
                worker_id,
                worker_fingerprint,
                space_id,
                status,
                kind,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json,
                list_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, turn_id) DO UPDATE SET
                worker_id = excluded.worker_id,
                worker_fingerprint = excluded.worker_fingerprint,
                space_id = excluded.space_id,
                status = excluded.status,
                kind = excluded.kind,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                str(host_id),
                turn_id,
                worker_id,
                item.get("worker_fingerprint"),
                item.get("space_id"),
                str(item.get("status") or "unknown"),
                str(item.get("kind") or "unknown"),
                item.get("updated_at"),
                str(item.get("fingerprint") or ""),
                content_fingerprint,
                current_time,
                _canonical_json(item),
                list_sequence,
            ),
            )
            _ensure_payload_turn_content_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=turn_id,
                payload=item,
                observed_at=current_time,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return sanitize_public_mapping(item)


def turns_payload_from_store(
    db_path: Path | str,
    host_id: str,
    *,
    snapshot: Snapshot | None = None,
    schema_version: int = 1,
    limit: int = TURN_LIST_DEFAULT_LIMIT,
    cursor: str | None = None,
    since: str | None = None,
    now: float | int | None = None,
    work_counters: TurnContentWorkCounters | None = None,
) -> dict[str, Any]:
    """Return one insertion-stable, byte-bounded turn-list page."""
    requested_schema = int(schema_version)
    if requested_schema not in {1, TURN_LIST_SCHEMA_VERSION}:
        return {
            "schema_version": requested_schema,
            "ok": False,
            "status": "unsupported_turn_schema_version",
            "required_turn_schema_version": TURN_LIST_SCHEMA_VERSION,
        }
    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 1 <= limit <= TURN_LIST_MAX_LIMIT
        or cursor is not None and since is not None
        or cursor is not None and (not isinstance(cursor, str) or not cursor)
        or since is not None and (not isinstance(since, str) or not since)
    ):
        return {
            "schema_version": requested_schema,
            "ok": False,
            "status": "invalid_cursor",
        }
    if not _sqlite_store_exists(db_path):
        return {
            "schema_version": requested_schema,
            "host_id": str(host_id),
            "ok": False,
            "status": "store_unavailable",
        }
    current_clock = time.time() if now is None else float(now)
    try:
        decoded_cursor = (
            decode_turn_list_cursor(
                cursor,
                host_id=str(host_id),
                schema_version=requested_schema,
                limit=limit,
                now=current_clock,
            )
            if cursor is not None
            else None
        )
        decoded_since = (
            decode_turn_since_token(
                since,
                host_id=str(host_id),
                schema_version=requested_schema,
            )
            if since is not None
            else None
        )
    except ValueError as exc:
        status = "cursor_expired" if str(exc) == "cursor_expired" else "invalid_cursor"
        return {
            "schema_version": requested_schema,
            "ok": False,
            "status": status,
        }
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN")
        try:
            store_epoch = _turn_list_store_epoch_conn(conn)
            row = conn.execute(
                """
                SELECT COALESCE(MIN(list_sequence), 0)
                FROM turns
                WHERE host_id = ?
                """,
                (str(host_id),),
            ).fetchone()
            current_floor = int(row[0] if row is not None else 0)
            current_high, current_generation = _turn_list_host_state_conn(
                conn,
                str(host_id),
            )
            if decoded_cursor is not None:
                if (
                    decoded_cursor.store_epoch != store_epoch
                    or decoded_cursor.watermark > current_high
                    or decoded_cursor.traversal_generation != current_generation
                    or (
                        decoded_cursor.floor_sequence
                        and current_floor > decoded_cursor.floor_sequence
                    )
                ):
                    conn.rollback()
                    return {
                        "schema_version": requested_schema,
                        "ok": False,
                        "status": "cursor_expired",
                    }
                anchor = conn.execute(
                    """
                    SELECT 1
                    FROM turns
                    WHERE host_id = ?
                      AND worker_id = ?
                      AND list_sequence = ?
                      AND turn_id = ?
                    """,
                    (
                        str(host_id),
                        decoded_cursor.worker_id,
                        decoded_cursor.list_sequence,
                        decoded_cursor.turn_id,
                    ),
                ).fetchone()
                if anchor is None:
                    conn.rollback()
                    return {
                        "schema_version": requested_schema,
                        "ok": False,
                        "status": "cursor_expired",
                    }
                original_since = decoded_cursor.since_sequence
                watermark = decoded_cursor.watermark
                floor_sequence = decoded_cursor.floor_sequence
                traversal_generation = decoded_cursor.traversal_generation
                expires_at = decoded_cursor.expires_at
            else:
                if decoded_since is not None:
                    if (
                        decoded_since.store_epoch != store_epoch
                        or decoded_since.watermark > current_high
                    ):
                        conn.rollback()
                        return {
                            "schema_version": requested_schema,
                            "ok": False,
                            "status": "since_expired",
                        }
                    original_since = decoded_since.watermark
                else:
                    original_since = 0
                watermark = current_high
                floor_sequence = current_floor
                traversal_generation = current_generation
                expires_at = int(current_clock) + TURN_LIST_CURSOR_TTL_SECONDS
            parameters: list[Any] = [
                TURN_TEXT_MAX_CHARS,
                TURN_TEXT_MAX_CHARS,
                TURN_CONTENT_PREVIEW_MAX_CHARS,
                TURN_TEXT_MAX_CHARS,
                TURN_TEXT_MAX_CHARS,
                TURN_CONTENT_PREVIEW_MAX_CHARS,
                str(host_id),
                original_since,
                watermark,
            ]
            continuation = ""
            if decoded_cursor is not None:
                continuation = """
                  AND (
                        turns.worker_id > ?
                     OR (
                            turns.worker_id = ?
                        AND (
                               turns.list_sequence < ?
                            OR (
                                   turns.list_sequence = ?
                               AND turns.turn_id > ?
                            )
                        )
                     )
                  )
                """
                parameters.extend(
                    [
                        decoded_cursor.worker_id,
                        decoded_cursor.worker_id,
                        decoded_cursor.list_sequence,
                        decoded_cursor.list_sequence,
                        decoded_cursor.turn_id,
                    ]
                )
            parameters.append(limit + 1)
            rows = conn.execute(
                f"""
                SELECT
                    turns.payload_json,
                    turns.observed_at,
                    turns.turn_id,
                    turns.worker_id,
                    turns.list_sequence,
                    revisions.content_revision,
                    revisions.user_state,
                    revisions.user_char_length,
                    revisions.user_byte_length,
                    CASE WHEN revisions.user_state = 'complete'
                         THEN revisions.user_page_count ELSE 0 END,
                    CASE
                        WHEN revisions.user_state = 'complete'
                         AND revisions.user_char_length BETWEEN 1 AND ?
                        THEN revisions.user_text
                    END,
                    CASE
                        WHEN revisions.user_state != 'absent'
                         AND NOT (
                             revisions.user_state = 'complete'
                             AND revisions.user_char_length BETWEEN 1 AND ?
                         )
                        THEN substr(revisions.user_text, 1, ?)
                    END,
                    revisions.final_state,
                    revisions.final_char_length,
                    revisions.final_byte_length,
                    CASE WHEN revisions.final_state = 'complete'
                         THEN revisions.final_page_count ELSE 0 END,
                    CASE
                        WHEN revisions.final_state = 'complete'
                         AND revisions.final_char_length BETWEEN 1 AND ?
                        THEN revisions.assistant_final_text
                    END,
                    CASE
                        WHEN revisions.final_state != 'absent'
                         AND NOT (
                             revisions.final_state = 'complete'
                             AND revisions.final_char_length BETWEEN 1 AND ?
                         )
                        THEN substr(revisions.assistant_final_text, 1, ?)
                    END
                FROM turns
                LEFT JOIN turn_content_revisions AS revisions
                  ON revisions.host_id = turns.host_id
                 AND revisions.turn_id = turns.turn_id
                 AND revisions.is_current = 1
                WHERE turns.host_id = ?
                  AND turns.list_sequence > ?
                  AND turns.list_sequence <= ?
                  {continuation}
                ORDER BY
                    turns.worker_id ASC,
                    turns.list_sequence DESC,
                    turns.turn_id ASC
                LIMIT ?
                """,
                tuple(parameters),
            ).fetchall()
            if work_counters is not None:
                work_counters.list_sql_queries += 1
                work_counters.list_descriptor_rows += len(rows)
                work_counters.list_inline_chars_examined += sum(
                    len(value)
                    for row in rows
                    for value in (row[10], row[16])
                    if isinstance(value, str)
                )
                work_counters.list_preview_chars_examined += sum(
                    len(value)
                    for row in rows
                    for value in (row[11], row[17])
                    if isinstance(value, str)
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    turns: list[dict[str, Any]] = []
    positions: list[tuple[str, int, str]] = []
    observed_values: list[str] = []
    incompatible_v1 = False
    accumulated_row_bytes = 0
    more_unprocessed = False
    last_scanned_position: tuple[str, int, str] | None = None
    for row_index, row in enumerate(rows):
        if row_index >= limit:
            more_unprocessed = True
            break
        (
            payload_json,
            observed_at,
            turn_id,
            worker_id,
            list_sequence,
            revision,
            user_state,
            user_char_length,
            user_byte_length,
            user_page_count,
            user_inline,
            user_preview,
            final_state,
            final_char_length,
            final_byte_length,
            final_page_count,
            final_inline,
            final_preview,
        ) = row
        current_position = (str(worker_id), int(list_sequence), str(turn_id))
        try:
            loaded = json.loads(str(payload_json or "{}"))
        except (TypeError, json.JSONDecodeError):
            loaded = {}
        if not isinstance(loaded, Mapping):
            loaded = {}
        turn_payload = sanitize_public_mapping(loaded)
        if not turn_payload or is_internal_automation_turn_payload(turn_payload):
            last_scanned_position = current_position
            continue
        serialized = Turn.from_dict(turn_payload).to_dict()
        item = _strip_canonical_turn_payload(serialized)
        if revision is not None:
            projection = project_persisted_turn_content(
                str(revision),
                user_state=str(user_state),
                user_char_length=int(user_char_length),
                user_byte_length=int(user_byte_length),
                user_page_count=int(user_page_count),
                user_inline=user_inline,
                user_preview=user_preview,
                final_state=str(final_state),
                final_char_length=int(final_char_length),
                final_byte_length=int(final_byte_length),
                final_page_count=int(final_page_count),
                final_inline=final_inline,
                final_preview=final_preview,
            )
            fields = projection["content"]["fields"]
            incompatible_v1 = incompatible_v1 or any(
                descriptor["availability"] != "absent"
                and not descriptor["inline"]
                for descriptor in fields.values()
            )
            item.update(projection)
        else:
            legacy_user, legacy_user_state = _legacy_canonical_field(
                serialized.get("user_text")
            )
            legacy_final, legacy_final_state = _legacy_canonical_field(
                serialized.get("assistant_final_text")
            )
            if requested_schema == TURN_LIST_SCHEMA_VERSION and (
                legacy_user_state != "absent" or legacy_final_state != "absent"
            ):
                item.update(
                    project_turn_content(
                        str(turn_id),
                        legacy_user,
                        legacy_final,
                        user_state=legacy_user_state,
                        final_state=legacy_final_state,
                    )
                )
            elif requested_schema == 1:
                incompatible_v1 = incompatible_v1 or (
                    legacy_user_state == "known_incomplete"
                    or legacy_final_state == "known_incomplete"
                )
                if legacy_user_state == "complete":
                    item["user_text"] = legacy_user
                if legacy_final_state == "complete":
                    item["assistant_final_text"] = legacy_final
        if requested_schema == 1:
            item["schema_version"] = 1
            item.pop("content", None)
            item.pop("user_preview", None)
            item.pop("assistant_final_preview", None)
            item.setdefault("user_text", None)
            item.setdefault("assistant_final_text", None)
        item_bytes = len(_canonical_json(item).encode("utf-8")) + 1
        if turns and accumulated_row_bytes + item_bytes > 850_000:
            more_unprocessed = True
            break
        turns.append(item)
        positions.append((str(worker_id), int(list_sequence), str(turn_id)))
        last_scanned_position = current_position
        accumulated_row_bytes += item_bytes
        if observed_at:
            observed_values.append(str(observed_at))
    if requested_schema == 1 and incompatible_v1:
        return {
            "schema_version": 1,
            "ok": False,
            "status": "upgrade_required",
            "required_turn_schema_version": TURN_LIST_SCHEMA_VERSION,
        }
    has_more = more_unprocessed
    if has_more and last_scanned_position is not None:
        last_worker, last_sequence, last_turn = last_scanned_position
        next_cursor = turn_list_cursor(
            str(host_id),
            schema_version=requested_schema,
            limit=limit,
            since_sequence=original_since,
            watermark=watermark,
            floor_sequence=floor_sequence,
            traversal_generation=traversal_generation,
            worker_id=last_worker,
            list_sequence=last_sequence,
            turn_id=last_turn,
            store_epoch=store_epoch,
            expires_at=expires_at,
        )
    else:
        has_more = False
        next_cursor = None
    watermark_token = turn_since_token(
        str(host_id),
        schema_version=requested_schema,
        watermark=watermark,
        store_epoch=store_epoch,
    )
    backend_health = sanitize_public_value(
        [health.to_dict() for health in snapshot.backend_health]
        if snapshot is not None
        else []
    )
    if not isinstance(backend_health, list):
        backend_health = []
    payload = {
        "schema_version": requested_schema,
        "host_id": str(host_id),
        "updated_at": (
            max(observed_values)
            if observed_values
            else (snapshot.updated_at if snapshot is not None else None)
        ),
        "turns": turns,
        "backend_health": backend_health,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "as_of": watermark_token,
        "since": watermark_token,
    }
    payload["content_fingerprint"] = stable_fingerprint(
        {
            "schema_version": requested_schema,
            "host_id": str(host_id),
            "turns": turns,
            "backend_health": backend_health,
            "has_more": has_more,
            "as_of": watermark_token,
        }
    )
    while (
        len(_canonical_json(payload).encode("utf-8")) >= 1024 * 1024
        and len(turns) > 1
    ):
        turns.pop()
        last_worker, last_sequence, last_turn = positions[len(turns) - 1]
        payload["has_more"] = True
        payload["next_cursor"] = turn_list_cursor(
            str(host_id),
            schema_version=requested_schema,
            limit=limit,
            since_sequence=original_since,
            watermark=watermark,
            floor_sequence=floor_sequence,
            traversal_generation=traversal_generation,
            worker_id=last_worker,
            list_sequence=last_sequence,
            turn_id=last_turn,
            store_epoch=store_epoch,
            expires_at=expires_at,
        )
        payload["content_fingerprint"] = stable_fingerprint(
            {
                "schema_version": requested_schema,
                "host_id": str(host_id),
                "turns": turns,
                "backend_health": backend_health,
                "has_more": True,
                "as_of": watermark_token,
            }
        )
    _record_response_size(work_counters, payload)
    return payload


def _bounded_utf8_blob_page(raw: bytes) -> str:
    """Decode the longest code-point-complete prefix of one bounded byte window."""
    end = len(raw)
    minimum = max(0, end - 3)
    while end >= minimum:
        try:
            return raw[:end].decode("utf-8")
        except UnicodeDecodeError as exc:
            if exc.end != end:
                raise ValueError("invalid_canonical_utf8") from None
            end -= 1
    raise ValueError("invalid_canonical_utf8")


def _ensure_turn_content_page_boundaries_conn(
    conn: sqlite3.Connection,
    *,
    rowid: int,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    field: str,
    column: str,
    total_char_length: int,
    total_byte_length: int,
    page_count: int,
    work_counters: TurnContentWorkCounters | None,
    allow_rebuild: bool = True,
) -> None:
    existing_count = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_page_boundaries
            WHERE host_id = ?
              AND turn_id = ?
              AND content_revision = ?
              AND field = ?
            """,
            (
                str(host_id),
                str(turn_id),
                str(content_revision_value),
                str(field),
            ),
        ).fetchone()[0]
        or 0
    )
    if existing_count == page_count:
        return
    if existing_count:
        raise ValueError("invalid_content_metadata")
    if not allow_rebuild:
        raise ValueError("content_not_available")
    blob = conn.blobopen(
        "turn_content_revisions",
        str(column),
        int(rowid),
        readonly=True,
    )
    try:
        if len(blob) != total_byte_length:
            raise ValueError("invalid_content_metadata")
        start_byte = 0
        start_char = 0
        page_index = 0
        while start_byte < total_byte_length:
            blob.seek(start_byte)
            raw = blob.read(
                min(
                    TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
                    total_byte_length - start_byte,
                )
            )
            text = _bounded_utf8_blob_page(raw)
            segment_byte_length = len(text.encode("utf-8"))
            segment_char_length = len(text)
            if (
                not segment_byte_length
                or segment_byte_length > TURN_CONTENT_PAGE_MAX_UTF8_BYTES
            ):
                raise ValueError("invalid_content_metadata")
            conn.execute(
                """
                INSERT INTO turn_content_page_boundaries (
                    host_id,
                    turn_id,
                    content_revision,
                    field,
                    page_index,
                    start_char,
                    start_byte
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(host_id),
                    str(turn_id),
                    str(content_revision_value),
                    str(field),
                    page_index,
                    start_char,
                    start_byte,
                ),
            )
            page_index += 1
            start_byte += segment_byte_length
            start_char += segment_char_length
            if work_counters is not None:
                work_counters.page_blob_reads += 1
                work_counters.page_bytes_examined += len(raw)
                work_counters.page_chars_examined += segment_char_length
        if (
            page_index != page_count
            or start_byte != total_byte_length
            or start_char != total_char_length
        ):
            raise ValueError("invalid_content_metadata")
    finally:
        blob.close()


def _backfill_missing_turn_content_page_boundaries_conn(
    conn: sqlite3.Connection,
) -> int:
    """Stream complete legacy fields once and persist exact non-content boundaries."""
    repaired_fields = 0
    cursor = conn.execute(
        """
        SELECT
            rowid,
            host_id,
            turn_id,
            content_revision,
            user_state,
            user_char_length,
            user_byte_length,
            user_page_count,
            final_state,
            final_char_length,
            final_byte_length,
            final_page_count
        FROM turn_content_revisions
        WHERE (user_state = 'complete' AND user_page_count > 0)
           OR (final_state = 'complete' AND final_page_count > 0)
        ORDER BY host_id, turn_id, content_revision
        """
    )
    while True:
        rows = cursor.fetchmany(64)
        if not rows:
            return repaired_fields
        for row in rows:
            fields = (
                (
                    "user_text",
                    "user_text",
                    str(row[4]),
                    int(row[5]),
                    int(row[6]),
                    int(row[7]),
                ),
                (
                    "assistant_final_text",
                    "assistant_final_text",
                    str(row[8]),
                    int(row[9]),
                    int(row[10]),
                    int(row[11]),
                ),
            )
            for (
                field,
                column,
                state,
                total_char_length,
                total_byte_length,
                page_count,
            ) in fields:
                if state != "complete" or page_count < 1:
                    continue
                existing_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM turn_content_page_boundaries
                        WHERE host_id = ?
                          AND turn_id = ?
                          AND content_revision = ?
                          AND field = ?
                        """,
                        (
                            str(row[1]),
                            str(row[2]),
                            str(row[3]),
                            field,
                        ),
                    ).fetchone()[0]
                    or 0
                )
                if existing_count == page_count:
                    continue
                if existing_count:
                    conn.execute(
                        """
                        DELETE FROM turn_content_page_boundaries
                        WHERE host_id = ?
                          AND turn_id = ?
                          AND content_revision = ?
                          AND field = ?
                        """,
                        (
                            str(row[1]),
                            str(row[2]),
                            str(row[3]),
                            field,
                        ),
                    )
                _ensure_turn_content_page_boundaries_conn(
                    conn,
                    rowid=int(row[0]),
                    host_id=str(row[1]),
                    turn_id=str(row[2]),
                    content_revision_value=str(row[3]),
                    field=field,
                    column=column,
                    total_char_length=total_char_length,
                    total_byte_length=total_byte_length,
                    page_count=page_count,
                    work_counters=None,
                )
                repaired_fields += 1


def get_turn_content(
    db_path: Path,
    host_id: str,
    *,
    turn_id: str,
    content_revision: str,
    field: str,
    cursor: str | None = None,
    schema_version: int = 1,
    work_counters: TurnContentWorkCounters | None = None,
) -> dict[str, Any]:
    """Read one bounded page directly from the canonical SQLite value."""
    if schema_version != 1:
        return {
            "schema_version": int(schema_version),
            "ok": False,
            "status": "unsupported_content_schema_version",
            "required_content_schema_version": 1,
        }
    field_columns = {
        "user_text": (1, 3, 4, 5, "user_text"),
        "assistant_final_text": (2, 6, 7, 8, "assistant_final_text"),
    }
    if field not in field_columns:
        return {"schema_version": 1, "ok": False, "status": "invalid_content_field"}
    if not _sqlite_store_exists(db_path):
        return {
            "schema_version": 1,
            "ok": False,
            "status": "content_revision_not_found",
        }
    try:
        with _connect(db_path) as conn:
            _ensure_schema(conn)
            row = conn.execute(
                """
                SELECT
                    revisions.rowid,
                    revisions.user_state,
                    revisions.final_state,
                    revisions.user_char_length,
                    revisions.user_byte_length,
                    revisions.user_page_count,
                    revisions.final_char_length,
                    revisions.final_byte_length,
                    revisions.final_page_count,
                    revisions.is_current,
                    EXISTS (
                        SELECT 1
                        FROM turn_presentation_plans AS plans
                        WHERE plans.host_id = revisions.host_id
                          AND plans.turn_id = revisions.turn_id
                          AND plans.content_revision = revisions.content_revision
                    )
                FROM turn_content_revisions AS revisions
                WHERE host_id = ? AND turn_id = ? AND content_revision = ?
                """,
                (str(host_id), str(turn_id), str(content_revision)),
            ).fetchone()
            if work_counters is not None:
                work_counters.page_sql_queries += 1
            if row is None or (not bool(row[9]) and not bool(row[10])):
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "content_revision_not_found",
                }
            state_index, char_index, byte_index, count_index, column = field_columns[field]
            availability = str(row[state_index])
            if availability == "known_incomplete":
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "content_known_incomplete",
                }
            total_char_length = int(row[char_index])
            total_byte_length = int(row[byte_index])
            count = int(row[count_index])
            if (
                availability != "complete"
                or total_char_length < 1
                or total_byte_length < 1
                or count < 1
            ):
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "content_not_available",
                }
            position = (
                ContentCursorPosition(
                    index=0,
                    segment_id=content_segment_id(content_revision, field, 0),
                    start_char=0,
                    start_byte=0,
                )
                if cursor is None
                else decode_content_cursor(
                    cursor,
                    revision=content_revision,
                    field=field,
                    count=count,
                )
            )
            _ensure_turn_content_page_boundaries_conn(
                conn,
                rowid=int(row[0]),
                host_id=str(host_id),
                turn_id=str(turn_id),
                content_revision_value=str(content_revision),
                field=str(field),
                column=column,
                total_char_length=total_char_length,
                total_byte_length=total_byte_length,
                page_count=count,
                work_counters=work_counters,
                allow_rebuild=False,
            )
            expected_boundary = conn.execute(
                """
                SELECT start_char, start_byte
                FROM turn_content_page_boundaries
                WHERE host_id = ?
                  AND turn_id = ?
                  AND content_revision = ?
                  AND field = ?
                  AND page_index = ?
                """,
                (
                    str(host_id),
                    str(turn_id),
                    str(content_revision),
                    str(field),
                    position.index,
                ),
            ).fetchone()
            if expected_boundary is None:
                raise ValueError("invalid_content_metadata")
            if (
                position.start_char != int(expected_boundary[0])
                or position.start_byte != int(expected_boundary[1])
            ):
                raise ValueError("invalid_cursor")
            blob = conn.blobopen(
                "turn_content_revisions",
                column,
                int(row[0]),
                readonly=True,
            )
            try:
                if len(blob) != total_byte_length:
                    raise ValueError("invalid_content_metadata")
                blob.seek(position.start_byte)
                raw = blob.read(
                    min(
                        TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
                        total_byte_length - position.start_byte,
                    )
                )
            finally:
                blob.close()
            text = _bounded_utf8_blob_page(raw)
            segment_byte_length = len(text.encode("utf-8"))
            segment_char_length = len(text)
            if not segment_byte_length or segment_byte_length > TURN_CONTENT_PAGE_MAX_UTF8_BYTES:
                raise ValueError("invalid_content_metadata")
            end_byte = position.start_byte + segment_byte_length
            end_char = position.start_char + segment_char_length
            has_next = position.index + 1 < count
            if has_next:
                next_boundary = conn.execute(
                    """
                    SELECT start_char, start_byte
                    FROM turn_content_page_boundaries
                    WHERE host_id = ?
                      AND turn_id = ?
                      AND content_revision = ?
                      AND field = ?
                      AND page_index = ?
                    """,
                    (
                        str(host_id),
                        str(turn_id),
                        str(content_revision),
                        str(field),
                        position.index + 1,
                    ),
                ).fetchone()
                if (
                    next_boundary is None
                    or end_char != int(next_boundary[0])
                    or end_byte != int(next_boundary[1])
                ):
                    raise ValueError("invalid_content_metadata")
            elif (
                end_byte != total_byte_length
                or end_char != total_char_length
            ):
                raise ValueError("invalid_content_metadata")
            payload = {
                "schema_version": 1,
                "turn_id": str(turn_id),
                "content_revision": str(content_revision),
                "field": field,
                "availability": "complete",
                "segment_id": position.segment_id,
                "index": position.index,
                "count": count,
                "text": text,
                "segment_char_length": segment_char_length,
                "segment_byte_length": segment_byte_length,
                "total_char_length": total_char_length,
                "total_byte_length": total_byte_length,
                "next_cursor": (
                    content_cursor(
                        content_revision,
                        field,
                        position.index + 1,
                        start_char=end_char,
                        start_byte=end_byte,
                    )
                    if has_next
                    else None
                ),
            }
            if work_counters is not None:
                work_counters.page_blob_reads += 1
                work_counters.page_bytes_examined += len(raw)
                work_counters.page_chars_examined += segment_char_length
            _record_response_size(work_counters, payload)
            return payload
    except ValueError as exc:
        status = "invalid_cursor" if str(exc) == "invalid_cursor" else "content_not_available"
        return {"schema_version": 1, "ok": False, "status": status}
    except sqlite3.Error:
        return {
            "schema_version": 1,
            "ok": False,
            "status": "content_not_available",
        }


def save_snapshot(
    db_path: Path,
    snapshot: Snapshot,
    *,
    observation: SnapshotObservationContext | None = None,
) -> None:
    """Persist a canonical snapshot and its authorized lifecycle transitions."""
    context = observation or SnapshotObservationContext()
    private_snapshot_data = _snapshot_dict(snapshot)
    created_at = _strict_utc_timestamp(private_snapshot_data.get("updated_at"))
    if created_at is None:
        raise ValueError("invalid snapshot updated_at")
    public_snapshot = Snapshot.from_dict(
        sanitize_public_mapping(private_snapshot_data)
    )
    data, fingerprint = _snapshot_payload(public_snapshot.to_dict())
    payload = _canonical_json(data)
    with _connect(db_path, prepare=True, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            latest = conn.execute(
                """
                SELECT id, content_fingerprint, created_at, payload
                FROM snapshots
                WHERE host_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(public_snapshot.host_id),),
            ).fetchone()
            if latest is not None and str(latest[1]) == fingerprint:
                snapshot_id = int(latest[0])
                retained_created_at = str(latest[2])
                retained_at = _strict_utc_timestamp(retained_created_at)
                retained_is_unknown = (
                    retained_at is None
                    or (
                        retained_at
                        == _LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE
                        and not _legacy_snapshot_created_at_is_authoritative(
                            retained_created_at,
                            latest[3],
                        )
                    )
                )
                refresh_current = (
                    retained_is_unknown
                    or (
                        retained_at is not None
                        and _connector_datetime(created_at)
                        >= _connector_datetime(retained_at)
                    )
                )
                if refresh_current:
                    conn.execute(
                        """
                        UPDATE snapshots
                        SET created_at = ?, content_fingerprint = ?, payload = ?
                        WHERE id = ?
                        """,
                        (
                            created_at,
                            fingerprint,
                            payload,
                            snapshot_id,
                        ),
                    )
            else:
                refresh_current = True
                cursor = conn.execute(
                    """
                    INSERT INTO snapshots (
                        host_id, created_at, content_fingerprint, payload
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        public_snapshot.host_id,
                        created_at,
                        fingerprint,
                        payload,
                    ),
                )
                snapshot_id = int(cursor.lastrowid)
                _append_snapshot_saved_event_conn(
                    conn,
                    public_snapshot,
                    snapshot_id=snapshot_id,
                    content_fingerprint=fingerprint,
                    private_snapshot_data=private_snapshot_data,
                )
            if refresh_current:
                _refresh_snapshot_projections_conn(
                    conn,
                    public_snapshot,
                    data,
                    content_fingerprint=fingerprint,
                )
            _apply_attention_observation_conn(
                conn,
                snapshot=public_snapshot,
                payload_data=data,
                content_fingerprint=fingerprint,
                observation=context,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def latest_snapshot(db_path: Path, host_id: str | None = None) -> Snapshot | None:
    """Return the latest snapshot globally, or scoped to host_id when provided."""
    if not _sqlite_store_exists(db_path):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        if host_id is None:
            row = conn.execute(
                "SELECT payload FROM snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT payload
                FROM snapshots
                WHERE host_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (host_id,),
            ).fetchone()
    if row is None:
        return None
    return Snapshot.from_dict(sanitize_public_mapping(_json_object(row[0])))


def latest_healthy_backend_snapshot(
    db_path: Path,
    host_id: str,
    *,
    backend: str,
) -> Snapshot | None:
    """Return the newest snapshot reporting a healthy named backend."""
    if not _sqlite_store_exists(db_path):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT snapshot.payload
            FROM snapshots AS snapshot
            WHERE snapshot.host_id = ?
              AND EXISTS (
                  SELECT 1
                  FROM json_each(snapshot.payload, '$.backend_health') AS health
                  WHERE json_extract(health.value, '$.name') = ?
                    AND json_extract(health.value, '$.status') = 'healthy'
              )
            ORDER BY snapshot.id DESC
            LIMIT 1
            """,
            (str(host_id), str(backend)),
        ).fetchone()
    if row is None:
        return None
    return Snapshot.from_dict(sanitize_public_mapping(_json_object(row[0])))


def _attention_rows_conn(
    conn: sqlite3.Connection,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> list[Any]:
    columns = """
        i.attention_id,
        i.source,
        i.kind,
        i.severity,
        i.status,
        i.updated_at,
        i.fingerprint,
        i.snapshot_content_fingerprint,
        i.observed_at,
        i.payload_json,
        i.first_seen_at,
        i.last_seen_at,
        i.last_changed_at,
        i.resolved_at,
        i.lifecycle_status,
        i.resolved_reason,
        i.signal_count
    """
    if not include_resolved:
        return conn.execute(
            f"""
            SELECT {columns}
            FROM attention_lifecycles l
            JOIN attention_items i
              ON i.host_id = l.host_id
             AND i.attention_id = l.current_attention_id
            WHERE l.host_id = ? AND l.lifecycle_status = 'open'
            ORDER BY i.last_changed_at DESC, i.attention_id
            """,
            (str(host_id),),
        ).fetchall()
    return conn.execute(
        f"""
        SELECT {columns}, 0 AS sort_group
        FROM attention_lifecycles l
        JOIN attention_items i
          ON i.host_id = l.host_id
         AND i.attention_id = l.current_attention_id
        WHERE l.host_id = ? AND l.lifecycle_status = 'open'
        UNION ALL
        SELECT {columns}, 1 AS sort_group
        FROM attention_items i
        WHERE i.host_id = ?
          AND i.lifecycle_status != 'open'
          AND NOT EXISTS (
              SELECT 1 FROM attention_lifecycles l
              WHERE l.host_id = i.host_id
                AND l.current_attention_id = i.attention_id
          )
        ORDER BY sort_group, last_changed_at DESC, attention_id
        """,
        (str(host_id), str(host_id)),
    ).fetchall()


def _attention_item_from_row(row: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(row[9] or "{}")
    except (TypeError, json.JSONDecodeError):
        parsed = {}
    payload = sanitize_public_mapping(parsed)
    payload.update(
        {
            "id": str(row[0] or ""),
            "source": _store_public_text(row[1], default="unknown"),
            "kind": _store_public_label(row[2]),
            "severity": str(row[3] or "info"),
            "status": str(row[4] or "unknown"),
            "updated_at": row[5],
            "fingerprint": str(row[6] or ""),
        }
    )
    payload["reason"] = _store_public_text(
        payload.get("reason"),
        default="",
        free_text=True,
    )
    return _attention_lifecycle_payload(
        payload,
        attention_id=str(row[0] or ""),
        observed_at=str(row[8] or row[11] or ""),
        first_seen_at=str(row[10] or row[8] or ""),
        last_seen_at=str(row[11] or row[8] or ""),
        last_changed_at=str(row[12] or row[8] or ""),
        resolved_at=row[13],
        lifecycle_status=str(row[14] or ATTENTION_LIFECYCLE_OPEN),
        resolved_reason=_store_public_text(
            row[15],
            default="",
            free_text=True,
        ) or None,
        signal_count=int(row[16] or 1),
    )


def list_attention_items(
    db_path: Path,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> list[dict[str, Any]]:
    """Return public-safe persisted attention items for a host."""
    if not _sqlite_store_exists(db_path):
        return []
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = _attention_rows_conn(
            conn,
            host_id,
            include_resolved=include_resolved,
        )
    return sanitize_public_value([_attention_item_from_row(row) for row in rows])


def attention_payload_from_store(
    db_path: Path,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> dict[str, Any] | None:
    """Return a public attention.list payload from lifecycle rows or snapshot fallback."""
    if not _sqlite_store_exists(db_path):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = _attention_rows_conn(
            conn,
            host_id,
            include_resolved=include_resolved,
        )
        snapshot_row = conn.execute(
            """
            SELECT payload
            FROM snapshots
            WHERE host_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(host_id),),
        ).fetchone()
        attention_row_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM attention_items WHERE host_id = ?",
                (str(host_id),),
            ).fetchone()[0]
        )
        lifecycle_row_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM attention_lifecycles WHERE host_id = ?",
                (str(host_id),),
            ).fetchone()[0]
        )

    if snapshot_row is None and not rows:
        return None

    snapshot: Snapshot | None = None
    if snapshot_row is not None:
        try:
            snapshot = Snapshot.from_dict(
                sanitize_public_mapping(_json_object(snapshot_row[0]))
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            snapshot = None

    attention = [_attention_item_from_row(row) for row in rows]
    backend_health = [health.to_dict() for health in snapshot.backend_health] if snapshot is not None else []
    updated_at = snapshot.updated_at if snapshot is not None else utc_timestamp()
    if (
        not attention
        and attention_row_count == 0
        and lifecycle_row_count == 0
        and snapshot is not None
        and snapshot.attention
    ):
        attention = []
        for signal in snapshot.attention:
            item = signal.to_dict()
            attention.append(
                _attention_lifecycle_payload(
                    item,
                    attention_id=_attention_id_from_item(item),
                    observed_at=updated_at,
                    first_seen_at=updated_at,
                    last_seen_at=updated_at,
                    last_changed_at=updated_at,
                    lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
                    signal_count=1,
                )
            )
    if snapshot is None and attention:
        updated_at = str(attention[0].get("last_seen_at") or attention[0].get("observed_at") or updated_at)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "host_id": str(host_id),
        "updated_at": updated_at,
        "attention": attention,
        "backend_health": backend_health,
    }
    payload["content_fingerprint"] = stable_fingerprint(
        {
            "schema_version": payload["schema_version"],
            "host_id": payload["host_id"],
            "attention": attention,
            "backend_health": backend_health,
        }
    )
    return sanitize_public_value(payload)


def list_hosts(db_path: Path) -> list[str]:
    """Return distinct host_ids seen in the store."""
    if not _sqlite_store_exists(db_path):
        return []
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            "SELECT DISTINCT host_id FROM snapshots ORDER BY host_id"
        ).fetchall()
    return sanitize_public_value([row[0] for row in rows])


def upsert_worker_bindings(db_path: Path, bindings: Iterable[WorkerBinding]) -> int:
    """Persist observed private worker bindings by private identity.

    The upsert key is host/backend/private_fingerprint so a moved pane or
    changed backend target updates the private routing record while preserving
    the public worker identity associated with that private Herdr identity.
    """
    binding_list = separate_duplicate_worker_bindings(
        binding if isinstance(binding, WorkerBinding) else WorkerBinding(**binding)
        for binding in bindings
    )
    if not binding_list:
        return 0
    with _connect(db_path, prepare=True) as conn:
        _ensure_schema(conn)
        conn.executemany(
            """
            INSERT INTO worker_bindings (
                host_id,
                worker_id,
                worker_fingerprint,
                backend,
                target_kind,
                target_value,
                turn_target_kind,
                turn_target_value,
                sendable,
                reason,
                observed_at,
                expires_at,
                private_fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, backend, private_fingerprint) DO UPDATE SET
                worker_id = excluded.worker_id,
                worker_fingerprint = excluded.worker_fingerprint,
                target_kind = excluded.target_kind,
                target_value = excluded.target_value,
                turn_target_kind = excluded.turn_target_kind,
                turn_target_value = excluded.turn_target_value,
                sendable = excluded.sendable,
                reason = excluded.reason,
                observed_at = excluded.observed_at,
                expires_at = excluded.expires_at
            """,
            [
                (
                    binding.host_id,
                    binding.worker_id,
                    binding.worker_fingerprint,
                    binding.backend,
                    binding.target_kind,
                    binding.target_value,
                    binding.turn_target_kind,
                    binding.turn_target_value,
                    int(binding.sendable),
                    binding.reason,
                    binding.observed_at,
                    binding.expires_at,
                    binding.private_fingerprint,
                )
                for binding in binding_list
            ],
        )
    return len(binding_list)


def list_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str | None = None,
    include_expired: bool = False,
    now: str | None = None,
) -> list[WorkerBinding]:
    """Return private worker bindings for a host, current/unexpired by default."""
    if not _sqlite_store_exists(db_path):
        return []
    current_time = now or utc_timestamp()
    clauses = ["host_id = ?"]
    params: list[Any] = [str(host_id)]
    if backend is not None:
        clauses.append("backend = ?")
        params.append(str(backend))
    if not include_expired:
        clauses.append("expires_at > ?")
        params.append(current_time)
    where = " AND ".join(clauses)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            f"""
            SELECT
                host_id,
                worker_id,
                worker_fingerprint,
                backend,
                target_kind,
                target_value,
                turn_target_kind,
                turn_target_value,
                sendable,
                reason,
                observed_at,
                expires_at,
                private_fingerprint
            FROM worker_bindings
            WHERE {where}
            ORDER BY observed_at DESC, id DESC
            """,
            params,
        ).fetchall()
    return [_worker_binding_from_row(row) for row in rows]


def resolve_worker_binding(
    db_path: Path,
    host_id: str,
    worker_id: str,
    *,
    worker_fingerprint: str | None = None,
    backend: str | None = None,
    now: str | None = None,
) -> WorkerBinding | None:
    """Resolve a single current, sendable private binding for a public worker."""
    if not _sqlite_store_exists(db_path):
        return None
    current_time = now or utc_timestamp()
    clauses = ["host_id = ?", "worker_id = ?", "sendable = 1", "expires_at > ?"]
    params: list[Any] = [str(host_id), str(worker_id), current_time]
    if worker_fingerprint:
        clauses.append("worker_fingerprint = ?")
        params.append(str(worker_fingerprint))
    if backend is not None:
        clauses.append("backend = ?")
        params.append(str(backend))
    where = " AND ".join(clauses)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            f"""
            SELECT
                host_id,
                worker_id,
                worker_fingerprint,
                backend,
                target_kind,
                target_value,
                turn_target_kind,
                turn_target_value,
                sendable,
                reason,
                observed_at,
                expires_at,
                private_fingerprint
            FROM worker_bindings
            WHERE {where}
            ORDER BY observed_at DESC, id DESC
            LIMIT 2
            """,
            params,
        ).fetchall()
    if len(rows) != 1:
        return None
    return _worker_binding_from_row(rows[0])


def expire_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str | None = None,
    worker_id: str | None = None,
    private_fingerprints: Iterable[str] | None = None,
    now: str | None = None,
    reason: str = "expired",
) -> int:
    """Mark matching private bindings expired and unsendable without deleting rows."""
    current_time = now or utc_timestamp()
    fingerprints = [str(value) for value in (private_fingerprints or [])]
    clauses = ["host_id = ?", "expires_at > ?"]
    params: list[Any] = [str(host_id), current_time]
    if backend is not None:
        clauses.append("backend = ?")
        params.append(str(backend))
    if worker_id is not None:
        clauses.append("worker_id = ?")
        params.append(str(worker_id))
    if fingerprints:
        placeholders = ",".join("?" for _ in fingerprints)
        clauses.append(f"private_fingerprint IN ({placeholders})")
        params.extend(fingerprints)
    where = " AND ".join(clauses)
    with _connect(db_path, prepare=True) as conn:
        _ensure_schema(conn)
        cursor = conn.execute(
            f"""
            UPDATE worker_bindings
            SET sendable = 0,
                reason = ?,
                expires_at = ?
            WHERE {where}
            """,
            [str(reason), current_time, *params],
        )
        return int(cursor.rowcount or 0)


def expire_stale_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str,
    current_private_fingerprints: Iterable[str],
    now: str | None = None,
    reason: str = "stale_observation",
) -> int:
    """Expire host/backend bindings absent from a fresh successful observation."""
    current_time = now or utc_timestamp()
    current = {str(value) for value in current_private_fingerprints}
    with _connect(db_path, prepare=True) as conn:
        _ensure_schema(conn)
        if current:
            placeholders = ",".join("?" for _ in current)
            cursor = conn.execute(
                f"""
                UPDATE worker_bindings
                SET sendable = 0,
                    reason = ?,
                    expires_at = ?
                WHERE host_id = ?
                  AND backend = ?
                  AND expires_at > ?
                  AND private_fingerprint NOT IN ({placeholders})
                """,
                [
                    str(reason),
                    current_time,
                    str(host_id),
                    str(backend),
                    current_time,
                    *sorted(current),
                ],
            )
        else:
            cursor = conn.execute(
                """
                UPDATE worker_bindings
                SET sendable = 0,
                    reason = ?,
                    expires_at = ?
                WHERE host_id = ?
                  AND backend = ?
                  AND expires_at > ?
                """,
                [
                    str(reason),
                    current_time,
                    str(host_id),
                    str(backend),
                    current_time,
                ],
            )
        return int(cursor.rowcount or 0)


def get_command_receipt(
    db_path: Path,
    host_id: str,
    request_id: str,
    action: str,
) -> dict[str, Any] | None:
    """Return the latest command receipt for a host/request/action key, or None."""
    if not _sqlite_store_exists(db_path):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        row = _latest_command_receipt_row(conn, host_id, request_id, action)
    if row is None:
        return None
    return _command_receipt_from_row(row)


def reserve_command_receipt(
    db_path: Path,
    host_id: str,
    request_id: str,
    action: str,
    payload_fingerprint: str,
    pending_result_json: str,
    *,
    status: str = "pending",
    request_json: str = "{}",
) -> dict[str, Any]:
    """Atomically reserve a mutating command receipt key if it is unused.

    Returns {"reserved": True, "receipt": None} when this caller owns the
    mutation attempt. If another receipt already exists for the same key, the
    existing latest receipt is returned and no new row is inserted.
    """
    conn = _connect(db_path, isolation_level=None, prepare=True)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _latest_command_receipt_row(conn, host_id, request_id, action)
        if row is not None:
            _upsert_command_audit_from_receipt_row(conn, row)
            conn.execute("COMMIT")
            return {"reserved": False, "receipt": _command_receipt_from_row(row)}
        now = utc_timestamp()
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id,
                request_id,
                action,
                payload_fingerprint,
                status,
                result_json,
                created_at,
                completed_at,
                uncertain
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(host_id),
                str(request_id),
                str(action),
                str(payload_fingerprint),
                str(status),
                str(pending_result_json),
                now,
                None,
                1,
            ),
        )
        _upsert_command_audit(
            conn,
            host_id=str(host_id),
            request_id=str(request_id),
            action=str(action),
            payload_fingerprint=str(payload_fingerprint),
            status=str(status),
            result_json=str(pending_result_json),
            created_at=now,
            reserved_at=now,
            completed_at=None,
            uncertain=True,
            request_json=str(request_json),
        )
        conn.execute("COMMIT")
        return {"reserved": True, "receipt": None}
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def save_command_receipt(
    db_path: Path,
    host_id: str,
    request_id: str,
    action: str,
    payload_fingerprint: str,
    status: str,
    result_json: str,
    *,
    uncertain: bool = False,
) -> None:
    """Persist a neutral command receipt for idempotency tracking.

    Dry-runs must not call this function. The receipt records the final or
    pending state of a mutating command so repeated requests can be detected
    and rejected instead of retried blindly.
    """
    now = utc_timestamp()
    conn = _connect(db_path, isolation_level=None, prepare=True)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT id, payload_fingerprint
            FROM command_receipts
            WHERE host_id = ? AND request_id = ? AND action = ?
            LIMIT 1
            """,
            (str(host_id), str(request_id), str(action)),
        ).fetchone()
        completed_at = None if uncertain else now
        if row is not None:
            if str(row[1]) != str(payload_fingerprint):
                raise ValueError("receipt payload fingerprint mismatch")
            conn.execute(
                """
                UPDATE command_receipts
                SET
                    status = ?,
                    result_json = ?,
                    completed_at = ?,
                    uncertain = ?
                WHERE id = ? AND payload_fingerprint = ?
                """,
                (
                    str(status),
                    str(result_json),
                    completed_at,
                    int(uncertain),
                    int(row[0]),
                    str(payload_fingerprint),
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO command_receipts (
                    host_id,
                    request_id,
                    action,
                    payload_fingerprint,
                    status,
                    result_json,
                    created_at,
                    completed_at,
                    uncertain
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(host_id),
                    str(request_id),
                    str(action),
                    str(payload_fingerprint),
                    str(status),
                    str(result_json),
                    now,
                    completed_at,
                    int(uncertain),
                ),
            )
        _upsert_command_audit(
            conn,
            host_id=str(host_id),
            request_id=str(request_id),
            action=str(action),
            payload_fingerprint=str(payload_fingerprint),
            status=str(status),
            result_json=str(result_json),
            created_at=now,
            reserved_at=now,
            completed_at=completed_at,
            uncertain=uncertain,
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def envelope_to_receipt_json(envelope: CommandEnvelope) -> str:
    """Serialize a command envelope for storage in a receipt."""
    return _canonical_json(envelope.to_dict())
