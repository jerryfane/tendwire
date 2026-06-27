"""Pure deterministic attention helpers and rules.

This module performs no I/O and depends only on stdlib and sibling core models.
"""

from __future__ import annotations

import uuid
from typing import Iterable

from .models import AttentionSignal, Snapshot, Worker


def attention_for_worker(worker: Worker) -> list[AttentionSignal]:
    """Return deterministic attention signals for a single worker."""
    signals: list[AttentionSignal] = []
    status = worker.status.lower()

    if status in {"error", "failed", "crashed", "panic"}:
        signals.append(
            AttentionSignal(
                id=f"attn-{uuid.uuid4()}",
                level="critical",
                reason=f"Worker {worker.name!r} reports status {worker.status!r}",
                source=f"worker:{worker.id}",
            )
        )
    elif status in {"warn", "warning", "stalled", "blocked"}:
        signals.append(
            AttentionSignal(
                id=f"attn-{uuid.uuid4()}",
                level="warn",
                reason=f"Worker {worker.name!r} may need attention: {worker.status!r}",
                source=f"worker:{worker.id}",
            )
        )
    elif status in {"idle", "paused", "waiting"}:
        signals.append(
            AttentionSignal(
                id=f"attn-{uuid.uuid4()}",
                level="info",
                reason=f"Worker {worker.name!r} is {worker.status!r}",
                source=f"worker:{worker.id}",
            )
        )

    return signals


def attention_from_snapshot(snapshot: Snapshot) -> list[AttentionSignal]:
    """Return deterministic attention signals for an entire snapshot."""
    signals: list[AttentionSignal] = []

    if not snapshot.workers:
        signals.append(
            AttentionSignal(
                id=f"attn-{uuid.uuid4()}",
                level="info",
                reason="No workers observed on host",
                source="snapshot:workers",
            )
        )

    for worker in snapshot.workers:
        signals.extend(attention_for_worker(worker))

    return signals


def update_snapshot_attention(snapshot: Snapshot) -> Snapshot:
    """Return a new snapshot with attention recomputed from its workers."""
    return Snapshot(
        host_id=snapshot.host_id,
        updated_at=snapshot.updated_at,
        spaces=list(snapshot.spaces),
        workers=list(snapshot.workers),
        attention=attention_from_snapshot(snapshot),
    )
