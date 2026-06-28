"""Pure deterministic attention helpers and rules.

This module performs no I/O and depends only on stdlib and sibling core models.
"""

from __future__ import annotations

from .models import AttentionSignal, Snapshot, Worker, normalize_status


def _worker_attention_signal(
    worker: Worker,
    *,
    host_id: str,
    severity: str,
    reason: str,
    updated_at: str | None = None,
) -> AttentionSignal:
    status = normalize_status(worker.status)
    meta = {
        "worker_id": worker.id,
        "needs_human": severity in {"warning", "critical"},
    }
    if worker.space_id is not None:
        meta["space_id"] = worker.space_id
    return AttentionSignal(
        kind="worker_status",
        severity=severity,
        status=status,
        reason=reason,
        source=f"worker:{worker.id}",
        updated_at=updated_at or worker.last_seen_at,
        meta=meta,
        host_id=host_id,
    )


def attention_for_worker(
    worker: Worker,
    *,
    host_id: str = "",
    updated_at: str | None = None,
) -> list[AttentionSignal]:
    """Return deterministic attention signals for a single worker."""
    status = normalize_status(worker.status)

    if status == "failed":
        return [
            _worker_attention_signal(
                worker,
                host_id=host_id,
                severity="critical",
                reason=f"Worker {worker.name!r} reports status {status!r}",
                updated_at=updated_at,
            )
        ]

    if status in {"blocked", "warning"}:
        return [
            _worker_attention_signal(
                worker,
                host_id=host_id,
                severity="warning",
                reason=f"Worker {worker.name!r} may need attention: {status!r}",
                updated_at=updated_at,
            )
        ]

    if status in {"idle", "waiting"}:
        return [
            _worker_attention_signal(
                worker,
                host_id=host_id,
                severity="info",
                reason=f"Worker {worker.name!r} is {status!r}",
                updated_at=updated_at,
            )
        ]

    return []


def attention_from_snapshot(snapshot: Snapshot) -> list[AttentionSignal]:
    """Return deterministic attention signals for an entire snapshot."""
    signals: list[AttentionSignal] = []

    if not snapshot.workers:
        signals.append(
            AttentionSignal(
                kind="snapshot_empty",
                severity="info",
                status="unknown",
                reason="No workers observed on host",
                source="snapshot:workers",
                updated_at=snapshot.updated_at,
                host_id=snapshot.host_id,
            )
        )

    for worker in snapshot.workers:
        signals.extend(
            attention_for_worker(worker, host_id=snapshot.host_id, updated_at=snapshot.updated_at)
        )

    return sorted(signals, key=lambda signal: (signal.id, signal.fingerprint))


def update_snapshot_attention(snapshot: Snapshot) -> Snapshot:
    """Return a new snapshot with attention recomputed from its workers."""
    return Snapshot(
        host_id=snapshot.host_id,
        updated_at=snapshot.updated_at,
        spaces=list(snapshot.spaces),
        workers=list(snapshot.workers),
        attention=attention_from_snapshot(snapshot),
    )
