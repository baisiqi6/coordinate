from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from .db import (
    Workspace,
    append_event,
    get_workspace,
    row_to_dict,
    upsert_task_mirror,
)


@dataclass(frozen=True)
class PlanGateResult:
    workspace: Workspace
    task: dict[str, Any]
    event: dict[str, Any]
    event_created: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace.to_dict(),
            "task": self.task,
            "event": self.event,
            "event_created": self.event_created,
        }


def _require_workspace(conn: sqlite3.Connection, workspace_id: str) -> Workspace:
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    return workspace


def _require_task(conn: sqlite3.Connection, workspace_id: str, task_id: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()
    if row is None:
        raise ValueError(f"task mirror not found: {workspace_id}/{task_id}")
    return row_to_dict(row)


def _latest_plan_ready(
    conn: sqlite3.Connection, workspace_id: str, task_id: str
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM events WHERE workspace_id = ? AND task_id = ? AND event_type = 'plan.ready' ORDER BY rowid DESC LIMIT 1",
        (workspace_id, task_id),
    ).fetchone()
    return row_to_dict(row) if row else None


def _gate_counter(conn: sqlite3.Connection, workspace_id: str, task_id: str, event_type: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM events WHERE workspace_id = ? AND task_id = ? AND event_type = ?",
        (workspace_id, task_id, event_type),
    ).fetchone()
    return row["cnt"]


def review_request_plan(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    actor: str = "operator",
    idempotency_key: str | None = None,
) -> PlanGateResult:
    workspace = _require_workspace(conn, workspace_id)
    task = _require_task(conn, workspace_id, task_id)
    plan_event = _latest_plan_ready(conn, workspace_id, task_id)
    plan_payload = plan_event.get("payload", {}) if plan_event else {}

    payload = {
        "task_id": task_id,
        "plan_doc": plan_payload.get("plan_doc", ""),
        "title": plan_payload.get("title", task_id),
    }

    event_result = append_event(
        conn,
        workspace_id=workspace_id,
        event_type="plan.review_requested",
        actor=actor,
        target="reviewer",
        task_id=task_id,
        idempotency_key=idempotency_key or f"{workspace_id}:{task_id}:plan.review_requested",
        payload=payload,
    )

    return PlanGateResult(
        workspace=workspace,
        task=task,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


def approve_plan(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    scope: str,
    reviewer: str | None = None,
    notes: str | None = None,
    actor: str = "operator",
    idempotency_key: str | None = None,
) -> PlanGateResult:
    workspace = _require_workspace(conn, workspace_id)
    task = _require_task(conn, workspace_id, task_id)
    plan_event = _latest_plan_ready(conn, workspace_id, task_id)
    plan_payload = plan_event.get("payload", {}) if plan_event else {}

    reject_count = _gate_counter(conn, workspace_id, task_id, "plan.rejected")
    resolved_key = idempotency_key or f"{workspace_id}:{task_id}:plan.approved:{scope}:after_{reject_count}_rejects"

    payload = {
        "task_id": task_id,
        "reviewer": reviewer,
        "decision": "approved",
        "scope": scope,
        "source_plan": plan_payload.get("plan_doc") or plan_payload.get("absolute_plan_doc", ""),
        "plan_ready_event_id": plan_event["id"] if plan_event else None,
        "notes": notes,
    }

    event_result = append_event(
        conn,
        workspace_id=workspace_id,
        event_type="plan.approved",
        actor=actor,
        target="worker",
        task_id=task_id,
        idempotency_key=resolved_key,
        payload=payload,
    )

    task_row, _ = upsert_task_mirror(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        phase=task.get("phase"),
        owner=task.get("owner"),
        branch=task.get("branch"),
        pr=task.get("pr"),
        payload=task.get("payload"),
        last_event_id=(
            event_result.row["id"] if event_result.created else task.get("last_event_id")
        ),
    )

    return PlanGateResult(
        workspace=workspace,
        task=row_to_dict(task_row),
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


def reject_plan(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    scope: str,
    reviewer: str | None = None,
    reason: str | None = None,
    actor: str = "operator",
    idempotency_key: str | None = None,
) -> PlanGateResult:
    workspace = _require_workspace(conn, workspace_id)
    task = _require_task(conn, workspace_id, task_id)

    approve_count = _gate_counter(conn, workspace_id, task_id, "plan.approved")
    resolved_key = idempotency_key or f"{workspace_id}:{task_id}:plan.rejected:{scope}:after_{approve_count}_approves"

    payload = {
        "task_id": task_id,
        "reviewer": reviewer,
        "decision": "rejected",
        "scope": scope,
        "reason": reason,
    }

    event_result = append_event(
        conn,
        workspace_id=workspace_id,
        event_type="plan.rejected",
        actor=actor,
        target="worker",
        task_id=task_id,
        idempotency_key=resolved_key,
        payload=payload,
    )

    task_row, _ = upsert_task_mirror(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        phase=task.get("phase"),
        owner=task.get("owner"),
        branch=task.get("branch"),
        pr=task.get("pr"),
        payload=task.get("payload"),
        last_event_id=(
            event_result.row["id"] if event_result.created else task.get("last_event_id")
        ),
    )

    return PlanGateResult(
        workspace=workspace,
        task=row_to_dict(task_row),
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )
