"""Operator-facing pending-action inference from task mirrors and events."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .db import (
    get_task_mirror, get_workspace, latest_task_assignment, latest_task_decision,
    list_task_mirrors, list_task_recovery_events, row_to_dict,
)


def build_task_summary(
    conn: sqlite3.Connection, workspace_id: str, task_id: str,
) -> dict[str, Any]:
    """Recover one manual task from stored facts; never refresh or mutate it."""
    if get_workspace(conn, workspace_id) is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    row = get_task_mirror(conn, workspace_id, task_id)
    if row is None:
        raise ValueError(f"unknown task: {workspace_id}/{task_id}")
    task = row_to_dict(row)
    payload = task.get("payload") if isinstance(task.get("payload"), dict) else {}
    workflow = payload.get("workflow") if isinstance(payload.get("workflow"), dict) else {}
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    lease = payload.get("lease") if isinstance(payload.get("lease"), dict) else {}
    lease_state = _recovery_lease_state(lease)
    last_executor = _role(lease.get("owner"), "task_mirror.lease.owner")
    assignment = latest_task_assignment(conn, workspace_id, task_id)
    if not last_executor["value"] and assignment is not None:
        assignment = row_to_dict(assignment)
        assigned_payload = assignment.get("payload") or {}
        last_executor = _role(assigned_payload.get("owner") or assignment.get("target"), assignment["id"])
    plan = next((_text(value) for value in (
        payload.get("plan_doc"), payload.get("plan_path"), artifacts.get("plan"),
    ) if _text(value)), None)
    events = list_task_recovery_events(conn, workspace_id, task_id)
    feedback = latest_task_decision(conn, workspace_id, task_id)
    latest = events[0] if events else None
    blocked = task["phase"] == "blocked"
    return {
        "workspace_id": workspace_id, "task_id": task_id, "phase": task["phase"],
        "owner": task["owner"], "lease_state": lease_state,
        "roles": {
            "current_executor": _role(lease.get("owner") if lease_state == "active" else None,
                                      "task_mirror.lease.owner"),
            "last_executor": last_executor,
            "unblock_owner": _role(workflow.get("unblock_owner") if blocked else None,
                                   "task_mirror.workflow.unblock_owner"),
            "responsibility": {"state": "unknown", "operator": None,
                               "evidence_locator": plan or _text(artifacts.get("blocker_packet"))},
        },
        "blocker": {"reason": _text(payload.get("blocked_reason"), 500) if blocked else None,
                    "evidence_locator": _text(artifacts.get("blocker_packet")) if blocked else None},
        "latest_feedback": _recovery_event(feedback) if feedback else None,
        "recent_events": [_recovery_event(event) for event in events[:20]],
        "snapshot": {
            "source": "task_mirror+event_ledger", "harness_refreshed": False,
            "ledger_included": True, "mirror_may_be_stale": True,
            "mirror_updated_at": task["updated_at"], "event_limit": 20,
            "events_truncated": len(events) > 20,
            "latest_task_event_id": latest["id"] if latest else None,
            "latest_task_event_rowid": latest["event_rowid"] if latest else None,
            "latest_task_event_created_at": latest["created_at"] if latest else None,
        },
        "warnings": ["Responsibility is unknown; read the original plan/assignment before dispatching again.",
                     "An empty pending list or null owner does not prove completion or absence of responsibility."],
    }


def _text(value: Any, limit: int = 512) -> str | None:
    return value[:limit] if isinstance(value, str) and value.strip() else None


def _role(value: Any, source: str) -> dict[str, Any]:
    value = _text(value, 256)
    return {"value": value, "source": source if value else None}


def _recovery_lease_state(lease: dict[str, Any]) -> str:
    if not lease:
        return "absent"
    if lease.get("released_at"):
        return "released"
    try:
        expires = datetime.fromisoformat(lease["expires_at"].replace("Z", "+00:00"))
        if expires.tzinfo is not None:
            return "active" if expires > datetime.now(timezone.utc) else "expired"
    except (KeyError, TypeError, ValueError, AttributeError):
        pass
    return "unknown"


def _recovery_event(row: sqlite3.Row) -> dict[str, Any]:
    """Project declared feedback fields, never arbitrary provider payloads."""
    event = row_to_dict(row)
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    declared = payload.get("decision")
    known = isinstance(declared, str) and declared in {
        "approve", "approved", "changes_requested", "reject", "rejected", "blocked",
    }
    summary = next((value for key in ("summary", "result_summary", "reason")
                    if isinstance(value := payload.get(key), str) and value.strip()), None)
    locators = []
    for key in ("evidence_locator", "evidence_locators", "artifacts"):
        value = payload.get(key)
        locators.extend([value] if isinstance(value, str) else value if isinstance(value, list) else [])
    locators = [value for value in locators if isinstance(value, str) and value.strip()]
    return {
        "id": event["id"], "rowid": event["event_rowid"], "event_type": event["event_type"],
        "created_at": event["created_at"], "actor": _text(event.get("actor"), 256),
        "decision": declared if known else "unknown", "decision_known": known,
        "decision_source": "payload.decision" if "decision" in payload else None,
        "summary": _text(summary, 500), "evidence_locators": [value[:512] for value in locators[:8]],
        "truncated": bool(summary and len(summary) > 500) or len(locators) > 8
                     or any(len(value) > 512 for value in locators[:8]),
    }


@dataclass(frozen=True)
class PendingAction:
    task_id: str
    phase: str
    owner: str | None
    action: str          # "approve_plan" | "review_code" | "handoff" | "mark_done"
    reason: str
    latest_event_type: str | None
    latest_event_summary: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "phase": self.phase,
            "owner": self.owner,
            "action": self.action,
            "reason": self.reason,
            "latest_event_type": self.latest_event_type,
            "latest_event_summary": self.latest_event_summary,
        }


def list_pending_actions(
    conn: sqlite3.Connection,
    workspace_id: str,
) -> list[PendingAction]:
    """Return operator-pending actions for a workspace.

    Task phase remains a harness projection; Operator attention is event-derived.
    """
    tasks = list_task_mirrors(conn, workspace_id=workspace_id)
    results: list[PendingAction] = []

    for task_row in tasks:
        task = row_to_dict(task_row)
        task_id = task["task_id"]
        phase = task.get("phase")
        owner = task.get("owner")

        if phase in {"done", "closed", "released"}:
            continue
        if _has_task_done_event(conn, workspace_id, task_id):
            continue

        latest = _latest_relevant_event(conn, workspace_id, task_id)
        latest_type = latest["event_type"] if latest else None
        latest_payload = (latest.get("payload") or {}) if latest else {}
        latest_action = latest_payload.get("action")
        latest_decision = latest_payload.get("decision")
        latest_summary = _event_summary(latest)

        action = _infer_action(
            phase,
            owner,
            latest_type,
            latest_action,
            latest_decision,
        )
        if action is None:
            continue

        results.append(PendingAction(
            task_id=task_id,
            phase=phase or "unknown",
            owner=owner,
            action=action,
            reason=_reason_for(phase, owner, latest_type, action),
            latest_event_type=latest_type,
            latest_event_summary=latest_summary,
        ))

    return results


# -- inference rules ------------------------------------------------

def _infer_action(
    phase: str | None,
    owner: str | None,
    latest_event_type: str | None,
    latest_action: str | None = None,
    latest_decision: str | None = None,
) -> str | None:
    if latest_event_type == "task.done":
        return None
    if latest_event_type == "plan.review_requested" and phase in {"planned", "ready"}:
        return "approve_plan"
    if latest_event_type == "plan.rejected":
        return None
    if latest_event_type == "plan.approved":
        if phase in {"planned", "ready"} and not owner:
            return "handoff"
        return None
    if latest_event_type in {"closeout.requested", "review.rejected"}:
        return None
    if latest_event_type == "review.completed":
        if (latest_decision or "").lower() in {"approve", "approved"}:
            return "mark_done"
        return None
    if (
        phase in {"implementing", "running", "accepted", "awaiting_operator"}
        and latest_event_type == "agent.reported"
        and latest_action == "done"
    ):
        return "review_code"
    if phase == "ready" and not owner:
        return "handoff"
    return None


def _reason_for(
    phase: str | None,
    owner: str | None,
    latest_event_type: str | None,
    action: str,
) -> str:
    if action == "mark_done":
        return f"review approved → operator should mark-done (phase={phase})"
    if action == "approve_plan":
        return f"plan.review_requested pending → operator should approve or reject plan (phase={phase})"
    if action == "review_code":
        return f"agent.reported done → operator should review code and closeout (phase={phase})"
    if action == "handoff":
        return f"task ready with no owner → operator should handoff (phase={phase})"
    return f"operator action required (phase={phase}, event={latest_event_type})"


# -- event helpers --------------------------------------------------

_RELEVANT_EVENT_TYPES = (
    "agent.reported",
    "plan.review_requested",
    "plan.approved",
    "plan.rejected",
    "closeout.requested",
    "review.completed",
    "review.rejected",
    "task.done",
)


def _has_task_done_event(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM events
        WHERE workspace_id = ? AND task_id = ? AND event_type = 'task.done'
        LIMIT 1
        """,
        (workspace_id, task_id),
    ).fetchone()
    return row is not None


def _latest_relevant_event(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
) -> dict[str, Any] | None:
    placeholders = ",".join("?" for _ in _RELEVANT_EVENT_TYPES)
    row = conn.execute(
        f"""
        SELECT rowid AS event_rowid, * FROM events
        WHERE workspace_id = ?
          AND task_id = ?
          AND event_type IN ({placeholders})
        ORDER BY created_at DESC, rowid DESC
        LIMIT 1
        """,
        (workspace_id, task_id, *_RELEVANT_EVENT_TYPES),
    ).fetchone()
    return row_to_dict(row) if row else None


def pending_snapshot_metadata(
    conn: sqlite3.Connection,
    workspace_id: str,
) -> dict[str, Any]:
    """Describe the non-refreshing DB snapshot used by ``operator pending``."""
    task_row = conn.execute(
        "SELECT MAX(updated_at) AS updated_at FROM tasks WHERE workspace_id = ?",
        (workspace_id,),
    ).fetchone()
    event_row = conn.execute(
        """
        SELECT rowid AS event_rowid, id, created_at
        FROM events
        WHERE workspace_id = ?
        ORDER BY rowid DESC
        LIMIT 1
        """,
        (workspace_id,),
    ).fetchone()
    return {
        "source": "task_mirror+event_ledger",
        "harness_refreshed": False,
        "may_be_stale": True,
        "task_mirror_updated_at": task_row["updated_at"] if task_row else None,
        "latest_event_rowid": event_row["event_rowid"] if event_row else None,
        "latest_event_id": event_row["id"] if event_row else None,
        "latest_event_created_at": event_row["created_at"] if event_row else None,
    }


def _event_summary(event: dict[str, Any] | None) -> str | None:
    if event is None:
        return None
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    return (
        payload.get("summary")
        or payload.get("result_summary")
        or payload.get("reason")
        or payload.get("action")
    )
