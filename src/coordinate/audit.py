"""Read-only audit / drift detection service.

Compares coordinator state (task mirrors + events) against harness state
(checklist items) and reports drifts and mutation failures.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any

from .db import Workspace, get_workspace, list_events, list_task_mirrors, row_to_dict
from .harness import HarnessAdapter

# Event types considered successful mutations.
SUCCESSFUL_MUTATION_TYPES = frozenset({
    "assignment.requested",
    "assignment.accepted",
    "handoff.requested",
    "blocker.raised",
    "blocker.resolved",
    "closeout.requested",
    "review.completed",
    "task.done",
})

FAILED_MUTATION_TYPE = "harness.mutation_failed"
EVENT_OPERATION = {
    "assignment.requested": "assign",
    "assignment.accepted": "accept",
    "handoff.requested": "handoff",
    "blocker.raised": "blocker",
    "blocker.resolved": "unblock",
    "closeout.requested": "closeout",
    "review.completed": "review",
    "task.done": "mark-done",
}


@dataclass(frozen=True)
class AuditDrift:
    task_id: str
    kind: str  # e.g. "mirror_missing", "status_mismatch", "owner_mismatch", "harness_task_untracked"
    detail: str  # human-readable description
    coordinator: dict[str, Any] | None  # coordinator-side data
    harness: dict[str, Any] | None  # harness-side data


@dataclass(frozen=True)
class AuditReport:
    workspace_id: str
    harness_available: bool
    harness_error: str | None
    harnessctl_available: bool
    file_state_available: bool
    checklist_available: bool
    assignment_lifecycle_available: bool
    drifts: list[AuditDrift]
    mutation_failures: list[dict[str, Any]]
    summary: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "harness_available": self.harness_available,
            "harness_error": self.harness_error,
            "harnessctl_available": self.harnessctl_available,
            "file_state_available": self.file_state_available,
            "checklist_available": self.checklist_available,
            "assignment_lifecycle_available": self.assignment_lifecycle_available,
            "drifts": [
                {
                    "task_id": d.task_id,
                    "kind": d.kind,
                    "detail": d.detail,
                    "coordinator": d.coordinator,
                    "harness": d.harness,
                }
                for d in self.drifts
            ],
            "mutation_failures": self.mutation_failures,
            "summary": self.summary,
        }


def audit_workspace(
    conn: sqlite3.Connection,
    workspace_id: str,
    adapter: HarnessAdapter | None = None,
    refresh: bool = True,
) -> AuditReport:
    """Produce a read-only audit report comparing coordinator and harness state."""

    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")

    # --- 1. Coordinator-side data ---
    mirror_rows = list_task_mirrors(conn, workspace_id)
    mirrors = [row_to_dict(row) for row in mirror_rows]
    mirror_lookup: dict[str, dict[str, Any]] = {m["task_id"]: m for m in mirrors}

    event_rows = list_events(conn, workspace_id)
    events = [row_to_dict(row) for row in event_rows]

    successful_mutations = [
        e for e in events if e.get("event_type") in SUCCESSFUL_MUTATION_TYPES
    ]
    mutation_failures_raw = [
        e for e in events if e.get("event_type") == FAILED_MUTATION_TYPE
    ]

    # --- 2. Harness-side data ---
    harness_available = False
    harness_error: str | None = None
    harnessctl_available = False
    file_state_available = False
    checklist_available = False
    assignment_lifecycle_available = False
    harness_items: list[dict[str, Any]] = []
    harness_lookup: dict[str, dict[str, Any]] = {}

    try:
        harness = adapter or HarnessAdapter(workspace)
        if adapter is None:
            harnessctl_available = harness.harnessctl_available()
        else:
            harnessctl_available = True
        if refresh:
            harness.refresh_state()
        else:
            harness.read_state()
        file_state_available = True
        checklist = harness.read_checklist()
        checklist_available = True
        items = checklist.get("items", [])
        if isinstance(items, list):
            harness_items = [item for item in items if isinstance(item, dict) and item.get("id")]
        harness_lookup = {item["id"]: item for item in harness_items}
        harness_available = True
        assignment_lifecycle_available = harnessctl_available
    except Exception as exc:
        harness_error = str(exc)

    # --- 3. Drift checks ---
    drifts: list[AuditDrift] = []

    # (a) Mutation event has task_id but no task mirror.
    seen_task_ids: set[str] = set()
    for event in successful_mutations:
        tid = event.get("task_id")
        if tid and tid not in mirror_lookup and tid not in seen_task_ids:
            seen_task_ids.add(tid)
            payload = event.get("payload", {})
            drifts.append(AuditDrift(
                task_id=tid,
                kind="mirror_missing",
                detail=f"mutation event {event['event_type']} exists but no task mirror for {tid}",
                coordinator={"event_type": event["event_type"], "payload": payload},
                harness=None,
            ))

    if harness_available:
        # (b) Task mirror status vs harness status mismatch.
        for task_id, mirror in mirror_lookup.items():
            h_item = harness_lookup.get(task_id)
            if h_item is None:
                continue
            workflow = h_item.get("workflow") if isinstance(h_item.get("workflow"), dict) else {}
            h_workflow_status = workflow.get("status")
            h_coarse_status = h_item.get("status")
            mirror_phase = mirror.get("phase")
            if (
                mirror_phase is not None
                and h_workflow_status is not None
                and not _phase_matches_harness(mirror_phase, h_workflow_status, h_coarse_status)
            ):
                drifts.append(AuditDrift(
                    task_id=task_id,
                    kind="status_mismatch",
                    detail=f"coordinator phase '{mirror_phase}' vs harness status '{h_workflow_status}'",
                    coordinator={"phase": mirror_phase},
                    harness={"workflow_status": h_workflow_status, "status": h_coarse_status},
                ))

        # (c) Task mirror owner vs harness owner mismatch.
        for task_id, mirror in mirror_lookup.items():
            h_item = harness_lookup.get(task_id)
            if h_item is None:
                continue
            mirror_owner = mirror.get("owner")
            h_owner = h_item.get("owner")
            if (
                mirror_owner is not None
                and h_owner is not None
                and mirror_owner != h_owner
            ):
                drifts.append(AuditDrift(
                    task_id=task_id,
                    kind="owner_mismatch",
                    detail=f"coordinator owner '{mirror_owner}' vs harness owner '{h_owner}'",
                    coordinator={"owner": mirror_owner},
                    harness={"owner": h_owner},
                ))

        # (d) Harness task has no task mirror.
        for task_id, h_item in harness_lookup.items():
            if task_id not in mirror_lookup:
                workflow = h_item.get("workflow") if isinstance(h_item.get("workflow"), dict) else {}
                drifts.append(AuditDrift(
                    task_id=task_id,
                    kind="harness_task_untracked",
                    detail=f"harness task {task_id} has no coordinator task mirror",
                    coordinator=None,
                    harness={
                        "workflow_status": workflow.get("status"),
                        "status": h_item.get("status"),
                        "owner": h_item.get("owner"),
                    },
                ))

    # --- 4. Mutation failures ---
    mutation_failures: list[dict[str, Any]] = []
    for event in mutation_failures_raw:
        if _failure_was_later_resolved(event, successful_mutations):
            continue
        payload = event.get("payload", {})
        mutation_failures.append({
            "event_id": event.get("id"),
            "task_id": event.get("task_id"),
            "payload": payload,
            "created_at": event.get("created_at"),
        })

    # --- 5. Summary ---
    summary = {
        "mirrors": len(mirrors),
        "harness_tasks": len(harness_items) if harness_available else 0,
        "drifts": len(drifts),
        "mutation_failures": len(mutation_failures),
    }

    return AuditReport(
        workspace_id=workspace_id,
        harness_available=harness_available,
        harness_error=harness_error,
        harnessctl_available=harnessctl_available,
        file_state_available=file_state_available,
        checklist_available=checklist_available,
        assignment_lifecycle_available=assignment_lifecycle_available,
        drifts=drifts,
        mutation_failures=mutation_failures,
        summary=summary,
    )


def _mutation_operation(event: dict[str, Any]) -> str | None:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    mutation = payload.get("mutation")
    if isinstance(mutation, dict) and mutation.get("operation"):
        return str(mutation["operation"])
    if payload.get("operation"):
        return str(payload["operation"])
    event_type = event.get("event_type")
    return EVENT_OPERATION.get(str(event_type))


def _phase_matches_harness(
    mirror_phase: str,
    workflow_status: str | None,
    coarse_status: str | None,
) -> bool:
    if mirror_phase in {workflow_status, coarse_status}:
        return True
    if mirror_phase in {"ready", "planned"} and "todo" in {workflow_status, coarse_status}:
        return True
    return False


def _failure_was_later_resolved(
    failure: dict[str, Any],
    successful_mutations: list[dict[str, Any]],
) -> bool:
    failure_task = failure.get("task_id")
    failure_created = failure.get("created_at") or ""
    failure_payload = failure.get("payload")
    if not isinstance(failure_payload, dict):
        failure_payload = {}
    failure_operation = _mutation_operation(failure)
    failure_owner = failure_payload.get("owner") or failure.get("actor")

    for success in successful_mutations:
        if success.get("task_id") != failure_task:
            continue
        if (success.get("created_at") or "") <= failure_created:
            continue
        if success.get("event_type") == "task.done":
            return True
        if failure_operation and _mutation_operation(success) != failure_operation:
            continue
        success_payload = success.get("payload")
        if not isinstance(success_payload, dict):
            success_payload = {}
        success_owner = success_payload.get("owner") or success.get("actor")
        if failure_owner and success_owner and failure_owner != success_owner:
            continue
        return True
    return False
