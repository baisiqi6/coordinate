from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from .db import Workspace, append_event, upsert_task_mirror
from .harness import HarnessAdapter


@dataclass(frozen=True)
class ReconcileResult:
    workspace_id: str
    project: str | None
    created: int
    updated: int
    unchanged: int
    events_created: int
    tasks: list[dict[str, Any]]
    scope: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "workspace_id": self.workspace_id,
            "project": self.project,
            "created": self.created,
            "updated": self.updated,
            "unchanged": self.unchanged,
            "events_created": self.events_created,
            "tasks": self.tasks,
        }
        if self.scope is not None:
            # Additive: only targeted reconciles carry a scope. Full reconcile
            # keeps its exact key set so existing consumers are unaffected.
            result["scope"] = self.scope
        return result


class ReconcileConflictError(ValueError):
    """Harness state attempted to overwrite coordinator-owned task identity."""


class ReconcileTaskNotFoundError(ValueError):
    """Targeted reconcile referenced a task id absent from the checklist."""


def reconcile_workspace(
    conn: sqlite3.Connection,
    workspace: Workspace,
    *,
    refresh: bool = True,
    adapter: HarnessAdapter | None = None,
    task_id: str | None = None,
) -> ReconcileResult:
    harness = adapter or HarnessAdapter(workspace)
    state = harness.refresh_state() if refresh else harness.read_state()
    checklist = harness.read_checklist()
    items = checklist.get("items", [])
    if not isinstance(items, list):
        raise ValueError("checklist must contain an items array")

    scope: dict[str, Any] | None = None
    if task_id is not None:
        # Select the target in memory before any DB mutation. Zero matches is
        # an explicit error; duplicate ids are already rejected by the full
        # checklist validator, and we never pick an arbitrary match.
        matches = [
            item for item in items if isinstance(item, dict) and item.get("id") == task_id
        ]
        if not matches:
            raise ReconcileTaskNotFoundError(
                f"task {task_id!r} not found in checklist for workspace {workspace.id!r}"
            )
        if len(matches) > 1:
            raise ReconcileConflictError(
                f"task {task_id!r} appears {len(matches)} times in checklist; "
                "refusing to pick one"
            )
        scope = {"kind": "task", "task_id": task_id}

    counts = {"created": 0, "updated": 0, "unchanged": 0}
    events_created = 0
    task_summaries: list[dict[str, Any]] = []

    if task_id is None:
        for item in items:
            if not isinstance(item, dict) or not item.get("id"):
                continue
            mirror, action, event_created = _reconcile_item(conn, workspace, item, commit=True)
            counts[action] += 1
            task_summaries.append({**mirror, "action": action})
            if event_created:
                events_created += 1

        summary_event = append_event(
            conn,
            workspace_id=workspace.id,
            event_type="reconciliation.completed",
            actor="reconciler",
            idempotency_key=f"{workspace.id}:reconcile:{_state_fingerprint({'state': state, 'items': items})}",
            payload={
                "project": state.get("project") or checklist.get("project"),
                "created": counts["created"],
                "updated": counts["updated"],
                "unchanged": counts["unchanged"],
            },
        )
        if summary_event.created:
            events_created += 1
    else:
        # Targeted mode: the target mirror and this round's task/summary
        # events share one atomic boundary (SAVEPOINT + commit=False), so any
        # conflict or event failure rolls back every mutation. The scoped
        # idempotency key pins task_id and fingerprints only state + the
        # target item; it can never collide with the full-reconcile key or a
        # different task, and replaying the same input stays idempotent.
        target = matches[0]
        conn.execute("SAVEPOINT targeted_reconcile")
        try:
            mirror, action, event_created = _reconcile_item(conn, workspace, target, commit=False)
            counts[action] += 1
            task_summaries.append({**mirror, "action": action})
            if event_created:
                events_created += 1

            summary_event = append_event(
                conn,
                workspace_id=workspace.id,
                event_type="reconciliation.completed",
                actor="reconciler",
                idempotency_key=(
                    f"{workspace.id}:reconcile:{task_id}:"
                    f"{_state_fingerprint({'state': state, 'items': [target]})}"
                ),
                payload={
                    "project": state.get("project") or checklist.get("project"),
                    "created": counts["created"],
                    "updated": counts["updated"],
                    "unchanged": counts["unchanged"],
                    "task_id": task_id,
                },
                commit=False,
            )
            if summary_event.created:
                events_created += 1

            conn.execute("RELEASE SAVEPOINT targeted_reconcile")
            conn.commit()
        except Exception:
            conn.execute("ROLLBACK TO SAVEPOINT targeted_reconcile")
            conn.execute("RELEASE SAVEPOINT targeted_reconcile")
            conn.commit()
            raise

    return ReconcileResult(
        workspace_id=workspace.id,
        project=state.get("project") or checklist.get("project"),
        created=counts["created"],
        updated=counts["updated"],
        unchanged=counts["unchanged"],
        events_created=events_created,
        tasks=task_summaries,
        scope=scope,
    )


def _reconcile_item(
    conn: sqlite3.Connection,
    workspace: Workspace,
    item: dict[str, Any],
    *,
    commit: bool,
) -> tuple[dict[str, Any], str, bool]:
    """Merge coordinator-owned identity and upsert one task mirror.

    Returns ``(mirror, action, event_created)`` where *mirror* is the
    conflict-merged form also recorded in the result summary.
    """
    mirror = task_mirror_from_item(item)
    existing = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace.id, mirror["task_id"]),
    ).fetchone()
    last_event_id = None
    if existing is not None:
        # Harness files own lifecycle fields, but PR bindings, publish
        # metadata, and event pointers are coordinator-owned. An omitted
        # harness field is not an instruction to erase those values.
        for field in ("branch", "pr"):
            trusted = existing[field]
            supplied = mirror[field]
            if trusted and supplied and supplied != trusted:
                raise ReconcileConflictError(
                    f"task {mirror['task_id']} harness {field} {supplied!r} "
                    f"conflicts with coordinator value {trusted!r}"
                )
            if supplied is None:
                mirror[field] = trusted
        try:
            existing_payload = json.loads(existing["payload_json"])
        except (json.JSONDecodeError, TypeError):
            existing_payload = {}
        if isinstance(existing_payload, dict):
            trusted_publish = existing_payload.get("publish_metadata")
            supplied_publish = mirror["payload"].get("publish_metadata")
            if (
                isinstance(trusted_publish, dict)
                and trusted_publish
                and supplied_publish is not None
                and supplied_publish != trusted_publish
            ):
                raise ReconcileConflictError(
                    f"task {mirror['task_id']} harness publish_metadata "
                    "conflicts with coordinator value"
                )
            existing_payload.update(mirror["payload"])
            if isinstance(trusted_publish, dict) and trusted_publish:
                existing_payload["publish_metadata"] = trusted_publish
            mirror["payload"] = existing_payload
        last_event_id = existing["last_event_id"]
    _, action = upsert_task_mirror(
        conn,
        workspace_id=workspace.id,
        task_id=mirror["task_id"],
        phase=mirror["phase"],
        owner=mirror["owner"],
        branch=mirror["branch"],
        pr=mirror["pr"],
        payload=mirror["payload"],
        last_event_id=last_event_id,
        commit=commit,
    )
    event_created = False
    if action in {"created", "updated"}:
        event = append_event(
            conn,
            workspace_id=workspace.id,
            event_type=f"task_mirror.{action}",
            actor="reconciler",
            task_id=mirror["task_id"],
            payload={
                "phase": mirror["phase"],
                "owner": mirror["owner"],
                "branch": mirror["branch"],
                "pr": mirror["pr"],
            },
            commit=commit,
        )
        event_created = event.created
    return mirror, action, event_created


def task_mirror_from_item(item: dict[str, Any]) -> dict[str, Any]:
    workflow = item.get("workflow") if isinstance(item.get("workflow"), dict) else {}
    artifacts = item.get("artifacts") if isinstance(item.get("artifacts"), dict) else {}
    phase = workflow.get("status") or item.get("status")
    return {
        "task_id": item["id"],
        "phase": phase,
        "owner": item.get("owner"),
        "branch": workflow.get("branch") or artifacts.get("branch"),
        "pr": artifacts.get("pr") or artifacts.get("pull_request"),
        "payload": item,
    }


def _state_fingerprint(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]
