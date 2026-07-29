from __future__ import annotations

import json
import re
from dataclasses import dataclass

from coordinate.db import (
    append_event,
    get_workspace,
    row_to_dict,
    upsert_task_mirror,
)


@dataclass(frozen=True)
class BranchAllocationResult:
    workspace_id: str
    task_id: str
    branch: str
    owner: str
    event: dict
    event_created: bool
    existing: bool


def _sanitize_component(value: str) -> str:
    """Sanitize a string for use in a branch name component.

    Lowercases, replaces any character not in [a-z0-9.-] with '-',
    collapses consecutive dashes, and strips leading/trailing dashes.
    """
    lowered = value.lower()
    sanitized = re.sub(r"[^a-z0-9.\-]", "-", lowered)
    sanitized = re.sub(r"-{2,}", "-", sanitized)
    sanitized = sanitized.strip("-")
    return sanitized


def generate_branch_name(workspace, task_id: str, owner: str) -> str:
    """Generate a branch name from workspace config, task_id, and owner.

    Pure function -- no I/O.
    """
    sanitized_owner = _sanitize_component(owner)
    if not sanitized_owner:
        sanitized_owner = "agent"

    sanitized_task = _sanitize_component(task_id)
    if not sanitized_task:
        sanitized_task = "task"

    namespace = (workspace.branch_namespace or "").strip("/")
    if namespace:
        return f"{namespace}/{sanitized_owner}/{sanitized_task}"
    return f"{sanitized_owner}/{sanitized_task}"


def allocate_branch(
    conn,
    workspace_id: str,
    task_id: str,
    owner: str | None = None,
    actor: str = "operator",
) -> BranchAllocationResult:
    """Allocate a branch for a task within a workspace.

    Follows the core algorithm:
    1. Resolve workspace (ValueError if missing).
    2. Check for existing task mirror.
    3. Resolve owner via fallback chain: explicit -> mirror -> "agent".
    4. Generate branch name.
    5. Idempotent path if mirror already has this branch.
    6. ValueError if mirror has a different branch.
    7. ValueError if another task in the workspace already holds this branch.
    8. Append event and upsert mirror.
    """
    # Step 1: resolve workspace
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")

    # Step 2: query existing task mirror
    existing_row = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()

    # Step 3: resolve owner (raw fallback chain, then sanitize)
    raw_owner: str
    if owner is not None:
        raw_owner = owner
    elif existing_row is not None and existing_row["owner"] is not None:
        raw_owner = existing_row["owner"]
    else:
        raw_owner = "agent"

    resolved_owner = _sanitize_component(raw_owner)
    if not resolved_owner:
        resolved_owner = "agent"

    # Step 4: generate branch name
    branch = generate_branch_name(workspace, task_id, resolved_owner)

    # Idempotency key
    idempotency_key = f"{workspace_id}:branch:{task_id}:{branch}"

    # Event payload
    payload = {
        "task_id": task_id,
        "owner": resolved_owner,
        "branch": branch,
        "base_branch": workspace.base_branch,
        "branch_namespace": workspace.branch_namespace,
    }

    # Step 5: idempotent path -- mirror already has this branch
    if existing_row is not None and existing_row["branch"] == branch:
        event_result = append_event(
            conn,
            event_type="branch.allocated",
            actor=actor,
            workspace_id=workspace_id,
            task_id=task_id,
            idempotency_key=idempotency_key,
            payload=payload,
        )
        event_dict = row_to_dict(event_result.row)

        # Still upsert mirror for crash recovery (event written but mirror not updated)
        existing_payload = json.loads(existing_row["payload_json"]) if existing_row["payload_json"] else None
        upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=existing_row["phase"],
            owner=resolved_owner,
            branch=branch,
            pr=existing_row["pr"],
            payload=existing_payload,
            last_event_id=event_result.row["id"],
        )

        return BranchAllocationResult(
            workspace_id=workspace_id,
            task_id=task_id,
            branch=branch,
            owner=resolved_owner,
            event=event_dict,
            event_created=event_result.created,
            existing=True,
        )

    # Step 6: existing task has a different branch -> error
    if existing_row is not None and existing_row["branch"] is not None and existing_row["branch"] != branch:
        existing_branch = existing_row["branch"]
        raise ValueError(
            f"task {task_id} already has branch '{existing_branch}', cannot reallocate to '{branch}'"
        )

    # Step 7: check conflict -- another active task already holds this branch
    conflict_row = conn.execute(
        "SELECT task_id FROM tasks WHERE workspace_id = ? AND branch = ? "
        "AND task_id != ? AND phase IS NOT 'closed'",
        (workspace_id, branch, task_id),
    ).fetchone()
    if conflict_row is not None:
        conflict_task_id = conflict_row["task_id"]
        raise ValueError(
            f"branch '{branch}' already allocated to task {conflict_task_id} in workspace {workspace_id}"
        )

    # Step 8: append event
    event_result = append_event(
        conn,
        event_type="branch.allocated",
        actor=actor,
        workspace_id=workspace_id,
        task_id=task_id,
        idempotency_key=idempotency_key,
        payload=payload,
    )
    event_dict = row_to_dict(event_result.row)

    # Step 9: upsert task mirror, preserving existing fields if present
    if existing_row is not None:
        existing_payload = json.loads(existing_row["payload_json"]) if existing_row["payload_json"] else None
        upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=existing_row["phase"],
            owner=resolved_owner,
            branch=branch,
            pr=existing_row["pr"],
            payload=existing_payload,
            last_event_id=event_result.row["id"],
        )
    else:
        upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=None,
            owner=resolved_owner,
            branch=branch,
            pr=None,
            payload=None,
            last_event_id=event_result.row["id"],
        )

    return BranchAllocationResult(
        workspace_id=workspace_id,
        task_id=task_id,
        branch=branch,
        owner=resolved_owner,
        event=event_dict,
        event_created=event_result.created,
        existing=False,
    )
