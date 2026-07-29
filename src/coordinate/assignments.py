from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .db import append_event, get_workspace, row_to_dict
from .harness import HarnessAdapter, HarnessError, HarnessMutationResult
from .policy import create_delivery_for_event
from .reconcile import reconcile_workspace


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _post_mutation_reconcile(conn, workspace_id):
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        return
    try:
        reconcile_workspace(conn, workspace, refresh=True)
    except Exception as exc:
        logger.warning("post-mutation reconcile failed for workspace %s: %s", workspace_id, exc)


@dataclass(frozen=True)
class AssignmentRequestResult:
    mutation: HarnessMutationResult | None
    event: dict[str, Any]
    event_created: bool
    delivery: dict[str, Any] | None
    delivery_created: bool | None


def request_assignment(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    owner: str,
    session: str,
    actor: str = "operator",
    branch: str | None = None,
    platform: str | None = None,
    destination: str | None = None,
    adapter: HarnessAdapter | None = None,
    idempotency_hint: str | None = None,
) -> AssignmentRequestResult:
    hint = idempotency_hint or f"{workspace_id}:assign:{task_id}:{owner}:{session}"
    success_key = f"{hint}:assignment.requested"
    failed_key = f"{hint}:harness.mutation_failed"

    existing = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (success_key,)
    ).fetchone()
    if existing is not None:
        workspace = get_workspace(conn, workspace_id)
        event_dict = row_to_dict(existing)
        delivery_dict, delivery_created = _try_create_delivery(
            conn, existing["id"], workspace, platform, destination
        )
        return AssignmentRequestResult(
            mutation=None,
            event=event_dict,
            event_created=False,
            delivery=delivery_dict,
            delivery_created=delivery_created,
        )

    existing_failed = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (failed_key,)
    ).fetchone()
    if existing_failed is not None:
        workspace = get_workspace(conn, workspace_id)
        delivery_dict, delivery_created = _try_create_delivery(
            conn, existing_failed["id"], workspace, platform, destination
        )
        return AssignmentRequestResult(
            mutation=None,
            event=row_to_dict(existing_failed),
            event_created=False,
            delivery=delivery_dict,
            delivery_created=delivery_created,
        )

    if adapter is None:
        workspace = get_workspace(conn, workspace_id)
        if workspace is None:
            raise ValueError(f"unknown workspace: {workspace_id}")
        adapter = HarnessAdapter(workspace)

    workspace = adapter.workspace

    args = [owner, session, "--actor", actor]
    if branch:
        args.extend(["--branch", branch])

    try:
        mutation = adapter.run_mutation(
            operation="assign",
            task_id=task_id,
            actor=actor,
            args=args,
            idempotency_hint=hint,
        )
    except (HarnessError, OSError) as exc:
        mutation = _failed_mutation_result(
            operation="assign",
            task_id=task_id,
            actor=actor,
            idempotency_hint=hint,
            stderr=str(exc),
        )

    if mutation.success:
        result = _handle_success(
            conn, workspace_id, task_id, owner, session, branch,
            actor, mutation, success_key, workspace, platform, destination,
        )
        if result.event_created:
            _post_mutation_reconcile(conn, workspace_id)
        return result

    return _handle_failure(
        conn, workspace_id, task_id, owner, session, branch,
        actor, mutation, failed_key, workspace, platform, destination,
    )


def _handle_success(
    conn, workspace_id, task_id, owner, session, branch,
    actor, mutation, success_key, workspace, platform, destination,
):
    payload = {
        "task_id": task_id,
        "owner": owner,
        "session": session,
        "branch": branch,
        "mutation": mutation.to_dict(),
    }
    event_result = append_event(
        conn,
        event_type="assignment.requested",
        actor=actor,
        workspace_id=workspace_id,
        target=owner,
        task_id=task_id,
        idempotency_key=success_key,
        payload=payload,
    )
    event_dict = row_to_dict(event_result.row)
    delivery_dict, delivery_created = _try_create_delivery(
        conn, event_result.row["id"], workspace, platform, destination
    )
    return AssignmentRequestResult(
        mutation=mutation,
        event=event_dict,
        event_created=event_result.created,
        delivery=delivery_dict,
        delivery_created=delivery_created,
    )


def _handle_failure(
    conn, workspace_id, task_id, owner, session, branch,
    actor, mutation, failed_key, workspace, platform, destination,
):
    payload = {
        "operation": mutation.operation,
        "task_id": task_id,
        "owner": owner,
        "session": session,
        "branch": branch,
        "mutation": mutation.to_dict(),
        "stderr": mutation.stderr,
        "exit_code": mutation.exit_code,
    }
    event_result = append_event(
        conn,
        event_type="harness.mutation_failed",
        actor=actor,
        workspace_id=workspace_id,
        target=owner,
        task_id=task_id,
        idempotency_key=failed_key,
        payload=payload,
    )
    delivery_dict, delivery_created = _try_create_delivery(
        conn, event_result.row["id"], workspace, platform, destination
    )
    return AssignmentRequestResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
        delivery=delivery_dict,
        delivery_created=delivery_created,
    )


def _try_create_delivery(
    conn: sqlite3.Connection,
    event_id: str,
    workspace: Any,
    platform: str | None,
    destination: str | None,
) -> tuple[dict[str, Any] | None, bool | None]:
    effective_platform = platform or (workspace.default_bus if workspace else None)
    effective_destination = destination or (workspace.default_destination if workspace else None)
    if not effective_platform or not effective_destination:
        return None, None
    result = create_delivery_for_event(
        conn, event_id, platform=effective_platform, destination=effective_destination
    )
    return result.delivery, result.created


def _failed_mutation_result(
    *,
    operation: str,
    task_id: str,
    actor: str,
    idempotency_hint: str,
    stderr: str,
) -> HarnessMutationResult:
    timestamp = datetime.now(timezone.utc).isoformat()
    return HarnessMutationResult(
        operation=operation,
        task_id=task_id,
        actor=actor,
        idempotency_hint=idempotency_hint,
        started_at=timestamp,
        completed_at=timestamp,
        command=[],
        exit_code=1,
        stdout="",
        stderr=stderr,
        success=False,
    )
