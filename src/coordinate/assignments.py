from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .db import append_event, get_workspace, row_to_dict
from .harness import HarnessAdapter, HarnessError, HarnessMutationResult
from .policy import attempt_delivery_for_event, resolve_delivery_intent
from .reconcile import reconcile_workspace


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _post_mutation_reconcile(conn, workspace_id, task_id):
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        return
    try:
        reconcile_workspace(conn, workspace, refresh=True, task_id=task_id)
    except Exception as exc:
        logger.warning("post-mutation reconcile failed for workspace %s: %s", workspace_id, exc)


@dataclass(frozen=True)
class AssignmentRequestResult:
    mutation: HarnessMutationResult | None
    event: dict[str, Any]
    event_created: bool
    delivery: dict[str, Any] | None
    delivery_created: bool | None
    delivery_error: str | None = None


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
        delivery = attempt_delivery_for_event(
            conn, existing["id"], workspace=workspace,
            platform=platform, destination=destination,
        )
        return AssignmentRequestResult(
            mutation=None,
            event=event_dict,
            event_created=False,
            delivery=delivery.delivery,
            delivery_created=delivery.delivery_created,
            delivery_error=delivery.delivery_error,
        )

    existing_failed = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (failed_key,)
    ).fetchone()
    if existing_failed is not None:
        workspace = get_workspace(conn, workspace_id)
        delivery = attempt_delivery_for_event(
            conn, existing_failed["id"], workspace=workspace,
            platform=platform, destination=destination,
        )
        return AssignmentRequestResult(
            mutation=None,
            event=row_to_dict(existing_failed),
            event_created=False,
            delivery=delivery.delivery,
            delivery_created=delivery.delivery_created,
            delivery_error=delivery.delivery_error,
        )

    if adapter is None:
        workspace = get_workspace(conn, workspace_id)
        if workspace is None:
            raise ValueError(f"unknown workspace: {workspace_id}")
        adapter = HarnessAdapter(workspace)

    workspace = adapter.workspace

    # Prevalidate the delivery intent BEFORE the first authority mutation:
    # a policy-known unsupported platform must fail closed with zero
    # adapter/mutation/event/delivery writes. A missing effective value is a
    # normal skip and does not validate the other value.
    resolve_delivery_intent(
        platform=platform,
        destination=destination,
        default_bus=workspace.default_bus,
        default_destination=workspace.default_destination,
    )

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
            _post_mutation_reconcile(conn, workspace_id, task_id)
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
    delivery = attempt_delivery_for_event(
        conn, event_result.row["id"], workspace=workspace,
        platform=platform, destination=destination,
    )
    return AssignmentRequestResult(
        mutation=mutation,
        event=event_dict,
        event_created=event_result.created,
        delivery=delivery.delivery,
        delivery_created=delivery.delivery_created,
        delivery_error=delivery.delivery_error,
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
    delivery = attempt_delivery_for_event(
        conn, event_result.row["id"], workspace=workspace,
        platform=platform, destination=destination,
    )
    return AssignmentRequestResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
        delivery=delivery.delivery,
        delivery_created=delivery.delivery_created,
        delivery_error=delivery.delivery_error,
    )


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
