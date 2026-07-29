from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .completion import (
    CompletionReceiptError,
    MarkDoneGateResult,
    ReceiptEvidence,
    check_mark_done_gate,
    compute_item_fingerprint,
    compute_mark_done_fingerprints,
)
from .db import append_event, get_workspace, row_to_dict
from .harness import HarnessAdapter, HarnessError, HarnessMutationResult
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
class AcceptTaskResult:
    mutation: HarnessMutationResult | None
    event: dict[str, Any]
    event_created: bool


def accept_task(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    owner: str,
    session: str,
    actor: str | None = None,
    branch: str | None = None,
    adapter: HarnessAdapter | None = None,
    idempotency_hint: str | None = None,
) -> AcceptTaskResult:
    effective_actor = actor or owner
    hint = idempotency_hint or f"{workspace_id}:accept:{task_id}:{owner}:{session}"
    success_key = f"{hint}:assignment.accepted"
    failed_key = f"{hint}:harness.mutation_failed"

    existing = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (success_key,)
    ).fetchone()
    if existing is not None:
        return AcceptTaskResult(
            mutation=None,
            event=row_to_dict(existing),
            event_created=False,
        )

    existing_failed = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (failed_key,)
    ).fetchone()
    if existing_failed is not None:
        return AcceptTaskResult(
            mutation=None,
            event=row_to_dict(existing_failed),
            event_created=False,
        )

    if adapter is None:
        workspace = get_workspace(conn, workspace_id)
        if workspace is None:
            raise ValueError(f"unknown workspace: {workspace_id}")
        adapter = HarnessAdapter(workspace)

    args = [owner, session]
    if branch:
        args.extend(["--branch", branch])

    try:
        mutation = adapter.run_mutation(
            operation="accept",
            task_id=task_id,
            actor=effective_actor,
            args=args,
            idempotency_hint=hint,
        )
    except (HarnessError, OSError) as exc:
        mutation = _failed_mutation_result(
            operation="accept",
            task_id=task_id,
            actor=effective_actor,
            idempotency_hint=hint,
            stderr=str(exc),
        )

    if mutation.success:
        result = _handle_success(
            conn, workspace_id, task_id, owner, session, branch,
            effective_actor, mutation, success_key,
        )
        if result.event_created:
            _post_mutation_reconcile(conn, workspace_id)
        return result

    return _handle_failure(
        conn, workspace_id, task_id, owner, session, branch,
        effective_actor, mutation, failed_key,
    )


def _handle_success(
    conn, workspace_id, task_id, owner, session, branch,
    actor, mutation, success_key,
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
        event_type="assignment.accepted",
        actor=actor,
        workspace_id=workspace_id,
        target=owner,
        task_id=task_id,
        idempotency_key=success_key,
        payload=payload,
    )
    return AcceptTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


def _handle_failure(
    conn, workspace_id, task_id, owner, session, branch,
    actor, mutation, failed_key,
):
    payload = {
        "operation": "accept",
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
    return AcceptTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


@dataclass(frozen=True)
class HandoffTaskResult:
    mutation: HarnessMutationResult | None
    event: dict[str, Any]
    event_created: bool


def handoff_task(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    target: str,
    actor: str = "operator",
    reason: str | None = None,
    adapter: HarnessAdapter | None = None,
    idempotency_hint: str | None = None,
) -> HandoffTaskResult:
    hint = idempotency_hint or f"{workspace_id}:handoff:{task_id}:{target}:{actor}"
    success_key = f"{hint}:handoff.requested"
    failed_key = f"{hint}:harness.mutation_failed"

    existing = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (success_key,)
    ).fetchone()
    if existing is not None:
        return HandoffTaskResult(
            mutation=None,
            event=row_to_dict(existing),
            event_created=False,
        )

    existing_failed = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (failed_key,)
    ).fetchone()
    if existing_failed is not None:
        return HandoffTaskResult(
            mutation=None,
            event=row_to_dict(existing_failed),
            event_created=False,
        )

    if adapter is None:
        workspace = get_workspace(conn, workspace_id)
        if workspace is None:
            raise ValueError(f"unknown workspace: {workspace_id}")
        adapter = HarnessAdapter(workspace)

    args = [target, "--actor", actor]
    if reason:
        args.extend(["--reason", reason])

    try:
        mutation = adapter.run_mutation(
            operation="handoff",
            task_id=task_id,
            actor=actor,
            args=args,
            idempotency_hint=hint,
        )
    except (HarnessError, OSError) as exc:
        mutation = _failed_mutation_result(
            operation="handoff",
            task_id=task_id,
            actor=actor,
            idempotency_hint=hint,
            stderr=str(exc),
        )

    if mutation.success:
        result = _handle_handoff_success(
            conn, workspace_id, task_id, target, reason,
            actor, mutation, success_key,
        )
        if result.event_created:
            _post_mutation_reconcile(conn, workspace_id)
        return result

    return _handle_handoff_failure(
        conn, workspace_id, task_id, target, reason,
        actor, mutation, failed_key,
    )


def _handle_handoff_success(
    conn, workspace_id, task_id, target, reason,
    actor, mutation, success_key,
):
    payload = {
        "task_id": task_id,
        "target": target,
        "reason": reason,
        "mutation": mutation.to_dict(),
    }
    event_result = append_event(
        conn,
        event_type="handoff.requested",
        actor=actor,
        workspace_id=workspace_id,
        target=target,
        task_id=task_id,
        idempotency_key=success_key,
        payload=payload,
    )
    return HandoffTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


def _handle_handoff_failure(
    conn, workspace_id, task_id, target, reason,
    actor, mutation, failed_key,
):
    payload = {
        "operation": "handoff",
        "task_id": task_id,
        "target": target,
        "reason": reason,
        "mutation": mutation.to_dict(),
        "stderr": mutation.stderr,
        "exit_code": mutation.exit_code,
    }
    event_result = append_event(
        conn,
        event_type="harness.mutation_failed",
        actor=actor,
        workspace_id=workspace_id,
        target=target,
        task_id=task_id,
        idempotency_key=failed_key,
        payload=payload,
    )
    return HandoffTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


@dataclass(frozen=True)
class BlockerTaskResult:
    mutation: HarnessMutationResult | None
    event: dict[str, Any]
    event_created: bool


def blocker_task(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    actor: str = "operator",
    reason: str | None = None,
    adapter: HarnessAdapter | None = None,
    idempotency_hint: str | None = None,
) -> BlockerTaskResult:
    hint = idempotency_hint or f"{workspace_id}:blocker:{task_id}:{actor}"
    success_key = f"{hint}:blocker.raised"
    failed_key = f"{hint}:harness.mutation_failed"

    existing = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (success_key,)
    ).fetchone()
    if existing is not None:
        return BlockerTaskResult(
            mutation=None,
            event=row_to_dict(existing),
            event_created=False,
        )

    existing_failed = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (failed_key,)
    ).fetchone()
    if existing_failed is not None:
        return BlockerTaskResult(
            mutation=None,
            event=row_to_dict(existing_failed),
            event_created=False,
        )

    if adapter is None:
        workspace = get_workspace(conn, workspace_id)
        if workspace is None:
            raise ValueError(f"unknown workspace: {workspace_id}")
        adapter = HarnessAdapter(workspace)

    args = ["--actor", actor]
    if reason:
        args.extend(["--reason", reason])

    try:
        mutation = adapter.run_mutation(
            operation="blocker",
            task_id=task_id,
            actor=actor,
            args=args,
            idempotency_hint=hint,
        )
    except (HarnessError, OSError) as exc:
        mutation = _failed_mutation_result(
            operation="blocker",
            task_id=task_id,
            actor=actor,
            idempotency_hint=hint,
            stderr=str(exc),
        )

    if mutation.success:
        result = _handle_blocker_success(
            conn, workspace_id, task_id, reason,
            actor, mutation, success_key,
        )
        if result.event_created:
            _post_mutation_reconcile(conn, workspace_id)
        return result

    return _handle_blocker_failure(
        conn, workspace_id, task_id, reason,
        actor, mutation, failed_key,
    )


def _handle_blocker_success(
    conn, workspace_id, task_id, reason,
    actor, mutation, success_key,
):
    payload = {
        "task_id": task_id,
        "reason": reason,
        "mutation": mutation.to_dict(),
    }
    event_result = append_event(
        conn,
        event_type="blocker.raised",
        actor=actor,
        workspace_id=workspace_id,
        target=task_id,
        task_id=task_id,
        idempotency_key=success_key,
        payload=payload,
    )
    return BlockerTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


def _handle_blocker_failure(
    conn, workspace_id, task_id, reason,
    actor, mutation, failed_key,
):
    payload = {
        "operation": "blocker",
        "task_id": task_id,
        "reason": reason,
        "mutation": mutation.to_dict(),
        "stderr": mutation.stderr,
        "exit_code": mutation.exit_code,
    }
    event_result = append_event(
        conn,
        event_type="harness.mutation_failed",
        actor=actor,
        workspace_id=workspace_id,
        target=task_id,
        task_id=task_id,
        idempotency_key=failed_key,
        payload=payload,
    )
    return BlockerTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


@dataclass(frozen=True)
class UnblockTaskResult:
    mutation: HarnessMutationResult | None
    event: dict[str, Any]
    event_created: bool


def unblock_task(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    actor: str,
    decision: str,
    force: bool = False,
    reason: str | None = None,
    adapter: HarnessAdapter | None = None,
    idempotency_hint: str | None = None,
) -> UnblockTaskResult:
    hint = idempotency_hint or f"{workspace_id}:unblock:{task_id}:{actor}:{decision}"
    success_key = f"{hint}:blocker.resolved"
    failed_key = f"{hint}:harness.mutation_failed"

    existing = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (success_key,)
    ).fetchone()
    if existing is not None:
        return UnblockTaskResult(
            mutation=None,
            event=row_to_dict(existing),
            event_created=False,
        )

    existing_failed = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (failed_key,)
    ).fetchone()
    if existing_failed is not None:
        return UnblockTaskResult(
            mutation=None,
            event=row_to_dict(existing_failed),
            event_created=False,
        )

    if adapter is None:
        workspace = get_workspace(conn, workspace_id)
        if workspace is None:
            raise ValueError(f"unknown workspace: {workspace_id}")
        adapter = HarnessAdapter(workspace)

    args = [actor, "--decision", decision]
    if force:
        args.append("--force")
    if reason:
        args.extend(["--reason", reason])

    try:
        mutation = adapter.run_mutation(
            operation="unblock",
            task_id=task_id,
            actor=actor,
            args=args,
            idempotency_hint=hint,
        )
    except (HarnessError, OSError) as exc:
        mutation = _failed_mutation_result(
            operation="unblock",
            task_id=task_id,
            actor=actor,
            idempotency_hint=hint,
            stderr=str(exc),
        )

    if mutation.success:
        result = _handle_unblock_success(
            conn, workspace_id, task_id, decision, force, reason,
            actor, mutation, success_key,
        )
        if result.event_created:
            _post_mutation_reconcile(conn, workspace_id)
        return result

    return _handle_unblock_failure(
        conn, workspace_id, task_id, decision, force, reason,
        actor, mutation, failed_key,
    )


def _handle_unblock_success(
    conn, workspace_id, task_id, decision, force, reason,
    actor, mutation, success_key,
):
    payload = {
        "task_id": task_id,
        "decision": decision,
        "force": force,
        "reason": reason,
        "mutation": mutation.to_dict(),
    }
    event_result = append_event(
        conn,
        event_type="blocker.resolved",
        actor=actor,
        workspace_id=workspace_id,
        target=task_id,
        task_id=task_id,
        idempotency_key=success_key,
        payload=payload,
    )
    return UnblockTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


def _handle_unblock_failure(
    conn, workspace_id, task_id, decision, force, reason,
    actor, mutation, failed_key,
):
    payload = {
        "operation": "unblock",
        "task_id": task_id,
        "decision": decision,
        "force": force,
        "reason": reason,
        "mutation": mutation.to_dict(),
        "stderr": mutation.stderr,
        "exit_code": mutation.exit_code,
    }
    event_result = append_event(
        conn,
        event_type="harness.mutation_failed",
        actor=actor,
        workspace_id=workspace_id,
        target=task_id,
        task_id=task_id,
        idempotency_key=failed_key,
        payload=payload,
    )
    return UnblockTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


@dataclass(frozen=True)
class CloseoutTaskResult:
    mutation: HarnessMutationResult | None
    event: dict[str, Any]
    event_created: bool


def closeout_task(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    reviewer: str,
    actor: str = "operator",
    adapter: HarnessAdapter | None = None,
    idempotency_hint: str | None = None,
    self_test_evidence: str | None = None,
) -> CloseoutTaskResult:
    hint = idempotency_hint or f"{workspace_id}:closeout:{task_id}:{reviewer}:{actor}"
    success_key = f"{hint}:closeout.requested"
    failed_key = f"{hint}:harness.mutation_failed"

    existing = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (success_key,)
    ).fetchone()
    if existing is not None:
        return CloseoutTaskResult(
            mutation=None,
            event=row_to_dict(existing),
            event_created=False,
        )

    existing_failed = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (failed_key,)
    ).fetchone()
    if existing_failed is not None:
        return CloseoutTaskResult(
            mutation=None,
            event=row_to_dict(existing_failed),
            event_created=False,
        )

    if adapter is None:
        workspace = get_workspace(conn, workspace_id)
        if workspace is None:
            raise ValueError(f"unknown workspace: {workspace_id}")
        adapter = HarnessAdapter(workspace)

    args = [reviewer]
    if self_test_evidence:
        args.extend(["--self-test-evidence", self_test_evidence])

    try:
        mutation = adapter.run_mutation(
            operation="closeout",
            task_id=task_id,
            actor=actor,
            args=args,
            idempotency_hint=hint,
        )
    except (HarnessError, OSError) as exc:
        mutation = _failed_mutation_result(
            operation="closeout",
            task_id=task_id,
            actor=actor,
            idempotency_hint=hint,
            stderr=str(exc),
        )

    if mutation.success:
        result = _handle_closeout_success(
            conn, workspace_id, task_id, reviewer,
            actor, mutation, success_key,
            self_test_evidence=self_test_evidence,
        )
        if result.event_created:
            _post_mutation_reconcile(conn, workspace_id)
        return result

    return _handle_closeout_failure(
        conn, workspace_id, task_id, reviewer,
        actor, mutation, failed_key,
    )


def _handle_closeout_success(
    conn, workspace_id, task_id, reviewer,
    actor, mutation, success_key,
    self_test_evidence: str | None = None,
):
    payload = {
        "task_id": task_id,
        "reviewer": reviewer,
        "mutation": mutation.to_dict(),
        "self_test_evidence": self_test_evidence or "",
    }
    event_result = append_event(
        conn,
        event_type="closeout.requested",
        actor=actor,
        workspace_id=workspace_id,
        target=reviewer,
        task_id=task_id,
        idempotency_key=success_key,
        payload=payload,
    )
    return CloseoutTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


def _handle_closeout_failure(
    conn, workspace_id, task_id, reviewer,
    actor, mutation, failed_key,
):
    payload = {
        "operation": "closeout",
        "task_id": task_id,
        "reviewer": reviewer,
        "mutation": mutation.to_dict(),
        "stderr": mutation.stderr,
        "exit_code": mutation.exit_code,
    }
    event_result = append_event(
        conn,
        event_type="harness.mutation_failed",
        actor=actor,
        workspace_id=workspace_id,
        target=reviewer,
        task_id=task_id,
        idempotency_key=failed_key,
        payload=payload,
    )
    return CloseoutTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


@dataclass(frozen=True)
class ReviewResultTaskResult:
    mutation: HarnessMutationResult | None
    event: dict[str, Any]
    event_created: bool


def review_result_task(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    reviewer: str,
    decision: str,
    actor: str = "operator",
    summary: str | None = None,
    adapter: HarnessAdapter | None = None,
    idempotency_hint: str | None = None,
) -> ReviewResultTaskResult:
    hint = idempotency_hint or f"{workspace_id}:review-result:{task_id}:{reviewer}:{decision}:{actor}"
    success_key = f"{hint}:review.completed"
    failed_key = f"{hint}:harness.mutation_failed"

    existing = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (success_key,)
    ).fetchone()
    if existing is not None:
        return ReviewResultTaskResult(
            mutation=None,
            event=row_to_dict(existing),
            event_created=False,
        )

    existing_failed = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (failed_key,)
    ).fetchone()
    if existing_failed is not None:
        return ReviewResultTaskResult(
            mutation=None,
            event=row_to_dict(existing_failed),
            event_created=False,
        )

    if adapter is None:
        workspace = get_workspace(conn, workspace_id)
        if workspace is None:
            raise ValueError(f"unknown workspace: {workspace_id}")
        adapter = HarnessAdapter(workspace)

    args = [reviewer, decision]
    if summary:
        args.extend(["--summary", summary])

    try:
        mutation = adapter.run_mutation(
            operation="review-result",
            task_id=task_id,
            actor=actor,
            args=args,
            idempotency_hint=hint,
        )
    except (HarnessError, OSError) as exc:
        mutation = _failed_mutation_result(
            operation="review-result",
            task_id=task_id,
            actor=actor,
            idempotency_hint=hint,
            stderr=str(exc),
        )

    if mutation.success:
        result = _handle_review_result_success(
            conn, workspace_id, task_id, reviewer, decision, summary,
            actor, mutation, success_key,
        )
        if result.event_created:
            _post_mutation_reconcile(conn, workspace_id)
        return result

    return _handle_review_result_failure(
        conn, workspace_id, task_id, reviewer, decision, summary,
        actor, mutation, failed_key,
    )


def _handle_review_result_success(
    conn, workspace_id, task_id, reviewer, decision, summary,
    actor, mutation, success_key,
):
    payload = {
        "task_id": task_id,
        "reviewer": reviewer,
        "decision": decision,
        "summary": summary,
        "mutation": mutation.to_dict(),
    }
    event_result = append_event(
        conn,
        event_type="review.completed",
        actor=actor,
        workspace_id=workspace_id,
        target=reviewer,
        task_id=task_id,
        idempotency_key=success_key,
        payload=payload,
    )
    return ReviewResultTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


def _handle_review_result_failure(
    conn, workspace_id, task_id, reviewer, decision, summary,
    actor, mutation, failed_key,
):
    payload = {
        "operation": "review-result",
        "task_id": task_id,
        "reviewer": reviewer,
        "decision": decision,
        "summary": summary,
        "mutation": mutation.to_dict(),
        "stderr": mutation.stderr,
        "exit_code": mutation.exit_code,
    }
    event_result = append_event(
        conn,
        event_type="harness.mutation_failed",
        actor=actor,
        workspace_id=workspace_id,
        target=reviewer,
        task_id=task_id,
        idempotency_key=failed_key,
        payload=payload,
    )
    return ReviewResultTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


_LEGACY_MARK_DONE_HOST_AWARE_WARNING = (
    "`assignment mark-done` writes both mvp-checklist.json and DB event. "
    "For host-aware workflows, use `assignment mark-done-files` on the coding "
    "host, commit/deploy, then `assignment mark-done-record` against the runtime DB."
)


@dataclass(frozen=True)
class MarkDoneTaskResult:
    mutation: HarnessMutationResult | None
    event: dict[str, Any]
    event_created: bool
    gate: MarkDoneGateResult | None = None
    host_aware_warning: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "mutation": self.mutation.to_dict() if self.mutation else None,
            "event": self.event,
            "event_created": self.event_created,
        }
        if self.gate is not None:
            result["gate"] = {
                "passed": self.gate.passed,
                "reason": self.gate.reason,
                "task_status": self.gate.task_status,
            }
        if self.host_aware_warning:
            result["host_aware_warning"] = self.host_aware_warning
        return result


@dataclass(frozen=True)
class MarkDoneFilesResult:
    workspace_id: str
    task_id: str
    checklist_changed: bool
    verification: str | None = None
    receipt_id: str | None = None
    before_fingerprint: str | None = None
    after_fingerprint: str | None = None
    repair_only: bool = False
    repair_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "checklist_changed": self.checklist_changed,
            "verification": self.verification,
            "receipt_id": self.receipt_id,
            "before_fingerprint": self.before_fingerprint,
            "after_fingerprint": self.after_fingerprint,
            "repair_only": self.repair_only,
            "repair_reason": self.repair_reason,
        }


@dataclass(frozen=True)
class MarkDoneRecordResult:
    """Result of host-aware mark-done-record (repair-only): writes cloud DB, no files."""
    workspace_id: str
    task_id: str
    event: dict[str, Any]
    event_created: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "event": self.event,
            "event_created": self.event_created,
        }




def mark_done_task(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    actor: str = "operator",
    adapter: HarnessAdapter | None = None,
    idempotency_hint: str | None = None,
) -> MarkDoneTaskResult:
    hint = idempotency_hint or f"{workspace_id}:mark-done:{task_id}:{actor}"
    success_key = f"{hint}:task.done"
    failed_key = f"{hint}:harness.mutation_failed"

    existing = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (success_key,)
    ).fetchone()
    if existing is not None:
        return MarkDoneTaskResult(
            mutation=None,
            event=row_to_dict(existing),
            event_created=False,
            host_aware_warning=_LEGACY_MARK_DONE_HOST_AWARE_WARNING,
        )

    existing_failed = conn.execute(
        "SELECT * FROM events WHERE idempotency_key = ?", (failed_key,)
    ).fetchone()
    if existing_failed is not None:
        return MarkDoneTaskResult(
            mutation=None,
            event=row_to_dict(existing_failed),
            event_created=False,
            host_aware_warning=_LEGACY_MARK_DONE_HOST_AWARE_WARNING,
        )

    if adapter is None:
        workspace = get_workspace(conn, workspace_id)
        if workspace is None:
            raise ValueError(f"unknown workspace: {workspace_id}")
        adapter = HarnessAdapter(workspace)

    gate = check_mark_done_gate(adapter, task_id)
    if not gate.passed:
        return MarkDoneTaskResult(
            mutation=None,
            event={},
            event_created=False,
            gate=gate,
            host_aware_warning=_LEGACY_MARK_DONE_HOST_AWARE_WARNING,
        )

    args = [actor]

    try:
        mutation = adapter.run_mutation(
            operation="mark-done",
            task_id=task_id,
            actor=actor,
            args=args,
            idempotency_hint=hint,
        )
    except (HarnessError, OSError) as exc:
        mutation = _failed_mutation_result(
            operation="mark-done",
            task_id=task_id,
            actor=actor,
            idempotency_hint=hint,
            stderr=str(exc),
        )

    if mutation.success:
        result = _handle_mark_done_success(
            conn, workspace_id, task_id,
            actor, mutation, success_key,
        )
        if result.event_created:
            _post_mutation_reconcile(conn, workspace_id)
        return result

    return _handle_mark_done_failure(
        conn, workspace_id, task_id,
        actor, mutation, failed_key,
    )


def _handle_mark_done_success(
    conn, workspace_id, task_id,
    actor, mutation, success_key,
):
    payload = {
        "task_id": task_id,
        "mutation": mutation.to_dict(),
    }
    event_result = append_event(
        conn,
        event_type="task.done",
        actor=actor,
        workspace_id=workspace_id,
        target=task_id,
        task_id=task_id,
        idempotency_key=success_key,
        payload=payload,
    )
    return MarkDoneTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
        host_aware_warning=_LEGACY_MARK_DONE_HOST_AWARE_WARNING,
    )


def _handle_mark_done_failure(
    conn, workspace_id, task_id,
    actor, mutation, failed_key,
):
    payload = {
        "operation": "mark-done",
        "task_id": task_id,
        "mutation": mutation.to_dict(),
        "stderr": mutation.stderr,
        "exit_code": mutation.exit_code,
    }
    event_result = append_event(
        conn,
        event_type="harness.mutation_failed",
        actor=actor,
        workspace_id=workspace_id,
        target=task_id,
        task_id=task_id,
        idempotency_key=failed_key,
        payload=payload,
    )
    return MarkDoneTaskResult(
        mutation=mutation,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
        host_aware_warning=_LEGACY_MARK_DONE_HOST_AWARE_WARNING,
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


def mark_done_files(
    *,
    workspace_path: str,
    harness_root: str,
    task_id: str,
    actor: str = "operator",
    verification: str | None = None,
    allow_runtime_copy: bool = False,
    receipt: ReceiptEvidence | None = None,
    repair_reason: str | None = None,
) -> MarkDoneFilesResult:
    """Coding-host canonical write: local ``mvp-checklist.json`` only.

    Service-layer authorization (P1-7): exactly one path is accepted —

    - ``receipt`` (a ``ReceiptEvidence`` produced by a successful online
      ``claim_completion_receipt``) selects the normal receipt path; OR
    - non-empty ``repair_reason`` selects the explicit repair path.

    Any call missing both is rejected before touching the file, so the
    split command cannot be silently bypassed at the service layer.

    Normal path (P1-6) writes structured ``completion_receipt`` metadata
    into the task item (receipt_id, before/after fingerprints, applied_at).
    Free-text ``verification`` stays descriptive; the fingerprint projection
    in ``completion`` excludes this metadata, so it cannot author the
    binding it records. Idempotent retry validates the on-disk metadata
    against the same receipt + after-fingerprint.
    """
    if receipt is not None and (repair_reason and repair_reason.strip()):
        raise ValueError("specify either receipt or repair_reason, not both")
    if receipt is None and not (repair_reason and repair_reason.strip()):
        raise ValueError(
            "mark_done_files requires either receipt evidence (normal path) "
            "or a non-empty repair_reason (repair-only path)"
        )

    # /opt guard — refuse to mutate deploy-derived copies
    if not allow_runtime_copy:
        for label, path_val in (
            ("workspace-path", workspace_path),
            ("harness-root", harness_root),
        ):
            if path_val:
                resolved = Path(path_val).resolve()
                resolved_str = str(resolved)
                if resolved_str == "/opt" or resolved_str.startswith("/opt/"):
                    raise ValueError(
                        f"refusing to mutate harness in runtime deployment copy "
                        f"({label}={path_val}); use --allow-runtime-copy to override"
                    )

    checklist_path = Path(harness_root) / "mvp-checklist.json"
    if not checklist_path.is_file():
        raise ValueError(f"mvp-checklist.json not found at {checklist_path}")
    try:
        checklist = json.loads(checklist_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"mvp-checklist.json at {checklist_path} cannot be read: {exc}"
        ) from exc

    items = checklist.get("items")
    if not isinstance(items, list):
        raise ValueError(f"mvp-checklist.json at {checklist_path} has no 'items' list")

    for item in items:
        if not isinstance(item, dict) or item.get("id") != task_id:
            continue
        return _apply_mark_done_item(
            item, checklist, checklist_path, task_id, verification,
            receipt=receipt, repair_reason=repair_reason,
        )

    raise ValueError(
        f"task {task_id} not found in mvp-checklist.json at {checklist_path}"
    )


def _apply_mark_done_item(
    item, checklist, checklist_path, task_id, verification,
    *, receipt, repair_reason,
) -> MarkDoneFilesResult:
    workflow = item["workflow"] if isinstance(item.get("workflow"), dict) else {}
    before_fingerprint = compute_item_fingerprint(item)
    already_done = item.get("status") == "done" and workflow.get("status") == "closed"

    if receipt is not None:
        expected_after = receipt.after_fingerprint
        if already_done:
            # Idempotent retry: recompute the actual on-disk lifecycle
            # fingerprint and require it to match the receipt's
            # after-fingerprint. Do NOT trust the stored metadata — the item
            # could have drifted (e.g. branch changed) while leaving
            # completion_receipt metadata intact. Compared BEFORE any write.
            actual = compute_item_fingerprint(item)
            if actual != expected_after:
                raise ValueError(
                    f"on-disk lifecycle fingerprint {actual!r} does not match "
                    f"receipt after-fingerprint {expected_after!r}; the done "
                    f"item appears to have drifted"
                )
            metadata = item.get("completion_receipt") if isinstance(
                item.get("completion_receipt"), dict) else {}
            if metadata.get("receipt_id") != receipt.receipt_id:
                raise ValueError(
                    f"task {task_id} already done under receipt "
                    f"{metadata.get('receipt_id')!r}, cannot reuse {receipt.receipt_id!r}"
                )
            if metadata.get("after_fingerprint") != expected_after:
                raise ValueError(
                    f"task {task_id} on-disk after_fingerprint "
                    f"{metadata.get('after_fingerprint')!r} does not match receipt "
                    f"after-fingerprint {expected_after!r}"
                )
            return MarkDoneFilesResult(
                workspace_id="local", task_id=task_id, checklist_changed=False,
                verification=item.get("verification"),
                receipt_id=receipt.receipt_id,
                before_fingerprint=before_fingerprint,
                after_fingerprint=expected_after,
                repair_only=False, repair_reason=None,
            )
        # Fresh mutation: the current on-disk lifecycle MUST match the
        # before-fingerprint the receipt was reserved against. Compare BEFORE
        # any write so a drift between reserve and apply cannot mutate the
        # canonical file under the wrong authorization.
        if before_fingerprint != receipt.before_fingerprint:
            raise ValueError(
                f"on-disk lifecycle fingerprint {before_fingerprint!r} does "
                f"not match receipt before-fingerprint "
                f"{receipt.before_fingerprint!r}; the item appears to have "
                f"drifted since the receipt was reserved"
            )
        _mutate_item_done(item, verification)
        metadata = {
            "receipt_id": receipt.receipt_id,
            "before_fingerprint": receipt.before_fingerprint,
            "after_fingerprint": expected_after,
            "applied_at": item["updated_at"],
        }
        item["completion_receipt"] = metadata
        _write_checklist(checklist_path, checklist)
        actual_after = compute_item_fingerprint(item)
        if actual_after != expected_after:
            raise ValueError(
                f"post-write fingerprint {actual_after!r} does not match receipt "
                f"after-fingerprint {expected_after!r}"
            )
        return MarkDoneFilesResult(
            workspace_id="local", task_id=task_id, checklist_changed=True,
            verification=verification, receipt_id=receipt.receipt_id,
            before_fingerprint=before_fingerprint, after_fingerprint=actual_after,
            repair_only=False, repair_reason=None,
        )

    # Repair path.
    if already_done:
        return MarkDoneFilesResult(
            workspace_id="local", task_id=task_id, checklist_changed=False,
            verification=item.get("verification"), receipt_id=None,
            before_fingerprint=before_fingerprint,
            after_fingerprint=compute_item_fingerprint(item),
            repair_only=True, repair_reason=repair_reason,
        )
    _mutate_item_done(item, verification)
    _write_checklist(checklist_path, checklist)
    after_fingerprint = compute_item_fingerprint(item)
    return MarkDoneFilesResult(
        workspace_id="local", task_id=task_id, checklist_changed=True,
        verification=verification, receipt_id=None,
        before_fingerprint=before_fingerprint, after_fingerprint=after_fingerprint,
        repair_only=True, repair_reason=repair_reason,
    )


def _mutate_item_done(item, verification) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    item["status"] = "done"
    item["updated_at"] = now
    item["verification"] = verification or ""
    if isinstance(item.get("workflow"), dict):
        item["workflow"]["status"] = "closed"
        item["workflow"]["updated_at"] = now
    else:
        item["workflow"] = {"status": "closed", "branch": None, "updated_at": now}


def _write_checklist(checklist_path, checklist) -> None:
    if isinstance(checklist, dict) and "updated_at" in checklist:
        now_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        checklist["updated_at"] = now_date
    checklist_path.write_text(
        json.dumps(checklist, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def mark_done_record(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    actor: str = "operator",
    verification: str | None = None,
    idempotency_hint: str | None = None,
    repair_reason: str | None = None,
) -> MarkDoneRecordResult:
    """Repair-only server write of a ``task.done`` event (no harness files).

    The receipt terminal is ``consume_completion_receipt`` (in completion);
    this service exists only for explicit drift reconciliation and requires
    a non-empty ``repair_reason`` (P1-7). Wide-match idempotency ignores
    actor, so a historical ``task.done`` from another actor is a no-op —
    essential for drift tasks whose original completion predates the
    receipt protocol. The payload is stamped ``repair_only=true`` with the
    reason so audit/doctor can distinguish it from receipt completions.
    """
    if not (repair_reason and repair_reason.strip()):
        raise ValueError(
            "mark_done_record is repair-only; a non-empty repair_reason is required"
        )

    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")

    prior_rows = conn.execute(
        "SELECT * FROM events WHERE workspace_id = ? AND event_type = 'task.done'",
        (workspace_id,),
    ).fetchall()
    for prior_row in prior_rows:
        prior = row_to_dict(prior_row)
        prior_payload = prior.get("payload") or {}
        if prior_payload.get("task_id") == task_id:
            return MarkDoneRecordResult(
                workspace_id=workspace_id, task_id=task_id,
                event=prior, event_created=False,
            )

    hint = idempotency_hint or f"{workspace_id}:mark-done-record:{task_id}:{actor}"
    payload: dict[str, Any] = {
        "task_id": task_id,
        "host_aware": "record-only",
        "repair_only": True,
        "repair_reason": repair_reason,
    }
    if verification:
        payload["verification"] = verification

    event_result = append_event(
        conn,
        workspace_id=workspace_id,
        event_type="task.done",
        actor=actor,
        target=task_id,
        task_id=task_id,
        idempotency_key=f"{hint}:task.done",
        payload=payload,
    )
    return MarkDoneRecordResult(
        workspace_id=workspace_id, task_id=task_id,
        event=row_to_dict(event_result.row), event_created=event_result.created,
    )
