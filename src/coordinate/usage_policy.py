"""Task-scoped usage warning policy — set/status business logic and evaluation.

Issue #12. Coordinate holds ONE current policy revision per
``(workspace_id, task_id)`` scope. Warnings are append-only ``usage.warning``
events evaluated inside the terminal report transaction; they never cancel
jobs, never block claim/report and never enter the Discord/KOOK renderer.

Rules:

- ``revision`` must strictly increase; the same revision with the same body is
  an idempotent replay, a conflicting body fails closed.
- Warning boundary is ``observed_tokens >= threshold`` for a single accepted
  attempt (NOT the task aggregate) and at most ONE ``usage.warning`` event per
  policy revision per scope (guaranteed by the idempotency key).
- ``policy-set`` itself never evaluates; evaluation happens on the next
  successfully accepted terminal report in the scope.
- ``causation_id`` points at the terminal event only when that event was truly
  created by the current attempt; a reused terminal event (e.g. a second
  ``timed_out`` attempt sharing the old ``job.timed_out`` event) keeps
  ``causation_id=null`` — never forge causation.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

from .db import (
    append_event,
    get_workspace,
    get_task_usage_warning_policy,
    list_attempt_usage_for_scope,
    row_to_dict,
    set_task_usage_warning_policy as _set_policy_row,
    utc_now,
)


class UsagePolicyError(ValueError):
    pass


def _validate_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise UsagePolicyError(f"{label} must be a positive integer")
    return value


def set_task_usage_warning_policy(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    revision: int,
    observed_tokens_threshold: int,
    enabled: bool = True,
    actor: str = "operator",
    commit: bool = True,
) -> dict[str, Any]:
    """Set the current task-scoped policy revision with monotonic CAS.

    Raises ``UsagePolicyError`` on revision regression or same-revision
    conflicting bodies; returns the canonical policy dict on success.
    """
    if not isinstance(workspace_id, str) or not workspace_id.strip():
        raise UsagePolicyError("workspace_id is required")
    if not isinstance(task_id, str) or not task_id.strip():
        raise UsagePolicyError("task_id is required")
    if not isinstance(enabled, bool):
        raise UsagePolicyError("enabled must be a boolean")
    revision = _validate_positive_int(revision, "revision")
    threshold = _validate_positive_int(
        observed_tokens_threshold, "observed_tokens_threshold"
    )
    if get_workspace(conn, workspace_id) is None:
        raise UsagePolicyError(f"unknown workspace: {workspace_id}")

    owns_transaction = commit
    if owns_transaction:
        conn.execute("BEGIN IMMEDIATE")
    try:
        # Read only after acquiring the writer lock. Otherwise two processes
        # can both validate against a stale revision and the later writer can
        # silently regress the policy.
        current = get_task_usage_warning_policy(
            conn, workspace_id=workspace_id, task_id=task_id
        )
        now = utc_now()
        if current is None:
            created_at = now
        else:
            current_revision = int(current["revision"])
            if revision < current_revision:
                raise UsagePolicyError(
                    f"policy revision must strictly increase: current revision is "
                    f"{current_revision}, got {revision}"
                )
            body_matches = (
                int(current["observed_tokens_threshold"]) == threshold
                and bool(current["enabled"]) == enabled
            )
            if revision == current_revision:
                if not body_matches:
                    raise UsagePolicyError(
                        f"policy revision {revision} already exists with a conflicting body; "
                        "use a higher revision"
                    )
                if owns_transaction:
                    conn.commit()
                return row_to_dict(current)
            created_at = current["created_at"]

        row = _set_policy_row(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            revision=revision,
            observed_tokens_threshold=threshold,
            enabled=enabled,
            created_at=created_at,
            updated_at=now,
            commit=False,
        )
        if owns_transaction:
            conn.commit()
        return row_to_dict(row)
    except Exception:
        if owns_transaction and conn.in_transaction:
            conn.rollback()
        raise


def _warning_idempotency_key(workspace_id: str, task_id: str, revision: int) -> str:
    scope = json.dumps(
        [workspace_id, task_id, revision],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"runtime:usage:warning:{hashlib.sha256(scope).hexdigest()}"


def evaluate_task_usage_warning(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    job_id: str,
    attempt_token: int,
    observed_tokens: int,
    completeness: str,
    terminal_event_id: str | None,
    terminal_event_created: bool,
    actor: str = "runtime",
    agent_id: str | None = None,
    commit: bool = False,
) -> dict[str, Any] | None:
    """In-transaction warning evaluation for one accepted attempt.

    Appends at most one ``usage.warning`` event per policy revision; returns
    the event dict (created or the already-existing one) or None when the
    policy is absent/disabled or the boundary is not reached. Never raises on
    policy state — warnings are advisory.
    """
    policy = get_task_usage_warning_policy(
        conn, workspace_id=workspace_id, task_id=task_id
    )
    if policy is None or not policy["enabled"]:
        return None
    if observed_tokens < int(policy["observed_tokens_threshold"]):
        return None

    idempotency_key = _warning_idempotency_key(
        workspace_id, task_id, int(policy["revision"])
    )
    event = append_event(
        conn,
        workspace_id=workspace_id,
        event_type="usage.warning",
        actor=actor,
        target=agent_id,
        task_id=task_id,
        causation_id=terminal_event_id if terminal_event_created else None,
        idempotency_key=idempotency_key,
        payload={
            "revision": int(policy["revision"]),
            "scope": {"workspace_id": workspace_id, "task_id": task_id},
            "observed_tokens_threshold": int(policy["observed_tokens_threshold"]),
            "observed_tokens": observed_tokens,
            "completeness": completeness,
            "job_id": job_id,
            "attempt_token": attempt_token,
            "terminal_event_id": terminal_event_id,
            "terminal_event_created": terminal_event_created,
        },
        commit=commit,
    )
    return row_to_dict(event.row)


def build_usage_status(
    conn: sqlite3.Connection, *, workspace_id: str, task_id: str
) -> dict[str, Any]:
    """Task-scoped usage status: policy + attempt ledger + warning events.

    Aggregate ``provider_cost_microusd`` is only summed when every attempt row
    has a non-null cost — partial cost is never presented as total.
    """
    if get_workspace(conn, workspace_id) is None:
        raise UsagePolicyError(f"unknown workspace: {workspace_id}")

    policy = get_task_usage_warning_policy(
        conn, workspace_id=workspace_id, task_id=task_id
    )
    attempts: list[dict[str, Any]] = []
    total_tokens = 0
    has_observed_tokens = False
    cost_sum = 0
    all_cost_known = True
    for row in list_attempt_usage_for_scope(
        conn, workspace_id=workspace_id, task_id=task_id
    ):
        usage = row_to_dict(row)
        attempts.append(
            {
                "job_id": usage["job_id"],
                "attempt_token": usage["attempt_token"],
                "digest": usage["evidence_digest"],
                "observed_tokens": usage["observed_tokens"],
                "provider_cost_microusd": usage["provider_cost_microusd"],
                "completeness": usage["completeness"],
                "record_count": len(usage["evidence"].get("records") or []),
                "records": usage["evidence"].get("records"),
                "terminal_event_id": usage["terminal_event_id"],
                "event_created": usage["event_created"],
                "recorded_at": usage["recorded_at"],
            }
        )
        if usage["observed_tokens"] is not None:
            has_observed_tokens = True
            total_tokens += usage["observed_tokens"]
        if usage["provider_cost_microusd"] is None:
            all_cost_known = False
        else:
            cost_sum += usage["provider_cost_microusd"]

    warnings: list[dict[str, Any]] = []
    for row in conn.execute(
        """
        SELECT * FROM events
        WHERE event_type = 'usage.warning' AND workspace_id = ? AND task_id = ?
        ORDER BY rowid
        """,
        (workspace_id, task_id),
    ).fetchall():
        event = row_to_dict(row)
        warnings.append(
            {
                "event_id": event["id"],
                "created_at": event["created_at"],
                "causation_id": event["causation_id"],
                **event["payload"],
            }
        )

    return {
        "workspace_id": workspace_id,
        "task_id": task_id,
        "policy": row_to_dict(policy) if policy is not None else None,
        "aggregate": {
            "observed_tokens": (
                total_tokens if has_observed_tokens or not attempts else None
            ),
            "provider_cost_microusd": cost_sum if all_cost_known else None,
            "attempt_count": len(attempts),
            "record_count": sum(attempt["record_count"] for attempt in attempts),
        },
        "attempts": attempts,
        "warnings": warnings,
    }
