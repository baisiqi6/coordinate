"""Runtime lease orchestration: atomic claim, reap, renewal, and terminal release.

This module owns the caller-owned transactions described in P9-3B. It delegates
strict, commit-free lease primitives to ``execution_leases`` and uses existing
``append_event(..., commit=False)`` / ``create_delivery(..., commit=False)`` seams.
"""
from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .agent_report import AgentReport, parse_agent_report
from .db import append_event, create_delivery, get_job, get_workspace, get_workspace_host_profile, row_to_dict, utc_now
from .execution_context import (
    CONTRACT_VERSION,
    ContextError,
    ExecutionContextV1,
    resolve_execution_context_v1,
    validate_execution_context_snapshot,
)
from .execution_leases import (
    LEASE_DEFAULT_RENEW_INTERVAL_SECONDS,
    LEASE_DEFAULT_TTL_SECONDS,
    LEASE_REAP_BATCH_SIZE,
    LeaseError,
    attempt_has_any_lease,
    build_lease_envelope,
    count_active_leases_for_agent,
    release_attempt_lease,
    renew_attempt_lease,
    require_active_unexpired_lease,
    reserve_attempt_lease,
    _find_active_resource_lease,
    _find_due_active_leases,
    _validate_stored_resource,
    _validate_ttl,
)
from .execution_resources import build_worktree_resource, compute_resource_key
from .executor_identity import (
    executor_binding_claim_evidence,
    resolve_exact_executor_binding,
)
from .executor_routing import ExecutorRoutingError, routing_claim_evidence


class RuntimeLeaseError(ValueError):
    """Raised when a runtime lease orchestration invariant is violated.

    Carries optional selection diagnostics (``oldest_blocked_job_id``,
    ``oldest_blocked_resource_key``) when the error stems from candidate
    selection so they can propagate through the public ``RuntimeClaimResult``.
    """

    def __init__(
        self,
        *args: Any,
        oldest_blocked_job_id: str | None = None,
        oldest_blocked_resource_key: str | None = None,
    ) -> None:
        super().__init__(*args)
        self.oldest_blocked_job_id = oldest_blocked_job_id
        self.oldest_blocked_resource_key = oldest_blocked_resource_key


LEASE_CLAIM_SCAN_LIMIT = 256

_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_MAX_ID_LEN = 128
_MAX_REASON_LEN = 512


def _validate_id(value: Any, label: str) -> str:
    if not isinstance(value, str) or isinstance(value, bool):
        raise RuntimeLeaseError(f"{label} must be a non-blank string")
    if not value:
        raise RuntimeLeaseError(f"{label} must not be empty")
    if value != value.strip():
        raise RuntimeLeaseError(f"{label} must not have surrounding whitespace")
    if len(value) > _MAX_ID_LEN:
        raise RuntimeLeaseError(f"{label} exceeds {_MAX_ID_LEN} characters")
    if _CONTROL_RE.search(value):
        raise RuntimeLeaseError(f"{label} contains control characters")
    return value


def _validate_reason(value: Any, label: str) -> str:
    if not isinstance(value, str) or isinstance(value, bool):
        raise RuntimeLeaseError(f"{label} must be a non-blank string")
    if not value:
        raise RuntimeLeaseError(f"{label} must not be empty")
    if value != value.strip():
        raise RuntimeLeaseError(f"{label} must not have surrounding whitespace")
    if len(value) > _MAX_REASON_LEN:
        raise RuntimeLeaseError(f"{label} exceeds {_MAX_REASON_LEN} characters")
    if _CONTROL_RE.search(value):
        raise RuntimeLeaseError(f"{label} contains control characters")
    return value


def _validate_utc_timestamp(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise RuntimeLeaseError(f"{label} must be a UTC timestamp string")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value):
        raise RuntimeLeaseError(
            f"{label} must be exactly YYYY-MM-DDTHH:MM:SSZ in UTC"
        )
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise RuntimeLeaseError(f"{label} is not a valid UTC timestamp") from exc
    return value


@dataclass(frozen=True)
class _ValidatedAuthority:
    """Authority package computed once per selected candidate.

    Carries the decoded payload (with backfilled execution_context), the
    validated ExecutionContextV1, the validated executor binding snapshot, and
    the routing claim evidence so ``claim_leased_job`` can reuse it inside the
    same transaction without re-deriving anything.
    """

    payload: dict[str, Any]
    execution_context: ExecutionContextV1
    resource_key: str
    binding_snapshot: dict[str, Any]
    route_evidence: dict[str, Any]


@dataclass(frozen=True)
class ClaimCandidateResult:
    job: sqlite3.Row | None
    reason: str
    oldest_blocked_job_id: str | None = None
    oldest_blocked_resource_key: str | None = None
    validated_authority: _ValidatedAuthority | None = None


@dataclass(frozen=True)
class ClaimLeaseResult:
    lease_id: str
    job: sqlite3.Row
    attempt_token: int
    execution_context: ExecutionContextV1
    execution_lease: dict[str, Any]


@dataclass(frozen=True)
class RecoveryEvidence:
    recovery_reason: str
    prior_process_stopped: bool


def _lease_release_reason(status: str) -> str:
    return f"job_{status}"


def _job_payload(job: sqlite3.Row) -> dict[str, Any]:
    raw = job["payload_json"] if "payload_json" in job.keys() else None
    if not raw:
        return {}
    import json

    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _json(value: dict[str, Any] | None) -> str:
    import json

    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _decode_typed_payload(job: sqlite3.Row) -> dict[str, Any]:
    """Strict payload decoding for managed (typed) jobs.

    Unlike the legacy ``_job_payload`` helper, this fails closed:
    - payload_json column missing or NULL/empty → RuntimeLeaseError
    - invalid JSON → RuntimeLeaseError
    - decoded top-level value is not an object → RuntimeLeaseError
    """
    job_id = job["id"]
    if "payload_json" not in job.keys() or job["payload_json"] is None:
        raise RuntimeLeaseError(f"job {job_id} has no payload_json")
    raw = job["payload_json"]
    if not raw:
        raise RuntimeLeaseError(f"job {job_id} has empty payload_json")
    import json

    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeLeaseError(f"job {job_id} has invalid payload_json: {exc}") from exc
    if not isinstance(decoded, dict):
        raise RuntimeLeaseError(f"job {job_id} payload_json is not an object")
    return decoded


def _validate_claim_authority(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    agent_id: str,
    host_id: str,
) -> _ValidatedAuthority:
    """Validate full claim authority for one typed candidate.

    Authority order (all before resource check):
    1. runner_profile_id exists and matches the binding snapshot.
    2. payload_json is valid JSON object (strict typed decoding).
    3. ExecutionContextV1 is resolved/backfilled from authority and validated.
    4. Executor binding snapshot is present and matches current catalog.
    5. Routing claim evidence cross-links are valid.
    6. Compute the worktree resource_key from the validated context.

    This must run inside the same BEGIN IMMEDIATE transaction as the CAS so the
    validated context belongs to the row being claimed.
    """
    job_id = job["id"]
    runner_profile_id = job["runner_profile_id"]
    if not runner_profile_id:
        raise RuntimeLeaseError(f"job {job_id} has no runner_profile_id")

    payload = _decode_typed_payload(job)

    # Resolve context before binding/routing so missing context backfills first.
    execution_context = _resolve_claim_context(
        conn, job=job, payload=payload, agent_id=agent_id, host_id=host_id
    )
    if payload.get("execution_context") is None:
        payload = {**payload, "execution_context": execution_context.to_dict()}

    # Validate executor binding against the (possibly backfilled) payload.
    binding_snapshot = payload.get("executor_binding")
    _validate_binding_snapshot(conn, job=job, binding_snapshot=binding_snapshot)

    # Validate routing evidence; exact jobs return {}.
    try:
        route_evidence = routing_claim_evidence(payload, job=dict(job))
    except ExecutorRoutingError as exc:
        raise RuntimeLeaseError(f"invalid routing claim evidence: {exc}") from exc

    # Compute resource key from validated context.
    try:
        resource = build_worktree_resource(host_id, execution_context.worktree_path)
    except Exception as exc:
        raise RuntimeLeaseError(f"invalid execution context for {job_id}: {exc}") from exc
    resource_key = compute_resource_key(resource)

    return _ValidatedAuthority(
        payload=payload,
        execution_context=execution_context,
        resource_key=resource_key,
        binding_snapshot=binding_snapshot,
        route_evidence=route_evidence,
    )


def _stable_result_key(value: dict[str, Any]) -> str:
    import hashlib

    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()[:16]


def _terminal_event_type(status: str) -> str:
    if status == "timed_out":
        return "job.timed_out"
    if status == "done":
        return "job.completed"
    return "job.failed"


def _normalize_result(
    *,
    status: str,
    result: dict[str, Any],
    job: sqlite3.Row,
    now: str,
) -> dict[str, Any]:
    normalized = dict(result)
    session_id = normalized.get("session_id")
    if isinstance(session_id, str) and session_id.strip():
        normalized["session_id"] = session_id.strip()
    progress = _job_progress(job)
    if status == "timed_out":
        timeout = normalized.get("timeout") if isinstance(normalized.get("timeout"), dict) else {}
        normalized["timeout"] = {
            "kind": timeout.get("kind") or normalized.get("timeout_kind") or "recoverable",
            "configured_budget_seconds": (
                timeout.get("configured_budget_seconds")
                or normalized.get("timeout_seconds")
                or job["timeout_seconds"]
            ),
            "last_activity_at": timeout.get("last_activity_at") or job["last_activity_at"] or now,
            "session_id": timeout.get("session_id") or normalized.get("session_id") or job["terminal_session_id"] or "",
            "progress": timeout.get("progress") or progress,
            "resume_allowed": bool(timeout.get("resume_allowed", True)),
        }
        normalized["recoverable"] = bool(normalized["timeout"]["resume_allowed"])
        normalized.setdefault(
            "response_text",
            "Agent timed out; progress was saved and recovery is available.",
        )
    return normalized


def _job_progress(job: sqlite3.Row) -> dict[str, Any]:
    raw = job["progress_json"] if "progress_json" in job.keys() else None
    if not raw:
        return {}
    import json

    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _parse_job_agent_report(*, result: dict[str, Any], job: sqlite3.Row) -> dict[str, Any]:
    response_text = result.get("response_text") or result.get("text") or ""
    parsed = (
        parse_agent_report(
            response_text,
            fallback_workspace_id=job["workspace_id"],
            fallback_task_id=job["task_id"],
        )
        if isinstance(response_text, str) and response_text
        else None
    )
    decision = parsed.decision if parsed and parsed.decision in {"approve", "reject"} else None
    result_summary_raw = result.get("summary") or result.get("response_text") or result.get("text")
    if isinstance(result_summary_raw, str) and result_summary_raw.strip():
        result_summary = " ".join(result_summary_raw.split())[:500]
    else:
        result_summary = None
    return {"parsed": parsed, "decision": decision, "result_summary": result_summary}


def _validate_recovery_reason(value: Any) -> str:
    if not isinstance(value, str):
        raise RuntimeLeaseError("recovery_reason must be a string")
    stripped = value.strip()
    if not stripped:
        raise RuntimeLeaseError("recovery_reason must be non-empty")
    if len(stripped) > 512:
        raise RuntimeLeaseError("recovery_reason exceeds 512 characters")
    if any(ord(c) < 32 for c in stripped):
        raise RuntimeLeaseError("recovery_reason contains control characters")
    return stripped


def _validate_recovery_evidence(
    *,
    recoverable: bool,
    recovery_reason: str | None,
    prior_process_stopped: bool | None,
) -> RecoveryEvidence | None:
    """Validate and return audited Operator evidence for managed recovery.

    Ordinary agentd cannot auto-reclaim. Recovery mode requires both a bounded
    audited reason and explicit confirmation that the prior provider process or
    session has stopped.
    """
    if not recoverable:
        return None
    if recovery_reason is None or prior_process_stopped is None:
        raise RuntimeLeaseError(
            "managed recovery requires recovery_reason and prior_process_stopped"
        )
    if prior_process_stopped is not True:
        raise RuntimeLeaseError(
            "managed recovery requires prior_process_stopped=True"
        )
    return RecoveryEvidence(
        recovery_reason=_validate_recovery_reason(recovery_reason),
        prior_process_stopped=True,
    )


def _require_active_lease_for_reap(
    conn: sqlite3.Connection,
    *,
    lease_id: str,
    job_id: str,
    attempt_token: int,
    agent_id: str,
    now: str,
) -> dict[str, Any]:
    """Revalidate the lease row is still active and due with matching tuple.

    Unlike ``require_active_unexpired_lease``, this accepts a due (expired)
    lease because reap is the expiry authority.  Under the write lock it also
    re-checks the current ``expires_at`` against the authoritative ``now`` so
    a lease that was just renewed by another connection is skipped rather than
    expired.

    Integrity failures (missing row, corrupt resource snapshot, or tuple
    mismatch) raise ``RuntimeLeaseError`` so the reaper reports them as errors
    rather than silently skipping. Benign races (already released/expired or
    no longer due) raise ``LeaseError`` so ``_reap_one_lease`` can return a
    silent skip.
    """
    row = conn.execute(
        "SELECT * FROM execution_attempt_leases WHERE lease_id = ?",
        (lease_id,),
    ).fetchone()
    if row is None:
        raise RuntimeLeaseError(f"lease {lease_id!r} not found")
    current = dict(row)
    try:
        _validate_stored_resource(current)
    except LeaseError as exc:
        raise RuntimeLeaseError(f"lease {lease_id!r} resource snapshot invalid: {exc}") from exc
    if current["job_id"] != job_id:
        raise RuntimeLeaseError("lease job_id mismatch")
    if current["attempt_token"] != attempt_token:
        raise RuntimeLeaseError("lease attempt_token mismatch")
    if current["agent_id"] != agent_id:
        raise RuntimeLeaseError("lease agent_id mismatch")
    if current["status"] != "active":
        raise LeaseError(f"lease {lease_id!r} is {current['status']}")
    if current["expires_at"] > now:
        raise LeaseError(f"lease {lease_id!r} is no longer due")
    return current


def _expire_revalidated_lease(
    conn: sqlite3.Connection,
    *,
    current: dict[str, Any],
    actor: str,
    now: str,
) -> dict[str, Any]:
    """Expire an already-revalidated lease: mark expired, CAS job, append events."""
    lease_id = current["lease_id"]
    job_id = current["job_id"]
    attempt_token = current["attempt_token"]
    agent_id = current["agent_id"]

    job = get_job(conn, job_id)
    if job is None:
        raise RuntimeLeaseError(f"job {job_id!r} not found for expired lease")
    if (
        job["status"] != "running"
        or job["attempt_count"] != attempt_token
        or job["assigned_agent"] != agent_id
    ):
        raise RuntimeLeaseError(
            f"lease/job inconsistency: {job_id} status={job['status']} "
            f"attempt={job['attempt_count']} agent={job['assigned_agent']}"
        )

    # Mark lease expired.
    conn.execute(
        """
        UPDATE execution_attempt_leases
        SET status = 'expired', renewed_at = expires_at
        WHERE lease_id = ?
        """,
        (lease_id,),
    )

    # CAS job to timed_out+recoverable.
    timeout_result = {
        "kind": "execution_lease_expired",
        "configured_budget_seconds": LEASE_DEFAULT_TTL_SECONDS,
        "last_activity_at": job["last_activity_at"] or now,
        "session_id": job["terminal_session_id"] or "",
        "progress": _job_progress(job),
        "resume_allowed": True,
    }
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = ?, result_json = ?, completed_at = ?, last_activity_at = ?,
            recoverable = 1, updated_at = ?
        WHERE id = ? AND status = 'running' AND attempt_count = ?
        """,
        (
            "timed_out",
            _json({
                "response_text": "Lease expired; job is recoverable.",
                "timeout": timeout_result,
            }),
            now,
            now,
            now,
            job_id,
            attempt_token,
        ),
    )
    if cursor.rowcount == 0:
        raise RuntimeLeaseError(f"CAS failed while reaping lease for {job_id}")

    # Idempotent lifecycle events.
    append_event(
        conn,
        workspace_id=job["workspace_id"],
        event_type="execution_lease.expired",
        actor=actor,
        target=agent_id,
        task_id=job["task_id"],
        idempotency_key=f"runtime:lease:{lease_id}:expired",
        payload={
            "lease_id": lease_id,
            "job_id": job_id,
            "attempt_token": attempt_token,
            "expires_at": current["expires_at"],
            "reaped_at": now,
            "resource_key": current["resource_key"],
        },
        commit=False,
    )
    append_event(
        conn,
        workspace_id=job["workspace_id"],
        event_type="job.timed_out",
        actor=actor,
        target=agent_id,
        task_id=job["task_id"],
        idempotency_key=f"runtime:job:{job_id}:timed_out:lease_expired",
        payload={
            "job_id": job_id,
            "agent_id": agent_id,
            "status": "timed_out",
            "reason": "execution_lease_expired",
            "lease_id": lease_id,
            "timeout": timeout_result,
        },
        commit=False,
    )
    return {
        "lease_id": lease_id,
        "job_id": job_id,
        "attempt_token": attempt_token,
        "agent_id": agent_id,
    }


def _reap_one_lease(
    conn: sqlite3.Connection,
    *,
    lease: dict[str, Any],
    actor: str,
    now: str,
) -> dict[str, Any]:
    """Reap one due lease inside a caller-owned transaction."""
    lease_id = lease["lease_id"]
    job_id = lease["job_id"]
    attempt_token = lease["attempt_token"]
    agent_id = lease["agent_id"]

    # Revalidate the lease is still due and active under the lock.  The returned
    # ``current`` is the authoritative lease row for all subsequent writes.
    try:
        current = _require_active_lease_for_reap(
            conn,
            lease_id=lease_id,
            job_id=job_id,
            attempt_token=attempt_token,
            agent_id=agent_id,
            now=now,
        )
    except LeaseError:
        return {"skipped": True, "reason": "no_longer_due_or_active"}

    return _expire_revalidated_lease(conn, current=current, actor=actor, now=now)


def reap_due_leases(
    conn: sqlite3.Connection,
    *,
    actor: str = "runtime",
    now: str | None = None,
    batch_size: int = LEASE_REAP_BATCH_SIZE,
) -> dict[str, Any]:
    """Bounded global reap: expire due leases and make their exact jobs recoverable.

    Each due lease is processed in its own ``BEGIN IMMEDIATE`` transaction:
    validate stored snapshot, require matching running job at same attempt/agent,
    mark lease expired, CAS job to timed_out+recoverable, append idempotent
    expiry/timeout events, commit. Returns a summary of processed leases.
    """
    now = now or utc_now()
    due_leases = _find_due_active_leases(conn, now, batch_size)
    processed: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for lease in due_leases:
        lease_id = lease["lease_id"]
        job_id = lease["job_id"]
        try:
            conn.execute("BEGIN IMMEDIATE")
            outcome = _reap_one_lease(conn, lease=lease, actor=actor, now=now)
            if outcome.get("skipped"):
                conn.commit()
                continue
            conn.commit()
            processed.append(outcome)
        except Exception as exc:
            if conn.in_transaction:
                conn.rollback()
            errors.append({
                "lease_id": lease_id,
                "job_id": job_id,
                "error": str(exc),
            })

    return {
        "reaped_count": len(processed),
        "due_found": len(due_leases),
        "reaped": processed,
        "errors": errors,
    }


def reap_exact_lease(
    conn: sqlite3.Connection,
    *,
    lease_id: str,
    job_id: str,
    actor: str = "runtime",
    now: str | None = None,
) -> dict[str, Any]:
    """Reap ONE exact lease by id — no global scan.

    Validates ids/actor before transaction, then BEGIN IMMEDIATE, select the
    exact lease row, check job_id match, revalidate, mutate, commit.
    Not-found/job-mismatch/not-active/not-due all raise RuntimeLeaseError.
    """
    _validate_id(lease_id, "lease_id")
    _validate_id(job_id, "job_id")
    _validate_id(actor, "actor")

    now = _validate_utc_timestamp(now if now is not None else utc_now(), "now")

    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM execution_attempt_leases WHERE lease_id = ?",
            (lease_id,),
        ).fetchone()
        if row is None:
            raise RuntimeLeaseError(f"lease {lease_id!r} not found")

        current = dict(row)
        if current["job_id"] != job_id:
            raise RuntimeLeaseError(
                f"lease {lease_id!r} belongs to job {current['job_id']!r}, "
                f"not requested job {job_id!r}"
            )

        try:
            current = _require_active_lease_for_reap(
                conn,
                lease_id=lease_id,
                job_id=job_id,
                attempt_token=current["attempt_token"],
                agent_id=current["agent_id"],
                now=now,
            )
        except LeaseError as exc:
            raise RuntimeLeaseError(str(exc)) from exc

        # Shared post-revalidation mutation helper.
        try:
            result = _expire_revalidated_lease(
                conn, current=current, actor=actor, now=now
            )
        except KeyError as exc:
            raise RuntimeLeaseError(
                f"job {job_id!r} not found for expired lease"
            ) from exc

        conn.commit()
        return {
            "mode": "exact",
            "reaped_count": 1,
            **result,
        }
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise


def _reap_due_leases_in_transaction(
    conn: sqlite3.Connection,
    *,
    actor: str = "runtime",
    now: str | None = None,
    batch_size: int = LEASE_REAP_BATCH_SIZE,
) -> dict[str, Any]:
    """Reap due leases inside a caller-owned transaction (no BEGIN/COMMIT)."""
    now = now or utc_now()
    due_leases = _find_due_active_leases(conn, now, batch_size)
    processed: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for lease in due_leases:
        lease_id = lease["lease_id"]
        job_id = lease["job_id"]
        try:
            outcome = _reap_one_lease(conn, lease=lease, actor=actor, now=now)
            if outcome.get("skipped"):
                continue
            processed.append(outcome)
        except Exception as exc:
            errors.append({
                "lease_id": lease_id,
                "job_id": job_id,
                "error": str(exc),
            })

    return {
        "reaped_count": len(processed),
        "due_found": len(due_leases),
        "reaped": processed,
        "errors": errors,
    }


def select_claim_candidate(
    conn: sqlite3.Connection,
    *,
    agent_id: str,
    host_id: str,
    recoverable: bool,
) -> ClaimCandidateResult:
    """Deterministic candidate selection inside one read transaction.

    Orders by ``created_at ASC, id ASC`` with a fixed 256 scan limit. For each
    candidate, full claim authority is validated *before* checking the active
    resource lease; a candidate with forged or missing authority is a hard error
    that stops selection rather than a skippable blocked candidate. The first
    candidate whose authority is valid and whose resource is not actively leased
    is returned, carrying the already-validated authority package.
    """
    statuses = ("pending", "timed_out") if recoverable else ("pending",)
    placeholders = ",".join("?" for _ in statuses)
    rows = conn.execute(
        f"""
        SELECT * FROM jobs
        WHERE status IN ({placeholders}) AND assigned_agent = ?
          AND (status = 'pending' OR recoverable = 1)
        ORDER BY created_at, id
        LIMIT ?
        """,
        (*statuses, agent_id, LEASE_CLAIM_SCAN_LIMIT),
    ).fetchall()

    if not rows:
        return ClaimCandidateResult(job=None, reason="queue_empty")

    # Resolve capacity once per selection.
    from .executor_capacity import resolve_capacity_policy

    policy = resolve_capacity_policy(conn, agent_id)
    if policy is None:
        # P9-3B: fail closed on missing capacity policy for typed agents.
        return ClaimCandidateResult(job=None, reason="capacity_exhausted")
    max_concurrent_jobs = policy["max_concurrent_jobs"]
    active_count = count_active_leases_for_agent(conn, agent_id)
    if active_count >= max_concurrent_jobs:
        return ClaimCandidateResult(job=None, reason="capacity_exhausted")

    oldest_blocked_job_id: str | None = None
    oldest_blocked_resource_key: str | None = None

    for row in rows:
        job = row
        # Authority must be fully validated before the resource check.
        authority = _validate_claim_authority(
            conn, job=job, agent_id=agent_id, host_id=host_id
        )

        active_resource = _find_active_resource_lease(conn, authority.resource_key)
        if active_resource is not None:
            if oldest_blocked_job_id is None:
                oldest_blocked_job_id = job["id"]
                oldest_blocked_resource_key = authority.resource_key
            continue

        return ClaimCandidateResult(
            job=job,
            reason="selected",
            validated_authority=authority,
        )

    # If we exhausted the scan without finding a candidate, report why.
    if len(rows) >= LEASE_CLAIM_SCAN_LIMIT:
        return ClaimCandidateResult(
            job=None,
            reason="scan_limit_reached",
            oldest_blocked_job_id=oldest_blocked_job_id,
            oldest_blocked_resource_key=oldest_blocked_resource_key,
        )
    return ClaimCandidateResult(
        job=None,
        reason="resource_blocked",
        oldest_blocked_job_id=oldest_blocked_job_id,
        oldest_blocked_resource_key=oldest_blocked_resource_key,
    )


def _resolve_claim_context(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    payload: dict[str, Any],
    agent_id: str,
    host_id: str,
) -> ExecutionContextV1:
    """Derive and validate the authoritative ExecutionContextV1 for the selected job.

    ``payload`` is the strict-decoded payload from ``_decode_typed_payload``. Uses
    the stored snapshot if present, otherwise backfills from job/workspace/task
    authority. This function is called inside the claim transaction so the context
    is guaranteed to belong to the row selected inside that transaction.
    """
    workspace = get_workspace(conn, job["workspace_id"])
    if workspace is None:
        raise RuntimeLeaseError(f"workspace {job['workspace_id']!r} not found")
    profile = get_workspace_host_profile(
        conn, workspace_id=job["workspace_id"], host_id=host_id
    )
    if profile is None:
        raise RuntimeLeaseError(
            f"workspace {job['workspace_id']!r} has no host profile for host {host_id!r}"
        )

    task = None
    if job["task_id"]:
        task_row = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            (job["workspace_id"], job["task_id"]),
        ).fetchone()
        if task_row is None:
            raise RuntimeLeaseError(
                f"task mirror not found: {job['workspace_id']}/{job['task_id']}"
            )
        task = row_to_dict(task_row)

    snapshot = payload.get("execution_context")
    origin = payload.get("origin") if isinstance(payload.get("origin"), dict) else {}

    if snapshot is None:
        try:
            return resolve_execution_context_v1(
                job_id=job["id"],
                workspace=workspace,
                task=task,
                assigned_agent=agent_id,
                host_id=host_id,
                profile=profile,
                origin=origin,
                job_branch=job["branch"],
                job_worktree_path=job["worktree_path"],
                job_logs_path=job["logs_path"],
            )
        except ContextError as exc:
            raise RuntimeLeaseError(f"cannot backfill execution context: {exc}") from exc

    try:
        return validate_execution_context_snapshot(
            snapshot,
            job_id=job["id"],
            workspace_id=job["workspace_id"],
            task_id=job["task_id"],
            assigned_agent=agent_id,
            host_id=host_id,
        )
    except ContextError as exc:
        raise RuntimeLeaseError(f"invalid stored execution context: {exc}") from exc


def _validate_binding_snapshot(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    binding_snapshot: Any,
) -> None:
    """Fail closed if the stored executor binding is missing or forged for a typed job."""
    if binding_snapshot is None:
        raise RuntimeLeaseError(f"job {job['id']!r} has no executor_binding snapshot")
    try:
        from .executor_identity import validate_stored_executor_binding

        validate_stored_executor_binding(conn, binding_snapshot, job=dict(job))
    except Exception as exc:
        raise RuntimeLeaseError(f"invalid executor binding: {exc}") from exc

def _validate_claim_reap_policy(
    *, reap_mode: str, reap_reason: str | None
) -> tuple[str, str | None]:
    """Validate claim reap policy and return canonical (mode, reason)."""
    if not isinstance(reap_mode, str) or reap_mode not in {"global", "none"}:
        raise RuntimeLeaseError(
            f"reap_mode must be 'global' or 'none', got {reap_mode!r}"
        )
    if reap_mode == "global":
        if reap_reason is not None:
            raise RuntimeLeaseError("reap_reason must not be set with reap_mode=global")
        return ("global", None)
    # reap_mode == "none"
    if reap_reason is None:
        raise RuntimeLeaseError("reap_reason is required with reap_mode=none")
    _validate_reason(reap_reason, "reap_reason")
    return ("none", reap_reason)


def claim_leased_job(
    conn: sqlite3.Connection,
    *,
    agent_id: str,
    host_id: str,
    recoverable: bool = False,
    ttl_seconds: int = LEASE_DEFAULT_TTL_SECONDS,
    actor: str = "runtime",
    recovery_reason: str | None = None,
    prior_process_stopped: bool | None = None,
    reap_mode: str = "global",
    reap_reason: str | None = None,
) -> ClaimLeaseResult:
    """Atomic claim: reap, select, context, binding/routing, CAS, lease, event, envelope.

    Must be called inside a caller-owned ``BEGIN IMMEDIATE`` transaction. On
    success the caller must commit; on any error the caller must rollback.
    Selection, context backfill/validation, binding/routing validation, job CAS,
    lease reserve, and event append all happen inside that single transaction.
    """
    canonical_mode, canonical_reason = _validate_claim_reap_policy(
        reap_mode=reap_mode, reap_reason=reap_reason
    )
    agent_id = _validate_id(agent_id, "agent_id")
    host_id = _validate_id(host_id, "host_id")
    actor = _validate_id(actor, "actor")

    recovery_evidence = _validate_recovery_evidence(
        recoverable=recoverable,
        recovery_reason=recovery_reason,
        prior_process_stopped=prior_process_stopped,
    )

    ttl_seconds = _validate_ttl(ttl_seconds)
    now = utc_now()

    # 0b. In-transaction authority check (deactivate fence).
    agent_row = conn.execute(
        "SELECT * FROM agents WHERE id = ?", (agent_id,)
    ).fetchone()
    if agent_row is None:
        raise RuntimeLeaseError(f"agent {agent_id!r} not found")
    if agent_row["online_state"] != "online":
        raise RuntimeLeaseError(
            f"agent {agent_id!r} is {agent_row['online_state']}, not online"
        )
    stored_host_id = agent_row["host_id"]
    if not stored_host_id:
        raise RuntimeLeaseError(f"agent {agent_id!r} has no host_id")
    if stored_host_id != host_id:
        raise RuntimeLeaseError(
            f"agent {agent_id!r} host mismatch: stored={stored_host_id!r}, requested={host_id!r}"
        )

    if canonical_mode == "none":
        # Require exact executor binding for none mode.
        if resolve_exact_executor_binding(conn, agent_id) is None:
            raise RuntimeLeaseError(
                f"reap_mode=none requires a typed agent with exact executor binding; "
                f"agent {agent_id!r} has none"
            )

    # 1. Conditional global reap (commit-free inside this transaction).
    if canonical_mode == "global":
        reap_summary = _reap_due_leases_in_transaction(conn, actor=actor, now=now)
        if reap_summary.get("errors"):
            first = reap_summary["errors"][0]
            raise RuntimeLeaseError(f"reap error: {first['error']}")

    # 2. Deterministic candidate selection.
    selection = select_claim_candidate(
        conn, agent_id=agent_id, host_id=host_id, recoverable=recoverable
    )
    if selection.job is None:
        raise RuntimeLeaseError(
            selection.reason,
            oldest_blocked_job_id=selection.oldest_blocked_job_id,
            oldest_blocked_resource_key=selection.oldest_blocked_resource_key,
        )

    job = selection.job
    job_id = job["id"]
    authority = selection.validated_authority
    if authority is None:
        raise RuntimeLeaseError("selected candidate missing validated authority")
    previous_status = job["status"]
    new_attempt_token = int(job["attempt_count"]) + 1

    execution_context = authority.execution_context
    binding_snapshot = authority.binding_snapshot
    route_evidence = authority.route_evidence

    # Persist the context snapshot only for the selected pre-upgrade backfill job.
    payload_json = _json(authority.payload)

    runner_profile_id = job["runner_profile_id"]

    # 7. CAS job to running with incremented attempt token.
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = ?, attempt_count = ?, started_at = ?, last_activity_at = ?,
            recoverable = 0, updated_at = ?, payload_json = ?
        WHERE id = ? AND status = ?
        """,
        (
            "running",
            new_attempt_token,
            now,
            now,
            now,
            payload_json,
            job_id,
            previous_status,
        ),
    )
    if cursor.rowcount == 0:
        raise RuntimeLeaseError("job CAS failed during claim")

    # 8. Reserve the exact attempt lease.
    lease = reserve_attempt_lease(
        conn,
        job_id=job_id,
        attempt_token=new_attempt_token,
        agent_id=agent_id,
        runner_profile_id=runner_profile_id,
        host_id=host_id,
        worktree_path=execution_context.worktree_path,
        ttl_seconds=ttl_seconds,
    )

    # 9. Append job.claimed event with recovery evidence if applicable.
    event_payload: dict[str, Any] = {
        "job_id": job_id,
        "agent_id": agent_id,
        "previous_status": previous_status,
        "recovered": previous_status == "timed_out",
        "execution_context_id": execution_context.context_id,
        "context_version": CONTRACT_VERSION,
        "host_id": execution_context.host_id,
        "worktree_path": execution_context.worktree_path,
        "branch": execution_context.branch,
        "session_scope_id": execution_context.session_scope_id,
        "lease_id": lease["lease_id"],
        "reap_mode": canonical_mode,
        "reap_reason": canonical_reason,
    }
    if recovery_evidence is not None:
        event_payload["recovery_reason"] = recovery_evidence.recovery_reason
        event_payload["prior_process_stopped"] = recovery_evidence.prior_process_stopped
    event_payload.update(executor_binding_claim_evidence(binding_snapshot))
    event_payload.update(route_evidence)

    append_event(
        conn,
        workspace_id=job["workspace_id"],
        event_type="job.claimed",
        actor=agent_id,
        target=agent_id,
        task_id=job["task_id"],
        idempotency_key=f"runtime:job:{job_id}:claimed:{new_attempt_token}",
        payload=event_payload,
        commit=False,
    )

    # 10. Build strict v1 lease envelope.
    lease_row = {
        "lease_id": lease["lease_id"],
        "job_id": job_id,
        "attempt_token": new_attempt_token,
        "agent_id": agent_id,
        "runner_profile_id": runner_profile_id,
        "host_id": host_id,
        "resource_kind": "worktree",
        "resource_key": lease["resource_key"],
        "normalized_path": execution_context.worktree_path,
        "capacity_policy_id": "",
        "max_concurrent_jobs": 1,
    }
    full_lease = conn.execute(
        "SELECT * FROM execution_attempt_leases WHERE lease_id = ?",
        (lease["lease_id"],),
    ).fetchone()
    if full_lease is not None:
        lease_row.update({
            "capacity_policy_id": full_lease["capacity_policy_id"],
            "max_concurrent_jobs": full_lease["max_concurrent_jobs"],
            "acquired_at": full_lease["acquired_at"],
            "expires_at": full_lease["expires_at"],
        })

    # server_now is sampled after reading the committed lease row so it can
    # never be earlier than acquired_at.
    server_now = utc_now()
    execution_lease = build_lease_envelope(
        lease_row,
        server_now=server_now,
        ttl_seconds=ttl_seconds,
        renew_interval_seconds=LEASE_DEFAULT_RENEW_INTERVAL_SECONDS,
    )

    claimed_job = get_job(conn, job_id)
    if claimed_job is None:
        raise RuntimeLeaseError("claimed job disappeared")
    return ClaimLeaseResult(
        lease_id=lease["lease_id"],
        job=claimed_job,
        attempt_token=new_attempt_token,
        execution_context=execution_context,
        execution_lease=execution_lease,
    )


def require_managed_attempt_lease(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    attempt_token: int,
    agent_id: str,
    lease_id: str | None = None,
) -> dict[str, Any] | None:
    """Return the lease for a managed attempt, or raise.

    If ``lease_id`` is provided, require the exact lease tuple and that the
    lease is active and currently unexpired. If the stored lease is already
    released/expired/due, the managed attempt is no longer active and we raise.
    If no lease row exists for (job_id, attempt_token), return None to allow
    the legacy path.
    """
    if not attempt_has_any_lease(conn, job_id, attempt_token):
        return None
    if lease_id is None:
        raise RuntimeLeaseError(
            f"managed attempt {job_id}/{attempt_token} requires lease_id"
        )
    try:
        return require_active_unexpired_lease(
            conn,
            lease_id=lease_id,
            job_id=job_id,
            attempt_token=attempt_token,
            agent_id=agent_id,
        )
    except LeaseError as exc:
        raise RuntimeLeaseError(str(exc)) from exc


def require_mutation_authority(
    conn: sqlite3.Connection,
    *,
    current_attempt_token: int,
    supplied_attempt_token: int | None,
    job_id: str,
    agent_id: str,
    lease_id: str | None,
) -> dict[str, Any] | None:
    """Shared managed-or-legacy mutation authority check.

    Looks at the authoritative current attempt. If it has no lease row, this is
    the legacy unleased path and None is returned. If it has a lease row, the
    supplied attempt_token must be present, must equal the current attempt, and
    the exact lease must be active and unexpired via
    ``require_managed_attempt_lease``.
    """
    if not attempt_has_any_lease(conn, job_id, current_attempt_token):
        return None
    if supplied_attempt_token is None:
        raise RuntimeLeaseError(
            f"managed attempt {job_id}/{current_attempt_token} requires attempt_token"
        )
    if supplied_attempt_token != current_attempt_token:
        raise RuntimeLeaseError(
            f"managed attempt {job_id}/{current_attempt_token} received stale attempt_token {supplied_attempt_token}"
        )
    return require_managed_attempt_lease(
        conn,
        job_id=job_id,
        attempt_token=current_attempt_token,
        agent_id=agent_id,
        lease_id=lease_id,
    )


def release_lease_for_terminal_report(
    conn: sqlite3.Connection,
    *,
    lease_id: str,
    job_id: str,
    attempt_token: int,
    agent_id: str,
    status: str,
) -> dict[str, Any]:
    """Release the exact lease as part of a terminal report transaction."""
    return release_attempt_lease(
        conn,
        lease_id=lease_id,
        job_id=job_id,
        attempt_token=attempt_token,
        agent_id=agent_id,
        reason=_lease_release_reason(status),
    )


def renew_managed_lease(
    conn: sqlite3.Connection,
    *,
    lease_id: str,
    job_id: str,
    attempt_token: int,
    agent_id: str,
    ttl_seconds: int = LEASE_DEFAULT_TTL_SECONDS,
) -> dict[str, Any]:
    """Renew one managed lease and require the running job attempt match.

    Runs as a single ``BEGIN IMMEDIATE`` transaction: validate job state,
    validate active/unexpired lease, delegate to ``renew_attempt_lease`` for the
    row update, build the response containing Coordinate ``server_now`` and
    ``expires_at``, commit.  On any error the transaction is rolled back.
    """
    ttl_seconds = _validate_ttl(ttl_seconds)
    conn.execute("BEGIN IMMEDIATE")
    try:
        job = get_job(conn, job_id)
        if job is None or job["status"] != "running" or job["attempt_count"] != attempt_token:
            raise RuntimeLeaseError(
                f"lease renewal rejected: job {job_id} not running as attempt {attempt_token}"
            )
        # Delegate the strict lease update to the commit-free primitive.
        lease = renew_attempt_lease(
            conn,
            lease_id=lease_id,
            job_id=job_id,
            attempt_token=attempt_token,
            agent_id=agent_id,
            ttl_seconds=ttl_seconds,
        )
        # server_now is sampled after the primitive update so it can never be
        # earlier than renewed_at.
        server_now = utc_now()
        result = {
            "lease_id": lease_id,
            "job_id": job_id,
            "attempt_token": attempt_token,
            "agent_id": agent_id,
            "status": "active",
            "server_now": server_now,
            "expires_at": lease["expires_at"],
            "renewed_at": lease["renewed_at"],
        }
        conn.commit()
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    return result


def append_terminal_events_and_delivery(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    job_id: str,
    agent_id: str,
    status: str,
    result: dict[str, Any],
    actor: str | None,
) -> dict[str, Any]:
    """Append terminal, agent-report, review events and create delivery.

    All durable writes use ``commit=False``; the caller owns the transaction.
    """
    event_type = _terminal_event_type(status)
    event = append_event(
        conn,
        workspace_id=job["workspace_id"],
        event_type=event_type,
        actor=actor or agent_id,
        target=agent_id,
        task_id=job["task_id"],
        idempotency_key=f"runtime:job:{job_id}:result:{status}",
        payload={"job_id": job_id, "agent_id": agent_id, "status": status, **result},
        commit=False,
    )

    outcome = _parse_job_agent_report(result=result, job=job)
    payload_agent_reported: dict[str, Any] = {
        "source": "runtime",
        "job_id": job_id,
        "agent_id": agent_id,
        "status": status,
        "action": "done" if status == "done" else "blocker",
    }
    if outcome["decision"]:
        payload_agent_reported["decision"] = outcome["decision"]
    if outcome["result_summary"]:
        payload_agent_reported["result_summary"] = outcome["result_summary"]
    append_event(
        conn,
        workspace_id=job["workspace_id"],
        event_type="agent.reported",
        actor=actor or agent_id,
        target=agent_id,
        task_id=job["task_id"],
        idempotency_key=f"runtime:job:{job_id}:agent-reported:{status}:{outcome['decision'] or 'nodecision'}",
        payload=payload_agent_reported,
        commit=False,
    )

    if outcome["decision"] in {"approve", "reject"}:
        review_event_type = "review.completed" if outcome["decision"] == "approve" else "review.rejected"
        parsed: AgentReport | None = outcome["parsed"]
        append_event(
            conn,
            workspace_id=job["workspace_id"],
            event_type=review_event_type,
            actor=actor or agent_id,
            target=None,
            task_id=job["task_id"],
            idempotency_key=f"runtime:job:{job_id}:review-decision:{outcome['decision']}",
            payload={
                "reviewer": agent_id,
                "decision": outcome["decision"],
                "reason": parsed.reason if parsed else None,
                "summary": (parsed.summary if parsed else None) or outcome["result_summary"],
                "source": "runtime",
                "job_id": job_id,
            },
            commit=False,
        )

    delivery, delivery_created = _create_response_delivery(
        conn,
        job=get_job(conn, job_id),
        event_id=event.row["id"],
        result=result,
    )
    return {
        "event": row_to_dict(event.row),
        "event_created": event.created,
        "delivery": row_to_dict(delivery) if delivery is not None else None,
        "delivery_created": delivery_created,
    }


def _create_response_delivery(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row | None,
    event_id: str,
    result: dict[str, Any],
) -> tuple[Any, bool]:
    from .db import get_workspace

    if job is None:
        return None, False
    payload = _job_payload(job)
    reply = payload.get("reply")
    if not isinstance(reply, dict):
        return None, False
    text = result.get("response_text") or result.get("text") or result.get("summary")
    if not isinstance(text, str) or not text.strip():
        return None, False
    platform = reply.get("platform")
    destination = reply.get("destination")
    if not platform or not destination:
        return None, False
    workspace = get_workspace(conn, job["workspace_id"])
    default_bus = workspace.default_bus if workspace else None
    if platform == "discord" and default_bus != "discord":
        platform = "discord_webhook"
    if platform == "none":
        return None, False
    message_key = f"runtime:job:{job['id']}:response:{event_id}:{platform}:{destination}"
    delivery, created = create_delivery(
        conn,
        event_id=event_id,
        platform=platform,
        destination=str(destination),
        message_key=message_key,
        payload={
            "text": text,
            "workspace_id": job["workspace_id"],
            "task_id": job["task_id"],
            "job_id": job["id"],
            "origin": payload.get("origin"),
        },
        commit=False,
    )
    return delivery, created
