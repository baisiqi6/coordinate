"""Issue #11 trace projection R1: read-only typed projection from existing authority.

Rebuilds a task/job relational trace purely from stored rows (jobs, tasks,
events, execution_attempt_leases, deliveries). No projection table, cache, or
schema is added; the builder only SELECTs.

Privacy boundary: the projection contains metadata/locators only. It never
emits prompts, responses, delivery bodies, full result payloads, env, tokens,
transcripts, or reasoning. Progress carries exactly ``stage``/``session_id``/
``last_activity_at`` (no free-text ``summary``).
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Literal, NotRequired, TypedDict

from .db_support import utc_now
from .operator import list_pending_actions

TRACE_CONTRACT_VERSION = 1
DEFAULT_HISTORY_LIMIT = 20
MAX_HISTORY_LIMIT = 100

# Bounded upper edge for live-job candidates. Live jobs may exceed the task
# history limit; ambiguity detection must still see them, so this query keeps
# its own fixed bound instead of loading unbounded history.
_MAX_LIVE_CANDIDATES = MAX_HISTORY_LIMIT

LIVE_JOB_STATUSES = ("pending", "running")
_TERMINAL_JOB_STATUSES = ("done", "failed", "cancelled")

_CI_EVENT_TYPES = ("ci.passed", "ci.failed", "ci.pending")
_PR_REVIEW_EVENT_TYPES = (
    "pr_review.approved",
    "pr_review.changes_requested",
    "pr_review.required",
)


# -- public typed contract (TraceProjectionV1) ------------------------------


EvidenceState = Literal[
    "present", "missing", "unknown", "unavailable", "stale", "failed"
]


class TraceQuery(TypedDict):
    kind: str
    workspace_id: NotRequired[str]
    task_id: NotRequired[str]
    job_id: NotRequired[str]
    history_limit: NotRequired[int]


class WorkspaceEvidence(TypedDict):
    state: EvidenceState
    workspace_id: NotRequired[str]
    name: NotRequired[str]


class PlanLocator(TypedDict):
    state: EvidenceState
    source: NotRequired[str]
    plan_path: NotRequired[str]


class MirrorMetadata(TypedDict):
    source: str
    harness_refreshed: bool
    may_be_stale: bool
    mirror_updated_at: str | None


class TaskEvidence(TypedDict):
    state: EvidenceState
    task_id: NotRequired[str]
    phase: NotRequired[str | None]
    owner: NotRequired[str | None]
    branch: NotRequired[str | None]
    pr: NotRequired[str | None]
    plan_locator: NotRequired[PlanLocator]
    mirror: NotRequired[MirrorMetadata]


class JobSummary(TypedDict):
    job_id: str
    state: EvidenceState
    status: str
    attempt_count: int
    created_at: str | None
    completed_at: str | None


class JobsSection(TypedDict):
    total: int
    history_limit: int
    history_truncated: bool
    summaries: list[JobSummary]


class Selection(TypedDict, total=False):
    kind: str
    job_id: str
    candidate_job_ids: list[str]


class ExecutionSection(TypedDict, total=False):
    selection: Selection
    job: dict[str, Any]
    outcome: dict[str, Any]
    executor: dict[str, Any]
    execution_context: dict[str, Any]
    provider_session: dict[str, Any]
    last_progress: dict[str, Any]
    attempt_leases: dict[str, Any]
    delivery: dict[str, Any]


class ForgeSection(TypedDict, total=False):
    state: EvidenceState
    pr: str
    ci: dict[str, Any]
    pr_review: dict[str, Any]
    result_review: dict[str, Any]


class NextGate(TypedDict, total=False):
    state: EvidenceState
    source: str
    action: str


class TraceDiagnostic(TypedDict):
    classification: str
    component: str
    reason: str
    locator: dict[str, Any]


class TraceProjectionV1(TypedDict):
    """Typed contract for the Issue #11 read-only trace projection."""

    contract_version: int
    query: TraceQuery
    generated_at: str
    workspace: WorkspaceEvidence
    task: TaskEvidence
    jobs: JobsSection
    execution: ExecutionSection
    forge: ForgeSection
    next_gate: NextGate
    diagnostics: list[TraceDiagnostic]


class TraceQueryError(ValueError):
    """Unknown workspace/task/job, guard mismatch, or invalid bound parameter."""


def build_task_trace(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    history_limit: int | None = None,
    now: str | None = None,
) -> TraceProjectionV1:
    """Project the typed trace for one task. Raises TraceQueryError on bad ids."""
    limit = _normalize_history_limit(history_limit)
    generated_at = now or utc_now()
    workspace = _workspace_projection(conn, workspace_id)
    task = _task_projection(conn, workspace_id, task_id)
    if task["state"] == "missing":
        raise TraceQueryError(f"unknown task: {task_id} in workspace {workspace_id}")
    # Bounded history: an exact COUNT(*) for jobs.total plus a LIMIT-bounded
    # summary query; live jobs (which may exceed the history limit) get their
    # own bounded query so ambiguity detection never misses candidates.
    total = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()[0]
    summary_rows = conn.execute(
        """
        SELECT * FROM jobs
        WHERE workspace_id = ? AND task_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (workspace_id, task_id, limit),
    ).fetchall()
    live_placeholders = ",".join("?" for _ in LIVE_JOB_STATUSES)
    live_rows = conn.execute(
        f"""
        SELECT * FROM jobs
        WHERE workspace_id = ? AND task_id = ?
          AND (status IN ({live_placeholders})
               OR (status = 'timed_out' AND recoverable = 1))
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (workspace_id, task_id, *LIVE_JOB_STATUSES, _MAX_LIVE_CANDIDATES),
    ).fetchall()
    terminal_row = None
    if not live_rows:
        terminal_placeholders = ",".join("?" for _ in _TERMINAL_JOB_STATUSES)
        terminal_row = conn.execute(
            f"""
            SELECT * FROM jobs
            WHERE workspace_id = ? AND task_id = ?
              AND (status IN ({terminal_placeholders})
                   OR (status = 'timed_out' AND recoverable = 0))
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (workspace_id, task_id, *_TERMINAL_JOB_STATUSES),
        ).fetchone()
    selection = _task_selection(live_rows, terminal_row)
    selected_row = next(
        (
            row
            for row in (*live_rows, terminal_row)
            if row is not None and row["id"] == selection.get("job_id")
        ),
        None,
    )
    return _assemble(
        conn,
        query={
            "kind": "task",
            "workspace_id": workspace_id,
            "task_id": task_id,
            "history_limit": limit,
        },
        generated_at=generated_at,
        workspace=workspace,
        task=task,
        total=int(total),
        history_limit=limit,
        summaries=[_job_summary(row) for row in summary_rows],
        selection=selection,
        selected_row=selected_row,
    )


def build_job_trace(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    workspace_id: str | None = None,
    now: str | None = None,
) -> TraceProjectionV1:
    """Project the typed trace for one job, reverse-resolving its task."""
    generated_at = now or utc_now()
    job_row = conn.execute(
        "SELECT * FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    if job_row is None:
        raise TraceQueryError(f"unknown job: {job_id}")
    if workspace_id is not None and job_row["workspace_id"] != workspace_id:
        raise TraceQueryError(
            f"job {job_id} does not belong to workspace {workspace_id}"
        )
    selected_workspace_id = job_row["workspace_id"]
    if selected_workspace_id is None:
        workspace = {"state": "unknown"}
        task = {"state": "unknown", "task_id": job_row["task_id"]}
    else:
        workspace = _workspace_projection(conn, selected_workspace_id)
        task = _task_projection(conn, selected_workspace_id, job_row["task_id"])
    # Exact job query: the selection is the operator's input id, never a
    # lifecycle-inferred current/latest guess.
    return _assemble(
        conn,
        query={
            "kind": "job",
            "job_id": job_id,
            **({"workspace_id": workspace_id} if workspace_id is not None else {}),
        },
        generated_at=generated_at,
        workspace=workspace,
        task=task,
        total=1,
        history_limit=1,
        summaries=[_job_summary(job_row)],
        selection={"kind": "exact", "job_id": job_id},
        selected_row=job_row,
        orphan=selected_workspace_id is None,
    )


# -- selection and assembly ------------------------------------------------


def _assemble(
    conn: sqlite3.Connection,
    *,
    query: TraceQuery,
    generated_at: str,
    workspace: dict[str, Any],
    task: dict[str, Any],
    total: int,
    history_limit: int,
    summaries: list[JobSummary],
    selection: Selection,
    selected_row: sqlite3.Row | None,
    orphan: bool = False,
) -> TraceProjectionV1:
    diagnostics: list[TraceDiagnostic] = []
    if orphan:
        diagnostics.append({
            "classification": "orphan_workspace",
            "component": "workspace",
            "reason": "job.workspace_id is NULL (workspace row deleted)",
            "locator": {"job_id": query.get("job_id")},
        })
    if selection["kind"] == "ambiguous":
        diagnostics.append({
            "classification": "ambiguous_current_job",
            "component": "job",
            "reason": "multiple live jobs; use the exact job query",
            "locator": {"candidate_job_ids": list(selection["candidate_job_ids"])},
        })

    execution: ExecutionSection = {"selection": selection}
    if selected_row is not None:
        execution.update(_execution_projection(conn, selected_row, generated_at))

    has_task = task["state"] not in {"unavailable", "unknown"}
    trace: TraceProjectionV1 = {
        "contract_version": TRACE_CONTRACT_VERSION,
        "query": query,
        "generated_at": generated_at,
        "workspace": workspace,
        "task": task,
        "jobs": {
            "total": total,
            "history_limit": history_limit,
            "history_truncated": total > history_limit,
            "summaries": summaries,
        },
        "execution": execution,
        "next_gate": _next_gate(conn, workspace, task, selected_row),
        "diagnostics": diagnostics,
    }
    if has_task:
        trace["forge"] = _forge_projection(conn, workspace.get("workspace_id"), task)
    else:
        trace["forge"] = {"state": "unavailable"}
    return trace


def _task_selection(
    live_rows: list[sqlite3.Row], terminal_row: sqlite3.Row | None
) -> Selection:
    if len(live_rows) == 1:
        return {"kind": "current", "job_id": live_rows[0]["id"]}
    if len(live_rows) > 1:
        return {
            "kind": "ambiguous",
            "candidate_job_ids": [row["id"] for row in live_rows],
        }
    if terminal_row is not None:
        return {"kind": "latest", "job_id": terminal_row["id"]}
    return {"kind": "none"}


def _job_summary(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "job_id": row["id"],
        "state": "present",
        "status": row["status"],
        "attempt_count": row["attempt_count"],
        "created_at": row["created_at"],
        "completed_at": row["completed_at"],
    }


# -- per-section projections ----------------------------------------------


def _workspace_projection(conn: sqlite3.Connection, workspace_id: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT id, name FROM workspaces WHERE id = ?", (workspace_id,)
    ).fetchone()
    if row is None:
        raise TraceQueryError(f"unknown workspace: {workspace_id}")
    return {"state": "present", "workspace_id": row["id"], "name": row["name"]}


def _task_projection(
    conn: sqlite3.Connection, workspace_id: str, task_id: str | None
) -> dict[str, Any]:
    if task_id is None:
        return {"state": "unavailable"}
    row = conn.execute(
        """
        SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?
        """,
        (workspace_id, task_id),
    ).fetchone()
    if row is None:
        return {"state": "missing", "task_id": task_id}
    payload = _decode(row["payload_json"])
    return {
        "state": "present",
        "task_id": task_id,
        "phase": row["phase"],
        "owner": row["owner"],
        "branch": row["branch"],
        "pr": row["pr"],
        "plan_locator": _plan_locator(payload),
        "mirror": {
            "source": "task_mirror",
            "harness_refreshed": False,
            "may_be_stale": True,
            "mirror_updated_at": row["updated_at"],
        },
    }


def _plan_locator(payload: dict[str, Any]) -> PlanLocator:
    """Resolve the canonical plan locator from real task-mirror write paths.

    ``plan_doc`` is the primary authority field: the coordinator record paths
    (``task create``/``create-record`` and issue materialize) write it, with
    ``absolute_plan_doc`` alongside. Harness reconcile stores the checklist
    item payload verbatim, whose locator keys are ``plan_path`` with
    ``artifacts.plan`` as a strict alias (always equal to ``plan_path``).
    Merged mirrors can carry both shapes; the order below is fixed.
    """
    artifacts = payload.get("artifacts")
    candidates = (
        ("plan_doc", payload.get("plan_doc")),
        ("plan_path", payload.get("plan_path")),
        ("artifacts.plan", artifacts.get("plan") if isinstance(artifacts, dict) else None),
    )
    for source, value in candidates:
        if isinstance(value, str) and value:
            return {"state": "present", "source": source, "plan_path": value}
    return {"state": "missing"}


def _execution_projection(
    conn: sqlite3.Connection, job: sqlite3.Row, generated_at: str
) -> dict[str, Any]:
    payload = _decode(job["payload_json"])
    ctx = payload.get("execution_context") if isinstance(payload, dict) else None
    binding = payload.get("executor_binding") if isinstance(payload, dict) else None
    status = job["status"]
    attempt = int(job["attempt_count"] or 0)
    return {
        "job": {
            "state": "present",
            "job_id": job["id"],
            "status": status,
            "attempt_count": attempt,
            "assigned_agent": job["assigned_agent"],
            "runner_profile_id": job["runner_profile_id"],
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
            "last_activity_at": job["last_activity_at"],
        },
        "outcome": {"state": "failed" if status == "failed" else "present", "status": status},
        "executor": _executor_projection(binding),
        "execution_context": _context_projection(ctx),
        "provider_session": _session_projection(
            job, _progress_session_id(conn, job)
        ),
        "last_progress": _progress_projection(conn, job, attempt),
        "attempt_leases": _leases_projection(conn, job, attempt, generated_at),
        "delivery": _delivery_projection(conn, job["id"]),
    }


def _executor_projection(binding: Any) -> dict[str, Any]:
    if not isinstance(binding, dict) or not binding.get("provider"):
        return {"state": "unknown"}
    return {
        "state": "present",
        "source": "stored_snapshot",
        "provider": binding.get("provider"),
        "adapter": binding.get("adapter"),
        "executor_definition_id": binding.get("executor_definition_id"),
    }


def _context_projection(ctx: Any) -> dict[str, Any]:
    if not isinstance(ctx, dict) or not ctx.get("host_id"):
        return {"state": "unknown"}
    return {
        "state": "present",
        "host_id": ctx.get("host_id"),
        "worktree_path": ctx.get("worktree_path"),
        "branch": ctx.get("branch"),
        "session_scope_id": ctx.get("session_scope_id"),
    }

def _session_projection(
    job: sqlite3.Row, progress_session_id: str | None
) -> dict[str, Any]:
    terminal = job["terminal_session_id"] or None
    stored = _decode(job["progress_json"]).get("session_id") or None
    sources = [
        {"source": "jobs.terminal_session_id", "session_id": terminal},
        {"source": "jobs.progress_json.session_id", "session_id": stored},
        {"source": "job.progress.session_id", "session_id": progress_session_id},
    ]
    result = _decode(job["result_json"])
    timeout = result.get("timeout")
    # Only these fixed session locators are metadata; never project result content.
    for source, candidate in (
        ("jobs.result_json.timeout.session_id", timeout.get("session_id") if isinstance(timeout, dict) else None),
        ("jobs.result_json.session_id", result.get("session_id")),
    ):
        if isinstance(candidate, str) and candidate.strip():
            sources.append({"source": source, "session_id": candidate.strip()})
    sources = [s for s in sources if s["session_id"]]
    if not sources or len({s["session_id"] for s in sources}) > 1:
        return {"state": "unknown", "sources": sources}
    return {"state": "present", "source": sources[0]["source"], "session_id": sources[0]["session_id"]}


def _current_attempt_claim(
    conn: sqlite3.Connection, job_id: str, attempt_token: int
) -> sqlite3.Row | None:
    if attempt_token <= 0:
        return None
    return conn.execute(
        "SELECT created_at FROM events WHERE idempotency_key = ?",
        (f"runtime:job:{job_id}:claimed:{attempt_token}",),
    ).fetchone()


def _progress_session_id(conn: sqlite3.Connection, job: sqlite3.Row) -> str | None:
    event = _latest_job_progress(conn, job["id"])
    if event is None:
        return None
    payload = _decode(event["payload_json"])
    return payload.get("session_id") if isinstance(payload, dict) else None


def _latest_job_progress(
    conn: sqlite3.Connection, job_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM events WHERE event_type = 'job.progress' AND "
        "json_extract(payload_json, '$.job_id') = ? "
        "ORDER BY created_at DESC, rowid DESC LIMIT 1",
        (job_id,),
    ).fetchone()


def _progress_projection(
    conn: sqlite3.Connection, job: sqlite3.Row, attempt: int
) -> dict[str, Any]:
    event = _latest_job_progress(conn, job["id"])
    if event is None:
        return {"state": "unavailable"}
    claim = _current_attempt_claim(conn, job["id"], attempt)
    if claim is not None and event["created_at"] < claim["created_at"]:
        return {
            "state": "stale",
            "reason": "latest job.progress predates the exact current-attempt claim",
            "attempt_token": attempt,
        }
    payload = _decode(event["payload_json"])
    stage = payload.get("stage") if isinstance(payload, dict) else None
    session_id = payload.get("session_id") if isinstance(payload, dict) else None
    last_activity = payload.get("last_activity_at") if isinstance(payload, dict) else None
    if stage is None and session_id is None and last_activity is None:
        return {"state": "unavailable"}
    evidence: dict[str, Any] = {
        "state": "present",
        "source": "job.progress",
        **({"stage": stage} if stage is not None else {}),
        **({"session_id": session_id} if session_id is not None else {}),
        **({"last_activity_at": last_activity} if last_activity is not None else {}),
    }
    return evidence


def _leases_projection(
    conn: sqlite3.Connection, job: sqlite3.Row, attempt: int, generated_at: str
) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT * FROM execution_attempt_leases
        WHERE job_id = ? ORDER BY attempt_token ASC
        """,
        (job["id"],),
    ).fetchall()
    typed = _decode(job["payload_json"]).get("executor_binding") is not None
    if not rows:
        expected = attempt > 0 and typed and job["status"] == "running"
        return {
            "state": "missing" if expected else "unavailable",
            "leases": [],
        }
    current_lease_id = None
    leases = []
    for row in rows:
        state: str = "present"
        if row["status"] == "active" and row["expires_at"] <= generated_at:
            state = "stale"
        evidence = {
            "state": state,
            "lease_id": row["lease_id"],
            "attempt_token": row["attempt_token"],
            "status": row["status"],
            "expires_at": row["expires_at"],
            "host_id": row["host_id"],
            "resource_kind": row["resource_kind"],
            "resource_key": row["resource_key"],
        }
        leases.append(evidence)
        if (
            row["status"] == "active"
            and int(row["attempt_token"]) == attempt
            and state == "present"
        ):
            current_lease_id = row["lease_id"]
    return {
        "state": "present",
        "current_lease_id": current_lease_id,
        "leases": leases,
    }


def _delivery_projection(conn: sqlite3.Connection, job_id: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT d.* FROM deliveries d
        JOIN events e ON e.id = d.event_id
        WHERE e.event_type IN ('job.completed', 'job.failed', 'job.timed_out')
          AND json_extract(e.payload_json, '$.job_id') = ?
        ORDER BY d.created_at DESC, d.id DESC
        """,
        (job_id,),
    ).fetchall()
    if not rows:
        return {"state": "unavailable", "deliveries": []}
    return {
        "state": "present",
        "deliveries": [
            {
                "state": "failed" if row["status"] == "failed" else "present",
                "delivery_id": row["id"],
                "platform": row["platform"],
                "destination": row["destination"],
                "status": row["status"],
                "platform_message_id": row["platform_message_id"],
            }
            for row in rows
        ],
    }


def _latest_event(
    conn: sqlite3.Connection,
    workspace_id: str | None,
    task_id: str,
    event_types: tuple[str, ...],
    pr: str | None = None,
) -> sqlite3.Row | None:
    placeholders = ",".join("?" for _ in event_types)
    # ``pr`` calibrates the latest-event query to the task's current PR: CI
    # events record the exact task-mirror PR at write time (ci.py resolves the
    # event PR from the mirror), so a superseded PR's event is excluded here
    # instead of being mislabeled as evidence for the current PR.
    pr_filter = " AND json_extract(payload_json, '$.pr') = ?" if pr else ""
    return conn.execute(
        f"""
        SELECT * FROM events
        WHERE workspace_id = ? AND task_id = ?
          AND event_type IN ({placeholders}){pr_filter}
        ORDER BY created_at DESC, rowid DESC
        LIMIT 1
        """,
        (workspace_id, task_id, *event_types, *((pr,) if pr else ())),
    ).fetchone()


def _forge_projection(
    conn: sqlite3.Connection, workspace_id: str | None, task: dict[str, Any]
) -> dict[str, Any]:
    pr = task.get("pr")
    task_id = task.get("task_id")
    if not pr:
        return {"state": "unavailable"}

    ci_row = _latest_event(conn, workspace_id, task_id, _CI_EVENT_TYPES, pr=pr)
    review_row = _latest_event(conn, workspace_id, task_id, _PR_REVIEW_EVENT_TYPES)
    result_review_row = _latest_event(
        conn,
        workspace_id,
        task_id,
        ("review.completed", "review.rejected"),
    )

    forge: dict[str, Any] = {
        "state": "present",
        "pr": pr,
        "ci": _ci_evidence(ci_row),
        "pr_review": _review_evidence(review_row, ci_row, pr),
        "result_review": _result_review_evidence(result_review_row),
    }
    return forge


def _ci_evidence(ci_row: sqlite3.Row | None) -> dict[str, Any]:
    if ci_row is None:
        return {"state": "unavailable"}
    payload = _decode(ci_row["payload_json"])
    event_type = ci_row["event_type"]
    outcome = event_type.split(".", 1)[1]
    return {
        "state": "failed" if outcome == "failed" else "present",
        "status": outcome,
        "event_type": event_type,
        "created_at": ci_row["created_at"],
        "pr": payload.get("pr"),
        "head_sha": payload.get("head_sha"),
    }


def _review_evidence(
    review_row: sqlite3.Row | None, ci_row: sqlite3.Row | None, task_pr: str
) -> dict[str, Any]:
    if review_row is None:
        return {"state": "unavailable"}
    payload = _decode(review_row["payload_json"])
    decision = review_row["event_type"].split(".", 1)[1]
    review_pr = payload.get("pr")
    if review_pr != task_pr:
        # Review evidence belongs to a superseded PR; for the current PR it is
        # absent, not stale (per plan C3: no cross-PR stale inference).
        return {
            "state": "unavailable",
            "reason": "pr_review event is for a different PR than the task's current PR",
            "review_decision": decision,
            "created_at": review_row["created_at"],
            "pr": review_pr,
        }
    if ci_row is not None:
        ci_payload = _decode(ci_row["payload_json"])
        same_pr = ci_payload.get("pr") == payload.get("pr")
        ci_head = ci_payload.get("head_sha")
        review_head = payload.get("head_sha")
        if (
            same_pr
            and isinstance(ci_head, str)
            and isinstance(review_head, str)
            and ci_head != review_head
        ):
            return {
                "state": "stale",
                "reason": "same-PR CI and PR-review head_sha mismatch",
                "review_decision": decision,
                "created_at": review_row["created_at"],
                "pr": payload.get("pr"),
                "head_sha": review_head,
            }
    return {
        "state": "present",
        "event_type": review_row["event_type"],
        "review_decision": decision,
        "created_at": review_row["created_at"],
        "pr": review_pr,
        "head_sha": payload.get("head_sha"),
    }


def _result_review_evidence(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {"state": "unavailable"}
    payload = _decode(row["payload_json"])
    return {
        "state": "present",
        "event_type": row["event_type"],
        "decision": payload.get("decision"),
        "reviewer": payload.get("reviewer"),
        "created_at": row["created_at"],
    }


def _next_gate(
    conn: sqlite3.Connection,
    workspace: dict[str, Any],
    task: dict[str, Any],
    selected_row: sqlite3.Row | None,
) -> NextGate:
    workspace_id = workspace.get("workspace_id")
    task_id = task.get("task_id")
    if workspace_id is not None and task_id is not None:
        for action in list_pending_actions(conn, workspace_id=workspace_id):
            if action.task_id == task_id:
                return {
                    "state": "present",
                    "source": "operator.pending",
                    "action": action.action,
                }
    if selected_row is None:
        return {"state": "unknown"}
    status = selected_row["status"]
    if status == "pending":
        action = "claim"
    elif status == "running":
        action = "progress-or-result"
    elif status == "timed_out" and selected_row["recoverable"]:
        action = "recover"
    elif status == "done":
        action = "operator-review"
    else:
        return {"state": "unknown"}
    return {"state": "present", "source": "job_lifecycle", "action": action}


# -- helpers ---------------------------------------------------------------


def _normalize_history_limit(history_limit: int | None) -> int:
    if history_limit is None:
        return DEFAULT_HISTORY_LIMIT
    if isinstance(history_limit, bool) or not isinstance(history_limit, int):
        raise TraceQueryError("history_limit must be an integer")
    if not 1 <= history_limit <= MAX_HISTORY_LIMIT:
        raise TraceQueryError(
            f"history_limit must be between 1 and {MAX_HISTORY_LIMIT}"
        )
    return history_limit


def _decode(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        decoded = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}
