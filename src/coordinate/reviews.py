from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any

from coordinate.db import append_event, get_workspace, row_to_dict
from coordinate.github import query_pr_head_sha


@dataclass(frozen=True)
class PrReviewResult:
    workspace_id: str
    task_id: str
    pr_url: str
    head_sha: str
    review_decision: str  # "approved", "changes_requested", "review_required"
    event: dict[str, Any] | None
    event_created: bool
    existing: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "pr_url": self.pr_url,
            "head_sha": self.head_sha,
            "review_decision": self.review_decision,
            "event": self.event,
            "event_created": self.event_created,
            "existing": self.existing,
        }


@dataclass(frozen=True)
class MergeGateResult:
    workspace_id: str
    task_id: str
    ready: bool
    human_gate_required: bool  # always True
    checks: dict[str, dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "ready": self.ready,
            "human_gate_required": self.human_gate_required,
            "checks": self.checks,
        }


_REVIEW_DECISION_MAP: dict[str | None, str] = {
    "APPROVED": "approved",
    "CHANGES_REQUESTED": "changes_requested",
}


def _query_pr_review(
    workspace_path: str,
    pr_url: str,
    *,
    run: object = subprocess.run,
) -> str:
    try:
        proc = run(
            ["gh", "pr", "view", pr_url, "--json", "reviewDecision"],
            timeout=30,
            check=False,
            capture_output=True,
            text=True,
            cwd=workspace_path,
        )
    except FileNotFoundError:
        raise ValueError("gh CLI not available")

    if proc.returncode != 0:
        raise ValueError(f"gh pr view failed: {proc.stderr}")

    parsed: dict | None = None
    try:
        raw = json.loads(proc.stdout)
        if isinstance(raw, dict):
            parsed = raw
    except (json.JSONDecodeError, TypeError):
        pass

    if parsed is None:
        raise ValueError("gh pr view returned invalid JSON")

    decision = parsed.get("reviewDecision")
    return _REVIEW_DECISION_MAP.get(decision, "review_required")


def check_pr_review(
    conn,
    workspace_id: str,
    task_id: str,
    *,
    pr_url: str | None = None,
    branch: str | None = None,
    actor: str = "operator",
    run: object = subprocess.run,
) -> PrReviewResult:
    # 1. Resolve workspace
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")

    # 2. Query existing task mirror
    existing_row = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()

    # 3. Resolve PR URL
    mirror_pr = existing_row["pr"] if existing_row is not None else None
    if pr_url is not None:
        if mirror_pr is not None and pr_url != mirror_pr:
            raise ValueError(
                f"task {task_id} already has pr '{mirror_pr}', cannot check review for '{pr_url}'"
            )
        resolved_pr = pr_url
    else:
        if not mirror_pr:
            raise ValueError(f"task {task_id} has no PR; cannot check review")
        resolved_pr = mirror_pr

    # 4. Resolve branch
    resolved_branch = branch or (
        existing_row["branch"] if existing_row is not None else None
    )

    # 5. Query PR review decision
    review_decision = _query_pr_review(workspace.path, resolved_pr, run=run)
    head_sha = query_pr_head_sha(workspace.path, resolved_pr, run=run)

    # 6. Find latest pr_review.* event
    latest = conn.execute(
        "SELECT * FROM events WHERE workspace_id=? AND task_id=? "
        "AND event_type IN ('pr_review.approved','pr_review.changes_requested','pr_review.required') "
        "ORDER BY created_at DESC, rowid DESC LIMIT 1",
        (workspace_id, task_id),
    ).fetchone()

    # 7. Dedup
    if latest is not None:
        latest_payload = json.loads(latest["payload_json"])
        if (
            latest_payload.get("review_decision") == review_decision
            and latest_payload.get("pr") == resolved_pr
            and latest_payload.get("head_sha") == head_sha
        ):
            return PrReviewResult(
                workspace_id=workspace_id,
                task_id=task_id,
                pr_url=resolved_pr,
                head_sha=head_sha,
                review_decision=review_decision,
                event=row_to_dict(latest),
                event_created=False,
                existing=True,
            )

    # 8. Write event
    if review_decision == "approved":
        event_type = "pr_review.approved"
    elif review_decision == "changes_requested":
        event_type = "pr_review.changes_requested"
    else:
        event_type = "pr_review.required"

    payload = {
        "task_id": task_id,
        "pr": resolved_pr,
        "pr_url": resolved_pr,
        "head_sha": head_sha,
        "branch": resolved_branch,
        "review_decision": review_decision,
        "base_branch": workspace.base_branch,
        "branch_namespace": workspace.branch_namespace,
    }
    event_result = append_event(
        conn,
        event_type=event_type,
        actor=actor,
        workspace_id=workspace_id,
        task_id=task_id,
        payload=payload,
    )

    # 9. Return
    return PrReviewResult(
        workspace_id=workspace_id,
        task_id=task_id,
        pr_url=resolved_pr,
        head_sha=head_sha,
        review_decision=review_decision,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
        existing=False,
    )


def check_merge_gate(
    conn,
    workspace_id: str,
    task_id: str,
    *,
    run: object = subprocess.run,
) -> MergeGateResult:
    # 1. Resolve workspace
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")

    # 2. Query existing task mirror
    existing_task = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()

    # 3. has_pr check
    current_pr = existing_task["pr"] if existing_task is not None else None
    if current_pr:
        has_pr_check: dict[str, Any] = {"passed": True, "pr": current_pr}
    else:
        reason = "no task mirror" if existing_task is None else "no PR linked"
        has_pr_check = {"passed": False, "reason": reason}

    current_head_sha: str | None = None
    if has_pr_check["passed"]:
        try:
            current_head_sha = query_pr_head_sha(workspace.path, current_pr, run=run)
            current_head_check: dict[str, Any] = {
                "passed": True,
                "head_sha": current_head_sha,
            }
        except ValueError as exc:
            current_head_check = {
                "passed": False,
                "reason": f"current PR head unavailable: {exc}",
            }
    else:
        current_head_check = {"passed": False, "reason": "no PR linked"}

    # 4. review_approved check (only if has_pr passed)
    if has_pr_check["passed"] and current_head_check["passed"]:
        review_row = conn.execute(
            "SELECT * FROM events WHERE workspace_id=? AND task_id=? "
            "AND event_type IN ('pr_review.approved','pr_review.changes_requested','pr_review.required') "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (workspace_id, task_id),
        ).fetchone()

        if review_row is None:
            review_check: dict[str, Any] = {"passed": False, "reason": "no pr_review event"}
        elif review_row["event_type"] != "pr_review.approved":
            review_check = {
                "passed": False,
                "reason": f"latest review event is {review_row['event_type']}",
            }
        else:
            review_payload = json.loads(review_row["payload_json"])
            if review_payload.get("pr") != current_pr:
                review_check = {
                    "passed": False,
                    "reason": f"review event PR mismatch: event pr={review_payload.get('pr')}, current pr={current_pr}",
                }
            else:
                event_head_sha = review_payload.get("head_sha")
                if not event_head_sha:
                    review_check = {
                        "passed": False,
                        "reason": "review event missing head_sha",
                    }
                elif event_head_sha != current_head_sha:
                    review_check = {
                        "passed": False,
                        "reason": f"review event head mismatch: event head={event_head_sha}, current head={current_head_sha}",
                    }
                else:
                    review_check = {"passed": True, "head_sha": event_head_sha}
    else:
        reason = current_head_check.get("reason", "no PR linked")
        review_check = {"passed": False, "reason": reason}

    # 5. ci_passed check (only if has_pr passed)
    if has_pr_check["passed"] and current_head_check["passed"]:
        ci_row = conn.execute(
            "SELECT * FROM events WHERE workspace_id=? AND task_id=? "
            "AND event_type IN ('ci.passed','ci.failed','ci.pending') "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (workspace_id, task_id),
        ).fetchone()

        if ci_row is None:
            ci_check: dict[str, Any] = {"passed": False, "reason": "no ci event"}
        elif ci_row["event_type"] != "ci.passed":
            ci_check = {
                "passed": False,
                "reason": f"latest ci event is {ci_row['event_type']}",
            }
        else:
            ci_payload = json.loads(ci_row["payload_json"])
            if ci_payload.get("pr") != current_pr:
                ci_check = {
                    "passed": False,
                    "reason": f"ci event PR mismatch: event pr={ci_payload.get('pr')}, current pr={current_pr}",
                }
            else:
                event_head_sha = ci_payload.get("head_sha")
                if not event_head_sha:
                    ci_check = {
                        "passed": False,
                        "reason": "ci event missing head_sha",
                    }
                elif event_head_sha != current_head_sha:
                    ci_check = {
                        "passed": False,
                        "reason": f"ci event head mismatch: event head={event_head_sha}, current head={current_head_sha}",
                    }
                else:
                    ci_check = {"passed": True, "head_sha": event_head_sha}
    else:
        reason = current_head_check.get("reason", "no PR linked")
        ci_check = {"passed": False, "reason": reason}

    # 6. Determine readiness
    ready = (
        has_pr_check["passed"]
        and current_head_check["passed"]
        and review_check["passed"]
        and ci_check["passed"]
    )

    # 7. Return
    return MergeGateResult(
        workspace_id=workspace_id,
        task_id=task_id,
        ready=ready,
        human_gate_required=True,
        checks={
            "has_pr": has_pr_check,
            "current_head": current_head_check,
            "review_approved": review_check,
            "ci_passed": ci_check,
        },
    )
