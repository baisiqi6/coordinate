"""CI check polling and event writer.

Writes ci.passed, ci.failed, and ci.pending events.  The ci.pending
event_type is intentionally omitted from the public SUPPORTED_EVENT_TYPES
constant -- it is an internal ephemeral signal used by the merge gate
to invalidate stale ci.passed markers.
"""
from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass

from coordinate.db import append_event, get_workspace, row_to_dict
from coordinate.github import query_pr_head_sha


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str  # "passed", "failed", "pending", "skipped"

    def to_dict(self) -> dict:
        return {"name": self.name, "status": self.status}


@dataclass(frozen=True)
class CheckCiResult:
    workspace_id: str
    task_id: str
    pr_url: str
    head_sha: str
    checks: list[CheckResult]
    aggregated_status: str  # "passed", "failed", "pending"
    event: dict | None
    event_created: bool
    existing: bool

    def to_dict(self) -> dict:
        return {
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "pr_url": self.pr_url,
            "head_sha": self.head_sha,
            "checks": [c.to_dict() for c in self.checks],
            "aggregated_status": self.aggregated_status,
            "event": self.event,
            "event_created": self.event_created,
            "existing": self.existing,
        }


_BUCKET_MAP: dict[str, str] = {
    "pass": "passed",
    "fail": "failed",
    "pending": "pending",
    "skipping": "skipped",
    "cancel": "failed",
}
_ALLOWED_PR_CHECKS_RETURNCODES = {0, 1, 8}


def _query_checks(
    workspace_path: str,
    pr_url: str,
    *,
    run: object = subprocess.run,
) -> list[CheckResult]:
    try:
        proc = run(
            ["gh", "pr", "checks", pr_url, "--json", "name,state,bucket"],
            timeout=30,
            check=False,
            capture_output=True,
            text=True,
            cwd=workspace_path,
        )
    except FileNotFoundError:
        raise ValueError("gh CLI not available")

    parsed: list[dict] | None = None
    try:
        raw = json.loads(proc.stdout)
        if isinstance(raw, list):
            parsed = raw
    except (json.JSONDecodeError, TypeError):
        pass

    if (
        parsed is None
        and proc.returncode == 1
        and not (proc.stdout or "").strip()
        and re.fullmatch(
            r"no checks reported(?: on the '.+' branch)?",
            (proc.stderr or "").strip(),
        )
        is not None
    ):
        parsed = []

    if parsed is None:
        if proc.returncode != 0:
            raise ValueError(f"gh pr checks failed: {proc.stderr}")
        raise ValueError("gh pr checks returned invalid JSON")
    if proc.returncode not in _ALLOWED_PR_CHECKS_RETURNCODES:
        raise ValueError(f"gh pr checks failed: {proc.stderr}")

    results: list[CheckResult] = []
    for item in parsed:
        bucket = item.get("bucket")
        status = _BUCKET_MAP.get(bucket, "pending") if bucket is not None else "pending"
        results.append(CheckResult(name=item.get("name", ""), status=status))
    return results


def _aggregate_status(checks: list[CheckResult]) -> str:
    if not checks:
        return "pending"
    for check in checks:
        if check.status == "failed":
            return "failed"
    for check in checks:
        if check.status == "pending":
            return "pending"
    return "passed"


def check_ci(
    conn,
    workspace_id: str,
    task_id: str,
    *,
    pr_url: str | None = None,
    branch: str | None = None,
    actor: str = "operator",
    run: object = subprocess.run,
) -> CheckCiResult:
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
                f"task {task_id} already has pr '{mirror_pr}', cannot check CI for '{pr_url}'"
            )
        resolved_pr = pr_url
    else:
        if not mirror_pr:
            raise ValueError(f"task {task_id} has no PR; cannot check CI")
        resolved_pr = mirror_pr

    # 4. Resolve branch
    resolved_branch = branch or (
        existing_row["branch"] if existing_row is not None else None
    )

    # 5. Query checks
    checks = _query_checks(workspace.path, resolved_pr, run=run)
    head_sha = query_pr_head_sha(workspace.path, resolved_pr, run=run)

    # 6. Aggregate status
    aggregated = _aggregate_status(checks)

    # 7. Find latest ci.* event
    latest = conn.execute(
        "SELECT * FROM events WHERE workspace_id=? AND task_id=? "
        "AND event_type IN ('ci.passed','ci.failed','ci.pending') "
        "ORDER BY created_at DESC, rowid DESC LIMIT 1",
        (workspace_id, task_id),
    ).fetchone()

    # 9. Dedup
    if latest is not None:
        latest_payload = json.loads(latest["payload_json"])
        if (
            latest_payload.get("status") == aggregated
            and latest_payload.get("pr") == resolved_pr
            and latest_payload.get("head_sha") == head_sha
        ):
            return CheckCiResult(
                workspace_id=workspace_id,
                task_id=task_id,
                pr_url=resolved_pr,
                head_sha=head_sha,
                checks=checks,
                aggregated_status=aggregated,
                event=row_to_dict(latest),
                event_created=False,
                existing=True,
            )

    # 10. Write event
    if aggregated == "passed":
        event_type = "ci.passed"
    elif aggregated == "failed":
        event_type = "ci.failed"
    else:
        event_type = "ci.pending"
    payload = {
        "task_id": task_id,
        "pr": resolved_pr,
        "pr_url": resolved_pr,
        "head_sha": head_sha,
        "branch": resolved_branch,
        "status": aggregated,
        "checks": [c.to_dict() for c in checks],
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

    # 11. Return
    return CheckCiResult(
        workspace_id=workspace_id,
        task_id=task_id,
        pr_url=resolved_pr,
        head_sha=head_sha,
        checks=checks,
        aggregated_status=aggregated,
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
        existing=False,
    )
