from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass

from coordinate import github as github_module
from coordinate.db import (
    append_event,
    get_workspace,
    row_to_dict,
    upsert_task_mirror,
)


# Backwards-compat alias for tests/CLI that imported GitHubCommandError from
# prs in the original 8.4 design sketch. We now expose it via github module.
GitHubCommandError = github_module.GitHubCommandError


@dataclass(frozen=True)
class LinkPrResult:
    workspace_id: str
    task_id: str
    pr_url: str
    branch: str | None
    event: dict
    event_created: bool
    existing: bool


def _discover_pr(
    workspace_path: str,
    branch: str,
    *,
    run: type(subprocess.run) = subprocess.run,
) -> str | None:
    """Discover an open PR for the given branch using the gh CLI.

    Kept for backward compatibility with `link_pr` discovery mode. Returns
    the PR URL or None. Raises ValueError("gh CLI not available") when gh
    is missing — callers should treat this as a hard error, not a missing PR.
    """
    try:
        proc = run(
            [
                "gh", "pr", "list",
                "--head", branch,
                "--state", "open",
                "--json", "url",
                "--limit", "1",
            ],
            timeout=30,
            check=True,
            capture_output=True,
            text=True,
            cwd=workspace_path,
        )
    except FileNotFoundError:
        raise ValueError("gh CLI not available")
    except subprocess.CalledProcessError as exc:
        raise ValueError(f"gh pr list failed: {exc.stderr}")

    try:
        result = json.loads(proc.stdout)
    except (json.JSONDecodeError, TypeError):
        raise ValueError("gh pr list returned invalid JSON")

    if not isinstance(result, list):
        raise ValueError("gh pr list returned invalid JSON")

    if not result:
        return None

    first = result[0]
    if not isinstance(first, dict) or "url" not in first:
        return None

    return first["url"]


def link_pr(
    conn,
    workspace_id: str,
    task_id: str,
    *,
    pr_url: str | None = None,
    branch: str | None = None,
    actor: str = "operator",
    run: type(subprocess.run) = subprocess.run,
) -> LinkPrResult:
    # 1. Resolve workspace
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")

    # 2. Query existing task mirror
    existing_row = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()

    # 3. Mirror branch
    mirror_branch = existing_row["branch"] if existing_row is not None else None

    # 4. Branch conflict check
    if branch is not None and mirror_branch is not None and branch != mirror_branch:
        raise ValueError(
            f"task {task_id} already has branch '{mirror_branch}', "
            f"cannot link PR from branch '{branch}'"
        )

    # 5-6. Resolve PR and branch
    if pr_url is not None:
        # Explicit mode
        resolved_pr = pr_url
        resolved_branch = branch or mirror_branch
    else:
        # Discovery mode
        resolved_branch = branch or mirror_branch
        if not resolved_branch:
            raise ValueError(
                f"task {task_id} has no branch; cannot discover PR"
            )
        resolved_pr = _discover_pr(workspace.path, resolved_branch, run=run)
        if resolved_pr is None:
            raise ValueError(
                f"no open PR found for branch '{resolved_branch}'"
            )

    # 7. PR overwrite check
    if existing_row is not None and existing_row["pr"] is not None:
        existing_pr = existing_row["pr"]
        if existing_pr != resolved_pr:
            raise ValueError(
                f"task {task_id} already has pr '{existing_pr}', "
                f"cannot relink to '{resolved_pr}'"
            )

    # 8. Conflict check — same PR on a different task
    conflict_row = conn.execute(
        "SELECT task_id FROM tasks WHERE workspace_id = ? AND pr = ? AND task_id != ?",
        (workspace_id, resolved_pr, task_id),
    ).fetchone()
    if conflict_row is not None:
        raise ValueError(
            f"pr '{resolved_pr}' already linked to task {conflict_row['task_id']} "
            f"in workspace {workspace_id}"
        )

    # 11 (check before event write, but conflict is already checked above)
    existing = existing_row is not None and existing_row["pr"] == resolved_pr

    # 9. Append event (idempotent)
    idem_key = f"{workspace_id}:pr:{task_id}:{resolved_pr}"
    payload = {
        "task_id": task_id,
        "pr": resolved_pr,
        "pr_url": resolved_pr,
        "branch": resolved_branch,
        "base_branch": workspace.base_branch,
        "branch_namespace": workspace.branch_namespace,
    }
    event_result = append_event(
        conn,
        event_type="pr.linked",
        actor=actor,
        workspace_id=workspace_id,
        task_id=task_id,
        idempotency_key=idem_key,
        payload=payload,
    )

    # 10. Upsert task mirror
    if existing_row is not None:
        phase = existing_row["phase"]
        owner = existing_row["owner"]
        mirror_branch_final = resolved_branch or mirror_branch
        payload_dict = json.loads(existing_row["payload_json"])
        upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=phase,
            owner=owner,
            branch=mirror_branch_final,
            pr=resolved_pr,
            payload=payload_dict,
            last_event_id=event_result.row["id"],
        )
    else:
        upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=None,
            owner=None,
            branch=resolved_branch,
            pr=resolved_pr,
            payload=None,
            last_event_id=event_result.row["id"],
        )

    event_dict = row_to_dict(event_result.row)

    return LinkPrResult(
        workspace_id=workspace_id,
        task_id=task_id,
        pr_url=resolved_pr,
        branch=resolved_branch,
        event=event_dict,
        event_created=event_result.created,
        existing=existing,
    )


# Compatibility exports for the Phase 8.4 host publish API.
from coordinate.pr_contracts import (  # noqa: E402
    check_cross_task_conflict as _cross_task_conflict_check,
    check_existing_pr_rebind as _check_existing_pr_rebind,
    check_mirror_conflict as _mirror_conflict_check,
    extract_mirror_publish_identity as _mirror_publish_identity,
    publish_idempotency_key as _idempotency_key,
    read_task_mirror as _read_task_mirror,
)
from coordinate.pr_publishing import (  # noqa: E402
    PublishError,
    PublishGhRunner,
    PublishResult,
    _blocked_payload,
    _emit_publish_event,
    _finalize_created,
    _finalize_link,
    _push_required_payload,
    mirror_branch_update,
    publish_pr,
    publish_pr_existing,
)


# Compatibility exports: callers continue importing these from coordinate.prs.
from coordinate.pr_recording import (  # noqa: E402
    RecordPublishError,
    RecordPublishResult,
    _ACTION_TO_EVENT_TYPE,
    _RECORDABLE_ACTIONS,
    _blocked_extra_idem,
    _record_event_payload,
    _record_event_type_for,
    _record_upsert_mirror,
    _validate_record_success_facts,
    record_publish_preflight,
    record_publish_result,
)
