"""Stable Phase 8.4 PR publish contracts and shared invariants.

This module is the single source of truth for types, event mappings,
idempotency keys, mirror identity extraction, and conflict comparisons
that both the host publishing path (`pr_publishing`) and the remote
recording sink (`pr_recording`) must agree on.  It intentionally depends
only on `coordinate.github` and `coordinate.db` so it can be imported by
both sides without creating import cycles.
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Any, Callable

from coordinate import github as github_module
from coordinate.db import row_to_dict


class PublishError(ValueError):
    """Request-level failure inside the PR publish flow."""

    def __init__(self, message: str, *, reason: str):
        super().__init__(message)
        self.reason = reason


PublishGhRunner = Callable[[list[str]], subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class PublishResult:
    """Canonical outcome of a PR publish attempt.

    Attributes:
        action: canonical action taken (created, linked, push_required,
            blocked).
        pr_url: the PR URL when action is ``created`` or ``linked``.
        pr_number: numeric PR identifier when action is ``created`` or
            ``linked``.
        event_created: whether a remote event was appended to the sink.
        mirror_updated: whether the task mirror row was written.
        remote: identifier of the GitHub remote used.
        validation: validation mode used.
        message: short human-readable status message.
        detail: optional extra detail.
    """

    action: str | None = None
    pr_url: str | None = None
    pr_number: int | None = None
    event_created: bool = False
    mirror_updated: bool = False
    remote: str | None = None
    validation: str | None = None
    message: str = ""
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "pr_url": self.pr_url,
            "pr_number": self.pr_number,
            "event_created": self.event_created,
            "mirror_updated": self.mirror_updated,
            "remote": self.remote,
            "validation": self.validation,
            "message": self.message,
            "detail": self.detail,
        }


# Canonical actions that flow through the publish/record pipeline.
PUBLISH_ACTIONS = frozenset({"created", "linked", "push_required", "blocked"})

# Action name used in payloads -> event type recorded in the event sink.
ACTION_TO_EVENT_TYPE = {
    "created": "pr.created",
    "linked": "pr.linked",
    "push_required": "push.required",
    "blocked": "publish.blocked",
}


def publish_idempotency_key(
    workspace_id: str,
    task_id: str,
    event_type: str,
    repo: str,
    branch: str,
    commit: str,
    extra: str | None = None,
) -> str:
    """Return a deterministic idempotency key for a publish event.

    Shape: ``workspace:task:event_type:repo:branch:commit[:extra]``.
    """
    parts = [
        workspace_id,
        task_id,
        event_type,
        repo,
        branch,
        commit,
    ]
    if extra:
        parts.append(extra)
    return ":".join(parts)


def read_task_mirror(conn: Any, workspace_id: str, task_id: str) -> dict[str, Any] | None:
    """Read the current mirror row for a task, if one exists."""
    cur = conn.execute(
        """
        SELECT * FROM tasks
        WHERE workspace_id = ? AND task_id = ?
        """,
        (workspace_id, task_id),
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        return None
    return row_to_dict(row)


def extract_mirror_publish_identity(
    mirror: dict[str, Any] | None,
) -> tuple[str | None, str | None]:
    """Return ``(repo, commit)`` recorded in a mirror row.

    Handles both the current ``publish_metadata`` envelope (inside
    ``payload``) and the legacy top-level ``repo``/``commit`` payload
    fields.
    """
    if not mirror:
        return (None, None)
    payload = mirror.get("payload") or {}
    if not isinstance(payload, dict):
        return (None, None)
    metadata = payload.get("publish_metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}
    repo = metadata.get("repo") or payload.get("repo")
    commit = (
        metadata.get("reported_commit")
        or metadata.get("commit")
        or payload.get("commit")
    )
    return (
        repo if isinstance(repo, str) and repo else None,
        commit if isinstance(commit, str) and commit else None,
    )


def check_mirror_conflict(
    *,
    mirror: dict[str, Any] | None,
    repo: str,
    branch: str,
    commit: str,
    allow_commit_change: bool = False,
) -> str | None:
    """Compare a worker report against existing task mirror metadata.

    Returns ``None`` if the report is consistent (or no mirror exists).
    Returns a human-readable conflict description otherwise. The mirror
    is never silently overwritten.
    """
    if mirror is None:
        return None
    recorded_repo, recorded_commit = extract_mirror_publish_identity(mirror)
    recorded_branch = mirror.get("branch")
    conflicts: list[str] = []
    if recorded_branch and recorded_branch != branch:
        conflicts.append(
            f"mirror branch '{recorded_branch}' != worker branch '{branch}'"
        )
    if recorded_repo and recorded_repo != repo:
        conflicts.append(
            f"mirror repo '{recorded_repo}' != worker repo '{repo}'"
        )
    if recorded_commit and recorded_commit != commit and not allow_commit_change:
        conflicts.append(
            f"mirror commit '{recorded_commit}' != worker commit '{commit}'"
        )
    if conflicts:
        return "; ".join(conflicts)
    return None


def check_existing_pr_rebind(
    conn: Any,
    *,
    workspace_id: str,
    task_id: str,
    pr_url: str,
) -> str | None:
    """Return an error if the task mirror already points at a different PR."""
    mirror = read_task_mirror(conn, workspace_id, task_id)
    if mirror is None:
        return None
    existing_pr = mirror.get("pr")
    if existing_pr is not None and existing_pr != pr_url:
        return (
            f"task {task_id} already has pr '{existing_pr}', "
            f"cannot relink to '{pr_url}'"
        )
    return None


def check_cross_task_conflict(
    conn: Any,
    *,
    workspace_id: str,
    task_id: str,
    branch: str,
    pr_url: str | None = None,
) -> str | None:
    """Return a reason if the branch or PR is bound to another active task.

    Closed tasks may reuse a branch historically, so they are excluded
    from the branch check. PR URLs remain globally unique per workspace
    regardless of phase.
    """
    cur = conn.execute(
        """
        SELECT task_id, branch, pr FROM tasks
        WHERE workspace_id = ? AND task_id != ? AND branch = ?
        AND phase IS NOT 'closed'
        """,
        (workspace_id, task_id, branch),
    )
    row = cur.fetchone()
    cur.close()
    if row is not None:
        return (
            f"branch '{branch}' already allocated to task "
            f"{row['task_id']}"
        )

    if pr_url:
        cur = conn.execute(
            """
            SELECT task_id FROM tasks
            WHERE workspace_id = ? AND task_id != ? AND pr = ?
            """,
            (workspace_id, task_id, pr_url),
        )
        row = cur.fetchone()
        cur.close()
        if row is not None:
            return (
                f"pr '{pr_url}' already linked to task {row['task_id']} "
                f"in workspace {workspace_id}"
            )
    return None


def validate_publish_success_facts(
    *,
    workspace_id: str,
    result: dict[str, Any],
    repo: str,
    branch: str,
    reported_commit: str,
    head_ref: str,
    base: str,
    pr_url: str,
    remote_sha: str | None,
) -> None:
    """Validate facts that can grant a remote task a merge-gate PR.

    Blocked results intentionally preserve invalid worker input for audit,
    but ``created``/``linked`` results mutate the trusted task mirror.
    Those success facts therefore need the same canonical checks as the
    host-side GitHub path.

    Raises `github_module.GitHubCommandError`; the caller is responsible
    for wrapping it in a domain-specific exception.
    """
    try:
        github_module.validate_repo(repo)
        github_module.validate_branch(branch)
        github_module.validate_commit(reported_commit)
        github_module.validate_branch(base)
        if remote_sha is None:
            raise github_module.GitHubCommandError(
                "created/linked result requires remote_sha",
                reason="invalid_commit",
            )
        github_module.validate_commit(remote_sha)
    except github_module.GitHubCommandError:
        raise

    expected_head_ref = f"{repo.split('/', 1)[0]}:{branch}"
    if head_ref != expected_head_ref:
        raise github_module.GitHubCommandError(
            f"head_ref {head_ref!r} does not match {expected_head_ref!r}",
            reason="head_ref_mismatch",
        )

    try:
        github_module.validate_pr_url(pr_url, repo)
    except github_module.GitHubCommandError:
        raise

    if remote_sha != reported_commit:
        raise github_module.GitHubCommandError(
            f"remote_sha {remote_sha!r} does not match reported_commit "
            f"{reported_commit!r}",
            reason="sha_mismatch",
        )

    host_commit = result.get("commit")
    if host_commit is not None and host_commit != reported_commit:
        raise github_module.GitHubCommandError(
            f"commit {host_commit!r} does not match reported_commit "
            f"{reported_commit!r}",
            reason="commit_mismatch",
        )
