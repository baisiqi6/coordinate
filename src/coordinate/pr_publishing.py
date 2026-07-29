"""Host-side Phase 8.4 PR create-or-link orchestration."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from coordinate import github as github_module
from coordinate.db import append_event, get_workspace, row_to_dict, upsert_task_mirror
from coordinate.pr_contracts import (
    ACTION_TO_EVENT_TYPE,
    PUBLISH_ACTIONS,
    PublishError,
    PublishGhRunner,
    check_existing_pr_rebind,
    check_mirror_conflict,
    extract_mirror_publish_identity,
    publish_idempotency_key,
    read_task_mirror,
)


GitHubCommandError = github_module.GitHubCommandError

# --------------------------------------------------------------------------
# Phase 8.4 — Worker push + PR create-or-link publish flow.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class PublishResult:
    workspace_id: str
    task_id: str
    repo: str
    branch: str
    head_ref: str
    base: str
    commit: str
    reported_commit: str
    remote_sha: str | None
    pr_url: str | None
    action: str  # "created" | "linked" | "push_required" | "blocked"
    event: dict
    event_created: bool
    existing: bool
    reason: str | None = None
    remote: str | None = None
    validation: str | None = None
    message: str | None = None
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "repo": self.repo,
            "branch": self.branch,
            "head_ref": self.head_ref,
            "base": self.base,
            "commit": self.commit,
            "reported_commit": self.reported_commit,
            "remote_sha": self.remote_sha,
            "pr_url": self.pr_url,
            "action": self.action,
            "event": self.event,
            "event_created": self.event_created,
            "existing": self.existing,
            "reason": self.reason,
        }
        # Audit-only fields from the worker host must round-trip through
        # the record-only sink so the remote DB event preserves them.
        if self.remote is not None:
            d["remote"] = self.remote
        if self.validation is not None:
            d["validation"] = self.validation
        if self.message is not None:
            d["message"] = self.message
        if self.detail is not None:
            d["detail"] = self.detail
        return d



@dataclass(frozen=True)
class _ValidatedRequest:
    workspace: Any
    workspace_id: str
    task_id: str
    repo: str
    branch: str
    head_owner: str
    head_ref: str
    base: str
    title: str
    body: str
    commit: str
    reported_commit: str
    pushed: bool
    actor: str
    remote: str | None
    validation: str | None
    run: PublishGhRunner
    expected_pr_url: str | None = None
    outcome: "_PublishOutcome" | None = None


@dataclass(frozen=True)
class _PublishOutcome:
    action: str
    event_type: str | None = None
    extra_idem: str | None = None
    reason: str | None = None
    message: str | None = None
    remote_sha: str | None = None
    detail: str | None = None
    pr_url: str | None = None
    existing_pr_payload: dict[str, Any] | None = None


def _runner_for(run: PublishGhRunner | None) -> PublishGhRunner:
    return run or github_module._run_gh


def validate_publish_request(
    conn,
    workspace_id: str,
    task_id: str,
    *,
    repo: str,
    branch: str,
    head_owner: str,
    base: str,
    title: str,
    body: str,
    commit: str,
    pushed: bool,
    actor: str,
    remote: str | None,
    validation: str | None,
    run: PublishGhRunner | None,
) -> _ValidatedRequest:
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise PublishError(f"unknown workspace: {workspace_id}", reason="unknown_workspace")

    try:
        validated_repo = github_module.validate_repo(repo)
        validated_branch = github_module.validate_branch(branch)
        validated_commit = github_module.validate_commit(commit)
        parsed_pushed = github_module.parse_pushed(pushed)
        if not isinstance(head_owner, str) or not head_owner.strip():
            raise GitHubCommandError("head_owner is required", reason="invalid_head")
        repo_owner = validated_repo.split("/", 1)[0]
        if head_owner.strip() != repo_owner:
            raise GitHubCommandError(
                f"head_owner {head_owner!r} != repo owner {repo_owner!r}; "
                f"Phase 8.4 only supports PRs from the same owner as the "
                f"target repo (fork workflow is out of scope)",
                reason="head_owner_mismatch",
            )
        if not isinstance(base, str) or not base.strip():
            raise GitHubCommandError("base is required", reason="invalid_base")
        github_module.validate_branch(base)
        if not isinstance(title, str) or not title.strip():
            raise GitHubCommandError("title is required", reason="invalid_title")
        if not isinstance(body, str):
            raise GitHubCommandError("body must be a string", reason="invalid_body")
    except GitHubCommandError as exc:
        return _ValidatedRequest(
            workspace=workspace,
            workspace_id=workspace_id,
            task_id=task_id,
            repo=repo,
            branch=branch,
            head_owner=head_owner,
            head_ref=f"{head_owner}:{branch}",
            base=base,
            title=title,
            body=body,
            commit=commit,
            reported_commit=commit,
            pushed=False,
            actor=actor,
            remote=remote,
            validation=validation,
            run=_runner_for(run),
            outcome=_PublishOutcome(
                action="blocked",
                event_type="publish.blocked",
                extra_idem="validation",
                reason=exc.reason,
                message=str(exc),
            ),
        )

    mirror = read_task_mirror(conn, workspace_id, task_id)
    conflict = check_mirror_conflict(
        mirror=mirror,
        repo=validated_repo,
        branch=validated_branch,
        commit=validated_commit,
    )
    if conflict is not None:
        return _ValidatedRequest(
            workspace=workspace,
            workspace_id=workspace_id,
            task_id=task_id,
            repo=validated_repo,
            branch=validated_branch,
            head_owner=head_owner,
            head_ref=f"{head_owner}:{validated_branch}",
            base=base,
            title=title,
            body=body,
            commit=validated_commit,
            reported_commit=validated_commit,
            pushed=False,
            actor=actor,
            remote=remote,
            validation=validation,
            run=_runner_for(run),
            outcome=_PublishOutcome(
                action="blocked",
                event_type="publish.blocked",
                extra_idem="mirror_conflict",
                reason="mirror_conflict",
                message=conflict,
            ),
        )

    if not parsed_pushed:
        return _ValidatedRequest(
            workspace=workspace,
            workspace_id=workspace_id,
            task_id=task_id,
            repo=validated_repo,
            branch=validated_branch,
            head_owner=head_owner,
            head_ref=f"{head_owner}:{validated_branch}",
            base=base,
            title=title,
            body=body,
            commit=validated_commit,
            reported_commit=validated_commit,
            pushed=False,
            actor=actor,
            remote=remote,
            validation=validation,
            run=_runner_for(run),
            outcome=_PublishOutcome(
                action="push_required",
                event_type="push.required",
                extra_idem="not_pushed",
                reason="not_pushed",
                message="",
            ),
        )

    return _ValidatedRequest(
        workspace=workspace,
        workspace_id=workspace_id,
        task_id=task_id,
        repo=validated_repo,
        branch=validated_branch,
        head_owner=head_owner,
        head_ref=f"{head_owner}:{validated_branch}",
        base=base,
        title=title,
        body=body,
        commit=validated_commit,
        reported_commit=validated_commit,
        pushed=True,
        actor=actor,
        remote=remote,
        validation=validation,
        run=_runner_for(run),
    )

def validate_publish_request_existing(
    conn,
    workspace_id: str,
    task_id: str,
    *,
    repo: str,
    branch: str,
    head_owner: str,
    base: str,
    commit: str,
    expected_pr_url: str,
    actor: str,
    remote: str | None,
    validation: str | None,
    run: PublishGhRunner | None,
) -> _ValidatedRequest:
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise PublishError(f"unknown workspace: {workspace_id}", reason="unknown_workspace")

    try:
        validated_repo = github_module.validate_repo(repo)
        validated_branch = github_module.validate_branch(branch)
        validated_commit = github_module.validate_commit(commit)
        if not isinstance(head_owner, str) or not head_owner.strip():
            raise GitHubCommandError("head_owner is required", reason="invalid_head")
        repo_owner = validated_repo.split("/", 1)[0]
        if head_owner.strip() != repo_owner:
            raise GitHubCommandError(
                f"head_owner {head_owner!r} != repo owner {repo_owner!r}; "
                f"Phase 8.4 only supports PRs from the same owner as the "
                f"target repo (fork workflow is out of scope)",
                reason="head_owner_mismatch",
            )
        if not isinstance(base, str) or not base.strip():
            raise GitHubCommandError("base is required", reason="invalid_base")
        github_module.validate_branch(base)
        github_module.validate_pr_url(expected_pr_url, validated_repo)
    except GitHubCommandError as exc:
        return _ValidatedRequest(
            workspace=workspace,
            workspace_id=workspace_id,
            task_id=task_id,
            repo=repo,
            branch=branch,
            head_owner=head_owner,
            head_ref=f"{head_owner}:{branch}",
            base=base,
            title="",
            body="",
            commit=commit,
            reported_commit=commit,
            pushed=True,
            actor=actor,
            remote=remote,
            validation=validation,
            run=_runner_for(run),
            expected_pr_url=expected_pr_url,
            outcome=_PublishOutcome(
                action="blocked",
                event_type="publish.blocked",
                extra_idem="validation",
                reason=exc.reason,
                message=str(exc),
            ),
        )

    mirror = read_task_mirror(conn, workspace_id, task_id)
    existing_pr = mirror.get("pr") if mirror else None
    conflict = check_mirror_conflict(
        mirror=mirror,
        repo=validated_repo,
        branch=validated_branch,
        commit=validated_commit,
        allow_commit_change=(existing_pr == expected_pr_url),
    )
    if conflict is not None:
        return _ValidatedRequest(
            workspace=workspace,
            workspace_id=workspace_id,
            task_id=task_id,
            repo=validated_repo,
            branch=validated_branch,
            head_owner=head_owner,
            head_ref=f"{head_owner}:{validated_branch}",
            base=base,
            title="",
            body="",
            commit=validated_commit,
            reported_commit=validated_commit,
            pushed=True,
            actor=actor,
            remote=remote,
            validation=validation,
            run=_runner_for(run),
            expected_pr_url=expected_pr_url,
            outcome=_PublishOutcome(
                action="blocked",
                event_type="publish.blocked",
                extra_idem="mirror_conflict",
                reason="mirror_conflict",
                message=conflict,
            ),
        )

    if existing_pr is not None and existing_pr != expected_pr_url:
        message = (
            f"task {task_id} local mirror has pr {existing_pr!r}, "
            f"remote preflight expected {expected_pr_url!r}"
        )
        return _ValidatedRequest(
            workspace=workspace,
            workspace_id=workspace_id,
            task_id=task_id,
            repo=validated_repo,
            branch=validated_branch,
            head_owner=head_owner,
            head_ref=f"{head_owner}:{validated_branch}",
            base=base,
            title="",
            body="",
            commit=validated_commit,
            reported_commit=validated_commit,
            pushed=True,
            actor=actor,
            remote=remote,
            validation=validation,
            run=_runner_for(run),
            expected_pr_url=expected_pr_url,
            outcome=_PublishOutcome(
                action="blocked",
                event_type="publish.blocked",
                extra_idem="local_pr_mismatch",
                reason="pr_already_linked",
                message=message,
            ),
        )

    return _ValidatedRequest(
        workspace=workspace,
        workspace_id=workspace_id,
        task_id=task_id,
        repo=validated_repo,
        branch=validated_branch,
        head_owner=head_owner,
        head_ref=f"{head_owner}:{validated_branch}",
        base=base,
        title="",
        body="",
        commit=validated_commit,
        reported_commit=validated_commit,
        pushed=True,
        actor=actor,
        remote=remote,
        validation=validation,
        run=_runner_for(run),
        expected_pr_url=expected_pr_url,
    )


def discover_publish_target(validated: _ValidatedRequest) -> _PublishOutcome:
    repo = validated.repo
    branch = validated.branch
    commit = validated.commit
    head_ref = validated.head_ref
    base = validated.base
    runner = validated.run
    title = validated.title
    body = validated.body

    try:
        remote_sha = github_module.fetch_remote_ref(repo, branch, run=runner)
    except GitHubCommandError as exc:
        return _PublishOutcome(
            action="blocked",
            event_type="publish.blocked",
            extra_idem=f"ref_lookup:{exc.reason}",
            reason=exc.reason,
            message=str(exc),
        )

    if remote_sha is None:
        return _PublishOutcome(
            action="push_required",
            event_type="push.required",
            extra_idem="ref_missing",
            reason="ref_missing",
            message="",
            detail="remote ref not found on GitHub",
        )

    if remote_sha != commit:
        return _PublishOutcome(
            action="blocked",
            event_type="publish.blocked",
            extra_idem="sha_mismatch",
            reason="sha_mismatch",
            message=f"remote SHA {remote_sha} != worker commit {commit}",
            remote_sha=remote_sha,
        )

    try:
        existing_pr = github_module.discover_open_pr_for_head(
            repo,
            head_ref,
            expected_head_sha=commit,
            expected_base=base,
            run=runner,
        )
    except GitHubCommandError as exc:
        return _PublishOutcome(
            action="blocked",
            event_type="publish.blocked",
            extra_idem=f"discover:{exc.reason}",
            reason=exc.reason,
            message=str(exc),
            remote_sha=remote_sha,
        )

    if existing_pr:
        pr_url = str(existing_pr.get("url") or "")
        if not pr_url:
            return _PublishOutcome(
                action="blocked",
                event_type="publish.blocked",
                extra_idem="discover_missing_url",
                reason="discover_missing_url",
                message="gh pr list returned a PR object without url",
                remote_sha=remote_sha,
            )
        return _PublishOutcome(
            action="linked",
            pr_url=pr_url,
            existing_pr_payload=existing_pr,
            remote_sha=remote_sha,
        )

    try:
        pr_url = github_module.create_pr(
            repo,
            head_ref,
            base,
            title=title,
            body=body,
            run=runner,
        )
    except GitHubCommandError as exc:
        return _PublishOutcome(
            action="blocked",
            event_type="publish.blocked",
            extra_idem=f"create:{exc.reason}",
            reason=exc.reason,
            message=str(exc),
            remote_sha=remote_sha,
        )

    return _PublishOutcome(
        action="created",
        pr_url=pr_url,
        remote_sha=remote_sha,
    )


def discover_existing_target(validated: _ValidatedRequest) -> _PublishOutcome:
    repo = validated.repo
    branch = validated.branch
    commit = validated.commit
    head_ref = validated.head_ref
    base = validated.base
    expected_pr_url = validated.expected_pr_url
    runner = validated.run

    try:
        discovered = github_module.discover_open_pr_for_head(
            repo,
            head_ref,
            expected_head_sha=commit,
            expected_base=base,
            run=runner,
        )
    except GitHubCommandError as exc:
        return _PublishOutcome(
            action="blocked",
            event_type="publish.blocked",
            extra_idem=f"discover:{exc.reason}",
            reason=exc.reason,
            message=str(exc),
        )

    if discovered is None:
        message = (
            f"preflight expected pr {expected_pr_url!r} but "
            f"no open PR found for {head_ref}"
        )
        return _PublishOutcome(
            action="blocked",
            event_type="publish.blocked",
            extra_idem="discover_missing_pr",
            reason="discover_missing_pr",
            message=message,
        )

    discovered_url = str(discovered.get("url") or "")
    if discovered_url != expected_pr_url:
        message = (
            f"preflight expected pr {expected_pr_url!r} but "
            f"GitHub discovered {discovered_url!r}"
        )
        return _PublishOutcome(
            action="blocked",
            event_type="publish.blocked",
            extra_idem="discover_url_mismatch",
            reason="discover_url_mismatch",
            message=message,
        )

    return _PublishOutcome(
        action="linked",
        pr_url=discovered_url,
        existing_pr_payload=discovered,
        remote_sha=commit,
    )


def persist_publish_outcome(
    conn,
    validated: _ValidatedRequest,
    outcome: _PublishOutcome,
    *,
    actor: str,
    remote: str | None,
    validation: str | None,
) -> PublishResult:
    repo = validated.repo
    branch = validated.branch
    head_ref = validated.head_ref
    base = validated.base
    commit = validated.commit
    reported_commit = validated.reported_commit

    if outcome.action == "blocked":
        payload_commit = outcome.remote_sha if outcome.reason == "sha_mismatch" else commit
        return _emit_publish_event(
            conn,
            workspace_id=validated.workspace_id,
            task_id=validated.task_id,
            actor=actor,
            event_type=outcome.event_type,
            extra_idem=outcome.extra_idem,
            payload=_blocked_payload(
                repo=repo,
                branch=branch,
                commit=commit,
                remote_sha=outcome.remote_sha,
                reason=outcome.reason,
                message=outcome.message,
                remote=remote,
                validation=validation,
                head_ref=head_ref,
                base=base,
            ),
            result_kwargs=dict(
                repo=repo,
                branch=branch,
                head_ref=head_ref,
                base=base,
                commit=payload_commit,
                reported_commit=reported_commit,
                remote_sha=outcome.remote_sha,
                pr_url=None,
                action="blocked",
                existing=False,
                reason=outcome.reason,
            ),
        )

    if outcome.action == "push_required":
        return _emit_publish_event(
            conn,
            workspace_id=validated.workspace_id,
            task_id=validated.task_id,
            actor=actor,
            event_type=outcome.event_type,
            extra_idem=outcome.extra_idem,
            payload=_push_required_payload(
                repo=repo,
                branch=branch,
                commit=commit,
                remote=remote,
                validation=validation,
                detail=outcome.detail,
            ),
            result_kwargs=dict(
                repo=repo,
                branch=branch,
                head_ref=head_ref,
                base=base,
                commit=commit,
                reported_commit=reported_commit,
                remote_sha=None,
                pr_url=None,
                action="push_required",
                existing=False,
                reason=outcome.reason,
            ),
        )

    if outcome.action == "linked":
        return _finalize_link(
            conn,
            workspace_id=validated.workspace_id,
            task_id=validated.task_id,
            actor=actor,
            repo=repo,
            branch=branch,
            head_ref=head_ref,
            base=base,
            commit=commit,
            remote_sha=outcome.remote_sha,
            pr_url=outcome.pr_url,
            remote=remote,
            validation=validation,
            existing_pr_payload=outcome.existing_pr_payload,
        )

    if outcome.action == "created":
        return _finalize_created(
            conn,
            workspace_id=validated.workspace_id,
            task_id=validated.task_id,
            actor=actor,
            repo=repo,
            branch=branch,
            head_ref=head_ref,
            base=base,
            commit=commit,
            remote_sha=outcome.remote_sha,
            pr_url=outcome.pr_url,
            remote=remote,
            validation=validation,
        )

    raise PublishError(
        f"unknown publish outcome: {outcome.action}",
        reason="internal_error",
    )

def publish_pr(
    conn,
    workspace_id: str,
    task_id: str,
    *,
    repo: str,
    branch: str,
    head_owner: str,
    base: str,
    title: str,
    body: str,
    commit: str,
    pushed: bool,
    actor: str = "operator",
    remote: str | None = None,
    validation: str | None = None,
    run: PublishGhRunner | None = None,
) -> PublishResult:
    """Server-side publish orchestration.

    Decision tree:
      1. validate_publish_request validates inputs/mirror; failures become a
         blocked/push_required precondition outcome.
      2. discover_publish_target performs the irreversible GitHub write only
         after validation succeeds.
      3. persist_publish_outcome emits the event and updates the mirror.

    All `gh` invocations go through the injected `run` so tests can fake
    CompletedProcess objects without touching GitHub.
    """
    validated = validate_publish_request(
        conn,
        workspace_id,
        task_id,
        repo=repo,
        branch=branch,
        head_owner=head_owner,
        base=base,
        title=title,
        body=body,
        commit=commit,
        pushed=pushed,
        actor=actor,
        remote=remote,
        validation=validation,
        run=run,
    )
    if validated.outcome is not None:
        return persist_publish_outcome(
            conn, validated, validated.outcome,
            actor=actor, remote=remote, validation=validation,
        )
    target = discover_publish_target(validated)
    return persist_publish_outcome(
        conn, validated, target,
        actor=actor, remote=remote, validation=validation,
    )



# --------------------------------------------------------------------------
# Internals: event emission + payload shaping
# --------------------------------------------------------------------------


def _blocked_payload(
    *,
    repo: str,
    branch: str,
    commit: str,
    remote_sha: str | None,
    reason: str,
    message: str,
    remote: str | None,
    validation: str | None,
    head_ref: str | None = None,
    base: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "repo": repo,
        "branch": branch,
        "reported_commit": commit,
        "reason": reason,
        "message": message,
    }
    if remote_sha:
        payload["remote_sha"] = remote_sha
    if remote:
        payload["remote"] = remote
    if validation:
        payload["validation"] = validation
    if head_ref:
        payload["head_ref"] = head_ref
    if base:
        payload["base"] = base
    return payload


def _push_required_payload(
    *,
    repo: str,
    branch: str,
    commit: str,
    remote: str | None,
    validation: str | None,
    detail: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "repo": repo,
        "branch": branch,
        "reported_commit": commit,
        "remote": remote or "origin",
        "next_action": (
            f"push the branch to {remote or 'origin'} from the worker host "
            f"({branch} -> {commit}) then rerun `pr publish`"
        ),
    }
    if validation:
        payload["validation"] = validation
    if detail:
        payload["detail"] = detail
    return payload


def _emit_publish_event(
    conn,
    *,
    workspace_id: str,
    task_id: str,
    actor: str,
    event_type: str,
    extra_idem: str,
    payload: dict[str, Any],
    result_kwargs: dict[str, Any],
) -> PublishResult:
    """Append a publish-related event and update the task mirror (branch only).

    The task mirror is updated with the reported branch so the merge gate can
    see progress; the `pr` column is intentionally left untouched for blocked /
    push_required events.
    """
    payload_with_task = {"task_id": task_id, **payload}
    idem = publish_idempotency_key(
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=event_type,
        repo=str(payload.get("repo", "")),
        branch=str(payload.get("branch", "")),
        commit=str(payload.get("reported_commit", "")),
        extra=extra_idem,
    )
    event_result = append_event(
        conn,
        event_type=event_type,
        actor=actor,
        workspace_id=workspace_id,
        task_id=task_id,
        idempotency_key=idem,
        payload=payload_with_task,
    )
    event_dict = row_to_dict(event_result.row)
    kwargs = dict(result_kwargs)
    kwargs.setdefault("existing", False)
    # Round-trip audit-only fields through to_dict() so the remote sink
    # preserves worker context (remote name, validation summary, message).
    kwargs.setdefault("remote", payload.get("remote"))
    kwargs.setdefault("validation", payload.get("validation"))
    kwargs.setdefault("message", payload.get("message"))
    kwargs.setdefault("detail", payload.get("detail"))
    return PublishResult(
        workspace_id=workspace_id,
        task_id=task_id,
        event=event_dict,
        event_created=event_result.created,
        **kwargs,
    )


def mirror_branch_update(
    conn,
    *,
    workspace_id: str,
    task_id: str,
    branch: str,
    last_event_id: str,
) -> bool:
    """Update the task mirror's `branch` column without touching pr/owner.

    Returns True if a mirror row already existed (regardless of whether the
    branch value changed). This is purely a helper for the publish flow.
    """
    existing = read_task_mirror(conn, workspace_id, task_id)
    if existing is None:
        upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=None,
            owner=None,
            branch=branch or None,
            pr=None,
            payload=None,
            last_event_id=last_event_id,
        )
        return False
    payload_dict = (
        existing.get("payload")
        if existing.get("payload") is not None
        else None
    )
    if isinstance(payload_dict, str):
        # Defensive: should already be a dict after row_to_dict, but
        # older code paths or direct row access may hand us raw JSON.
        payload_dict = json.loads(payload_dict)
    upsert_task_mirror(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        phase=existing["phase"],
        owner=existing["owner"],
        branch=branch or existing["branch"],
        pr=existing["pr"],
        payload=payload_dict,
        last_event_id=last_event_id,
    )
    return True


def publish_pr_existing(
    conn,
    workspace_id: str,
    task_id: str,
    *,
    repo: str,
    branch: str,
    head_owner: str,
    base: str,
    commit: str,
    expected_pr_url: str,
    actor: str = "operator",
    remote: str | None = None,
    validation: str | None = None,
    run: PublishGhRunner | None = None,
) -> PublishResult:
    """Host-side read-only link path for an already-recorded PR.

    Phase 8.4 idempotency: when the remote mirror already knows the PR for
    this task, the host must not call `gh pr create`. It performs a read-only
    discovery (`gh pr list`) and, if the discovered PR matches the expected
    URL, SHA, and base, writes a local `pr.linked` event and updates the local
    mirror exactly as `publish_pr` would on a normal linked path. Any mismatch
    returns a `publish.blocked` result instead of creating a duplicate PR.
    """
    validated = validate_publish_request_existing(
        conn,
        workspace_id,
        task_id,
        repo=repo,
        branch=branch,
        head_owner=head_owner,
        base=base,
        commit=commit,
        expected_pr_url=expected_pr_url,
        actor=actor,
        remote=remote,
        validation=validation,
        run=run,
    )
    if validated.outcome is not None:
        return persist_publish_outcome(
            conn, validated, validated.outcome,
            actor=actor, remote=remote, validation=validation,
        )
    target = discover_existing_target(validated)
    return persist_publish_outcome(
        conn, validated, target,
        actor=actor, remote=remote, validation=validation,
    )



def _finalize_link(
    conn,
    *,
    workspace_id: str,
    task_id: str,
    actor: str,
    repo: str,
    branch: str,
    head_ref: str,
    base: str,
    commit: str,
    remote_sha: str,
    pr_url: str,
    remote: str | None,
    validation: str | None,
    existing_pr_payload: dict[str, Any],
) -> PublishResult:
    """Write `pr.linked` and update mirror with the discovered PR URL.

    Reuses the same idempotency key shape as `link_pr` for tasks already
    linked to this PR, so a manual `pr link` after discovery does not
    produce duplicate events.
    """
    rebind_error = check_existing_pr_rebind(
        conn, workspace_id=workspace_id, task_id=task_id, pr_url=pr_url
    )
    if rebind_error is not None:
        return _emit_publish_event(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            actor=actor,
            event_type="publish.blocked",
            extra_idem="rebind",
            payload=_blocked_payload(
                repo=repo,
                branch=branch,
                commit=commit,
                remote_sha=remote_sha,
                reason="pr_already_linked",
                message=rebind_error,
                remote=remote,
                validation=validation,
                head_ref=head_ref,
                base=base,
            ),
            result_kwargs=dict(
                repo=repo,
                branch=branch,
                head_ref=head_ref,
                base=base,
                commit=commit,
                reported_commit=commit,
                remote_sha=remote_sha,
                pr_url=None,
                action="blocked",
                existing=False,
                reason="pr_already_linked",
            ),
        )

    idem = publish_idempotency_key(
        workspace_id=workspace_id,
        task_id=task_id,
        event_type="pr.linked",
        repo=repo,
        branch=branch,
        commit=commit,
        extra=f"publish:{pr_url}",
    )
    payload: dict[str, Any] = {
        "task_id": task_id,
        "pr": pr_url,
        "pr_url": pr_url,
        "branch": branch,
        "head_ref": head_ref,
        "base": base,
        "repo": repo,
        "reported_commit": commit,
        "remote_sha": remote_sha,
        "source": "publish_pr",
    }
    if remote:
        payload["remote"] = remote
    if validation:
        payload["validation"] = validation
    event_result = append_event(
        conn,
        event_type="pr.linked",
        actor=actor,
        workspace_id=workspace_id,
        task_id=task_id,
        idempotency_key=idem,
        payload=payload,
    )
    existing_mirror = read_task_mirror(conn, workspace_id, task_id)
    if existing_mirror is not None:
        payload_dict = existing_mirror.get("payload") or {}
        if not isinstance(payload_dict, dict):
            payload_dict = {}
        payload_dict.setdefault("publish_metadata", {})
        payload_dict["publish_metadata"].update({
            "repo": repo,
            "branch": branch,
            "reported_commit": commit,
            "remote_sha": remote_sha,
        })
        upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=existing_mirror["phase"],
            owner=existing_mirror["owner"],
            branch=branch,
            pr=pr_url,
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
            branch=branch,
            pr=pr_url,
            payload={"publish_metadata": {
                "repo": repo,
                "branch": branch,
                "reported_commit": commit,
                "remote_sha": remote_sha,
            }},
            last_event_id=event_result.row["id"],
        )
    # Detect whether the mirror already pointed at this PR before.
    was_existing = bool(existing_mirror and existing_mirror.get("pr") == pr_url)
    return PublishResult(
        workspace_id=workspace_id,
        task_id=task_id,
        repo=repo,
        branch=branch,
        head_ref=head_ref,
        base=base,
        commit=remote_sha,
        reported_commit=commit,
        remote_sha=remote_sha,
        pr_url=pr_url,
        action="linked",
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
        existing=was_existing or not event_result.created,
        remote=remote,
        validation=validation,
    )


def _finalize_created(
    conn,
    *,
    workspace_id: str,
    task_id: str,
    actor: str,
    repo: str,
    branch: str,
    head_ref: str,
    base: str,
    commit: str,
    remote_sha: str,
    pr_url: str,
    remote: str | None,
    validation: str | None,
) -> PublishResult:
    """Write `pr.created` after a successful `gh pr create`."""
    rebind_error = check_existing_pr_rebind(
        conn, workspace_id=workspace_id, task_id=task_id, pr_url=pr_url
    )
    if rebind_error is not None:
        return _emit_publish_event(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            actor=actor,
            event_type="publish.blocked",
            extra_idem="rebind",
            payload=_blocked_payload(
                repo=repo,
                branch=branch,
                commit=commit,
                remote_sha=remote_sha,
                reason="pr_already_linked",
                message=rebind_error,
                remote=remote,
                validation=validation,
                head_ref=head_ref,
                base=base,
            ),
            result_kwargs=dict(
                repo=repo,
                branch=branch,
                head_ref=head_ref,
                base=base,
                commit=commit,
                reported_commit=commit,
                remote_sha=remote_sha,
                pr_url=None,
                action="blocked",
                existing=False,
                reason="pr_already_linked",
            ),
        )

    idem = publish_idempotency_key(
        workspace_id=workspace_id,
        task_id=task_id,
        event_type="pr.created",
        repo=repo,
        branch=branch,
        commit=commit,
        extra=f"publish:{pr_url}",
    )
    payload: dict[str, Any] = {
        "task_id": task_id,
        "pr": pr_url,
        "pr_url": pr_url,
        "branch": branch,
        "head_ref": head_ref,
        "base": base,
        "repo": repo,
        "reported_commit": commit,
        "remote_sha": remote_sha,
        "source": "publish_pr",
    }
    if remote:
        payload["remote"] = remote
    if validation:
        payload["validation"] = validation
    event_result = append_event(
        conn,
        event_type="pr.created",
        actor=actor,
        workspace_id=workspace_id,
        task_id=task_id,
        idempotency_key=idem,
        payload=payload,
    )
    existing_mirror = read_task_mirror(conn, workspace_id, task_id)
    if existing_mirror is not None:
        payload_dict = existing_mirror.get("payload") or {}
        if not isinstance(payload_dict, dict):
            payload_dict = {}
        payload_dict.setdefault("publish_metadata", {})
        payload_dict["publish_metadata"].update({
            "repo": repo,
            "branch": branch,
            "reported_commit": commit,
            "remote_sha": remote_sha,
            "pr_url": pr_url,
        })
        upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=existing_mirror["phase"],
            owner=existing_mirror["owner"],
            branch=branch,
            pr=pr_url,
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
            branch=branch,
            pr=pr_url,
            payload={"publish_metadata": {
                "repo": repo,
                "branch": branch,
                "reported_commit": commit,
                "remote_sha": remote_sha,
                "pr_url": pr_url,
            }},
            last_event_id=event_result.row["id"],
        )
    was_existing = bool(existing_mirror and existing_mirror.get("pr") == pr_url)
    return PublishResult(
        workspace_id=workspace_id,
        task_id=task_id,
        repo=repo,
        branch=branch,
        head_ref=head_ref,
        base=base,
        commit=remote_sha,
        reported_commit=commit,
        remote_sha=remote_sha,
        pr_url=pr_url,
        action="created",
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
        existing=was_existing or not event_result.created,
        remote=remote,
        validation=validation,
    )
