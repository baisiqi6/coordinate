"""Remote record/preflight half of the Phase 8.4 PR publish protocol."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from coordinate import github as github_module
from coordinate.db import append_event, get_workspace, row_to_dict, upsert_task_mirror
from coordinate.pr_contracts import (
    ACTION_TO_EVENT_TYPE,
    PUBLISH_ACTIONS,
    check_cross_task_conflict,
    check_existing_pr_rebind,
    check_mirror_conflict,
    publish_idempotency_key,
    read_task_mirror,
    validate_publish_success_facts,
)

# --------------------------------------------------------------------------
# Phase 8.4 — record-only sink for host -> remote forwarding
# --------------------------------------------------------------------------


class RecordPublishError(ValueError):
    """Raised by the record-only sink when a host's PublishResult cannot
    be faithfully recorded into a remote DB.

    The sink is intentionally narrow: it never invokes `gh`. It only
    re-validates the host's claim against the remote task mirror, then
    appends the event and (on success) upserts the mirror. Anything else
    — missing keys, schema mismatch, mirror conflict — fails closed with
    a typed `reason`.
    """

    def __init__(self, message: str, *, reason: str):
        super().__init__(message)
        self.reason = reason


_RECORDABLE_ACTIONS = set(PUBLISH_ACTIONS)

_ACTION_TO_EVENT_TYPE = dict(ACTION_TO_EVENT_TYPE)


@dataclass(frozen=True)
class RecordPublishResult:
    workspace_id: str
    task_id: str
    event: dict[str, Any]
    event_created: bool
    mirror_updated: bool
    action: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "event": self.event,
            "event_created": self.event_created,
            "mirror_updated": self.mirror_updated,
            "action": self.action,
        }


def _record_event_type_for(action: str) -> str:
    """Server-side canonical event type for a publish action.

    We never accept the event_type from the host envelope — the host
    might be wrong, stale, or hostile. The server recomputes from the
    host's action decision and raises if the recomputed type is not
    one of the four supported publish events.
    """
    event_type = _ACTION_TO_EVENT_TYPE.get(action)
    if event_type is None:
        raise RecordPublishError(
            f"unsupported action for record-only sink: {action!r}",
            reason="invalid_result",
        )
    return event_type


def _record_event_payload(
    *,
    action: str,
    task_id: str,
    repo: str,
    branch: str,
    reported_commit: str,
    head_ref: str,
    base: str,
    pr_url: str,
    remote_sha: str | None,
    audit_remote: str | None,
    audit_validation: str | None,
    blocked_reason: str | None,
    blocked_message: str | None,
    blocked_detail: str | None,
    blocked_remote_sha: str | None,
    blocked_head_ref: str | None,
    blocked_base: str | None,
) -> dict[str, Any]:
    """Build the canonical event payload for the record-only sink.

    Independent of any envelope the host may have passed — this is what
    the merge gate and audit log will see, so it must come from the
    server. Field selection follows the existing policy renderer
    expectations.
    """
    payload: dict[str, Any] = {"task_id": task_id}
    if action in {"created", "linked"}:
        payload.update({
            "pr": pr_url,
            "pr_url": pr_url,
            "branch": branch,
            "repo": repo,
            "reported_commit": reported_commit,
        })
        if head_ref:
            payload["head_ref"] = head_ref
        if base:
            payload["base"] = base
        if remote_sha:
            payload["remote_sha"] = remote_sha
        if audit_remote:
            payload["remote"] = audit_remote
        if audit_validation:
            payload["validation"] = audit_validation
    elif action == "push_required":
        payload.update({
            "repo": repo,
            "branch": branch,
            "reported_commit": reported_commit,
            "remote": audit_remote or "origin",
            "next_action": (
                f"push the branch to {audit_remote or 'origin'} from the "
                f"worker host ({branch} -> {reported_commit}) then rerun "
                f"`pr publish`"
            ),
        })
        if audit_validation:
            payload["validation"] = audit_validation
        if blocked_detail:
            payload["detail"] = blocked_detail
    elif action == "blocked":
        payload.update({
            "repo": repo,
            "branch": branch,
            "reported_commit": reported_commit,
            "reason": blocked_reason or "unknown",
            "message": blocked_message or "",
        })
        if blocked_remote_sha:
            payload["remote_sha"] = blocked_remote_sha
        if blocked_head_ref:
            payload["head_ref"] = blocked_head_ref
        if blocked_base:
            payload["base"] = blocked_base
        if audit_remote:
            payload["remote"] = audit_remote
        if audit_validation:
            payload["validation"] = audit_validation
        if blocked_detail:
            payload["detail"] = blocked_detail
    return payload


def _validate_record_success_facts(
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

    Delegates to the canonical contract validator and wraps any
    ``GitHubCommandError`` in a ``RecordPublishError`` so the sink fails
    closed with a typed ``reason``.
    """
    try:
        validate_publish_success_facts(
            workspace_id=workspace_id,
            result=result,
            repo=repo,
            branch=branch,
            reported_commit=reported_commit,
            head_ref=head_ref,
            base=base,
            pr_url=pr_url,
            remote_sha=remote_sha,
        )
    except github_module.GitHubCommandError as exc:
        raise RecordPublishError(str(exc), reason="invalid_result") from exc


def record_publish_preflight(
    conn,
    *,
    workspace_id: str,
    repo: str,
    branch: str,
    reported_commit: str,
    task_id: str,
) -> dict[str, Any]:
    """Read-only check: would `record_publish_result` accept this
    host's claim against the remote state?

    Returns a dict with `ok`, `reason`, `message`, and optionally the
    conflicting mirror payload. Never writes any event or mirror row.
    This is what the host CLI calls *before* invoking `gh pr create` so
    that mirror conflicts are caught before any GitHub write happens.
    """
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        return {
            "ok": False,
            "reason": "unknown_workspace",
            "message": f"unknown workspace: {workspace_id}",
        }
    if not (repo and branch and reported_commit and task_id):
        return {
            "ok": False,
            "reason": "invalid_result",
            "message": "missing required fact (repo/branch/reported_commit/task_id)",
        }
    mirror = read_task_mirror(conn, workspace_id, task_id)
    existing_pr = mirror.get("pr") if mirror else None
    conflict = check_mirror_conflict(
        mirror=mirror,
        repo=repo,
        branch=branch,
        commit=reported_commit,
        allow_commit_change=bool(existing_pr),
    )
    if conflict is not None:
        return {
            "ok": False,
            "reason": "mirror_conflict",
            "message": conflict,
            "mirror_branch": mirror.get("branch") if mirror else None,
        }
    # If the task already has a PR, the host must NOT call `gh pr create`.
    # Instead it should discover/link the existing PR. When the worker's
    # branch/commit/repo are consistent with the mirror, we return ok=true
    # plus expected_pr_url so the host can verify the PR on GitHub (read-only)
    # and return linked on a subsequent run.
    if existing_pr:
        return {
            "ok": True,
            "reason": None,
            "message": (
                f"task {task_id} already has pr '{existing_pr}'; "
                f"host must verify it read-only and link"
            ),
            "mode": "link_existing",
            "expected_pr_url": existing_pr,
        }
    # Preflight also surfaces cross-task branch conflicts so the host can
    # fail before any GitHub write.
    cross_conflict = check_cross_task_conflict(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        branch=branch,
        pr_url=None,
    )
    if cross_conflict is not None:
        return {
            "ok": False,
            "reason": "cross_task_conflict",
            "message": cross_conflict,
        }
    return {
        "ok": True,
        "reason": None,
        "message": "",
    }


def record_publish_result(
    conn,
    *,
    workspace_id: str,
    result: dict[str, Any],
    actor: str = "operator",
) -> RecordPublishResult:
    """Atomically record a host-side publish decision into the remote DB.

    The sink is **deliberately paranoid** about the host envelope. It
    accepts only the minimal set of facts (action, repo, branch,
    reported_commit, task_id, head_ref, base, pr_url, remote_sha,
    audit-only remote/validation, blocked-context fields) and
    recomputes the event_type, idempotency_key, and event payload
    from those facts alone. The host is never allowed to inject its
    own event_type, idempotency_key, or event_payload.

    Steps:

    1. Validate workspace + minimal fact shape.
    2. Re-derive the canonical event_type from the action
       (`created`→`pr.created`, `linked`→`pr.linked`,
       `push_required`→`push.required`, `blocked`→`publish.blocked`).
       Any disagreement with a host-supplied event_type is recorded
       as `invalid_result` because the host should not be passing it.
    3. Re-derive the idempotency_key from the canonical tuple
       `(workspace_id, task_id, event_type, repo, branch, commit,
       extra_idem)` — same algorithm `publish_pr` uses locally, so
       host and remote agree on the dedup boundary.
    4. Re-run the mirror conflict check against the remote task
       mirror.
    5. Run `append_event` + (on success) `upsert_task_mirror` inside a
       single SQLite transaction. A failure mid-transaction leaves no
       half-state.
    6. Replay-safe: if the event already exists from a prior call but
       the mirror is missing (action=created/linked), re-upsert the
       mirror from the host's facts. If the action=created/linked but
       pr_url is missing, raise — the host's claim is incomplete.
    """
    if not isinstance(result, dict):
        raise RecordPublishError(
            "result must be an object",
            reason="invalid_result",
        )
    action = result.get("action")
    if action not in _RECORDABLE_ACTIONS:
        raise RecordPublishError(
            f"action must be one of {sorted(_RECORDABLE_ACTIONS)}, "
            f"got: {action!r}",
            reason="invalid_result",
        )
    if not workspace_id:
        raise RecordPublishError(
            "workspace_id is required",
            reason="invalid_result",
        )
    envelope_workspace = result.get("workspace_id")
    if envelope_workspace != workspace_id:
        raise RecordPublishError(
            f"result workspace_id {envelope_workspace!r} does not match "
            f"record target {workspace_id!r}",
            reason="invalid_result",
        )
    repo = result.get("repo")
    branch = result.get("branch")
    reported_commit = result.get("reported_commit")
    task_id = result.get("task_id")
    if not all(
        isinstance(value, str) and value
        for value in (repo, branch, reported_commit, task_id)
    ):
        raise RecordPublishError(
            "result missing required fact "
            "(repo/branch/reported_commit/task_id)",
            reason="invalid_result",
        )
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise RecordPublishError(
            f"unknown workspace: {workspace_id}",
            reason="unknown_workspace",
        )

    # Step 1: server-computed canonical event_type / payload / idem key.
    # The host is not trusted to supply these — we derive them from
    # the action + minimal facts and ignore any host-supplied
    # event_type/idempotency_key/event_payload to prevent drift.
    event_type = _record_event_type_for(action)
    optional_string_fields = (
        "head_ref", "base", "pr_url", "remote_sha", "commit", "remote",
        "validation", "audit_remote", "audit_validation", "reason", "message",
        "detail", "blocked_reason", "blocked_message", "blocked_detail",
        "blocked_remote_sha", "blocked_head_ref", "blocked_base",
    )
    for field in optional_string_fields:
        value = result.get(field)
        if value is not None and not isinstance(value, str):
            raise RecordPublishError(
                f"result field {field!r} must be a string or null",
                reason="invalid_result",
            )
    head_ref = result.get("head_ref") or ""
    base = result.get("base") or ""
    pr_url = result.get("pr_url") or "" if action in {"created", "linked"} else ""
    remote_sha = result.get("remote_sha") if action in {"created", "linked"} else None
    audit_remote = result.get("audit_remote") or result.get("remote") or None
    audit_validation = result.get("audit_validation") or result.get("validation") or None
    # Host message/detail/reason are audit context, not security gate
    # inputs. The sink copies them into the payload verbatim so Discord /
    # audit readers see the worker's exact wording.
    blocked_reason = result.get("blocked_reason") or result.get("reason")
    blocked_message = result.get("blocked_message") or result.get("message")
    blocked_detail = result.get("blocked_detail") or result.get("detail")
    blocked_remote_sha = result.get("blocked_remote_sha") or result.get("remote_sha")
    blocked_head_ref = result.get("blocked_head_ref") or head_ref
    blocked_base = result.get("blocked_base") or base

    # Validate: created/linked must carry a pr_url.
    if action in {"created", "linked"} and not pr_url:
        raise RecordPublishError(
            f"action={action} requires non-empty pr_url",
            reason="invalid_result",
        )
    if action in {"created", "linked"}:
        _validate_record_success_facts(
            workspace_id=workspace_id,
            result=result,
            repo=repo,
            branch=branch,
            reported_commit=reported_commit,
            head_ref=head_ref,
            base=base,
            pr_url=pr_url,
            remote_sha=remote_sha,
        )

    payload = _record_event_payload(
        action=action,
        task_id=task_id,
        repo=repo,
        branch=branch,
        reported_commit=reported_commit,
        head_ref=head_ref,
        base=base,
        pr_url=pr_url,
        remote_sha=remote_sha,
        audit_remote=audit_remote,
        audit_validation=audit_validation,
        blocked_reason=blocked_reason,
        blocked_message=blocked_message,
        blocked_detail=blocked_detail,
        blocked_remote_sha=blocked_remote_sha,
        blocked_head_ref=blocked_head_ref,
        blocked_base=blocked_base,
    )

    # Step 2: server-computed idempotency_key. Use the same shape as
    # `publish_pr` so a host running publish_pr locally and then
    # forwarding to this sink gets the same key.
    extra = {
        "created": "publish",
        "linked": "publish",
        "push_required": "not_pushed",
        "blocked": _blocked_extra_idem(result),
    }[action]
    if action in {"created", "linked"} and pr_url:
        # The host's pr URL is part of the idempotency boundary for
        # the create/linked event (replay after transient write must
        # not collide with a different PR).
        extra = f"publish:{pr_url}"
    idem_key = publish_idempotency_key(
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=event_type,
        repo=repo,
        branch=branch,
        commit=reported_commit,
        extra=extra,
    )

    # Step 3: atomic validation + append_event + (conditional) mirror upsert.
    #
    # All reads and writes that enforce invariants must happen inside the
    # same SAVEPOINT. A read outside the transaction followed by a write
    # inside is vulnerable to TOCTOU: another connection can commit a
    # conflicting row after our read but before our write. SQLite unique
    # indexes on (workspace_id, branch) and (workspace_id, pr) make the
    # final write fail-closed; we catch the resulting IntegrityError and
    # roll back so no half-state is visible.
    #
    # The DB helpers are called with commit=False so the SAVEPOINT owns both
    # writes on every supported Python version. Releasing the outermost
    # SAVEPOINT commits; rollback removes both event and mirror.
    mirror_updated = False
    event_dict: dict[str, Any] = {}
    event_result_created = False
    try:
        conn.execute("SAVEPOINT record_publish")

        # Re-run the mirror conflict check inside the transaction so we
        # see the latest committed state and hold the SAVEPOINT until write.
        mirror = read_task_mirror(conn, workspace_id, task_id)
        existing_pr = mirror.get("pr") if mirror else None
        conflict = check_mirror_conflict(
            mirror=mirror,
            repo=repo,
            branch=branch,
            commit=reported_commit,
            allow_commit_change=(
                action == "linked" and existing_pr == pr_url
            ),
        )
        if conflict is not None:
            raise RecordPublishError(conflict, reason="mirror_conflict")

        # Cross-task uniqueness (branch and PR must not belong to another
        # task). Concurrent checks outside the transaction are not enough;
        # this is the last read before write, and the unique indexes below
        # are the final arbiter.
        cross_conflict = check_cross_task_conflict(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            branch=branch,
            pr_url=pr_url if action in {"created", "linked"} else None,
        )
        if cross_conflict is not None:
            raise RecordPublishError(cross_conflict, reason="cross_task_conflict")

        # Same-task PR rebind protection. `link_pr` enforces this; the
        # record-only sink must enforce it too, otherwise /pull/1 can be
        # silently overwritten by /pull/2 for the same task.
        if action in {"created", "linked"}:
            rebind_error = check_existing_pr_rebind(
                conn, workspace_id=workspace_id, task_id=task_id, pr_url=pr_url
            )
            if rebind_error is not None:
                raise RecordPublishError(rebind_error, reason="pr_already_linked")

        event_result = append_event(
            conn,
            event_type=event_type,
            actor=actor,
            workspace_id=workspace_id,
            task_id=task_id,
            idempotency_key=idem_key,
            payload=payload,
            commit=False,
        )
        event_dict = row_to_dict(event_result.row)
        event_result_created = event_result.created

        if action in {"created", "linked"}:
            mirror_updated = _record_upsert_mirror(
                conn,
                workspace_id=workspace_id,
                task_id=task_id,
                repo=repo,
                branch=branch,
                reported_commit=reported_commit,
                head_ref=head_ref,
                base=base,
                pr_url=pr_url,
            remote_sha=remote_sha,
            last_event_id=event_result.row["id"],
            event_created=event_result.created,
        )
        conn.execute("RELEASE SAVEPOINT record_publish")
    except Exception as exc:
        try:
            conn.execute("ROLLBACK TO SAVEPOINT record_publish")
        except sqlite3.Error:
            pass
        try:
            conn.execute("RELEASE SAVEPOINT record_publish")
        except sqlite3.Error:
            pass
        if isinstance(exc, sqlite3.IntegrityError):
            msg = str(exc)
            # SQLite reports unique-constraint failures with the table
            # column list, not the index name. We match the canonical
            # (workspace_id, branch) and (workspace_id, pr) combinations
            # so the guard is robust even if the index is renamed.
            if "tasks.workspace_id, tasks.branch" in msg:
                raise RecordPublishError(
                    f"branch '{branch}' already allocated to another task "
                    f"in workspace {workspace_id}",
                    reason="cross_task_conflict",
                ) from exc
            if "tasks.workspace_id, tasks.pr" in msg:
                raise RecordPublishError(
                    f"pr '{pr_url}' already linked to another task "
                    f"in workspace {workspace_id}",
                    reason="cross_task_conflict",
                ) from exc
        raise
    return RecordPublishResult(
        workspace_id=workspace_id,
        task_id=task_id,
        event=event_dict,
        event_created=event_result_created,
        mirror_updated=mirror_updated,
        action=action,
    )


def _blocked_extra_idem(result: dict[str, Any]) -> str:
    """Derive a stable `extra` suffix for blocked events from the host's
    reason. Replays with the same reason must collide on the
    idempotency_key.
    """
    reason = result.get("blocked_reason") or result.get("reason") or "unknown"
    message = result.get("blocked_message") or result.get("message") or ""
    return f"validation:{reason}:{message}" if message else f"validation:{reason}"


def _record_upsert_mirror(
    conn,
    *,
    workspace_id: str,
    task_id: str,
    repo: str,
    branch: str,
    reported_commit: str,
    head_ref: str,
    base: str,
    pr_url: str,
    remote_sha: str | None,
    last_event_id: str,
    event_created: bool,
) -> bool:
    """Upsert the remote task mirror from a host's publish facts.

    Returns True when the mirror was newly written or updated to
    include the PR URL. Returns False when the existing mirror already
    has the same pr_url and publish_metadata (replay with no drift).
    """
    publish_metadata: dict[str, Any] = {
        "repo": repo,
        "branch": branch,
        "reported_commit": reported_commit,
    }
    if head_ref:
        publish_metadata["head_ref"] = head_ref
    if base:
        publish_metadata["base"] = base
    if remote_sha:
        publish_metadata["remote_sha"] = remote_sha
    publish_metadata["pr_url"] = pr_url

    existing = read_task_mirror(conn, workspace_id, task_id)
    if existing is None:
        _, status = upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=None,
            owner=None,
            branch=branch,
            pr=pr_url,
            payload={"publish_metadata": publish_metadata},
            last_event_id=last_event_id,
            commit=False,
        )
        return status != "unchanged"

    payload_dict = existing.get("payload") or {}
    if not isinstance(payload_dict, dict):
        payload_dict = {}
    payload_dict.setdefault("publish_metadata", {})
    payload_dict["publish_metadata"].update(publish_metadata)
    # Only set branch when the remote mirror has none — never silently
    # overwrite a trusted branch recorded earlier.
    new_branch = existing.get("branch") or branch
    effective_last_event_id = last_event_id
    current_last_event_id = existing.get("last_event_id")
    if not event_created and current_last_event_id:
        rowids = {
            row["id"]: row["rowid"]
            for row in conn.execute(
                "SELECT rowid, id FROM events WHERE id IN (?, ?)",
                (current_last_event_id, last_event_id),
            ).fetchall()
        }
        current_rowid = rowids.get(current_last_event_id)
        publish_rowid = rowids.get(last_event_id)
        if (
            current_rowid is not None
            and publish_rowid is not None
            and current_rowid >= publish_rowid
        ):
            effective_last_event_id = current_last_event_id
    _, status = upsert_task_mirror(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        phase=existing.get("phase"),
        owner=existing.get("owner"),
        branch=new_branch,
        pr=pr_url,
        payload=payload_dict,
        last_event_id=effective_last_event_id,
        commit=False,
    )
    return status != "unchanged"
