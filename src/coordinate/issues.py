from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import sqlite3

from .db import append_event, get_workspace, row_to_dict, upsert_task_mirror
from .onboarding import create_plan_task
from .policy import create_delivery_for_event
from .split_operations import (
    REASON_FILES_NOT_DEPLOYED,
    REASON_OPERATION_CONFLICT,
    REASON_VALIDATION_ERROR,
    SplitOperationError,
    apply_issue_materialize_files,
    apply_issue_materialize_record,
)


RunGh = Callable[[list[str]], subprocess.CompletedProcess[str]]
RunCli = Callable[[list[str]], subprocess.CompletedProcess[str]]


class IssueScanError(ValueError):
    pass


@dataclass(frozen=True)
class IssueCandidate:
    repo: str
    number: int
    url: str
    title: str
    labels: list[str]
    author: str
    state: str
    updated_at: str
    body_excerpt: str | None = None

    def payload(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "repo": self.repo,
            "number": self.number,
            "url": self.url,
            "title": self.title,
            "labels": self.labels,
            "author": self.author,
            "state": self.state,
            "updated_at": self.updated_at,
            "content_trust": "untrusted",
        }
        if self.body_excerpt:
            result["body_excerpt"] = self.body_excerpt
        return result

    def idempotency_key(self, workspace_id: str) -> str:
        return f"{workspace_id}:github_issue:{self.repo}:{self.number}:{self.updated_at}"


@dataclass(frozen=True)
class IssueScanResult:
    workspace_id: str
    repo: str
    scanned: int
    created: int
    existing: int
    events: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "repo": self.repo,
            "scanned": self.scanned,
            "created": self.created,
            "existing": self.existing,
            "events": self.events,
        }


def scan_github_issues(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    repo: str,
    label: str | None = None,
    limit: int = 50,
    actor: str = "github",
    run: RunGh | None = None,
) -> IssueScanResult:
    get_workspace(conn, workspace_id)
    candidates = list_github_issue_candidates(repo=repo, label=label, limit=limit, run=run)
    events: list[dict[str, Any]] = []
    created = 0
    existing = 0
    for issue in candidates:
        result = append_event(
            conn,
            workspace_id=workspace_id,
            event_type="issue.spotted",
            actor=actor,
            target=repo,
            idempotency_key=issue.idempotency_key(workspace_id),
            payload=issue.payload(),
        )
        if result.created:
            created += 1
        else:
            existing += 1
        event = row_to_dict(result.row)
        event["created"] = result.created
        events.append(event)
    return IssueScanResult(
        workspace_id=workspace_id,
        repo=repo,
        scanned=len(candidates),
        created=created,
        existing=existing,
        events=events,
    )


def scan_github_issues_via_event_cli(
    *,
    workspace_id: str,
    repo: str,
    event_cli_path: str,
    label: str | None = None,
    limit: int = 50,
    actor: str = "github",
    run_gh: RunGh | None = None,
    run_cli: RunCli | None = None,
) -> IssueScanResult:
    candidates = list_github_issue_candidates(repo=repo, label=label, limit=limit, run=run_gh)
    events: list[dict[str, Any]] = []
    created = 0
    existing = 0
    runner = run_cli or _run_cli
    for issue in candidates:
        completed = runner(_event_append_cmd(event_cli_path, workspace_id, actor, repo, issue))
        if completed.returncode != 0:
            stderr = (completed.stderr or "").strip()
            raise IssueScanError(
                f"event append failed for {repo}#{issue.number}: {stderr or completed.returncode}"
            )
        try:
            raw = json.loads(completed.stdout or "{}")
        except json.JSONDecodeError as exc:
            raise IssueScanError(f"event append returned invalid JSON: {exc}") from exc
        event = raw.get("event")
        if not isinstance(event, dict):
            raise IssueScanError("event append JSON missing event object")
        was_created = bool(raw.get("created"))
        if was_created:
            created += 1
        else:
            existing += 1
        event["created"] = was_created
        events.append(event)
    return IssueScanResult(
        workspace_id=workspace_id,
        repo=repo,
        scanned=len(candidates),
        created=created,
        existing=existing,
        events=events,
    )


def _event_append_cmd(
    event_cli_path: str,
    workspace_id: str,
    actor: str,
    repo: str,
    issue: IssueCandidate,
) -> list[str]:
    return [
        event_cli_path,
        "event",
        "append",
        "issue.spotted",
        "--workspace-id",
        workspace_id,
        "--actor",
        actor,
        "--target",
        repo,
        "--idempotency-key",
        issue.idempotency_key(workspace_id),
        "--payload-json",
        json.dumps(issue.payload(), ensure_ascii=False, separators=(",", ":")),
    ]


def list_github_issue_candidates(
    *,
    repo: str,
    label: str | None = None,
    limit: int = 50,
    run: RunGh | None = None,
) -> list[IssueCandidate]:
    if limit < 1:
        raise IssueScanError("limit must be >= 1")
    cmd = [
        "gh", "issue", "list",
        "--repo", repo,
        "--state", "open",
        "--limit", str(limit),
        "--json", "number,title,url,labels,author,state,updatedAt,body",
    ]
    if label:
        cmd.extend(["--label", label])
    runner = run or _run_gh
    completed = runner(cmd)
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        raise IssueScanError(f"gh issue list failed for {repo}: {stderr or completed.returncode}")
    try:
        raw = json.loads(completed.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise IssueScanError(f"gh issue list returned invalid JSON: {exc}") from exc
    if not isinstance(raw, list):
        raise IssueScanError("gh issue list JSON must be an array")
    return [_candidate_from_gh(repo, item) for item in raw]


def _run_gh(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")


def _run_cli(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")


def _candidate_from_gh(repo: str, item: Any) -> IssueCandidate:
    if not isinstance(item, dict):
        raise IssueScanError("gh issue item must be an object")
    number = item.get("number")
    updated_at = item.get("updatedAt") or item.get("updated_at")
    if not isinstance(number, int):
        raise IssueScanError("gh issue item missing integer number")
    if not updated_at:
        raise IssueScanError(f"gh issue {number} missing updatedAt")
    return IssueCandidate(
        repo=repo,
        number=number,
        url=str(item.get("url") or ""),
        title=str(item.get("title") or ""),
        labels=_label_names(item.get("labels")),
        author=_author_login(item.get("author")),
        state=str(item.get("state") or "open").lower(),
        updated_at=str(updated_at),
        body_excerpt=_excerpt(item.get("body")),
    )


def _label_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    labels: list[str] = []
    for item in value:
        if isinstance(item, dict):
            name = item.get("name")
        else:
            name = item
        if name:
            labels.append(str(name))
    return labels


def _author_login(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("login") or "")
    return str(value or "")


def _excerpt(value: Any, *, limit: int = 500) -> str | None:
    if not value:
        return None
    text = " ".join(str(value).split())
    if not text:
        return None
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


class IssueTriageError(ValueError):
    """Issue-flow error with an optional stable machine-readable reason.

    ``reason`` mirrors the split-operation reason constants
    (``files_not_deployed``, ``operation_conflict``, ``fingerprint_drift``,
    ``lock_timeout``, ``validation_error``) for C2 CLI error output, while
    preserving the existing string-message contract for non-C2 callers.
    """

    def __init__(self, message: str, reason: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason


TRIAGE_DECISIONS = ("accept", "reject", "defer")


@dataclass(frozen=True)
class IssueTriageResult:
    workspace_id: str
    source_event_id: str
    decision: str
    task_id: str | None
    event: dict[str, Any]
    event_created: bool
    task: dict[str, Any] | None
    delivery: dict[str, Any] | None
    delivery_created: bool | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "source_event_id": self.source_event_id,
            "decision": self.decision,
            "task_id": self.task_id,
            "event": self.event,
            "event_created": self.event_created,
            "task": self.task,
            "delivery": self.delivery,
            "delivery_created": self.delivery_created,
        }


def triage_issue(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    event_id: str,
    decision: str,
    task_id: str | None = None,
    title: str | None = None,
    owner: str | None = None,
    phase: str = "phase-8",
    actor: str = "operator",
    reason: str | None = None,
    platform: str | None = None,
    destination: str | None = None,
) -> IssueTriageResult:
    """Triage an issue.spotted event into accept (task mirror) / reject / defer.

    Idempotency: same (event_id, decision, task_id) reuses the existing
    issue.triaged event. A conflicting decision on an already-triaged issue is
    rejected. Issue body excerpts are preserved as untrusted payload and must
    never be treated as system instructions.
    """
    if decision not in TRIAGE_DECISIONS:
        raise IssueTriageError(
            f"decision must be one of {TRIAGE_DECISIONS}, got: {decision}"
        )

    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise IssueTriageError(f"unknown workspace: {workspace_id}")

    spotted_row = conn.execute(
        "SELECT * FROM events WHERE id = ?", (event_id,)
    ).fetchone()
    if spotted_row is None:
        raise IssueTriageError(f"issue.spotted event not found: {event_id}")
    spotted = row_to_dict(spotted_row)
    if spotted["workspace_id"] != workspace_id:
        raise IssueTriageError(
            f"event {event_id} belongs to workspace {spotted['workspace_id']}, "
            f"not {workspace_id}"
        )
    if spotted["event_type"] != "issue.spotted":
        raise IssueTriageError(
            f"event {event_id} is {spotted['event_type']}, not issue.spotted"
        )

    spotted_payload = spotted.get("payload") or {}
    repo = spotted_payload.get("repo") or spotted.get("target") or ""
    number = spotted_payload.get("number")
    issue_url = spotted_payload.get("url") or ""
    issue_title = spotted_payload.get("title") or ""
    body_excerpt = spotted_payload.get("body_excerpt")
    # GitHub issue content is never trusted. Do not honor any trust flag carried
    # in the spotted payload — a tampered/malicious payload could self-declare
    # "trusted" to escape the untrusted boundary.
    content_trust = "untrusted"

    if decision == "accept" and not task_id:
        raise IssueTriageError("task_id is required when decision is accept")

    # Idempotency + conflict detection: any prior issue.triaged on this event.
    prior_rows = conn.execute(
        "SELECT * FROM events WHERE workspace_id = ? AND event_type = 'issue.triaged'",
        (workspace_id,),
    ).fetchall()
    for prior_row in prior_rows:
        prior = row_to_dict(prior_row)
        prior_payload = prior.get("payload") or {}
        if prior_payload.get("source_event_id") != event_id:
            continue
        prior_decision = prior_payload.get("decision")
        prior_task = prior_payload.get("task_id")
        if prior_decision == decision and prior_task == task_id:
            delivery_dict, delivery_created = _try_create_delivery(
                conn, prior["id"], workspace, platform, destination
            )
            return IssueTriageResult(
                workspace_id=workspace_id,
                source_event_id=event_id,
                decision=decision,
                task_id=task_id,
                event=prior,
                event_created=False,
                task=None,
                delivery=delivery_dict,
                delivery_created=delivery_created,
            )
        prior_task_suffix = f" (task_id={prior_task})" if prior_task else ""
        raise IssueTriageError(
            f"issue {event_id} already triaged as {prior_decision}{prior_task_suffix}; "
            f"refusing conflicting decision={decision}"
        )

    # accept: create task mirror carrying GitHub issue metadata (untrusted).
    task_dict: dict[str, Any] | None = None
    if decision == "accept":
        resolved_title = title or issue_title or task_id
        task_payload: dict[str, Any] = {
            "task_id": task_id,
            "title": resolved_title,
            "source": "github_issue",
            "repo": repo,
            "issue_number": number,
            "issue_url": issue_url,
            "content_trust": content_trust,
            "triage_phase": phase,
        }
        if body_excerpt:
            task_payload["issue_body_excerpt"] = body_excerpt
        if owner:
            task_payload["owner"] = owner
        task_row, _ = upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=phase,
            owner=owner,
            branch=None,
            pr=None,
            payload=task_payload,
        )
        task_dict = row_to_dict(task_row)

    triage_payload: dict[str, Any] = {
        "source_event_id": event_id,
        "decision": decision,
        "repo": repo,
        "number": number,
        "url": issue_url,
        "title": issue_title,
        "content_trust": content_trust,
    }
    if task_id:
        triage_payload["task_id"] = task_id
    if reason:
        triage_payload["reason"] = reason
    if body_excerpt:
        triage_payload["issue_body_excerpt"] = body_excerpt

    hint_task = task_id or ""
    idempotency_key = f"{workspace_id}:issue.triaged:{event_id}:{decision}:{hint_task}"

    event_result = append_event(
        conn,
        workspace_id=workspace_id,
        event_type="issue.triaged",
        actor=actor,
        target=task_id or repo,
        task_id=task_id,
        idempotency_key=idempotency_key,
        payload=triage_payload,
    )
    event_dict = row_to_dict(event_result.row)

    delivery_dict, delivery_created = _try_create_delivery(
        conn, event_result.row["id"], workspace, platform, destination
    )

    return IssueTriageResult(
        workspace_id=workspace_id,
        source_event_id=event_id,
        decision=decision,
        task_id=task_id,
        event=event_dict,
        event_created=event_result.created,
        task=task_dict,
        delivery=delivery_dict,
        delivery_created=delivery_created,
    )


def _try_create_delivery(
    conn: sqlite3.Connection,
    event_id: str,
    workspace: Any,
    platform: str | None,
    destination: str | None,
) -> tuple[dict[str, Any] | None, bool | None]:
    effective_platform = platform or (workspace.default_bus if workspace else None)
    effective_destination = destination or (
        workspace.default_destination if workspace else None
    )
    if not effective_platform or not effective_destination:
        return None, None
    result = create_delivery_for_event(
        conn, event_id, platform=effective_platform, destination=effective_destination
    )
    return result.delivery, result.created


@dataclass(frozen=True)
class IssueMaterializeResult:
    workspace_id: str
    triage_event_id: str
    spotted_event_id: str | None
    task_id: str
    plan_doc: str
    task: dict[str, Any]
    plan_ready_event: dict[str, Any]
    event: dict[str, Any]
    event_created: bool
    delivery: dict[str, Any] | None
    delivery_created: bool | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "triage_event_id": self.triage_event_id,
            "spotted_event_id": self.spotted_event_id,
            "task_id": self.task_id,
            "plan_doc": self.plan_doc,
            "task": self.task,
            "plan_ready_event": self.plan_ready_event,
            "event": self.event,
            "event_created": self.event_created,
            "delivery": self.delivery,
            "delivery_created": self.delivery_created,
        }


def _refuse_runtime_copy(workspace: Any, *, allow_runtime_copy: bool = False) -> None:
    """Refuse to mutate harness files inside a server runtime deployment copy.

    `/opt/*` is a deploy-derived copy; the harness source of truth is the
    coding-host git checkout. materialize must not default to writing /opt harness
    state that the next deploy would overwrite.
    """
    if allow_runtime_copy:
        return
    for label, path in (("path", workspace.path), ("harness_root", workspace.harness_root)):
        if path and "/opt/" in str(path):
            raise IssueTriageError(
                f"refusing to materialize into runtime deployment copy "
                f"(workspace.{label}={path}); materialize must run from a coding "
                "host git checkout. Use the host-aware flow: "
                "`issue materialize-files` on the coding host, commit/push, "
                "deploy, then `issue materialize-record` via coord-ssh."
            )


def materialize_issue(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    event_id: str,
    plan_doc: str,
    task_id: str | None = None,
    title: str | None = None,
    owner: str | None = None,
    branch: str | None = None,
    phase: str = "ready",
    actor: str = "operator",
    platform: str | None = None,
    destination: str | None = None,
    allow_runtime_copy: bool = False,
) -> IssueMaterializeResult:
    """Materialize an accepted issue.triaged event into a plan-backed harness task.

    Reuses onboarding.create_plan_task to write plan.ready + sync the harness
    mvp-checklist + upsert the task mirror, so `task handoff` can pass the
    `_require_harness_task` preflight (checklist still needs a separate
    `plan approve` for the plan-gate). The issue body is never used as a plan
    or system prompt — the operator must supply a real --plan-doc file.
    """
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise IssueTriageError(f"unknown workspace: {workspace_id}")

    _refuse_runtime_copy(workspace, allow_runtime_copy=allow_runtime_copy)

    triage_row = conn.execute(
        "SELECT * FROM events WHERE id = ?", (event_id,)
    ).fetchone()
    if triage_row is None:
        raise IssueTriageError(f"issue.triaged event not found: {event_id}")
    triage = row_to_dict(triage_row)
    if triage["workspace_id"] != workspace_id:
        raise IssueTriageError(
            f"event {event_id} belongs to workspace {triage['workspace_id']}, "
            f"not {workspace_id}"
        )
    if triage["event_type"] != "issue.triaged":
        raise IssueTriageError(
            f"event {event_id} is {triage['event_type']}, not issue.triaged"
        )

    triage_payload = triage.get("payload") or {}
    if triage_payload.get("decision") != "accept":
        raise IssueTriageError(
            f"issue.triaged event {event_id} decision is "
            f"{triage_payload.get('decision')!r}; only accept can be materialized"
        )

    resolved_task_id = task_id or triage_payload.get("task_id")
    if not resolved_task_id:
        raise IssueTriageError(
            f"issue.triaged event {event_id} has no task_id and none was provided"
        )

    spotted_event_id = triage_payload.get("source_event_id")
    repo = triage_payload.get("repo") or ""
    number = triage_payload.get("number")
    issue_url = triage_payload.get("url") or ""
    issue_title = triage_payload.get("title") or ""

    # The accept step must have created the DB task mirror.
    task_mirror_row = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, resolved_task_id),
    ).fetchone()
    if task_mirror_row is None:
        raise IssueTriageError(
            f"task mirror not found for {workspace_id}/{resolved_task_id}; "
            "run `issue triage --decision accept` first"
        )

    # Operator must provide a real plan file. Refuse to synthesize one from
    # the issue body (untrusted input must never become a worker plan/prompt).
    if not plan_doc:
        raise IssueTriageError(
            "plan_doc is required; refusing to generate a plan from issue body"
        )
    plan_abs = _resolve_materialize_plan_path(workspace, plan_doc)
    if not plan_abs.is_file():
        raise IssueTriageError(
            f"plan_doc does not exist: {plan_abs}; operator must provide a real plan file"
        )

    # Idempotency + conflict detection on prior issue.materialized for this triage event.
    prior_rows = conn.execute(
        "SELECT * FROM events WHERE workspace_id = ? AND event_type = 'issue.materialized'",
        (workspace_id,),
    ).fetchall()
    for prior_row in prior_rows:
        prior = row_to_dict(prior_row)
        prior_payload = prior.get("payload") or {}
        if prior_payload.get("triage_event_id") != event_id:
            continue
        prior_task = prior_payload.get("task_id")
        prior_plan = prior_payload.get("plan_doc")
        if prior_task == resolved_task_id and prior_plan == plan_doc:
            delivery_dict, delivery_created = _try_create_delivery(
                conn, prior["id"], workspace, platform, destination
            )
            current_task = _read_task_mirror(conn, workspace_id, resolved_task_id)
            return IssueMaterializeResult(
                workspace_id=workspace_id,
                triage_event_id=event_id,
                spotted_event_id=spotted_event_id,
                task_id=resolved_task_id,
                plan_doc=plan_doc,
                task=current_task,
                plan_ready_event=_latest_plan_ready_for_task(
                    conn, workspace_id, resolved_task_id
                )
                or {},
                event=prior,
                event_created=False,
                delivery=delivery_dict,
                delivery_created=delivery_created,
            )
        raise IssueTriageError(
            f"issue.triaged event {event_id} already materialized as "
            f"task_id={prior_task}, plan_doc={prior_plan}; "
            f"refusing conflicting materialize task_id={resolved_task_id}, "
            f"plan_doc={plan_doc}"
        )

    # Reuse create_plan_task: writes plan.ready + syncs mvp-checklist +
    # upserts the task mirror. GitHub issue metadata rides in the payload,
    # always flagged content_trust=untrusted.
    plan_result = create_plan_task(
        conn,
        workspace_id=workspace_id,
        task_id=resolved_task_id,
        plan_doc=plan_doc,
        title=title or issue_title or resolved_task_id,
        owner=owner,
        branch=branch,
        phase=phase,
        actor=actor,
        target="worker",
        payload={
            "source": "github_issue",
            "repo": repo,
            "issue_number": number,
            "issue_url": issue_url,
            "content_trust": "untrusted",
            "spotted_event_id": spotted_event_id,
            "triage_event_id": event_id,
        },
    )

    materialize_payload = {
        "triage_event_id": event_id,
        "spotted_event_id": spotted_event_id,
        "repo": repo,
        "number": number,
        "issue_url": issue_url,
        "task_id": resolved_task_id,
        "plan_doc": plan_doc,
        "content_trust": "untrusted",
        "plan_ready_event_id": plan_result.event["id"],
    }
    idempotency_key = (
        f"{workspace_id}:issue.materialized:{event_id}:{resolved_task_id}:{plan_doc}"
    )
    event_result = append_event(
        conn,
        workspace_id=workspace_id,
        event_type="issue.materialized",
        actor=actor,
        target=resolved_task_id,
        task_id=resolved_task_id,
        idempotency_key=idempotency_key,
        payload=materialize_payload,
    )
    event_dict = row_to_dict(event_result.row)

    delivery_dict, delivery_created = _try_create_delivery(
        conn, event_result.row["id"], workspace, platform, destination
    )

    current_task = _read_task_mirror(conn, workspace_id, resolved_task_id)
    return IssueMaterializeResult(
        workspace_id=workspace_id,
        triage_event_id=event_id,
        spotted_event_id=spotted_event_id,
        task_id=resolved_task_id,
        plan_doc=plan_doc,
        task=current_task,
        plan_ready_event=plan_result.event,
        event=event_dict,
        event_created=event_result.created,
        delivery=delivery_dict,
        delivery_created=delivery_created,
    )


def _resolve_materialize_plan_path(workspace: Any, plan_doc: str) -> Path:
    candidate = Path(plan_doc).expanduser()
    if not candidate.is_absolute():
        candidate = Path(workspace.path) / candidate
    return candidate.resolve()


def _read_task_mirror(
    conn: sqlite3.Connection, workspace_id: str, task_id: str
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()
    return row_to_dict(row)


def _latest_plan_ready_for_task(
    conn: sqlite3.Connection, workspace_id: str, task_id: str
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM events WHERE workspace_id = ? AND task_id = ? "
        "AND event_type = 'plan.ready' ORDER BY rowid DESC LIMIT 1",
        (workspace_id, task_id),
    ).fetchone()
    return row_to_dict(row) if row else None


@dataclass(frozen=True)
class IssueMaterializeFilesResult:
    workspace_id: str
    task_id: str
    plan_doc: str
    checklist_changed: bool
    operation_id: str | None = None
    operation_kind: str | None = None
    contract_version: int | None = None
    source_event_id: str | None = None
    source_kind: str | None = None
    target_kind: str | None = None
    target_id: str | None = None
    input_fingerprint: str | None = None
    before_fingerprint: str | None = None
    after_fingerprint: str | None = None
    files_applied_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "plan_doc": self.plan_doc,
            "checklist_changed": self.checklist_changed,
        }
        if self.operation_id is not None:
            result["operation_id"] = self.operation_id
        if self.operation_kind is not None:
            result["operation_kind"] = self.operation_kind
        if self.contract_version is not None:
            result["contract_version"] = self.contract_version
        if self.source_event_id is not None:
            result["source_event_id"] = self.source_event_id
        if self.source_kind is not None:
            result["source_kind"] = self.source_kind
        if self.target_kind is not None:
            result["target_kind"] = self.target_kind
        if self.target_id is not None:
            result["target_id"] = self.target_id
        if self.input_fingerprint is not None:
            result["input_fingerprint"] = self.input_fingerprint
        if self.before_fingerprint is not None:
            result["before_fingerprint"] = self.before_fingerprint
        if self.after_fingerprint is not None:
            result["after_fingerprint"] = self.after_fingerprint
        if self.files_applied_at is not None:
            result["files_applied_at"] = self.files_applied_at
        return result


@dataclass(frozen=True)
class IssueMaterializeRecordHostResult:
    """Host-aware ``materialize-record`` result exposing the operation ledger."""

    workspace_id: str
    triage_event_id: str
    spotted_event_id: str | None
    task_id: str
    plan_doc: str
    task: dict[str, Any]
    plan_ready_event: dict[str, Any]
    event: dict[str, Any]
    event_created: bool
    operation: dict[str, Any]
    delivery: dict[str, Any] | None
    delivery_created: bool | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "triage_event_id": self.triage_event_id,
            "spotted_event_id": self.spotted_event_id,
            "task_id": self.task_id,
            "plan_doc": self.plan_doc,
            "task": self.task,
            "plan_ready_event": self.plan_ready_event,
            "event": self.event,
            "event_created": self.event_created,
            "operation": self.operation,
            "delivery": self.delivery,
            "delivery_created": self.delivery_created,
        }


def materialize_issue_files(
    *,
    workspace_path: str,
    harness_root: str,
    workspace_id: str,
    operation_id: str,
    event_id: str,
    task_id: str,
    plan_doc: str,
    title: str | None = None,
    phase: str = "ready",
    priority: str = "p1",
    allow_runtime_copy: bool = False,
) -> IssueMaterializeFilesResult:
    """Coding-host half of host-aware materialize: write mvp-checklist.json only.

    Runs on the Mac/Windows git checkout (operator passes the local paths).
    Does NOT touch the coordinate DB — that is the job of `materialize-record`
    via coord-ssh. Refuses /opt runtime copies so it cannot be pointed at a
    deploy-derived tree.
    """
    workspace = SimpleNamespace(
        id=workspace_id, path=str(workspace_path), harness_root=str(harness_root),
    )
    try:
        _refuse_runtime_copy(workspace, allow_runtime_copy=allow_runtime_copy)
    except IssueTriageError as exc:
        # Host-aware files path only: classify the /opt guard as a stable
        # validation_error reason while keeping the legacy combined guard
        # output unchanged.
        raise IssueTriageError(str(exc), reason=REASON_VALIDATION_ERROR) from exc
    if not task_id:
        raise IssueTriageError(
            "task_id is required for materialize-files",
            reason=REASON_VALIDATION_ERROR,
        )
    if not plan_doc:
        raise IssueTriageError(
            "plan_doc is required; refusing to generate a plan from issue body",
            reason=REASON_VALIDATION_ERROR,
        )
    plan_abs = _resolve_materialize_plan_path(workspace, plan_doc)
    if not plan_abs.is_file():
        raise IssueTriageError(
            f"plan_doc does not exist: {plan_abs}; operator must provide a real plan file",
            reason=REASON_FILES_NOT_DEPLOYED,
        )
    try:
        split_result = apply_issue_materialize_files(
            workspace_path=workspace_path,
            harness_root=harness_root,
            task_id=task_id,
            plan_doc=plan_doc,
            title=title,
            phase=phase,
            priority=priority,
            operation_id=operation_id,
            workspace_id=workspace_id,
            source_event_id=event_id,
        )
    except SplitOperationError as exc:
        raise IssueTriageError(str(exc), reason=exc.reason) from exc
    return IssueMaterializeFilesResult(
        workspace_id=split_result.workspace_id,
        task_id=split_result.task_id,
        plan_doc=split_result.plan_doc,
        checklist_changed=split_result.checklist_changed,
        operation_id=split_result.operation_id,
        operation_kind=split_result.operation_kind,
        contract_version=split_result.contract_version,
        source_event_id=split_result.source_event_id,
        source_kind="issue_triaged_event",
        target_kind="checklist_task",
        target_id=split_result.task_id,
        input_fingerprint=split_result.input_fingerprint,
        before_fingerprint=split_result.before_fingerprint,
        after_fingerprint=split_result.after_fingerprint,
        files_applied_at=split_result.files_applied_at,
    )


def materialize_issue_record(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    event_id: str,
    plan_doc: str,
    operation_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
    task_id: str | None = None,
    title: str | None = None,
    owner: str | None = None,
    branch: str | None = None,
    phase: str | None = None,
    actor: str = "operator",
    platform: str | None = None,
    destination: str | None = None,
) -> IssueMaterializeRecordHostResult:
    """Server half of host-aware materialize: write control-plane DB only.

    Verifies the accepted ``issue.triaged`` event and the exact C2 envelope
    deployed by ``materialize-files`` before writing the ledger, task mirror,
    ``plan.ready``, ``issue.materialized`` and optional delivery under one
    transaction. Does NOT write harness files.
    """
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise IssueTriageError(
            f"unknown workspace: {workspace_id}", reason=REASON_OPERATION_CONFLICT
        )

    triage_row = conn.execute(
        "SELECT * FROM events WHERE id = ?", (event_id,)
    ).fetchone()
    if triage_row is None:
        raise IssueTriageError(
            f"issue.triaged event not found: {event_id}",
            reason=REASON_OPERATION_CONFLICT,
        )
    triage = row_to_dict(triage_row)
    if triage["workspace_id"] != workspace_id:
        raise IssueTriageError(
            f"event {event_id} belongs to workspace {triage['workspace_id']}, "
            f"not {workspace_id}",
            reason=REASON_OPERATION_CONFLICT,
        )
    if triage["event_type"] != "issue.triaged":
        raise IssueTriageError(
            f"event {event_id} is {triage['event_type']}, not issue.triaged",
            reason=REASON_OPERATION_CONFLICT,
        )
    triage_payload = triage.get("payload") or {}
    if triage_payload.get("decision") != "accept":
        raise IssueTriageError(
            f"issue.triaged event {event_id} decision is "
            f"{triage_payload.get('decision')!r}; only accept can be materialized",
            reason=REASON_OPERATION_CONFLICT,
        )
    resolved_task_id = task_id or triage_payload.get("task_id")
    if not resolved_task_id:
        raise IssueTriageError(
            f"issue.triaged event {event_id} has no task_id and none was provided",
            reason=REASON_OPERATION_CONFLICT,
        )

    try:
        split_result = apply_issue_materialize_record(
            conn,
            workspace_id=workspace_id,
            source_event_id=event_id,
            task_id=resolved_task_id,
            plan_doc=plan_doc,
            operation_id=operation_id,
            input_fingerprint=input_fingerprint,
            before_fingerprint=before_fingerprint,
            after_fingerprint=after_fingerprint,
            title=title,
            phase=phase,
            owner=owner,
            branch=branch,
            actor=actor,
            target="worker",
            platform=platform,
            destination=destination,
        )
    except SplitOperationError as exc:
        raise IssueTriageError(str(exc), reason=exc.reason) from exc
    return IssueMaterializeRecordHostResult(
        workspace_id=workspace_id,
        triage_event_id=event_id,
        spotted_event_id=triage_payload.get("source_event_id"),
        task_id=resolved_task_id,
        plan_doc=plan_doc,
        task=split_result.task,
        plan_ready_event=split_result.plan_ready_event,
        event=split_result.event,
        event_created=split_result.event_created,
        operation=split_result.operation,
        delivery=split_result.delivery,
        delivery_created=split_result.delivery_created,
    )
