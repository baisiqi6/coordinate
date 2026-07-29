from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .db import (
    Workspace,
    append_event,
    get_workspace,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
    utc_now,
)
from .split_operations import (
    CONTRACT_VERSION,
    OPERATION_KIND_ISSUE_MATERIALIZE,
    OPERATION_KIND_TASK_CREATE,
    apply_task_create_files,
    apply_task_create_record,
    compute_plan_sha256,
    validate_sha256,
    validate_uuid,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskCreateResult:
    workspace: Workspace
    task: dict[str, Any]
    event: dict[str, Any]
    event_created: bool
    host_aware_warning: str | None = None
    operation: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "workspace": self.workspace.to_dict(),
            "task": self.task,
            "event": self.event,
            "event_created": self.event_created,
        }
        if self.host_aware_warning:
            payload["host_aware_warning"] = self.host_aware_warning
        if self.operation is not None:
            payload["operation"] = self.operation
        return payload


@dataclass(frozen=True)
class TaskCreateFilesResult:
    workspace_id: str
    workspace_path: str
    harness_root: str
    task_id: str
    plan_doc: str
    checklist_changed: bool
    operation_id: str | None = None
    operation_kind: str | None = None
    contract_version: int | None = None
    input_fingerprint: str | None = None
    before_fingerprint: str | None = None
    after_fingerprint: str | None = None
    files_applied_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "workspace_path": self.workspace_path,
            "harness_root": self.harness_root,
            "task_id": self.task_id,
            "plan_doc": self.plan_doc,
            "checklist_changed": self.checklist_changed,
            "operation_id": self.operation_id,
            "operation_kind": self.operation_kind,
            "contract_version": self.contract_version,
            "input_fingerprint": self.input_fingerprint,
            "before_fingerprint": self.before_fingerprint,
            "after_fingerprint": self.after_fingerprint,
            "files_applied_at": self.files_applied_at,
        }


@dataclass(frozen=True)
class InitHarnessResult:
    workspace: Workspace
    harness_root: str
    files: list[str]
    event: dict[str, Any]
    event_created: bool
    task: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace.to_dict(),
            "harness_root": self.harness_root,
            "files": self.files,
            "event": self.event,
            "event_created": self.event_created,
            "task": self.task,
        }


def sync_to_checklist(
    workspace: Workspace,
    *,
    task_id: str,
    title: str,
    plan_path: str,
    priority: str = "p1",
    phase: str = "ready",
) -> bool:
    """Add a task item to the workspace's mvp-checklist.json if not already present."""
    checklist_path = Path(workspace.harness_root) / "mvp-checklist.json"
    if not checklist_path.is_file():
        log.debug("No mvp-checklist.json at %s, skipping sync", checklist_path)
        return False

    raw = checklist_path.read_text(encoding="utf-8")
    checklist = json.loads(raw)
    items = checklist.get("items", [])

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    coarse_status = _checklist_status_for_phase(phase)
    workflow_status = _workflow_status_for_phase(phase)
    acceptance = f"Use the plan acceptance criteria as source of truth: {plan_path}"
    defaults = {
        "id": task_id,
        "title": title,
        "status": coarse_status,
        "phase": phase,
        "priority": priority,
        "owner": None,
        "human_gate_required": True,
        "plan_path": plan_path,
        "acceptance": acceptance,
        "blocked_by": [],
        "blocked_reason": "",
        "dependencies": [],
        "handoff": {"from": None, "to": None, "reason": None},
        "selected_in_session": None,
        "updated_at": now,
        "workflow": {"status": workflow_status, "branch": None, "updated_at": now},
        "artifacts": {"plan": plan_path},
        "verification": "",
        "review": {},
    }
    for item in items:
        if item.get("id") != task_id:
            continue
        changed = False
        for key, value in defaults.items():
            if key in {"status", "workflow"}:
                continue
            if key not in item or item[key] in ("", None):
                item[key] = value
                changed = True
        if not isinstance(item.get("workflow"), dict):
            item["workflow"] = defaults["workflow"]
            changed = True
        if changed:
            item["updated_at"] = now
            checklist["updated_at"] = now.split("T")[0]
            checklist_path.write_text(
                json.dumps(checklist, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            log.info("Repaired task %s in %s", task_id, checklist_path)
        else:
            log.debug("Task %s already in checklist", task_id)
        return changed

    items.append(defaults)
    checklist["items"] = items
    checklist["updated_at"] = now.split("T")[0]
    checklist_path.write_text(
        json.dumps(checklist, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    log.info("Synced task %s to %s", task_id, checklist_path)
    return True


def _plan_content_hash(path) -> str | None:
    """Return a short content hash of the plan doc, or None if unreadable.

    Used in plan.ready idempotency so revising the plan doc (same task) produces
    a new plan.ready event instead of being de-duped (backlog #9c). Without this,
    plan content changes don't change the event, so reviewer handoff (idempotency
    keyed on plan_event_id) returns the stale handoff.
    """
    try:
        import hashlib
        h = hashlib.sha256()
        with open(path, "rb") as f:
            h.update(f.read())
        return h.hexdigest()[:16]
    except (OSError, ValueError):
        return None


_SPLIT_OPERATION_META_KEYS = frozenset({
    "contract_version",
    "operation_id",
    "operation_kind",
    "input_fingerprint",
    "before_fingerprint",
    "after_fingerprint",
})

_KNOWN_OPERATION_KINDS = frozenset({
    OPERATION_KIND_TASK_CREATE,
    OPERATION_KIND_ISSUE_MATERIALIZE,
})


def _validate_split_operation_metadata(value: Any, *, source: str) -> dict[str, Any]:
    """Fail closed if *value* is not the exact task-mirror operation metadata shape.

    The task mirror stores a reduced, six-key record derived from the split-operation
    envelope.  This helper validates the exact keys and the contract-version,
    operation-kind, UUID, and SHA-256 shapes enforced by the split-operation code.
    """
    if not isinstance(value, dict):
        raise ValueError(f"{source} split_operation must be a dict, got {type(value).__name__}")
    if set(value.keys()) != _SPLIT_OPERATION_META_KEYS:
        raise ValueError(
            f"{source} split_operation must have exactly keys "
            f"{sorted(_SPLIT_OPERATION_META_KEYS)}, got {sorted(value.keys())}"
        )
    if value["contract_version"] != CONTRACT_VERSION:
        raise ValueError(
            f"{source} split_operation contract_version must be {CONTRACT_VERSION}, "
            f"got {value['contract_version']!r}"
        )
    if value["operation_kind"] not in _KNOWN_OPERATION_KINDS:
        raise ValueError(
            f"{source} split_operation operation_kind must be one of "
            f"{sorted(_KNOWN_OPERATION_KINDS)}, got {value['operation_kind']!r}"
        )
    validate_uuid(value["operation_id"])
    for key in ("input_fingerprint", "before_fingerprint", "after_fingerprint"):
        validate_sha256(value[key])
    return value


def _load_existing_task_mirror_payload(
    conn: sqlite3.Connection, workspace_id: str, task_id: str
) -> dict[str, Any] | None:
    """Return the existing task mirror payload dict, or None if no mirror row exists."""
    row = conn.execute(
        "SELECT payload_json FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()
    if row is None:
        return None
    payload = json.loads(row["payload_json"])
    if not isinstance(payload, dict):
        raise ValueError(
            "stored task mirror payload must be a dict, "
            f"got {type(payload).__name__}"
        )
    return payload


def _load_existing_split_operation_metadata(
    conn: sqlite3.Connection, workspace_id: str, task_id: str
) -> dict[str, Any] | None:
    """Return the validated stored split_operation metadata, or None if absent."""
    payload = _load_existing_task_mirror_payload(conn, workspace_id, task_id)
    if payload is None:
        return None
    if "split_operation" not in payload:
        return None
    # Key present (even if null) must be validated; a null or malformed
    # stored value fails closed rather than being silently dropped.
    return _validate_split_operation_metadata(
        payload["split_operation"], source="stored task mirror"
    )


def _carry_split_operation_metadata(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Validate a caller-supplied split_operation and return the stored value to carry.

    Fail closed if the caller tries to forge or replace the reserved metadata.
    """
    stored = _load_existing_split_operation_metadata(conn, workspace_id, task_id)
    caller_payload = payload or {}
    # Key presence (not value truthiness) determines caller-supplied: a
    # null value is still a reserved-key attempt and must fail closed.
    if "split_operation" in caller_payload:
        caller = caller_payload["split_operation"]
        if stored is None:
            raise ValueError(
                "split_operation is reserved metadata and cannot be supplied "
                "for a task without existing operation metadata"
            )
        _validate_split_operation_metadata(caller, source="caller-supplied payload")
        if caller != stored:
            raise ValueError(
                "caller-supplied split_operation does not match the stored task mirror metadata"
            )
    return stored


def create_plan_task(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    plan_doc: str,
    title: str | None = None,
    owner: str | None = None,
    branch: str | None = None,
    phase: str = "ready",
    actor: str = "operator",
    target: str | None = "worker",
    payload: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> TaskCreateResult:
    result = create_plan_task_record(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        plan_doc=plan_doc,
        title=title,
        owner=owner,
        branch=branch,
        phase=phase,
        actor=actor,
        target=target,
        payload=payload,
        idempotency_key=idempotency_key,
    )
    sync_to_checklist(
        result.workspace,
        task_id=task_id,
        title=title or task_id,
        plan_path=plan_doc,
        phase=phase,
    )
    return TaskCreateResult(
        workspace=result.workspace,
        task=result.task,
        event=result.event,
        event_created=result.event_created,
        host_aware_warning=(
            "`task create` writes both DB and mvp-checklist.json. "
            "For host-aware workflows, use `task create-files` on the coding "
            "host, commit/deploy, then `task create-record` against the runtime DB."
        ),
    )


def create_plan_task_record(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    plan_doc: str,
    title: str | None = None,
    owner: str | None = None,
    branch: str | None = None,
    phase: str = "ready",
    actor: str = "operator",
    target: str | None = "worker",
    payload: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
    operation_id: str | None = None,
    input_fingerprint: str | None = None,
    before_fingerprint: str | None = None,
    after_fingerprint: str | None = None,
) -> TaskCreateResult:
    """Server half of host-aware task create: DB mirror + plan.ready only."""
    if operation_id is not None:
        split_result = apply_task_create_record(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            plan_doc=plan_doc,
            title=title,
            phase=phase,
            owner=owner,
            branch=branch,
            actor=actor,
            target=target,
            payload=payload,
            idempotency_key=idempotency_key,
            operation_id=operation_id,
            input_fingerprint=input_fingerprint,
            before_fingerprint=before_fingerprint,
            after_fingerprint=after_fingerprint,
        )
        return TaskCreateResult(
            workspace=split_result.workspace,
            task=split_result.task,
            event=split_result.event,
            event_created=split_result.event_created,
            operation=split_result.operation,
        )

    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    if not task_id:
        raise ValueError("task_id is required")
    if not plan_doc:
        raise ValueError("plan_doc is required")

    extra_payload = dict(payload or {})
    plan_abs = _resolve_workspace_path(workspace, plan_doc)
    if not plan_abs.is_file():
        raise ValueError(
            f"plan_doc is not a regular readable file: {plan_doc} ({plan_abs})"
        )
    try:
        plan_sha256 = compute_plan_sha256(plan_abs)
    except OSError as exc:
        raise ValueError(
            f"plan_doc is not a readable file: {plan_doc} ({plan_abs})"
        ) from exc
    plan_content_hash = plan_sha256[:16]

    # Validate and preserve an existing task mirror's split-operation metadata
    # before any DB write.  Caller cannot forge or replace this reserved key.
    stored_split_operation = _carry_split_operation_metadata(
        conn, workspace_id, task_id, extra_payload
    )

    task_payload = {
        **extra_payload,
        "task_id": task_id,
        "title": title or task_id,
        "plan_doc": plan_doc,
        "absolute_plan_doc": str(plan_abs),
        "status": phase,
    }
    if branch:
        task_payload["branch"] = branch
    if owner:
        task_payload["owner"] = owner
    if stored_split_operation is not None:
        task_payload["split_operation"] = stored_split_operation
        # The split path writes "phase" into the payload; carry it so the
        # projection doctor's mirror-vs-deployed record check does not flag
        # a phase mismatch after a legacy revision.  Ordinary legacy tasks
        # without split metadata are unaffected.
        task_payload["phase"] = phase

    task_row, _ = upsert_task_mirror(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        phase=phase,
        owner=owner,
        branch=branch,
        pr=None,
        payload=task_payload,
    )
    resolved_idempotency_key = idempotency_key or f"{workspace_id}:{task_id}:plan.ready:{plan_content_hash}"
    prior_ready = conn.execute(
        "SELECT id FROM events WHERE workspace_id = ? AND task_id = ? AND event_type = 'plan.ready' "
        "AND idempotency_key != ?"
        "ORDER BY rowid DESC LIMIT 1",
        (workspace_id, task_id, resolved_idempotency_key),
    ).fetchone()
    supersedes_plan_ready_event_id = prior_ready["id"] if prior_ready else None
    event_payload = {
        **task_payload,
        "workspace_path": workspace.path,
        "current_branch": workspace.base_branch,
        "allocated_branch": branch,
        "status": "ready_for_worker" if phase in {"ready", "planned"} else phase,
        "plan_content_hash": plan_content_hash,
        "plan_sha256": plan_sha256,
        "supersedes_plan_ready_event_id": supersedes_plan_ready_event_id,
    }
    event_result = append_event(
        conn,
        workspace_id=workspace_id,
        event_type="plan.ready",
        actor=actor,
        target=target,
        task_id=task_id,
        idempotency_key=resolved_idempotency_key,
        payload=event_payload,
    )
    task_row, _ = upsert_task_mirror(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        phase=phase,
        owner=owner,
        branch=branch,
        pr=None,
        payload=task_payload,
        last_event_id=event_result.row["id"],
    )

    return TaskCreateResult(
        workspace=workspace,
        task=row_to_dict(task_row),
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
    )


def create_plan_task_files(
    *,
    workspace_path: str,
    harness_root: str,
    task_id: str,
    plan_doc: str,
    title: str | None = None,
    phase: str = "ready",
    priority: str = "p1",
    allow_runtime_copy: bool = False,
    operation_id: str | None = None,
    workspace_id: str | None = None,
) -> TaskCreateFilesResult:
    """Coding-host half of host-aware task create: mvp-checklist.json only."""
    if not task_id:
        raise ValueError("task_id is required for task create-files")
    if not plan_doc:
        raise ValueError("plan_doc is required for task create-files")

    if operation_id is not None:
        if not workspace_id:
            raise ValueError("workspace_id is required for split task create-files")
        workspace = Workspace(
            id=workspace_id,
            name=workspace_id,
            path=str(workspace_path),
            harness_root=str(harness_root),
        )
        _refuse_runtime_copy(workspace, allow_runtime_copy=allow_runtime_copy)
        split_result = apply_task_create_files(
            workspace_path=workspace_path,
            harness_root=harness_root,
            task_id=task_id,
            plan_doc=plan_doc,
            title=title,
            phase=phase,
            priority=priority,
            operation_id=operation_id,
            workspace_id=workspace_id,
        )
        return TaskCreateFilesResult(
            workspace_id=split_result.workspace_id,
            workspace_path=split_result.workspace_path,
            harness_root=split_result.harness_root,
            task_id=split_result.task_id,
            plan_doc=split_result.plan_doc,
            checklist_changed=split_result.checklist_changed,
            operation_id=split_result.operation_id,
            operation_kind=split_result.operation_kind,
            contract_version=split_result.contract_version,
            input_fingerprint=split_result.input_fingerprint,
            before_fingerprint=split_result.before_fingerprint,
            after_fingerprint=split_result.after_fingerprint,
            files_applied_at=split_result.files_applied_at,
        )

    # Legacy non-split path: no operation envelope, no workspace binding.
    workspace = Workspace(
        id="local",
        name="local",
        path=str(workspace_path),
        harness_root=str(harness_root),
    )
    _refuse_runtime_copy(workspace, allow_runtime_copy=allow_runtime_copy)
    plan_abs = _resolve_workspace_path(workspace, plan_doc)
    if not plan_abs.is_file():
        raise ValueError(f"plan_doc does not exist: {plan_abs}")
    changed = sync_to_checklist(
        workspace,
        task_id=task_id,
        title=title or task_id,
        plan_path=plan_doc,
        priority=priority,
        phase=phase,
    )
    if not changed and not _checklist_contains_task(Path(harness_root), task_id):
        raise ValueError(
            f"mvp-checklist.json not found or task {task_id} could not be synced at "
            f"{harness_root}; ensure the harness root has a mvp-checklist.json"
        )
    return TaskCreateFilesResult(
        workspace_id="local",
        workspace_path=str(workspace_path),
        harness_root=str(harness_root),
        task_id=task_id,
        plan_doc=plan_doc,
        checklist_changed=changed,
    )


def _checklist_contains_task(harness_root: Path, task_id: str) -> bool:
    checklist_path = harness_root / "mvp-checklist.json"
    if not checklist_path.is_file():
        return False
    try:
        checklist = json.loads(checklist_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    items = checklist.get("items")
    if not isinstance(items, list):
        return False
    return any(isinstance(item, dict) and item.get("id") == task_id for item in items)


def _refuse_runtime_copy(workspace: Workspace, *, allow_runtime_copy: bool = False) -> None:
    if allow_runtime_copy:
        return
    paths = [Path(workspace.path), Path(workspace.harness_root)]
    if any(str(path).startswith("/opt/") or str(path) == "/opt" for path in paths):
        raise ValueError(
            "task create-files must run against the coding-host git checkout, not "
            "an /opt runtime copy. Use --allow-runtime-copy only for explicit repair."
        )


def _checklist_status_for_phase(phase: str) -> str:
    if phase in {"done", "closed", "released"}:
        return "done"
    if phase == "blocked":
        return "blocked"
    if phase in {
        "accepted",
        "awaiting_operator",
        "running",
        "handoff_requested",
        "review_requested",
        "ready_for_review",
        "closeout_requested",
        "review_approved",
        "changes_requested",
        "unblocked",
    }:
        return "doing"
    return "todo"


def _workflow_status_for_phase(phase: str) -> str:
    if phase in {"ready", "planned"}:
        return "todo"
    if phase == "done":
        return "closed"
    allowed = {
        "todo",
        "assigned",
        "accepted",
        "awaiting_operator",
        "running",
        "handoff_requested",
        "review_requested",
        "ready_for_review",
        "closeout_requested",
        "review_approved",
        "changes_requested",
        "blocked",
        "unblocked",
        "released",
        "closed",
    }
    if phase in allowed:
        return phase
    return "todo"


def init_file_harness(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    root: str,
    task_id: str,
    plan_doc: str,
    title: str | None = None,
    owner: str = "worker",
    status: str = "ready",
    actor: str = "operator",
) -> InitHarnessResult:
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    root_path = _resolve_workspace_path(workspace, root)
    root_path.mkdir(parents=True, exist_ok=True)
    (root_path / "tasks" / task_id).mkdir(parents=True, exist_ok=True)

    now = utc_now()
    rel_root = _relative_to_workspace(workspace, root_path)
    rel_plan = _relative_to_workspace(workspace, _resolve_workspace_path(workspace, plan_doc))
    task_plan_path = root_path / "tasks" / task_id / "plan.md"
    rel_task_plan = _relative_to_workspace(workspace, task_plan_path)
    display_title = title or task_id

    checklist = {
        "project": workspace.id,
        "harness_root": rel_root,
        "version": 1,
        "items": [
            {
                "id": task_id,
                "title": display_title,
                "status": status,
                "priority": "high",
                "owner": owner,
                "human_gate_required": True,
                "plan_path": rel_plan,
                "acceptance": {
                    "source": f"{rel_plan}#验收标准",
                    "summary": "Use the phase plan acceptance criteria as the source of truth.",
                },
                "workflow": {
                    "status": status,
                    "branch": _branch_for(workspace, owner, task_id),
                    "updated_at": now,
                },
                "artifacts": {
                    "plan": rel_task_plan,
                    "source_plan": rel_plan,
                },
                "review": {
                    "decision": None,
                    "human_gate_required": True,
                },
            }
        ],
    }
    harness_state = {
        "project": workspace.id,
        "harness_root": rel_root,
        "generated_at": now,
        "current_status": f"{display_title} is ready for worker implementation.",
        "current_item": checklist["items"][0],
        "checklist_summary": {status: 1},
        "workflow_summary": {status: 1},
        "paths": {
            "checklist": f"{rel_root}/mvp-checklist.json",
            "progress": f"{rel_root}/progress.md",
            "config": f"{rel_root}/harness-config.json",
            "events": f"{rel_root}/events.jsonl",
            "current_task_plan": rel_task_plan,
        },
        "commands": {},
        "message_bus": {
            "event_log": f"{rel_root}/events.jsonl",
            "visible_bus": "coordinator",
        },
        "open_risks": [
            "No harnessctl runtime is present yet; coordinator can read file state but cannot perform harness mutation lifecycle operations."
        ],
        "recent_events": [
            {
                "id": f"evt-{now.replace(':', '').replace('-', '')}-harness-initialized",
                "type": "harness.initialized",
                "task": task_id,
                "actor": actor,
                "status": "initialized",
            }
        ],
    }
    harness_config = {
        "project": workspace.id,
        "runtime": {
            "session_init_commands": [],
            "lease_ttl_minutes": 120,
        },
        "git": {
            "base_branch": workspace.base_branch,
            "branch_namespace": workspace.branch_namespace,
        },
        "message_bus": {
            "event_log": "events.jsonl",
        },
    }
    progress = (
        f"# {workspace.id} Harness Progress\n\n"
        f"## {now}\n\n"
        f"- Initialized minimal file-backed harness at `{rel_root}`.\n"
        f"- Current item: `{task_id}`.\n"
        f"- Source plan: `{rel_plan}`.\n"
    )
    task_plan = (
        f"# {display_title}\n\n"
        f"Canonical plan: `{rel_plan}`\n\n"
        "Do not copy the full plan body into this task file. Keep the phase plan as the source of truth.\n"
    )
    event_line = {
        "id": harness_state["recent_events"][0]["id"],
        "type": "harness.initialized",
        "task_id": task_id,
        "actor": actor,
        "created_at": now,
        "harness_root": rel_root,
        "source_plan": rel_plan,
    }

    written = {
        "mvp-checklist.json": json.dumps(checklist, ensure_ascii=False, indent=2) + "\n",
        "harness-state.json": json.dumps(harness_state, ensure_ascii=False, indent=2) + "\n",
        "harness-config.json": json.dumps(harness_config, ensure_ascii=False, indent=2) + "\n",
        "progress.md": progress,
        f"tasks/{task_id}/plan.md": task_plan,
    }
    for rel_path, content in written.items():
        path = root_path / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text(content, encoding="utf-8")
    events_path = root_path / "events.jsonl"
    if not events_path.exists():
        events_path.write_text(json.dumps(event_line, ensure_ascii=False) + "\n", encoding="utf-8")

    updated_workspace = upsert_workspace(
        conn,
        workspace_id=workspace.id,
        name=workspace.name,
        path=workspace.path,
        harness_root=root_path,
        harnessctl_path=workspace.harnessctl_path,
        default_bus=workspace.default_bus,
        default_destination=workspace.default_destination,
        base_branch=workspace.base_branch,
        branch_namespace=workspace.branch_namespace,
    )
    task_result = create_plan_task(
        conn,
        workspace_id=workspace.id,
        task_id=task_id,
        plan_doc=rel_plan,
        title=display_title,
        owner=owner,
        branch=_branch_for(updated_workspace, owner, task_id),
        phase=status,
        actor=actor,
        payload={"harness_root": rel_root, "human_gate_required": True},
        idempotency_key=f"{workspace.id}:{task_id}:plan.ready",
    )
    event_result = append_event(
        conn,
        workspace_id=workspace.id,
        event_type="harness.initialized",
        actor=actor,
        target=owner,
        task_id=task_id,
        idempotency_key=f"{workspace.id}:{task_id}:harness.initialized:{rel_root}",
        payload={
            "task_id": task_id,
            "harness_root": str(root_path),
            "harness_root_relative": rel_root,
            "source_plan": rel_plan,
            "initialized_files": sorted([*written.keys(), "events.jsonl"]),
            "human_gate_required": True,
        },
    )

    return InitHarnessResult(
        workspace=updated_workspace,
        harness_root=str(root_path),
        files=sorted([str(root_path / p) for p in [*written.keys(), "events.jsonl"]]),
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
        task=task_result.task,
    )


@dataclass(frozen=True)
class FullInitResult:
    workspace: Workspace
    harness_root: str
    scripts_copied: list[str]
    scripts_existing: list[str]
    files_created: list[str]
    files_existing: list[str]
    warnings: list[str]
    harnessctl_path_updated: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace.to_dict(),
            "harness_root": self.harness_root,
            "scripts_copied": self.scripts_copied,
            "scripts_existing": self.scripts_existing,
            "files_created": self.files_created,
            "files_existing": self.files_existing,
            "warnings": self.warnings,
            "harnessctl_path_updated": self.harnessctl_path_updated,
        }


# Protocol file templates for full init
_SCOPE_TEMPLATE = (
    "# {project} Scope\n\n"
    "## Project\n\n{project}: description pending.\n\n"
    "## In Scope\n\n- TBD\n\n"
    "## Out of Scope\n\n- TBD\n\n"
    "## Boundaries\n\n- TBD\n"
)

_ARCH_TEMPLATE = (
    "# {project} Architecture\n\n"
    "## Module Map\n\nTBD\n\n"
    "## Key References\n\nTBD\n"
)

_DOMAIN_TEMPLATE = (
    "# {project} Domain Model\n\n"
    "## Core Entities\n\nTBD\n"
)

_RUNBOOK_TEMPLATE = (
    "# {project} Runbook\n\n"
    "## Quick Reference\n\n"
    "```bash\n"
    "# Harness commands\n"
    "scripts/harness/harnessctl state\n"
    "scripts/harness/harnessctl validate\n"
    "scripts/harness/harnessctl doctor\n"
    "scripts/harness/harnessctl session-init\n"
    "```\n\n"
    "## New Workspace Onboarding Order\n\n"
    "1. Register workspace in coordinator: `workspace add <id> --path ... --harness-root ...`\n"
    "2. Run `workspace init-harness <id> --mode full --source <reference-workspace-scripts/harness>` to create full harness runtime\n"
    "3. Run `workspace doctor <id>` to verify full_harness_runtime status\n"
    "4. Create plans under `docs/project-harness/tasks/<task-id>/plan.md`\n"
    "5. Use coordinator `task create` to register first task\n"
    "6. Run `workspace audit <id>` to confirm no drift\n"
)

PROTOCOL_TEMPLATES = {
    "scope.md": _SCOPE_TEMPLATE,
    "architecture.md": _ARCH_TEMPLATE,
    "domain-model.md": _DOMAIN_TEMPLATE,
    "runbook.md": _RUNBOOK_TEMPLATE,
}


def init_full_harness(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    source: str | Path,
    dry_run: bool = False,
    actor: str = "operator",
) -> FullInitResult:
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")

    source_path = Path(source).resolve()
    if not source_path.is_dir():
        raise ValueError(f"source directory does not exist: {source_path}")

    # Validate source is within workspace to prevent path traversal
    ws_root = Path(workspace.path).resolve()
    # source can be external (e.g. another workspace), but target must be inside workspace
    hr_path = Path(workspace.harness_root).resolve()

    # Security: ensure harness_root is within workspace
    try:
        hr_path.relative_to(ws_root)
    except ValueError:
        raise ValueError(
            f"harness_root ({hr_path}) is outside workspace path ({ws_root}); "
            "refusing to write files outside workspace"
        )

    scripts_copied: list[str] = []
    scripts_existing: list[str] = []
    files_created: list[str] = []
    files_existing: list[str] = []
    warnings: list[str] = []

    # 1. Copy scripts/harness/ runtime from source
    source_files = sorted(source_path.iterdir()) if source_path.is_dir() else []
    harness_scripts = [f for f in source_files if f.is_file()]

    if not harness_scripts:
        warnings.append(f"no files found in source directory: {source_path}")

    for src_file in harness_scripts:
        rel_script = f"scripts/harness/{src_file.name}"
        dst = ws_root / rel_script
        if dst.exists():
            scripts_existing.append(rel_script)
            continue
        # Security: verify destination is within workspace
        try:
            dst.resolve().relative_to(ws_root)
        except ValueError:
            warnings.append(f"skipped {src_file.name}: destination outside workspace")
            continue
        if not dry_run:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dst)
            # Ensure executable for script files
            if src_file.suffix in ("", ".sh", ".py"):
                dst.chmod(dst.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        scripts_copied.append(rel_script)

    # 2. Ensure harness root directory exists
    if not dry_run:
        hr_path.mkdir(parents=True, exist_ok=True)

    # 3. Create or supplement protocol files
    project = workspace.id
    for fname, template in PROTOCOL_TEMPLATES.items():
        fpath = hr_path / fname
        if fpath.exists():
            files_existing.append(str(fpath))
            continue
        content = template.format(project=project)
        if not dry_run:
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_text(content, encoding="utf-8")
        files_created.append(str(fpath))

    # 4. Ensure minimal harness files exist (harness-config.json, mvp-checklist.json,
    #    events.jsonl, progress.md) — create empty/stub versions if missing
    _ensure_minimal_files(hr_path, project, dry_run, files_created, files_existing, warnings)

    # 5. Update workspace harnessctl_path if scripts/harness/harnessctl was created
    harnessctl_updated = False
    expected_harnessctl = ws_root / "scripts" / "harness" / "harnessctl"
    if expected_harnessctl.exists() or (dry_run and scripts_copied):
        new_ctl_path = str(expected_harnessctl)
        if workspace.harnessctl_path != new_ctl_path:
            harnessctl_updated = True
            if not dry_run:
                upsert_workspace(
                    conn,
                    workspace_id=workspace.id,
                    name=workspace.name,
                    path=workspace.path,
                    harness_root=workspace.harness_root,
                    harnessctl_path=new_ctl_path,
                    default_bus=workspace.default_bus,
                    default_destination=workspace.default_destination,
                    base_branch=workspace.base_branch,
                    branch_namespace=workspace.branch_namespace,
                )

    if not dry_run:
        append_event(
            conn,
            workspace_id=workspace.id,
            event_type="harness.full_init",
            actor=actor,
            target="operator",
            task_id=None,
            idempotency_key=f"{workspace.id}:harness.full_init:{hr_path}",
            payload={
                "harness_root": str(hr_path),
                "scripts_copied": scripts_copied,
                "scripts_existing": scripts_existing,
                "files_created": [str(f) for f in files_created],
                "files_existing": [str(f) for f in files_existing],
                "dry_run": dry_run,
                "harnessctl_path_updated": harnessctl_updated,
            },
        )

    # Re-read workspace after potential update
    updated_workspace = get_workspace(conn, workspace_id) or workspace

    return FullInitResult(
        workspace=updated_workspace,
        harness_root=str(hr_path),
        scripts_copied=scripts_copied,
        scripts_existing=scripts_existing,
        files_created=files_created,
        files_existing=files_existing,
        warnings=warnings,
        harnessctl_path_updated=harnessctl_updated,
    )


def _ensure_minimal_files(
    hr_path: Path,
    project: str,
    dry_run: bool,
    files_created: list[str],
    files_existing: list[str],
    warnings: list[str],
) -> None:
    """Create stub versions of minimal harness files if they don't exist."""
    stubs: dict[str, str] = {}

    # harness-config.json
    config_path = hr_path / "harness-config.json"
    if not config_path.exists():
        stubs["harness-config.json"] = json.dumps({
            "project": project,
            "runtime": {
                "session_init_commands": [],
                "lease_ttl_minutes": 120,
            },
            "git": {},
            "message_bus": {"event_log": "events.jsonl"},
        }, indent=2) + "\n"

    # mvp-checklist.json
    checklist_path = hr_path / "mvp-checklist.json"
    if not checklist_path.exists():
        stubs["mvp-checklist.json"] = json.dumps({
            "project": project,
            "harness_root": str(hr_path),
            "version": 1,
            "items": [],
        }, indent=2) + "\n"

    # events.jsonl
    events_path = hr_path / "events.jsonl"
    if not events_path.exists():
        stubs["events.jsonl"] = ""

    # progress.md
    progress_path = hr_path / "progress.md"
    if not progress_path.exists():
        stubs["progress.md"] = f"# {project} Harness Progress\n\nInitialized by coordinator full harness init.\n"

    # harness-state.json
    state_path = hr_path / "harness-state.json"
    if not state_path.exists():
        stubs["harness-state.json"] = json.dumps({
            "project": project,
            "harness_root": str(hr_path),
            "generated_at": utc_now(),
            "current_status": "",
            "current_item": None,
            "checklist_summary": {"todo": 0, "doing": 0, "done": 0, "blocked": 0},
            "workflow_summary": {"closed": 0, "running": 0},
            "paths": {},
            "commands": {},
            "message_bus": {"event_log": "events.jsonl"},
            "open_risks": [],
            "recent_events": [],
        }, indent=2) + "\n"

    for fname, content in stubs.items():
        fpath = hr_path / fname
        if not dry_run:
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_text(content, encoding="utf-8")
        files_created.append(str(fpath))

    # Track existing files
    for fname in ["harness-config.json", "mvp-checklist.json", "events.jsonl",
                   "progress.md", "harness-state.json"]:
        fpath = hr_path / fname
        if fpath.exists() and fname not in stubs:
            files_existing.append(str(fpath))


def _resolve_workspace_path(workspace: Workspace, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = Path(workspace.path) / candidate
    return candidate.resolve()


def _relative_to_workspace(workspace: Workspace, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(Path(workspace.path).resolve()))
    except ValueError:
        return str(path.resolve())


def _branch_for(workspace: Workspace, owner: str, task_id: str) -> str | None:
    namespace = (workspace.branch_namespace or "").strip("/")
    if not namespace:
        return None
    return f"{namespace}/{owner}/{task_id}"
