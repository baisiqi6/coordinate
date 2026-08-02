from __future__ import annotations

import json
import logging
import os
import re
import shlex
import sqlite3
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .checklist_io import (
    CHECKLIST_LEGACY_NAME,
    REASON_CHECKLIST_MISSING,
    ChecklistError,
    ResolvedChecklist,
    create_empty_checklist,
    load_checklist,
    resolve_checklist,
    resolve_checklist_for_init,
    sha256_bytes,
)
from .db import (
    Workspace,
    append_event,
    get_workspace,
    list_split_operations,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
    utc_now,
)
from .split_operations import (
    CONTRACT_VERSION,
    OPERATION_KIND_ISSUE_MATERIALIZE,
    OPERATION_KIND_TASK_CREATE,
    REASON_FILES_NOT_DEPLOYED,
    REASON_LEGACY_UNBOUND_ITEM,
    REASON_OPERATION_CONFLICT,
    TARGET_KIND_CHECKLIST_TASK,
    SplitOperationError,
    apply_task_create_files,
    apply_task_create_record,
    build_task_create_input_fingerprint,
    compute_plan_sha256,
    validate_sha256,
    validate_task_create_contract,
    validate_uuid,
    validate_workspace_relative_path,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskCreateResult:
    workspace: Workspace
    task: dict[str, Any]
    event: dict[str, Any]
    event_created: bool
    operation: dict[str, Any] | None = None
    files: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "workspace": self.workspace.to_dict(),
            "task": self.task,
            "event": self.event,
            "event_created": self.event_created,
        }
        if self.operation is not None:
            payload["operation"] = self.operation
        if self.files is not None:
            payload["files"] = self.files
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


@dataclass(frozen=True)
class TaskCreateRecovery:
    """Structured recovery material for a record-half failure.

    The file half already committed; the checklist item and its envelope stay
    authoritative. ``recovery_argv`` re-runs ``task create-record`` with the
    same operation id and fingerprints to complete the DB half idempotently.
    """

    workspace_id: str
    task_id: str
    plan_doc: str
    phase: str
    actor: str
    target: str | None
    operation_id: str
    input_fingerprint: str
    before_fingerprint: str
    after_fingerprint: str
    title: str | None = None
    owner: str | None = None
    branch: str | None = None
    payload: dict[str, Any] | None = None
    idempotency_key: str | None = None
    error_message: str = ""

    def recovery_argv(self) -> list[str]:
        argv = [
            "coordinate",
            "task",
            "create-record",
            self.workspace_id,
            "--operation-id",
            self.operation_id,
            "--input-fingerprint",
            self.input_fingerprint,
            "--before-fingerprint",
            self.before_fingerprint,
            "--after-fingerprint",
            self.after_fingerprint,
            "--task-id",
            self.task_id,
            "--plan-doc",
            self.plan_doc,
            "--phase",
            self.phase,
            "--actor",
            self.actor,
            "--target",
            self.target or "worker",
            "--payload-json",
            json.dumps(self.payload or {}, ensure_ascii=False, sort_keys=True),
        ]
        if self.title:
            argv += ["--title", self.title]
        if self.owner:
            argv += ["--owner", self.owner]
        if self.branch:
            argv += ["--branch", self.branch]
        if self.idempotency_key:
            argv += ["--idempotency-key", self.idempotency_key]
        return argv

    def to_dict(self) -> dict[str, Any]:
        argv = self.recovery_argv()
        return {
            "recovery_required": True,
            "operation_id": self.operation_id,
            "input_fingerprint": self.input_fingerprint,
            "before_fingerprint": self.before_fingerprint,
            "after_fingerprint": self.after_fingerprint,
            "recovery_argv": argv,
            "recovery_command": shlex.join(argv),
            "error": self.error_message,
        }


class TaskCreateRecoveryError(ValueError):
    """The record half failed after the file half committed.

    ``recovery`` carries the same-operation idempotent completion material.
    """

    def __init__(self, message: str, recovery: TaskCreateRecovery):
        super().__init__(message)
        self.recovery = recovery


def _resolve_task_create_operation(
    conn: sqlite3.Connection,
    workspace: Workspace,
    *,
    task_id: str,
    plan_doc: str,
    title: str | None,
    phase: str,
    priority: str,
    explicit_operation_id: str | None,
) -> tuple[str, str]:
    """Choose or reuse the operation id for a combined task create.

    Rules (plan §5.2), applied identically for auto and explicit operation ids:

    - the input fingerprint is always computed first;
    - item already carrying a task.create envelope with identical inputs:
      reused when no explicit id is given; an explicit id must equal the
      deployed envelope's operation id exactly, else ``operation_conflict``;
    - item without an envelope -> ``legacy_unbound_item``; envelope kind or
      inputs differ -> ``operation_conflict``;
    - item absent but the DB ledger already binds the target ->
      ``operation_conflict`` BEFORE any file write (a lost checklist item is
      never re-authored under a second authority);
    - only an absent item with no DB binding may take a fresh (or explicit)
      operation id.

    The file half re-checks everything under the lock, so this pre-read is
    advisory only — but it runs before ANY file/DB mutation.
    """
    plan_abs = _resolve_workspace_path(workspace, plan_doc)
    try:
        plan_sha256 = compute_plan_sha256(plan_abs)
    except OSError as exc:
        raise ValueError(
            f"plan_doc is not a readable file: {plan_doc} ({plan_abs})"
        ) from exc
    input_fingerprint = build_task_create_input_fingerprint(
        workspace_id=workspace.id,
        task_id=task_id,
        plan_doc=plan_doc,
        plan_sha256=plan_sha256,
        title=title or task_id,
        phase=phase,
        priority=priority,
    )
    explicit = validate_uuid(explicit_operation_id) if explicit_operation_id is not None else None
    try:
        checklist, _ = load_checklist(workspace.harness_root, purpose="read")
    except ChecklistError as exc:
        if exc.reason == REASON_CHECKLIST_MISSING:
            # Defer to the file half, which raises checklist_missing before any
            # DB write with the init hint.
            return explicit or str(uuid.uuid4()), input_fingerprint
        raise
    items = checklist.get("items")
    item = None
    if isinstance(items, list):
        for candidate in items:
            if isinstance(candidate, dict) and candidate.get("id") == task_id:
                item = candidate
                break
    if item is None:
        # Reconciliation pre-flight: the DB already binds this target to a
        # split operation but the checklist item is absent. Create must not
        # guess authority by writing a second item/operation.
        for existing_op in list_split_operations(
            conn,
            workspace_id=workspace.id,
            target_kind=TARGET_KIND_CHECKLIST_TASK,
            target_id=task_id,
        ):
            raise SplitOperationError(
                f"DB ledger already binds task {task_id} to operation "
                f"{existing_op.operation_id} but the checklist has no item for "
                "it; run doctor/audit to reconcile, do not re-create",
                REASON_OPERATION_CONFLICT,
            )
        return explicit or str(uuid.uuid4()), input_fingerprint
    envelope = item.get("split_operation")
    if not isinstance(envelope, dict):
        raise SplitOperationError(
            f"task {task_id} already exists in the checklist without a "
            "split-operation envelope; refusing to adopt a legacy unbound item. "
            "Reconcile it explicitly instead.",
            REASON_LEGACY_UNBOUND_ITEM,
        )
    if envelope.get("operation_kind") != OPERATION_KIND_TASK_CREATE:
        raise SplitOperationError(
            f"task {task_id} already exists in the checklist under a different "
            f"operation kind ({envelope.get('operation_kind')!r}); refusing to "
            "overwrite it",
            REASON_OPERATION_CONFLICT,
        )
    if envelope.get("input_fingerprint") != input_fingerprint:
        raise SplitOperationError(
            f"task {task_id} already exists under operation "
            f"{envelope.get('operation_id')!r} with different inputs (plan digest, "
            "title, phase, or priority changed); revise the plan explicitly and "
            "do not re-create",
            REASON_OPERATION_CONFLICT,
        )
    if explicit is not None:
        if explicit != envelope.get("operation_id"):
            raise SplitOperationError(
                f"task {task_id} already exists under operation "
                f"{envelope.get('operation_id')!r}; explicit --operation-id "
                f"{explicit} does not match the deployed envelope",
                REASON_OPERATION_CONFLICT,
            )
        return explicit, input_fingerprint
    return envelope["operation_id"], input_fingerprint


def _normalize_plan_doc(workspace: Workspace, plan_doc: str) -> str:
    """Return *plan_doc* as a workspace-relative path for Coordinate-managed.

    Absolute paths that resolve inside the workspace are rewritten to their
    workspace-relative form; paths outside the workspace fail closed (the U1
    locator contract keeps Coordinate-managed plans workspace-relative).
    """
    candidate = Path(plan_doc).expanduser()
    if not candidate.is_absolute():
        return plan_doc
    try:
        rel = candidate.resolve().relative_to(Path(workspace.path).resolve())
    except ValueError:
        raise ValueError(
            f"plan_doc is outside the workspace ({workspace.path}); "
            "Coordinate-managed plans must be workspace-relative: "
            f"{plan_doc}"
        ) from None
    return str(rel)


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
    priority: str = "p1",
    actor: str = "operator",
    target: str | None = "worker",
    payload: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
    operation_id: str | None = None,
    allow_runtime_copy: bool = False,
) -> TaskCreateResult:
    """Combined managed task create: file-first, record-second.

    Orders the existing split halves inside one command: resolve/validate the
    checklist, choose or reuse the operation id, apply the checklist file half,
    then apply the DB record half with the same envelope and fingerprints.
    Any file-half failure leaves the DB untouched; a record-half failure keeps
    the committed file authority and raises ``TaskCreateRecoveryError`` with
    same-operation idempotent recovery argv.
    """
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    _refuse_runtime_copy(workspace, allow_runtime_copy=allow_runtime_copy)
    if not task_id:
        raise ValueError("task_id is required")
    if not plan_doc:
        raise ValueError("plan_doc is required")
    plan_doc = _normalize_plan_doc(workspace, plan_doc)
    plan_abs = _resolve_workspace_path(workspace, plan_doc)
    if not plan_abs.is_file():
        raise ValueError(
            f"plan_doc is not a regular readable file: {plan_doc} ({plan_abs})"
        )

    op_id, _ = _resolve_task_create_operation(
        conn,
        workspace,
        task_id=task_id,
        plan_doc=plan_doc,
        title=title,
        phase=phase,
        priority=priority,
        explicit_operation_id=operation_id,
    )

    try:
        files = apply_task_create_files(
            workspace_path=workspace.path,
            harness_root=workspace.harness_root,
            task_id=task_id,
            plan_doc=plan_doc,
            title=title,
            phase=phase,
            priority=priority,
            operation_id=op_id,
            workspace_id=workspace_id,
        )
    except SplitOperationError:
        # File half refused: zero DB writes by construction.
        raise

    try:
        record = apply_task_create_record(
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
            operation_id=op_id,
            input_fingerprint=files.input_fingerprint,
            before_fingerprint=files.before_fingerprint,
            after_fingerprint=files.after_fingerprint,
        )
    except Exception as exc:
        recovery = TaskCreateRecovery(
            workspace_id=workspace_id,
            task_id=task_id,
            plan_doc=plan_doc,
            phase=phase,
            actor=actor,
            target=target,
            title=title,
            owner=owner,
            branch=branch,
            payload=payload,
            idempotency_key=idempotency_key,
            operation_id=op_id,
            input_fingerprint=files.input_fingerprint,
            before_fingerprint=files.before_fingerprint,
            after_fingerprint=files.after_fingerprint,
            error_message=str(exc),
        )
        raise TaskCreateRecoveryError(
            f"checklist item committed but the DB record half failed; complete "
            f"the operation with `task create-record` using the same operation id "
            f"({op_id})",
            recovery=recovery,
        ) from exc

    return TaskCreateResult(
        workspace=record.workspace,
        task=record.task,
        event=record.event,
        event_created=record.event_created,
        operation=record.operation,
        files=files.to_dict(),
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
    """Server half of host-aware task create: DB mirror + plan.ready only.

    ``operation_id`` + fingerprints select the split-path record half. The
    no-operation branch is the explicit plan-revision authoring entry (backlog
    #9c: a revised plan produces a new plan.ready event with a supersede link
    while preserving split-operation metadata). The combined ``create_plan_task``
    NEVER calls this branch: combined create is file-first and uses the split
    half exclusively.
    """
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
    operation_id: str,
    workspace_id: str,
) -> TaskCreateFilesResult:
    """Coding-host half of host-aware task create: checklist file half only.

    ``operation_id`` and ``workspace_id`` are required by the Python signature
    (not just argparse): the legacy non-split sync path was removed in U2, so
    every create-files call binds a split operation.
    """
    if not task_id:
        raise ValueError("task_id is required for task create-files")
    if not plan_doc:
        raise ValueError("plan_doc is required for task create-files")
    if not operation_id:
        raise ValueError("operation_id is required for task create-files")
    if not workspace_id:
        raise ValueError("workspace_id is required for task create-files")

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


def _refuse_runtime_copy(workspace: Workspace, *, allow_runtime_copy: bool = False) -> None:
    if allow_runtime_copy:
        return
    paths = [Path(workspace.path), Path(workspace.harness_root)]
    if any(str(path).startswith("/opt/") or str(path) == "/opt" for path in paths):
        raise ValueError(
            "task create must run against the coding-host git checkout, not "
            "an /opt runtime copy. Use --allow-runtime-copy only for explicit repair."
        )


def _require_workspace_contained(workspace: Workspace, path: Path, *, label: str) -> None:
    """Fail closed when *path* resolves outside the workspace path."""
    ws_root = Path(workspace.path).resolve()
    try:
        path.relative_to(ws_root)
    except ValueError:
        raise ValueError(
            f"{label} ({path}) is outside workspace path ({ws_root}); "
            "refusing to write files outside workspace"
        ) from None


def _require_readable_plan(plan_path: Path) -> None:
    """Fail closed when the plan doc is missing, not a regular file, or
    unreadable, using the same classification as the create-time contract."""
    if not plan_path.is_file():
        raise SplitOperationError(
            f"plan_doc does not exist or is not a regular file: {plan_path}",
            REASON_FILES_NOT_DEPLOYED,
        )
    try:
        with open(plan_path, "rb") as handle:
            handle.read(1)
    except OSError as exc:
        raise SplitOperationError(
            f"plan_doc cannot be read: {plan_path}: {exc}",
            REASON_FILES_NOT_DEPLOYED,
        ) from exc


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
    priority: str = "p1",
    actor: str = "operator",
) -> InitHarnessResult:
    """Minimal file-backed harness init (coordinate-managed profile).

    Creates a validator-passing checklist (new filename by default; a
    legacy-only existing checklist is treated as compatible and reused), the
    coordinate-managed config with a workspace-relative event log, the progress/
    events/pointer files, then creates the initial node through the same
    ``apply_task_create_files`` -> ``apply_task_create_record`` split path and
    rebuilds ``harness-state.json`` from the actual checklist with a real
    source path + digest. The pointer file under ``tasks/<id>/plan.md`` is a
    pointer only and is never a second canonical plan. Any DB half failure
    raises the same-operation recovery error; init never fakes success.
    """
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")

    # --- 0. Preflight: every predictable failure before ANY mkdir/copy/
    # protocol write/workspace upsert/event/DB mutation. The checklist
    # authority matrix (none/new-only/legacy-only/both; existing candidates
    # regular + readable + schema-valid) and the create-time phase/priority
    # contract are all decided here, so a refusal leaves zero mutation.
    # The minimal harness root may be an external absolute path (formal
    # contract; operator policy avoids it but the implementation must not
    # forbid it), so no workspace containment is enforced on it — only the
    # plan stays workspace-relative/absolute-inside-workspace.
    root_path = _resolve_workspace_path(workspace, root)
    plan_path = _resolve_workspace_path(workspace, plan_doc)
    _require_readable_plan(plan_path)
    rel_plan = _normalize_plan_doc(workspace, plan_doc)
    validate_workspace_relative_path(rel_plan)
    validate_task_create_contract(phase=status, priority=priority)
    checklist_resolved, checklist_create_new = resolve_checklist_for_init(root_path)

    now = utc_now()
    rel_root = _relative_to_workspace(workspace, root_path)
    rel_events = f"{rel_root}/events.jsonl"
    display_title = title or task_id

    # --- 1. Directories, then the empty checklist through the unified
    # validator + atomic writer (init is the only entry that may create an
    # empty checklist, and only in the none case).
    root_path.mkdir(parents=True, exist_ok=True)
    (root_path / "tasks" / task_id).mkdir(parents=True, exist_ok=True)
    if checklist_create_new:
        create_empty_checklist(
            root_path, project=workspace.id, harness_root_rel=rel_root
        )

    # --- 2. progress / events / pointer files (the task plan is a pointer
    # only; the item's plan_path and artifacts.plan are the real plan doc).
    progress = (
        f"# {workspace.id} Harness Progress\n\n"
        f"## {now}\n\n"
        f"- Initialized minimal file-backed harness at `{rel_root}`.\n"
        f"- Current item: `{task_id}`.\n"
        f"- Source plan: `{rel_plan}`.\n"
    )
    task_plan_pointer = (
        f"# {display_title}\n\n"
        f"This file is a pointer, not the canonical plan.\n"
        f"Canonical plan: `{rel_plan}`\n\n"
        "Do not copy the full plan body into this file. Keep the source plan "
        "as the single authority.\n"
    )
    event_line = {
        "id": f"evt-{now.replace(':', '').replace('-', '')}-harness-initialized",
        "type": "harness.initialized",
        "task_id": task_id,
        "actor": actor,
        "created_at": now,
        "harness_root": rel_root,
        "source_plan": rel_plan,
    }
    written = {
        "progress.md": progress,
        f"tasks/{task_id}/plan.md": task_plan_pointer,
    }
    for rel_path, content in written.items():
        path = root_path / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text(content, encoding="utf-8")
    events_path = root_path / "events.jsonl"
    if not events_path.exists():
        events_path.write_text(
            json.dumps(event_line, ensure_ascii=False) + "\n", encoding="utf-8"
        )

    # --- 3. coordinate-managed config with the real workspace-relative events
    # path (never a bare filename that silently resolves elsewhere).
    harness_config = {
        "project": workspace.id,
        "deployment_profile": "coordinate-managed",
        "runtime": {
            "session_init_commands": [],
            "lease_ttl_minutes": 120,
        },
        "git": {
            "base_branch": workspace.base_branch,
            "branch_namespace": workspace.branch_namespace,
        },
        "message_bus": {
            "event_log": rel_events,
            "visible_bus": "coordinator",
        },
    }
    config_path = root_path / "harness-config.json"
    if not config_path.exists():
        config_path.write_text(
            json.dumps(harness_config, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    # --- 4. register the workspace against the target harness root.
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

    # --- 5. initial node through the split path (file first, record second).
    operation_id = str(uuid.uuid4())
    try:
        files_result = apply_task_create_files(
            workspace_path=workspace.path,
            harness_root=str(root_path),
            task_id=task_id,
            plan_doc=rel_plan,
            title=display_title,
            phase=status,
            priority=priority,
            operation_id=operation_id,
            workspace_id=workspace.id,
        )
        record_result = apply_task_create_record(
            conn,
            workspace_id=workspace.id,
            task_id=task_id,
            plan_doc=rel_plan,
            title=display_title,
            phase=status,
            owner=owner,
            branch=_branch_for(updated_workspace, owner, task_id),
            actor=actor,
            target="worker",
            payload={"harness_root": rel_root, "human_gate_required": True},
            operation_id=operation_id,
            input_fingerprint=files_result.input_fingerprint,
            before_fingerprint=files_result.before_fingerprint,
            after_fingerprint=files_result.after_fingerprint,
            idempotency_key=f"{workspace.id}:{task_id}:plan.ready",
        )
    except SplitOperationError:
        raise  # file half refused: zero DB writes; init aborts cleanly.
    except Exception as exc:
        recovery = TaskCreateRecovery(
            workspace_id=workspace.id,
            task_id=task_id,
            plan_doc=rel_plan,
            phase=status,
            actor=actor,
            target="worker",
            title=display_title,
            owner=owner,
            branch=_branch_for(updated_workspace, owner, task_id),
            payload={"harness_root": rel_root, "human_gate_required": True},
            idempotency_key=f"{workspace.id}:{task_id}:plan.ready",
            operation_id=operation_id,
            input_fingerprint=files_result.input_fingerprint,
            before_fingerprint=files_result.before_fingerprint,
            after_fingerprint=files_result.after_fingerprint,
            error_message=str(exc),
        )
        raise TaskCreateRecoveryError(
            f"init wrote the checklist node but the DB record half failed; "
            f"complete the operation with `task create-record` using operation "
            f"id {operation_id}",
            recovery=recovery,
        ) from exc

    # --- 6. rebuild state from the ACTUAL checklist with source path + digest.
    resolved = resolve_checklist(root_path, purpose="read")
    checklist_raw = resolved.path.read_bytes()
    checklist_data = json.loads(checklist_raw.decode("utf-8"))
    checklist_items = checklist_data.get("items")
    current_item = None
    if isinstance(checklist_items, list):
        for item in checklist_items:
            if isinstance(item, dict) and item.get("id") == task_id:
                current_item = item
                break
    rel_checklist = _relative_to_workspace(workspace, resolved.path)
    harness_state = {
        "project": workspace.id,
        "harness_root": rel_root,
        "generated_at": now,
        "source": {
            "checklist_path": rel_checklist,
            "checklist_sha256": sha256_bytes(checklist_raw),
        },
        "current_status": f"{display_title} is ready for worker implementation.",
        "current_item": current_item,
        "checklist_summary": {"todo": 1},
        "workflow_summary": {"todo": 1},
        "paths": {
            "checklist": rel_checklist,
            "progress": f"{rel_root}/progress.md",
            "config": f"{rel_root}/harness-config.json",
            "events": rel_events,
            "current_task_plan": f"{rel_root}/tasks/{task_id}/plan.md",
        },
        "commands": {},
        "message_bus": {
            "event_log": rel_events,
            "visible_bus": "coordinator",
        },
        "open_risks": [
            "No harnessctl runtime is present yet; coordinator can read file state but cannot perform harness mutation lifecycle operations."
        ],
        "recent_events": [event_line],
    }
    (root_path / "harness-state.json").write_text(
        json.dumps(harness_state, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
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
            "initialized_files": sorted([*written.keys(), "events.jsonl", "harness-state.json", "harness-config.json"]),
            "human_gate_required": True,
        },
    )

    return InitHarnessResult(
        workspace=updated_workspace,
        harness_root=str(root_path),
        files=sorted([
            str(root_path / p)
            for p in [
                *written.keys(),
                "events.jsonl",
                "harness-state.json",
                "harness-config.json",
            ]
        ]),
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
        task=record_result.task,
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


# ---------------------------------------------------------------------------
# Full-init runtime source contract (P1-3)
# ---------------------------------------------------------------------------
# ``init_full_harness`` copies a runtime into ``scripts/harness``. The only
# approved source is the U1 long-running-project-harness template, which
# instantiates exactly these four placeholders; an already-rendered runtime is
# accepted only when its embedded root/layout can be proven to match the
# target. Everything unprovable fails closed before the first mutation.
RUNTIME_TEMPLATE_PLACEHOLDERS = frozenset({
    "{{HARNESS_ROOT}}",
    "{{PROJECT_ROOT_DEPTH}}",
    "{{SCRIPTS_DIR}}",
    "{{PROJECT_NAME}}",
})
# Any {{...}} shape (not just {{UPPER_}}) counts as an unrendered
# placeholder: {{lowercase}} and {{MIXED-NAME}} must enter template/render
# validation and fail closed as residuals instead of being copied verbatim.
_PLACEHOLDER_RE = re.compile(r"\{\{.*?\}\}", re.DOTALL)
# Files required to run ``harnessctl validate`` / ``harnessctl state``.
_RUNTIME_KEY_FILES = (
    "harnessctl",
    "harness_common.py",
    "validate_checklist.py",
    "build_harness_state.py",
)
# Files that embed the harness root locator / script depth once rendered.
_RUNTIME_ROOT_CARRIER_FILES = frozenset({"harness_common.py", "harnessctl"})
_HARNESS_ROOT_LOCATOR_RE = re.compile(r'return project_root\(\) / "([^"]+)"')
_HARNESSCTL_ROOT_LOCATOR_RE = re.compile(r'HARNESS_ROOT="\$root/([^"]+)"')
_PROJECT_ROOT_DEPTH_RE = re.compile(r"parents\[([0-9]+)\]")

REASON_RUNTIME_SOURCE_INCOMPLETE = "runtime_source_incomplete"
REASON_RUNTIME_TEMPLATE_PLACEHOLDER = "runtime_template_placeholder"
REASON_RUNTIME_ROOT_INCOMPATIBLE = "runtime_root_incompatible"
REASON_RUNTIME_DESTINATION_MISMATCH = "runtime_destination_mismatch"


class RuntimeSourceError(ValueError):
    """Full-init runtime source/destination preflight failure.

    Raised before any file/DB/event mutation; ``reason`` is a stable
    machine-readable classification.
    """

    def __init__(self, message: str, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


_TEMPLATE_SOURCE_HINT = (
    "use the U1 template source (files containing {{HARNESS_ROOT}}, "
    "{{PROJECT_ROOT_DEPTH}}, {{SCRIPTS_DIR}}, {{PROJECT_NAME}}) instead of a "
    "hand-rendered runtime"
)
_DESTINATION_HINT = (
    "refresh/clean the target workspace explicitly (or use the U1 template "
    "source) before re-running full init"
)


def _extract_single_embedding(
    pattern: re.Pattern[str],
    body: str,
    *,
    file_name: str,
    what: str,
    hint: str,
) -> str:
    matches = list(pattern.finditer(body))
    if len(matches) != 1:
        raise RuntimeSourceError(
            f"cannot prove runtime root/layout compatibility: expected exactly "
            f"one {what} in {file_name}, found {len(matches)}; {hint}",
            REASON_RUNTIME_ROOT_INCOMPATIBLE,
        )
    return matches[0].group(1)


def _prove_rendered_root_carrier(
    file_name: str,
    body: str,
    *,
    harness_root_locator: str,
    target_depth: int,
    hint: str,
) -> None:
    """Prove a placeholder-free root-carrier file resolves the target root.

    ``harness_common.py`` must embed exactly one harness root locator and one
    script depth; ``harnessctl`` exactly one locator. Both must equal the
    target, otherwise the copied runtime would resolve a different harness
    root and ``harnessctl validate/state`` would fail — never guess-rewrite
    the embedded string.
    """
    if file_name == "harness_common.py":
        locator = _extract_single_embedding(
            _HARNESS_ROOT_LOCATOR_RE,
            body,
            file_name=file_name,
            what="harness root locator (`return project_root() / \"...\"`)",
            hint=hint,
        )
        depth = _extract_single_embedding(
            _PROJECT_ROOT_DEPTH_RE,
            body,
            file_name=file_name,
            what="project root depth (`parents[N]`)",
            hint=hint,
        )
        if depth != str(target_depth):
            raise RuntimeSourceError(
                f"already-rendered runtime {file_name} embeds script depth "
                f"{depth!r}, but the target layout requires depth "
                f"{target_depth} (scripts live under scripts/harness); {hint}",
                REASON_RUNTIME_ROOT_INCOMPATIBLE,
            )
    else:  # harnessctl
        locator = _extract_single_embedding(
            _HARNESSCTL_ROOT_LOCATOR_RE,
            body,
            file_name=file_name,
            what="harness root locator (`HARNESS_ROOT=\"$root/...\"`)",
            hint=hint,
        )
    if locator != harness_root_locator:
        raise RuntimeSourceError(
            f"already-rendered runtime {file_name} embeds harness root "
            f"{locator!r}, which does not match the target harness root "
            f"{harness_root_locator!r}; {hint}",
            REASON_RUNTIME_ROOT_INCOMPATIBLE,
        )


def _render_template_body(
    file_name: str,
    body: str,
    *,
    harness_root_locator: str,
    scripts_dir: str,
    project_name: str,
    target_depth: int,
) -> bytes:
    rendered = (
        body.replace("{{HARNESS_ROOT}}", harness_root_locator)
        .replace("{{PROJECT_ROOT_DEPTH}}", str(target_depth))
        .replace("{{SCRIPTS_DIR}}", scripts_dir)
        .replace("{{PROJECT_NAME}}", project_name)
    )
    residual = _PLACEHOLDER_RE.search(rendered)
    if residual is not None:
        raise RuntimeSourceError(
            f"runtime template source {file_name} contains unknown/residual "
            f"placeholder {residual.group(0)!r} after rendering; only the four "
            f"U1 placeholders ({', '.join(sorted(RUNTIME_TEMPLATE_PLACEHOLDERS))}) "
            "may be consumed",
            REASON_RUNTIME_TEMPLATE_PLACEHOLDER,
        )
    return rendered.encode("utf-8")


def _preflight_runtime_source(
    source_path: Path,
    *,
    harness_root_locator: str,
    scripts_dir: str,
    project_name: str,
    target_depth: int,
) -> dict[str, bytes]:
    """Render/validate the full-init runtime source entirely in memory.

    Returns ``{filename: bytes}`` that must end up under ``scripts/harness``.
    Files containing ``{{...}}`` placeholders are rendered with the four U1
    placeholders; placeholder-free root-carrier files (``harness_common.py`` /
    ``harnessctl``) must prove they embed the target root/layout; the key
    files required by ``harnessctl validate`` / ``harnessctl state`` must
    exist. Every failure raises before any mutation.
    """
    raw: dict[str, bytes] = {}
    for entry in sorted(source_path.iterdir()):
        if entry.is_file():
            raw[entry.name] = entry.read_bytes()
    missing = [name for name in _RUNTIME_KEY_FILES if name not in raw]
    if missing:
        raise RuntimeSourceError(
            f"runtime source {source_path} is missing key runtime file(s) "
            f"required by `harnessctl validate` / `harnessctl state`: "
            f"{', '.join(missing)}. Refusing to claim success over an "
            "incomplete runtime.",
            REASON_RUNTIME_SOURCE_INCOMPLETE,
        )
    rendered: dict[str, bytes] = {}
    for name, data in sorted(raw.items()):
        body = data.decode("utf-8", errors="replace")
        if _PLACEHOLDER_RE.search(body) is not None:
            rendered[name] = _render_template_body(
                name,
                body,
                harness_root_locator=harness_root_locator,
                scripts_dir=scripts_dir,
                project_name=project_name,
                target_depth=target_depth,
            )
        else:
            if name in _RUNTIME_ROOT_CARRIER_FILES:
                _prove_rendered_root_carrier(
                    name,
                    body,
                    harness_root_locator=harness_root_locator,
                    target_depth=target_depth,
                    hint=_TEMPLATE_SOURCE_HINT,
                )
            rendered[name] = data
    return rendered


def _preflight_existing_runtime_destination(
    ws_root: Path,
    runtime_files: dict[str, bytes],
    *,
    harness_root_locator: str,
    target_depth: int,
) -> None:
    """Prove every existing destination collision is this init's own runtime.

    For every file this init would place under ``scripts/harness`` that
    already exists at the destination, the uniform proof is exact byte
    equality with the rendered bytes: a collision must be a readable regular
    file, contain no residual ``{{...}}``, and be byte-identical to what this
    init would render. Anything else cannot be proven to be the same runtime
    and fails closed before any protocol/checklist/DB mutation — the file is
    never silently kept and never blindly overwritten. Root-carrier files get
    a more specific diagnostic when they differ, but byte equality is the
    only proof that matters.
    """
    for name in sorted(runtime_files):
        dst = ws_root / "scripts" / "harness" / name
        if not os.path.lexists(dst):
            continue
        # The collision itself must be a regular file: lstat (never is_file,
        # which follows links) rejects live AND dangling symlinks, so an
        # identical-bytes symlink cannot smuggle a second authority past the
        # byte-equality proof.
        try:
            mode = dst.lstat().st_mode
        except OSError as exc:
            raise RuntimeSourceError(
                f"existing runtime destination {dst} cannot be inspected: {exc}; "
                f"cannot prove it is the same runtime; {_DESTINATION_HINT}",
                REASON_RUNTIME_ROOT_INCOMPATIBLE,
            ) from exc
        if not stat.S_ISREG(mode):
            raise RuntimeSourceError(
                f"existing runtime destination {dst} is not a regular file "
                f"(directory, symlink, or other entry); cannot prove "
                f"it is the same runtime; {_DESTINATION_HINT}",
                REASON_RUNTIME_ROOT_INCOMPATIBLE,
            )
        existing = dst.read_bytes()
        text = existing.decode("utf-8", errors="replace")
        if _PLACEHOLDER_RE.search(text) is not None:
            raise RuntimeSourceError(
                f"existing runtime destination {dst} still contains "
                f"{{{{...}}}} placeholders (unrendered template); cannot prove "
                f"it is the same runtime; {_DESTINATION_HINT}",
                REASON_RUNTIME_TEMPLATE_PLACEHOLDER,
            )
        if existing != runtime_files[name]:
            # Give the most specific diagnosable reason when the collision is
            # a root-carrier; otherwise the uniform proof fails closed.
            if name in _RUNTIME_ROOT_CARRIER_FILES:
                _prove_rendered_root_carrier(
                    name,
                    text,
                    harness_root_locator=harness_root_locator,
                    target_depth=target_depth,
                    hint=_DESTINATION_HINT,
                )
            raise RuntimeSourceError(
                f"existing runtime destination {dst} differs from the runtime "
                f"this init would render; cannot prove it is the same "
                f"runtime; {_DESTINATION_HINT}",
                REASON_RUNTIME_DESTINATION_MISMATCH,
            )


def init_full_harness(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    source: str | Path,
    dry_run: bool = False,
    actor: str = "operator",
) -> FullInitResult:
    """Full harness init: render/copy a U1 runtime under ``scripts/harness``.

    The source must be either the U1 template (files containing the four
    placeholders ``{{HARNESS_ROOT}}`` / ``{{PROJECT_ROOT_DEPTH}}`` /
    ``{{SCRIPTS_DIR}}`` / ``{{PROJECT_NAME}}``, rendered in memory against the
    registered harness root) or an already-rendered runtime whose embedded
    root/layout provably matches the target. Unknown/residual placeholders,
    unprovable rendered sources, missing key runtime files, and incompatible
    existing destinations all fail closed before any file/DB/event mutation;
    dry-run runs the same compatibility preflight without writing.
    """
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")

    source_path = Path(source).resolve()
    if not source_path.is_dir():
        raise ValueError(f"source directory does not exist: {source_path}")

    # source can be external (e.g. another workspace), but the target harness
    # root must be inside the workspace.
    ws_root = Path(workspace.path).resolve()
    hr_path = Path(workspace.harness_root).resolve()
    _require_workspace_contained(workspace, hr_path, label="harness_root")

    # Preflight the checklist authority BEFORE copying any runtime/protocol
    # file or touching the DB: dual authority or a non-regular candidate must
    # leave the workspace and DB completely untouched.
    checklist_resolved, checklist_create_new = resolve_checklist_for_init(hr_path)

    # P1-3: preflight the runtime source and the existing destination before
    # any mutation. Template sources are rendered in memory here; anything
    # that cannot be proven compatible with the target root/layout fails
    # closed (no guessing, no second root config).
    scripts_dir = "scripts/harness"
    target_depth = len(Path(scripts_dir).parts)
    # POSIX locator per the U1 placeholder contract (never os-native slashes).
    harness_root_locator = hr_path.relative_to(ws_root).as_posix()
    runtime_files = _preflight_runtime_source(
        source_path,
        harness_root_locator=harness_root_locator,
        scripts_dir=scripts_dir,
        project_name=workspace.id,
        target_depth=target_depth,
    )
    _preflight_existing_runtime_destination(
        ws_root,
        runtime_files,
        harness_root_locator=harness_root_locator,
        target_depth=target_depth,
    )

    scripts_copied: list[str] = []
    scripts_existing: list[str] = []
    files_created: list[str] = []
    files_existing: list[str] = []
    warnings: list[str] = []

    # 1. Copy/render scripts/harness/ runtime from source (rendered above).
    for name in sorted(runtime_files):
        rel_script = f"{scripts_dir}/{name}"
        dst = ws_root / rel_script
        if dst.exists():
            scripts_existing.append(rel_script)
            continue
        # Security: verify destination is within workspace (a dangling symlink
        # or odd pre-existing entry must not redirect writes outside).
        try:
            dst.resolve().relative_to(ws_root)
        except ValueError:
            warnings.append(f"skipped {name}: destination outside workspace")
            continue
        if not dry_run:
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(runtime_files[name])
            # Ensure executable for script files
            if Path(name).suffix in ("", ".sh", ".py"):
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

    # 4. Ensure minimal harness files exist (harness-config.json, checklist,
    #    events.jsonl, progress.md) — create validator-passing versions if missing
    _ensure_minimal_files(
        hr_path,
        ws_root,
        project,
        dry_run,
        files_created,
        files_existing,
        warnings,
        resolved_checklist=checklist_resolved,
        create_new=checklist_create_new,
    )

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
    workspace_path: Path,
    project: str,
    dry_run: bool,
    files_created: list[str],
    files_existing: list[str],
    warnings: list[str],
    resolved_checklist: ResolvedChecklist,
    create_new: bool,
) -> None:
    """Create validator-passing minimal harness files if they don't exist.

    The checklist authority decision (none/new-only/legacy-only/both; existing
    candidates regular + readable + schema-valid) was already made by
    ``checklist_io.resolve_checklist_for_init`` before any mutation; this
    function only materializes the decision. New projects get the new
    checklist filename through the unified validator + atomic writer; a
    legacy-only existing checklist is treated as existing compatible (no
    competing new file is created). The config pins
    ``deployment_profile=coordinate-managed`` and a workspace-relative event
    log, and the state stub carries the real checklist path + digest so it is
    never a source-less cache.
    """
    try:
        rel_hr = str(hr_path.resolve().relative_to(workspace_path.resolve()))
    except ValueError:
        rel_hr = str(hr_path.resolve())
    rel_events = f"{rel_hr}/events.jsonl"

    stubs: dict[str, str] = {}
    existing_names: list[str] = []

    # harness-config.json
    config_path = hr_path / "harness-config.json"
    if not config_path.exists():
        stubs["harness-config.json"] = json.dumps({
            "project": project,
            "deployment_profile": "coordinate-managed",
            "runtime": {
                "session_init_commands": [],
                "lease_ttl_minutes": 120,
            },
            "git": {},
            "message_bus": {"event_log": rel_events},
        }, indent=2) + "\n"
    else:
        existing_names.append("harness-config.json")

    # Checklist: the authority decision came from the checklist_io init
    # boundary. Only the none case creates a new checklist — through the
    # unified validator + atomic writer, and its bytes feed the state digest
    # below (never a lookalike prediction).
    if create_new:
        checklist_updated_at = utc_now().split("T")[0]
        checklist_body = json.dumps({
            "project": project,
            "harness_root": rel_hr,
            "version": 1,
            "updated_at": checklist_updated_at,
            "items": [],
        }, ensure_ascii=False, indent=2) + "\n"
        if not dry_run:
            create_empty_checklist(
                hr_path,
                project=project,
                harness_root_rel=rel_hr,
                updated_at=checklist_updated_at,
            )
        files_created.append(str(resolved_checklist.path))
    else:
        existing_names.append(resolved_checklist.path.name)
        if resolved_checklist.kind == "legacy":
            warnings.append(
                f"legacy {CHECKLIST_LEGACY_NAME} found; treating as existing "
                "compatible checklist (no new file created)"
            )

    # events.jsonl
    events_path = hr_path / "events.jsonl"
    if not events_path.exists():
        stubs["events.jsonl"] = ""
    else:
        existing_names.append("events.jsonl")

    # progress.md
    progress_path = hr_path / "progress.md"
    if not progress_path.exists():
        stubs["progress.md"] = (
            f"# {project} Harness Progress\n\n"
            "Initialized by coordinator full harness init.\n"
        )
    else:
        existing_names.append("progress.md")

    # harness-state.json — must carry the real checklist path + digest, so it
    # is written after the checklist stub (source is never a lookalike cache).
    state_path = hr_path / "harness-state.json"
    if not state_path.exists():
        if create_new:
            checklist_bytes = checklist_body.encode("utf-8")
        else:
            try:
                checklist_bytes = resolved_checklist.path.read_bytes()
            except OSError:
                checklist_bytes = b""
        rel_checklist = (
            f"{rel_hr}/{resolved_checklist.path.name}"
        )
        stubs["harness-state.json"] = json.dumps({
            "project": project,
            "harness_root": rel_hr,
            "generated_at": utc_now(),
            "source": {
                "checklist_path": rel_checklist,
                "checklist_sha256": sha256_bytes(checklist_bytes),
            },
            "current_status": "",
            "current_item": None,
            "checklist_summary": {"todo": 0, "doing": 0, "done": 0, "blocked": 0},
            "workflow_summary": {"closed": 0, "running": 0},
            "paths": {
                "checklist": rel_checklist,
                "events": rel_events,
                "config": f"{rel_hr}/harness-config.json",
                "progress": f"{rel_hr}/progress.md",
            },
            "commands": {},
            "message_bus": {"event_log": rel_events},
            "open_risks": [],
            "recent_events": [],
        }, indent=2) + "\n"
    else:
        existing_names.append("harness-state.json")

    for fname, content in stubs.items():
        fpath = hr_path / fname
        if not dry_run:
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_text(content, encoding="utf-8")
        files_created.append(str(fpath))

    for fname in existing_names:
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
