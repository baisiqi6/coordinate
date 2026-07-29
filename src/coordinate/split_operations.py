"""Split-operation contract, lock, and task.create file/record halves.

This module owns the C1 contract so that C2 (issue.materialize) can reuse the
same ledger, fingerprint format, and envelope shape without a second schema or
special-cased target column.
"""
from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .db import (
    Workspace,
    append_event,
    get_split_operation,
    insert_split_operation,
    list_split_operations,
    row_to_dict,
    update_split_operation_event,
    upsert_task_mirror,
    utc_now,
)

CONTRACT_VERSION = 1
OPERATION_KIND_TASK_CREATE = "task.create"
OPERATION_KIND_ISSUE_MATERIALIZE = "issue.materialize"
TARGET_KIND_CHECKLIST_TASK = "checklist_task"
SOURCE_KIND_ISSUE_TRIAGED_EVENT = "issue_triaged_event"
STATUS_RECORD_APPLIED = "record_applied"

REASON_FILES_NOT_DEPLOYED = "files_not_deployed"
REASON_OPERATION_CONFLICT = "operation_conflict"
REASON_FINGERPRINT_DRIFT = "fingerprint_drift"
REASON_LOCK_TIMEOUT = "lock_timeout"
REASON_VALIDATION_ERROR = "validation_error"


class SplitOperationError(ValueError):
    """A split-operation half refused to apply.

    ``reason`` is a stable machine-readable classification string.
    """

    def __init__(self, message: str, reason: str):
        super().__init__(message)
        self.reason = reason


# ---------------------------------------------------------------------------
# Canonical hashing and validation
# ---------------------------------------------------------------------------


def _canonical_json(value: Any) -> str:
    """Compact, key-sorted, UTF-8 JSON used for every contract hash."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _canonical_hash(value: Any) -> str:
    """SHA-256 of the canonical JSON representation."""
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

# UTC whole-second Z timestamp, e.g. 2026-01-01T00:00:00Z.
_FILES_APPLIED_AT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
)


def validate_files_applied_at(value: str) -> str:
    """Return the value if it is a UTC whole-second Z timestamp, else raise."""
    if not isinstance(value, str) or not _FILES_APPLIED_AT_RE.match(value):
        raise SplitOperationError(
            f"files_applied_at must be a UTC whole-second Z timestamp, got {value!r}",
            REASON_FINGERPRINT_DRIFT,
        )
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise SplitOperationError(
            f"files_applied_at is not a valid UTC timestamp: {value!r}",
            REASON_FINGERPRINT_DRIFT,
        ) from exc
    return value


def validate_uuid(value: str) -> str:
    """Return a lowercase canonical UUID text or raise SplitOperationError."""
    if not isinstance(value, str):
        raise SplitOperationError("operation id must be a string", REASON_VALIDATION_ERROR)
    lowered = value.lower()
    if not _UUID_RE.match(lowered):
        raise SplitOperationError(
            f"operation id must be a lowercase RFC 4122 UUID, got {value!r}",
            REASON_VALIDATION_ERROR,
        )
    try:
        parsed = uuid.UUID(lowered)
    except ValueError as exc:
        raise SplitOperationError(
            f"operation id is not a valid UUID: {value!r}",
            REASON_VALIDATION_ERROR,
        ) from exc
    if lowered != str(parsed):
        raise SplitOperationError(
            f"operation id must be canonical lowercase, got {value!r}",
            REASON_VALIDATION_ERROR,
        )
    return lowered


def validate_sha256(value: str) -> str:
    """Return a lowercase 64-character SHA-256 hex string or raise."""
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise SplitOperationError(
            f"fingerprint must be a lowercase 64-character SHA-256 hex string, got {value!r}",
            REASON_VALIDATION_ERROR,
        )
    return value


def validate_workspace_relative_path(value: str) -> str:
    """Return a normalized POSIX workspace-relative path or raise.

    Rejects absolute paths, ``..``, empty segments, and backslashes.
    """
    if not isinstance(value, str):
        raise SplitOperationError("plan_doc must be a string", REASON_VALIDATION_ERROR)
    if "\\" in value:
        raise SplitOperationError(
            f"plan_doc must use POSIX separators, got {value!r}",
            REASON_VALIDATION_ERROR,
        )
    if value.startswith("/"):
        raise SplitOperationError(
            f"plan_doc must be workspace-relative, got {value!r}",
            REASON_VALIDATION_ERROR,
        )
    parts = value.split("/")
    if any(part == "" or part == ".." for part in parts):
        raise SplitOperationError(
            f"plan_doc must not contain empty segments or '..', got {value!r}",
            REASON_VALIDATION_ERROR,
        )
    return value


def compute_plan_sha256(path: Path) -> str:
    """Return the lowercase SHA-256 of the file bytes at *path*."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Contract v1 input fingerprint
# ---------------------------------------------------------------------------


def build_task_create_input_fingerprint(
    *,
    workspace_id: str,
    task_id: str,
    plan_doc: str,
    plan_sha256: str,
    title: str,
    phase: str,
    priority: str,
) -> str:
    """Compute the canonical v1 task.create input fingerprint."""
    return _canonical_hash({
        "contract_version": CONTRACT_VERSION,
        "operation_kind": OPERATION_KIND_TASK_CREATE,
        "workspace_id": workspace_id,
        "target": {"kind": TARGET_KIND_CHECKLIST_TASK, "id": task_id},
        "source": None,
        "plan_doc": validate_workspace_relative_path(plan_doc),
        "plan_sha256": validate_sha256(plan_sha256),
        "title": title,
        "phase": phase,
        "priority": priority,
    })


# ---------------------------------------------------------------------------
# Checklist-item projection for before/after fingerprints
# ---------------------------------------------------------------------------

_FINGERPRINT_EXCLUDED_KEYS = frozenset({
    "updated_at",
    "split_operation",
    "completion_receipt",
    "verification",
})


def _project_value(value: Any) -> Any:
    """Recursively canonicalize a value for fingerprinting.

    Dicts are sorted by key. Lists/tuples are preserved. Scalars pass through.
    """
    if isinstance(value, dict):
        return {
            key: _project_value(value[key])
            for key in sorted(value.keys())
            if key not in _FINGERPRINT_EXCLUDED_KEYS
        }
    if isinstance(value, list):
        return [_project_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_project_value(item) for item in value)
    return value


def project_checklist_item_for_fingerprint(
    item: dict[str, Any] | None,
    task_id: str,
) -> dict[str, Any]:
    """Project a checklist item to its canonical fingerprint shape.

    *item* may be ``None`` to represent an absent task.
    """
    if item is None:
        return {
            "state": "absent",
            "target": {"kind": TARGET_KIND_CHECKLIST_TASK, "id": task_id},
        }
    projected = dict(item)
    # Exclude top-level volatile/descriptive fields.
    for key in _FINGERPRINT_EXCLUDED_KEYS:
        projected.pop(key, None)
    # Exclude workflow.updated_at specifically.
    workflow = projected.get("workflow")
    if isinstance(workflow, dict):
        workflow = dict(workflow)
        workflow.pop("updated_at", None)
        projected["workflow"] = workflow
    return _project_value(projected)


def compute_task_item_fingerprint(
    *,
    item: dict[str, Any] | None,
    task_id: str,
) -> str:
    """Return the canonical fingerprint for a checklist-item projection."""
    return _canonical_hash(project_checklist_item_for_fingerprint(item, task_id))


# ---------------------------------------------------------------------------
# Operation envelope
# ---------------------------------------------------------------------------


def build_task_create_envelope(
    *,
    operation_id: str,
    workspace_id: str,
    task_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
    files_applied_at: str,
) -> dict[str, Any]:
    """Build the v1 task.create checklist envelope."""
    return {
        "contract_version": CONTRACT_VERSION,
        "operation_id": operation_id,
        "operation_kind": OPERATION_KIND_TASK_CREATE,
        "workspace_id": workspace_id,
        "target_kind": TARGET_KIND_CHECKLIST_TASK,
        "target_id": task_id,
        "source_kind": None,
        "source_id": None,
        "input_fingerprint": input_fingerprint,
        "before_fingerprint": before_fingerprint,
        "after_fingerprint": after_fingerprint,
        "files_applied_at": files_applied_at,
    }


def build_issue_materialize_input_fingerprint(
    *,
    workspace_id: str,
    task_id: str,
    source_id: str,
    plan_doc: str,
    plan_sha256: str,
    title: str,
    phase: str,
    priority: str,
) -> str:
    """Compute the canonical v1 issue.materialize input fingerprint."""
    return _canonical_hash({
        "contract_version": CONTRACT_VERSION,
        "operation_kind": OPERATION_KIND_ISSUE_MATERIALIZE,
        "workspace_id": workspace_id,
        "target": {"kind": TARGET_KIND_CHECKLIST_TASK, "id": task_id},
        "source": {"kind": SOURCE_KIND_ISSUE_TRIAGED_EVENT, "id": validate_uuid(source_id)},
        "plan_doc": validate_workspace_relative_path(plan_doc),
        "plan_sha256": validate_sha256(plan_sha256),
        "title": title,
        "phase": phase,
        "priority": priority,
    })


def build_issue_materialize_envelope(
    *,
    operation_id: str,
    workspace_id: str,
    task_id: str,
    source_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
    files_applied_at: str,
) -> dict[str, Any]:
    """Build the v1 issue.materialize checklist envelope."""
    return {
        "contract_version": CONTRACT_VERSION,
        "operation_id": operation_id,
        "operation_kind": OPERATION_KIND_ISSUE_MATERIALIZE,
        "workspace_id": workspace_id,
        "target_kind": TARGET_KIND_CHECKLIST_TASK,
        "target_id": task_id,
        "source_kind": SOURCE_KIND_ISSUE_TRIAGED_EVENT,
        "source_id": validate_uuid(source_id),
        "input_fingerprint": input_fingerprint,
        "before_fingerprint": before_fingerprint,
        "after_fingerprint": after_fingerprint,
        "files_applied_at": files_applied_at,
    }


# ---------------------------------------------------------------------------
# Cross-platform per-checklist lock
# ---------------------------------------------------------------------------


def _process_alive_default(pid: int) -> bool:
    """Best-effort check that *pid* names a live process.

    On Unix this uses ``os.kill(pid, 0)``. On Windows it tries ``psutil`` if
    available; otherwise it conservatively returns ``True`` so a stale lock is
    never deleted without explicit stale-owner evidence.
    """
    if pid == os.getpid():
        return True
    try:
        import psutil  # type: ignore[import-untyped]
        return psutil.pid_exists(pid)
    except Exception:
        pass
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


class ChecklistLock:
    """Advisory per-checklist process lock using an exclusive lock file.

    Acquisition uses ``O_CREAT | O_EXCL`` so two processes cannot both create
    the lock file. If the lock file already exists, the owner PID is inspected;
    dead owners are safe to break, live owners block up to *timeout* seconds.
    """

    def __init__(
        self,
        checklist_path: Path,
        *,
        timeout: float = 30.0,
        poll_interval: float = 0.05,
        _now: Callable[[], float] | None = None,
        _pid: int | None = None,
        _process_alive: Callable[[int], bool] | None = None,
    ):
        self.checklist_path = Path(checklist_path)
        self.lock_path = self.checklist_path.with_suffix(".json.lock")
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._now = _now or time.monotonic
        self._pid = _pid or os.getpid()
        self._process_alive = _process_alive or _process_alive_default
        self._owned = False

    def _lock_content(self) -> bytes:
        return _canonical_json({
            "owner_pid": self._pid,
            "created_at": _utc_now(),
        }).encode("utf-8")

    def _read_owner_pid(self) -> int | None:
        try:
            data = json.loads(self.lock_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        pid = data.get("owner_pid")
        return int(pid) if isinstance(pid, int) else None

    def _try_create(self) -> bool:
        try:
            fd = os.open(
                str(self.lock_path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o644,
            )
        except FileExistsError:
            return False
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                return False
            raise
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(self._lock_content())
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                self.lock_path.unlink()
            except OSError:
                pass
            raise
        return True

    def acquire(self) -> None:
        deadline = self._now() + self.timeout
        while True:
            if self._try_create():
                self._owned = True
                return
            owner_pid = self._read_owner_pid()
            if owner_pid is not None and not self._process_alive(owner_pid):
                # Stale lock from a dead owner: remove and retry immediately.
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    pass
                continue
            if self._now() >= deadline:
                raise SplitOperationError(
                    f"timed out waiting for checklist lock {self.lock_path}",
                    REASON_LOCK_TIMEOUT,
                )
            time.sleep(self.poll_interval)

    def release(self) -> None:
        if self._owned:
            try:
                self.lock_path.unlink()
            except FileNotFoundError:
                pass
            self._owned = False

    def __enter__(self) -> "ChecklistLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


# ---------------------------------------------------------------------------
# Atomic file write
# ---------------------------------------------------------------------------


def _atomic_write_json(target_path: Path, data: dict[str, Any]) -> None:
    """Write *data* to *target_path* atomically with fsync and mode preservation."""
    target_path = Path(target_path)
    parent = target_path.parent
    parent.mkdir(parents=True, exist_ok=True)
    tmp_path = parent / f".{target_path.name}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
            f.flush()
            os.fsync(f.fileno())
        if target_path.exists():
            mode = target_path.stat().st_mode
            tmp_path.chmod(mode)
        os.replace(tmp_path, target_path)
    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Checklist defaults and file-half helpers
# ---------------------------------------------------------------------------


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


def _build_checklist_item(
    *,
    task_id: str,
    title: str,
    plan_doc: str,
    priority: str,
    phase: str,
    now: str,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    """Build a deterministic checklist item including the split-operation envelope."""
    coarse_status = _checklist_status_for_phase(phase)
    workflow_status = _workflow_status_for_phase(phase)
    return {
        "id": task_id,
        "title": title,
        "status": coarse_status,
        "phase": phase,
        "priority": priority,
        "owner": None,
        "human_gate_required": True,
        "plan_path": plan_doc,
        "acceptance": f"Use the plan acceptance criteria as source of truth: {plan_doc}",
        "blocked_by": [],
        "blocked_reason": "",
        "dependencies": [],
        "handoff": {"from": None, "to": None, "reason": None},
        "selected_in_session": None,
        "updated_at": now,
        "workflow": {"status": workflow_status, "branch": None, "updated_at": now},
        "artifacts": {"plan": plan_doc},
        "verification": "",
        "review": {},
        "split_operation": envelope,
    }


def reconstruct_creation_time_checklist_item(
    *,
    task_id: str,
    title: str,
    plan_doc: str,
    priority: str,
    phase: str,
    files_applied_at: str,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    """Reconstruct the creation-time checklist projection from authoritative evidence.

    Uses the same canonical checklist-item constructor and projection used by
    the write path.  The result is read-only in intent: callers must not mutate
    the deployed checklist.
    """
    return _build_checklist_item(
        task_id=task_id,
        title=title,
        plan_doc=plan_doc,
        priority=priority,
        phase=phase,
        now=files_applied_at,
        envelope=envelope,
    )


# Standard top-level keys written by _build_checklist_item at task creation.
# Deriving this from the constructor keeps the doctor's recognized-field set
# synchronized with the write path.
STANDARD_CREATION_ITEM_FIELDS: frozenset[str] = frozenset(
    _build_checklist_item(
        task_id="",
        title="",
        plan_doc="",
        priority="",
        phase="",
        now="1970-01-01T00:00:00Z",
        envelope={},
    ).keys()
)

# Additional top-level keys written by supported lifecycle transitions.
# - completion_receipt: transitions.mark_done_record / mark_done_files
# - lease: MultiNexus scripts/harness/harness_common.py claim_lease / release_lease
LIFECYCLE_OWNED_ITEM_FIELDS: frozenset[str] = frozenset({
    "completion_receipt",
    "lease",
})


def _latest_prior_plan_ready_id(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    exclude_operation_id: str | None = None,
) -> str | None:
    """Return the id of the most recent prior plan.ready event, if any.

    When *exclude_operation_id* is given, ignore plan.ready events that belong
    to the same split operation so that an idempotent retry does not treat the
    operation's own event as a prior revision.
    """
    if exclude_operation_id:
        row = conn.execute(
            "SELECT id FROM events WHERE workspace_id = ? AND task_id = ? AND event_type = 'plan.ready' "
            "AND (json_extract(payload_json, '$.split_operation.operation_id') IS NULL "
            "OR json_extract(payload_json, '$.split_operation.operation_id') != ?) "
            "ORDER BY rowid DESC LIMIT 1",
            (workspace_id, task_id, exclude_operation_id),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT id FROM events WHERE workspace_id = ? AND task_id = ? AND event_type = 'plan.ready' "
            "ORDER BY rowid DESC LIMIT 1",
            (workspace_id, task_id),
        ).fetchone()
    return row["id"] if row else None


def _prior_plan_ready_id_before_rowid(
    conn: sqlite3.Connection,
    workspace_id: str,
    task_id: str,
    before_rowid: int,
) -> str | None:
    """Return the id of the latest same-workspace/same-task plan.ready event
    whose rowid is strictly less than *before_rowid*.

    This is the historical cutoff used for exact idempotent retries: it
    excludes the bound ready event itself, the current split operation, and
    any later unrelated ready events.
    """
    row = conn.execute(
        "SELECT id FROM events WHERE workspace_id = ? AND task_id = ? AND event_type = 'plan.ready' "
        "AND rowid < ? ORDER BY rowid DESC LIMIT 1",
        (workspace_id, task_id, before_rowid),
    ).fetchone()
    return row["id"] if row else None


def _read_checklist(checklist_path: Path) -> dict[str, Any]:
    if not checklist_path.is_file():
        return {"project": "", "harness_root": "", "version": 1, "items": []}
    try:
        return json.loads(checklist_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SplitOperationError(
            f"mvp-checklist.json at {checklist_path} cannot be read: {exc}",
            REASON_VALIDATION_ERROR,
        ) from exc


def _find_checklist_item(checklist: dict[str, Any], task_id: str) -> dict[str, Any] | None:
    items = checklist.get("items") if isinstance(checklist.get("items"), list) else None
    if not items:
        return None
    for item in items:
        if isinstance(item, dict) and item.get("id") == task_id:
            return item
    return None


def _write_checklist(checklist_path: Path, checklist: dict[str, Any]) -> None:
    _atomic_write_json(checklist_path, checklist)


@dataclass(frozen=True)
class TaskCreateFilesOperationResult:
    workspace_id: str
    workspace_path: str
    harness_root: str
    task_id: str
    plan_doc: str
    operation_id: str
    operation_kind: str
    contract_version: int
    input_fingerprint: str
    before_fingerprint: str
    after_fingerprint: str
    files_applied_at: str
    checklist_changed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "workspace_path": self.workspace_path,
            "harness_root": self.harness_root,
            "task_id": self.task_id,
            "plan_doc": self.plan_doc,
            "operation_id": self.operation_id,
            "operation_kind": self.operation_kind,
            "contract_version": self.contract_version,
            "input_fingerprint": self.input_fingerprint,
            "before_fingerprint": self.before_fingerprint,
            "after_fingerprint": self.after_fingerprint,
            "files_applied_at": self.files_applied_at,
            "checklist_changed": self.checklist_changed,
        }


def _validate_task_create_files_inputs(
    *,
    workspace_id: str,
    operation_id: str,
    task_id: str,
    plan_doc: str,
    title: str | None,
    phase: str,
    priority: str,
) -> tuple[str, str, str, str, str, str, str]:
    operation_id = validate_uuid(operation_id)
    if not workspace_id:
        raise SplitOperationError("workspace_id is required", REASON_VALIDATION_ERROR)
    if not task_id:
        raise SplitOperationError("task_id is required", REASON_VALIDATION_ERROR)
    plan_doc = validate_workspace_relative_path(plan_doc)
    title = (title or task_id).strip()
    if not title:
        raise SplitOperationError("resolved title is empty", REASON_VALIDATION_ERROR)
    if not phase:
        raise SplitOperationError("phase is required", REASON_VALIDATION_ERROR)
    if not priority:
        raise SplitOperationError("priority is required", REASON_VALIDATION_ERROR)
    return operation_id, workspace_id, task_id, plan_doc, title, phase, priority


def apply_task_create_files(
    *,
    workspace_path: str | Path,
    harness_root: str | Path,
    task_id: str,
    plan_doc: str,
    title: str | None,
    phase: str,
    priority: str,
    operation_id: str,
    workspace_id: str,
    now: str | None = None,
    _lock_timeout: float = 30.0,
    _lock: ChecklistLock | None = None,
) -> TaskCreateFilesOperationResult:
    """Apply the file half of a task.create split operation.

    Writes the operation envelope into ``mvp-checklist.json`` atomically under
    a per-checklist lock. Idempotent when the exact envelope is already present.
    """
    operation_id, workspace_id, task_id, plan_doc, title, phase, priority = (
        _validate_task_create_files_inputs(
            workspace_id=workspace_id,
            operation_id=operation_id,
            task_id=task_id,
            plan_doc=plan_doc,
            title=title,
            phase=phase,
            priority=priority,
        )
    )
    workspace_path = Path(workspace_path)
    harness_root = Path(harness_root)
    checklist_path = harness_root / "mvp-checklist.json"
    plan_abs = workspace_path / plan_doc
    if not plan_abs.is_file():
        raise SplitOperationError(
            f"plan_doc does not exist: {plan_abs}",
            REASON_FILES_NOT_DEPLOYED,
        )
    plan_sha256 = compute_plan_sha256(plan_abs)
    files_applied_at = now or _utc_now()

    input_fingerprint = build_task_create_input_fingerprint(
        workspace_id=workspace_id,
        task_id=task_id,
        plan_doc=plan_doc,
        plan_sha256=plan_sha256,
        title=title,
        phase=phase,
        priority=priority,
    )
    before_fingerprint = compute_task_item_fingerprint(item=None, task_id=task_id)
    after_item = _build_checklist_item(
        task_id=task_id,
        title=title,
        plan_doc=plan_doc,
        priority=priority,
        phase=phase,
        now=files_applied_at,
        envelope=build_task_create_envelope(
            operation_id=operation_id,
            workspace_id=workspace_id,
            task_id=task_id,
            input_fingerprint=input_fingerprint,
            before_fingerprint=before_fingerprint,
            after_fingerprint="",  # filled below
            files_applied_at=files_applied_at,
        ),
    )
    after_fingerprint = compute_task_item_fingerprint(item=after_item, task_id=task_id)
    envelope = build_task_create_envelope(
        operation_id=operation_id,
        workspace_id=workspace_id,
        task_id=task_id,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
        files_applied_at=files_applied_at,
    )
    after_item["split_operation"] = envelope

    lock = _lock or ChecklistLock(checklist_path, timeout=_lock_timeout)
    with lock:
        checklist = _read_checklist(checklist_path)
        existing = _find_checklist_item(checklist, task_id)

        if existing is not None:
            existing_envelope = existing.get("split_operation")
            if isinstance(existing_envelope, dict) and existing_envelope.get("operation_id") == operation_id:
                # Same operation id: verify the whole envelope and projected item.
                expected_keys = {
                    "contract_version",
                    "operation_id",
                    "operation_kind",
                    "workspace_id",
                    "target_kind",
                    "target_id",
                    "source_kind",
                    "source_id",
                    "input_fingerprint",
                    "before_fingerprint",
                    "after_fingerprint",
                    "files_applied_at",
                }
                if set(existing_envelope.keys()) != expected_keys:
                    raise SplitOperationError(
                        f"task {task_id} already has a malformed envelope for operation {operation_id}",
                        REASON_OPERATION_CONFLICT,
                    )
                for key in expected_keys:
                    if key == "files_applied_at":
                        # Exact retry is intentionally allowed across different
                        # apply timestamps; the original envelope time is returned.
                        continue
                    if existing_envelope.get(key) != envelope.get(key):
                        raise SplitOperationError(
                            f"task {task_id} has operation {operation_id} but envelope field {key} differs",
                            REASON_OPERATION_CONFLICT,
                        )
                current_fingerprint = compute_task_item_fingerprint(item=existing, task_id=task_id)
                if current_fingerprint != after_fingerprint:
                    raise SplitOperationError(
                        f"task {task_id} has operation {operation_id} but current projection has drifted",
                        REASON_FINGERPRINT_DRIFT,
                    )
                # Idempotent success: do not rewrite the file.
                return TaskCreateFilesOperationResult(
                    workspace_id=workspace_id,
                    workspace_path=str(workspace_path),
                    harness_root=str(harness_root),
                    task_id=task_id,
                    plan_doc=plan_doc,
                    operation_id=operation_id,
                    operation_kind=OPERATION_KIND_TASK_CREATE,
                    contract_version=CONTRACT_VERSION,
                    input_fingerprint=input_fingerprint,
                    before_fingerprint=before_fingerprint,
                    after_fingerprint=after_fingerprint,
                    files_applied_at=existing_envelope["files_applied_at"],
                    checklist_changed=False,
                )
            # Existing item is either unbound or bound to another operation.
            raise SplitOperationError(
                f"task {task_id} already exists in checklist with a different operation",
                REASON_OPERATION_CONFLICT,
            )

        # Absent task: append the new item atomically.
        items = checklist.setdefault("items", [])
        if not isinstance(items, list):
            raise SplitOperationError(
                "mvp-checklist.json items must be a list",
                REASON_VALIDATION_ERROR,
            )
        items.append(after_item)
        checklist["updated_at"] = files_applied_at.split("T")[0]
        _write_checklist(checklist_path, checklist)

    return TaskCreateFilesOperationResult(
        workspace_id=workspace_id,
        workspace_path=str(workspace_path),
        harness_root=str(harness_root),
        task_id=task_id,
        plan_doc=plan_doc,
        operation_id=operation_id,
        operation_kind=OPERATION_KIND_TASK_CREATE,
        contract_version=CONTRACT_VERSION,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
        files_applied_at=files_applied_at,
        checklist_changed=True,
    )


# ---------------------------------------------------------------------------
# Issue materialize file-half helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IssueMaterializeFilesOperationResult:
    workspace_id: str
    workspace_path: str
    harness_root: str
    task_id: str
    plan_doc: str
    operation_id: str
    operation_kind: str
    contract_version: int
    source_event_id: str
    input_fingerprint: str
    before_fingerprint: str
    after_fingerprint: str
    files_applied_at: str
    checklist_changed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "workspace_path": self.workspace_path,
            "harness_root": self.harness_root,
            "task_id": self.task_id,
            "plan_doc": self.plan_doc,
            "operation_id": self.operation_id,
            "operation_kind": self.operation_kind,
            "contract_version": self.contract_version,
            "source_event_id": self.source_event_id,
            "input_fingerprint": self.input_fingerprint,
            "before_fingerprint": self.before_fingerprint,
            "after_fingerprint": self.after_fingerprint,
            "files_applied_at": self.files_applied_at,
            "checklist_changed": self.checklist_changed,
        }


def _validate_issue_materialize_files_inputs(
    *,
    workspace_id: str,
    operation_id: str,
    source_event_id: str,
    task_id: str,
    plan_doc: str,
    title: str | None,
    phase: str,
    priority: str,
) -> tuple[str, str, str, str, str, str, str, str]:
    operation_id = validate_uuid(operation_id)
    source_event_id = validate_uuid(source_event_id)
    if not workspace_id:
        raise SplitOperationError("workspace_id is required", REASON_VALIDATION_ERROR)
    if not task_id:
        raise SplitOperationError("task_id is required", REASON_VALIDATION_ERROR)
    plan_doc = validate_workspace_relative_path(plan_doc)
    title = (title or task_id).strip()
    if not title:
        raise SplitOperationError("resolved title is empty", REASON_VALIDATION_ERROR)
    if not phase:
        raise SplitOperationError("phase is required", REASON_VALIDATION_ERROR)
    if not priority:
        raise SplitOperationError("priority is required", REASON_VALIDATION_ERROR)
    return (
        operation_id,
        workspace_id,
        source_event_id,
        task_id,
        plan_doc,
        title,
        phase,
        priority,
    )


def apply_issue_materialize_files(
    *,
    workspace_path: str | Path,
    harness_root: str | Path,
    task_id: str,
    plan_doc: str,
    title: str | None,
    phase: str,
    priority: str,
    operation_id: str,
    workspace_id: str,
    source_event_id: str,
    now: str | None = None,
    _lock_timeout: float = 30.0,
    _lock: ChecklistLock | None = None,
) -> IssueMaterializeFilesOperationResult:
    """Apply the file half of an issue.materialize split operation.

    Binds the accepted triage event as the operation source and writes the
    C2 envelope into ``mvp-checklist.json`` atomically under the shared
    per-checklist lock. Idempotent when the exact envelope is already present.
    """
    (
        operation_id,
        workspace_id,
        source_event_id,
        task_id,
        plan_doc,
        title,
        phase,
        priority,
    ) = _validate_issue_materialize_files_inputs(
        workspace_id=workspace_id,
        operation_id=operation_id,
        source_event_id=source_event_id,
        task_id=task_id,
        plan_doc=plan_doc,
        title=title,
        phase=phase,
        priority=priority,
    )
    workspace_path = Path(workspace_path)
    harness_root = Path(harness_root)
    checklist_path = harness_root / "mvp-checklist.json"
    plan_abs = workspace_path / plan_doc
    if not plan_abs.is_file():
        raise SplitOperationError(
            f"plan_doc does not exist: {plan_abs}",
            REASON_FILES_NOT_DEPLOYED,
        )
    plan_sha256 = compute_plan_sha256(plan_abs)
    files_applied_at = now or _utc_now()

    input_fingerprint = build_issue_materialize_input_fingerprint(
        workspace_id=workspace_id,
        task_id=task_id,
        source_id=source_event_id,
        plan_doc=plan_doc,
        plan_sha256=plan_sha256,
        title=title,
        phase=phase,
        priority=priority,
    )
    before_fingerprint = compute_task_item_fingerprint(item=None, task_id=task_id)
    envelope = build_issue_materialize_envelope(
        operation_id=operation_id,
        workspace_id=workspace_id,
        task_id=task_id,
        source_id=source_event_id,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint="",  # filled below
        files_applied_at=files_applied_at,
    )
    after_item = _build_checklist_item(
        task_id=task_id,
        title=title,
        plan_doc=plan_doc,
        priority=priority,
        phase=phase,
        now=files_applied_at,
        envelope=envelope,
    )
    after_fingerprint = compute_task_item_fingerprint(item=after_item, task_id=task_id)
    envelope = build_issue_materialize_envelope(
        operation_id=operation_id,
        workspace_id=workspace_id,
        task_id=task_id,
        source_id=source_event_id,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
        files_applied_at=files_applied_at,
    )
    after_item["split_operation"] = envelope

    lock = _lock or ChecklistLock(checklist_path, timeout=_lock_timeout)
    with lock:
        checklist = _read_checklist(checklist_path)
        existing = _find_checklist_item(checklist, task_id)

        if existing is not None:
            existing_envelope = existing.get("split_operation")
            if (
                isinstance(existing_envelope, dict)
                and existing_envelope.get("operation_id") == operation_id
            ):
                if (
                    existing_envelope.get("source_kind")
                    != SOURCE_KIND_ISSUE_TRIAGED_EVENT
                    or existing_envelope.get("source_id") != source_event_id
                ):
                    raise SplitOperationError(
                        f"task {task_id} has operation {operation_id} with a different source",
                        REASON_OPERATION_CONFLICT,
                    )
                _verify_issue_materialize_envelope_shape(
                    envelope=existing_envelope,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    source_event_id=source_event_id,
                    operation_id=operation_id,
                    input_fingerprint=input_fingerprint,
                    before_fingerprint=before_fingerprint,
                    after_fingerprint=after_fingerprint,
                )
                current_fingerprint = compute_task_item_fingerprint(
                    item=existing, task_id=task_id
                )
                if current_fingerprint != after_fingerprint:
                    raise SplitOperationError(
                        f"task {task_id} has operation {operation_id} but current projection has drifted",
                        REASON_FINGERPRINT_DRIFT,
                    )
                # Idempotent success: do not rewrite the file.
                return IssueMaterializeFilesOperationResult(
                    workspace_id=workspace_id,
                    workspace_path=str(workspace_path),
                    harness_root=str(harness_root),
                    task_id=task_id,
                    plan_doc=plan_doc,
                    operation_id=operation_id,
                    operation_kind=OPERATION_KIND_ISSUE_MATERIALIZE,
                    contract_version=CONTRACT_VERSION,
                    source_event_id=source_event_id,
                    input_fingerprint=input_fingerprint,
                    before_fingerprint=before_fingerprint,
                    after_fingerprint=after_fingerprint,
                    files_applied_at=existing_envelope["files_applied_at"],
                    checklist_changed=False,
                )
            # Existing item is either unbound or bound to another operation/source.
            raise SplitOperationError(
                f"task {task_id} already exists in checklist with a different operation",
                REASON_OPERATION_CONFLICT,
            )

        # Absent task: append the new item atomically.
        items = checklist.setdefault("items", [])
        if not isinstance(items, list):
            raise SplitOperationError(
                "mvp-checklist.json items must be a list",
                REASON_VALIDATION_ERROR,
            )
        items.append(after_item)
        checklist["updated_at"] = files_applied_at.split("T")[0]
        _write_checklist(checklist_path, checklist)

    return IssueMaterializeFilesOperationResult(
        workspace_id=workspace_id,
        workspace_path=str(workspace_path),
        harness_root=str(harness_root),
        task_id=task_id,
        plan_doc=plan_doc,
        operation_id=operation_id,
        operation_kind=OPERATION_KIND_ISSUE_MATERIALIZE,
        contract_version=CONTRACT_VERSION,
        source_event_id=source_event_id,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
        files_applied_at=files_applied_at,
        checklist_changed=True,
    )


# ---------------------------------------------------------------------------
# Record-half helpers
# ---------------------------------------------------------------------------


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


@dataclass(frozen=True)
class TaskCreateRecordResult:
    workspace: Workspace
    task: dict[str, Any]
    event: dict[str, Any]
    event_created: bool
    operation: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace.to_dict(),
            "task": self.task,
            "event": self.event,
            "event_created": self.event_created,
            "operation": self.operation,
        }


def _validate_record_inputs(
    *,
    workspace_id: str,
    operation_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
    task_id: str,
    plan_doc: str,
) -> tuple[str, str, str, str, str, str, str]:
    operation_id = validate_uuid(operation_id)
    input_fingerprint = validate_sha256(input_fingerprint)
    before_fingerprint = validate_sha256(before_fingerprint)
    after_fingerprint = validate_sha256(after_fingerprint)
    if not workspace_id:
        raise SplitOperationError("workspace_id is required", REASON_VALIDATION_ERROR)
    if not task_id:
        raise SplitOperationError("task_id is required", REASON_VALIDATION_ERROR)
    plan_doc = validate_workspace_relative_path(plan_doc)
    return (
        operation_id,
        workspace_id,
        input_fingerprint,
        before_fingerprint,
        after_fingerprint,
        task_id,
        plan_doc,
    )


def _load_deployed_envelope(
    *,
    workspace: Workspace,
    task_id: str,
    operation_id: str,
) -> dict[str, Any]:
    checklist_path = Path(workspace.harness_root) / "mvp-checklist.json"
    if not checklist_path.is_file():
        raise SplitOperationError(
            f"mvp-checklist.json not deployed at {workspace.harness_root}",
            REASON_FILES_NOT_DEPLOYED,
        )
    try:
        checklist = json.loads(checklist_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SplitOperationError(
            f"deployed mvp-checklist.json cannot be read: {exc}",
            REASON_FILES_NOT_DEPLOYED,
        ) from exc
    items = checklist.get("items") if isinstance(checklist.get("items"), list) else None
    item = None
    if items:
        for candidate in items:
            if isinstance(candidate, dict) and candidate.get("id") == task_id:
                item = candidate
                break
    if item is None:
        raise SplitOperationError(
            f"task {task_id} not found in deployed checklist",
            REASON_FILES_NOT_DEPLOYED,
        )
    envelope = item.get("split_operation")
    if not isinstance(envelope, dict):
        raise SplitOperationError(
            f"task {task_id} has no split_operation envelope",
            REASON_FILES_NOT_DEPLOYED,
        )
    if envelope.get("operation_id") != operation_id:
        raise SplitOperationError(
            f"task {task_id} envelope has operation {envelope.get('operation_id')!r}, expected {operation_id}",
            REASON_OPERATION_CONFLICT,
        )
    return item


_ENVELOPE_REQUIRED_KEYS = frozenset({
    "contract_version",
    "operation_id",
    "operation_kind",
    "workspace_id",
    "target_kind",
    "target_id",
    "source_kind",
    "source_id",
    "input_fingerprint",
    "before_fingerprint",
    "after_fingerprint",
    "files_applied_at",
})


def _verify_envelope_shape(
    *,
    envelope: dict[str, Any],
    workspace_id: str,
    task_id: str,
    operation_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
) -> None:
    """Fail closed if the deployed envelope is not the exact expected shape."""
    if set(envelope.keys()) != _ENVELOPE_REQUIRED_KEYS:
        raise SplitOperationError(
            "deployed envelope has unexpected keys",
            REASON_FINGERPRINT_DRIFT,
        )
    checks: list[tuple[Any, Any, str]] = [
        (envelope["contract_version"], CONTRACT_VERSION, "contract_version"),
        (envelope["operation_id"], operation_id, "operation_id"),
        (envelope["operation_kind"], OPERATION_KIND_TASK_CREATE, "operation_kind"),
        (envelope["workspace_id"], workspace_id, "workspace_id"),
        (envelope["target_kind"], TARGET_KIND_CHECKLIST_TASK, "target_kind"),
        (envelope["target_id"], task_id, "target_id"),
        (envelope["source_kind"], None, "source_kind"),
        (envelope["source_id"], None, "source_id"),
        (envelope["input_fingerprint"], input_fingerprint, "input_fingerprint"),
        (envelope["before_fingerprint"], before_fingerprint, "before_fingerprint"),
        (envelope["after_fingerprint"], after_fingerprint, "after_fingerprint"),
    ]
    for actual, expected, field in checks:
        if actual != expected:
            raise SplitOperationError(
                f"deployed envelope field {field} mismatch: {actual!r} != {expected!r}",
                REASON_FINGERPRINT_DRIFT,
            )

    validate_files_applied_at(envelope["files_applied_at"])


def _verify_envelope_fingerprints(
    *,
    item: dict[str, Any],
    envelope: dict[str, Any],
    workspace_id: str,
    task_id: str,
    plan_doc: str,
    plan_sha256: str,
    record_title: str | None,
    record_phase: str | None,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
) -> tuple[str, str, str]:
    """Derive deployed values and compare against supplied/envelope fingerprints.

    Returns the deployed title, phase, and priority.
    """
    title = item.get("title")
    phase = item.get("phase")
    priority = item.get("priority")
    if not isinstance(title, str) or not title:
        raise SplitOperationError(
            "deployed checklist item has no title",
            REASON_FINGERPRINT_DRIFT,
        )
    if not isinstance(phase, str) or not phase:
        raise SplitOperationError(
            "deployed checklist item has no phase",
            REASON_FINGERPRINT_DRIFT,
        )
    if not isinstance(priority, str) or not priority:
        raise SplitOperationError(
            "deployed checklist item has no priority",
            REASON_FINGERPRINT_DRIFT,
        )
    if record_title is not None and record_title != title:
        raise SplitOperationError(
            f"record title {record_title!r} does not match deployed title {title!r}",
            REASON_FINGERPRINT_DRIFT,
        )
    if record_phase is not None and record_phase != phase:
        raise SplitOperationError(
            f"record phase {record_phase!r} does not match deployed phase {phase!r}",
            REASON_FINGERPRINT_DRIFT,
        )

    expected_input = build_task_create_input_fingerprint(
        workspace_id=workspace_id,
        task_id=task_id,
        plan_doc=plan_doc,
        plan_sha256=plan_sha256,
        title=title,
        phase=phase,
        priority=priority,
    )
    expected_before = compute_task_item_fingerprint(item=None, task_id=task_id)
    expected_after = compute_task_item_fingerprint(item=item, task_id=task_id)

    if envelope.get("input_fingerprint") != expected_input:
        raise SplitOperationError(
            "deployed input fingerprint does not match recomputed input fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    if envelope.get("before_fingerprint") != expected_before:
        raise SplitOperationError(
            "deployed before fingerprint does not match recomputed absent fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    if envelope.get("after_fingerprint") != expected_after:
        raise SplitOperationError(
            "deployed after fingerprint does not match recomputed after fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    if input_fingerprint != expected_input:
        raise SplitOperationError(
            "supplied input fingerprint does not match deployed input fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    if before_fingerprint != expected_before:
        raise SplitOperationError(
            "supplied before fingerprint does not match deployed before fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    if after_fingerprint != expected_after:
        raise SplitOperationError(
            "supplied after fingerprint does not match deployed after fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    return title, phase, priority


def _verify_issue_materialize_envelope_shape(
    *,
    envelope: dict[str, Any],
    workspace_id: str,
    task_id: str,
    source_event_id: str,
    operation_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
) -> None:
    """Fail closed if the deployed C2 envelope is not the exact expected shape."""
    if set(envelope.keys()) != _ENVELOPE_REQUIRED_KEYS:
        raise SplitOperationError(
            "deployed envelope has unexpected keys",
            REASON_FINGERPRINT_DRIFT,
        )
    checks: list[tuple[Any, Any, str]] = [
        (envelope["contract_version"], CONTRACT_VERSION, "contract_version"),
        (envelope["operation_id"], operation_id, "operation_id"),
        (
            envelope["operation_kind"],
            OPERATION_KIND_ISSUE_MATERIALIZE,
            "operation_kind",
        ),
        (envelope["workspace_id"], workspace_id, "workspace_id"),
        (envelope["target_kind"], TARGET_KIND_CHECKLIST_TASK, "target_kind"),
        (envelope["target_id"], task_id, "target_id"),
        (
            envelope["source_kind"],
            SOURCE_KIND_ISSUE_TRIAGED_EVENT,
            "source_kind",
        ),
        (envelope["source_id"], source_event_id, "source_id"),
        (envelope["input_fingerprint"], input_fingerprint, "input_fingerprint"),
        (envelope["before_fingerprint"], before_fingerprint, "before_fingerprint"),
        (envelope["after_fingerprint"], after_fingerprint, "after_fingerprint"),
    ]
    for actual, expected, field in checks:
        if actual != expected:
            raise SplitOperationError(
                f"deployed envelope field {field} mismatch: {actual!r} != {expected!r}",
                REASON_FINGERPRINT_DRIFT,
            )

    validate_files_applied_at(envelope["files_applied_at"])


def _verify_issue_materialize_envelope_fingerprints(
    *,
    item: dict[str, Any],
    envelope: dict[str, Any],
    workspace_id: str,
    task_id: str,
    source_event_id: str,
    plan_doc: str,
    plan_sha256: str,
    record_title: str | None,
    record_phase: str | None,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
) -> tuple[str, str, str]:
    """Derive deployed values and compare against supplied/envelope fingerprints."""
    title = item.get("title")
    phase = item.get("phase")
    priority = item.get("priority")
    if not isinstance(title, str) or not title:
        raise SplitOperationError(
            "deployed checklist item has no title",
            REASON_FINGERPRINT_DRIFT,
        )
    if not isinstance(phase, str) or not phase:
        raise SplitOperationError(
            "deployed checklist item has no phase",
            REASON_FINGERPRINT_DRIFT,
        )
    if not isinstance(priority, str) or not priority:
        raise SplitOperationError(
            "deployed checklist item has no priority",
            REASON_FINGERPRINT_DRIFT,
        )
    if record_title is not None and record_title != title:
        raise SplitOperationError(
            f"record title {record_title!r} does not match deployed title {title!r}",
            REASON_FINGERPRINT_DRIFT,
        )
    if record_phase is not None and record_phase != phase:
        raise SplitOperationError(
            f"record phase {record_phase!r} does not match deployed phase {phase!r}",
            REASON_FINGERPRINT_DRIFT,
        )

    expected_input = build_issue_materialize_input_fingerprint(
        workspace_id=workspace_id,
        task_id=task_id,
        source_id=source_event_id,
        plan_doc=plan_doc,
        plan_sha256=plan_sha256,
        title=title,
        phase=phase,
        priority=priority,
    )
    expected_before = compute_task_item_fingerprint(item=None, task_id=task_id)
    expected_after = compute_task_item_fingerprint(item=item, task_id=task_id)

    if envelope.get("input_fingerprint") != expected_input:
        raise SplitOperationError(
            "deployed input fingerprint does not match recomputed input fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    if envelope.get("before_fingerprint") != expected_before:
        raise SplitOperationError(
            "deployed before fingerprint does not match recomputed absent fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    if envelope.get("after_fingerprint") != expected_after:
        raise SplitOperationError(
            "deployed after fingerprint does not match recomputed after fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    if input_fingerprint != expected_input:
        raise SplitOperationError(
            "supplied input fingerprint does not match deployed input fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    if before_fingerprint != expected_before:
        raise SplitOperationError(
            "supplied before fingerprint does not match deployed before fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    if after_fingerprint != expected_after:
        raise SplitOperationError(
            "supplied after fingerprint does not match deployed after fingerprint",
            REASON_FINGERPRINT_DRIFT,
        )
    return title, phase, priority


# ---------------------------------------------------------------------------
# Read-only diagnostic wrappers (no mutation, no lock, no write)
# ---------------------------------------------------------------------------


def resolve_workspace_path(workspace: Workspace, path: str | Path) -> Path:
    """Public read-only wrapper for resolving a workspace-relative path."""
    return _resolve_workspace_path(workspace, path)


def load_deployed_envelope_readonly(
    *,
    workspace: Workspace,
    task_id: str,
    operation_id: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Read-only variant of ``_load_deployed_envelope``.

    Returns ``(item, errors)`` instead of raising.  ``item`` is ``None`` when
    the checklist or task/envelope is missing or the envelope belongs to a
    different operation.
    """
    try:
        item = _load_deployed_envelope(
            workspace=workspace,
            task_id=task_id,
            operation_id=operation_id,
        )
        return item, []
    except SplitOperationError as exc:
        return None, [str(exc)]


def load_deployed_item_readonly(
    *,
    workspace: Workspace,
    task_id: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Read-only loader for a checklist item by task id alone.

    Returns ``(item, errors)`` instead of raising.  ``item`` is ``None`` when
    the checklist is missing, unreadable, or the task id is not present.  This
    helper intentionally does *not* validate the bound ``split_operation``
    operation id, so callers can distinguish missing envelopes from drifted
    identity.
    """
    checklist_path = Path(workspace.harness_root) / "mvp-checklist.json"
    if not checklist_path.is_file():
        return None, [f"mvp-checklist.json not deployed at {workspace.harness_root}"]
    try:
        checklist = json.loads(checklist_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, [f"deployed mvp-checklist.json cannot be read: {exc}"]
    items = checklist.get("items") if isinstance(checklist.get("items"), list) else None
    if not items:
        return None, ["deployed mvp-checklist.json has no items"]
    for candidate in items:
        if isinstance(candidate, dict) and candidate.get("id") == task_id:
            return candidate, []
    return None, [f"task {task_id} not found in deployed checklist"]


def verify_task_create_envelope_readonly(
    *,
    envelope: dict[str, Any],
    workspace_id: str,
    task_id: str,
    operation_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
) -> list[str]:
    """Read-only shape verifier returning a list of mismatch descriptions."""
    try:
        _verify_envelope_shape(
            envelope=envelope,
            workspace_id=workspace_id,
            task_id=task_id,
            operation_id=operation_id,
            input_fingerprint=input_fingerprint,
            before_fingerprint=before_fingerprint,
            after_fingerprint=after_fingerprint,
        )
        return []
    except SplitOperationError as exc:
        return [str(exc)]


def verify_task_create_fingerprints_readonly(
    *,
    item: dict[str, Any],
    envelope: dict[str, Any],
    workspace_id: str,
    task_id: str,
    plan_doc: str,
    plan_sha256: str,
    record_title: str | None = None,
    record_phase: str | None = None,
) -> tuple[list[str], tuple[str, str, str] | None]:
    """Read-only fingerprint verifier returning ``(errors, deployed_triple)``.

    ``deployed_triple`` is ``(title, phase, priority)`` when all fingerprints
    match; otherwise it is ``None``.
    """
    try:
        triple = _verify_envelope_fingerprints(
            item=item,
            envelope=envelope,
            workspace_id=workspace_id,
            task_id=task_id,
            plan_doc=plan_doc,
            plan_sha256=plan_sha256,
            record_title=record_title,
            record_phase=record_phase,
            input_fingerprint=envelope.get("input_fingerprint", ""),
            before_fingerprint=envelope.get("before_fingerprint", ""),
            after_fingerprint=envelope.get("after_fingerprint", ""),
        )
        return [], triple
    except SplitOperationError as exc:
        return [str(exc)], None


def verify_issue_materialize_envelope_readonly(
    *,
    envelope: dict[str, Any],
    workspace_id: str,
    task_id: str,
    source_event_id: str,
    operation_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
) -> list[str]:
    """Read-only C2 shape verifier returning a list of mismatch descriptions."""
    try:
        _verify_issue_materialize_envelope_shape(
            envelope=envelope,
            workspace_id=workspace_id,
            task_id=task_id,
            source_event_id=source_event_id,
            operation_id=operation_id,
            input_fingerprint=input_fingerprint,
            before_fingerprint=before_fingerprint,
            after_fingerprint=after_fingerprint,
        )
        return []
    except SplitOperationError as exc:
        return [str(exc)]


def verify_issue_materialize_fingerprints_readonly(
    *,
    item: dict[str, Any],
    envelope: dict[str, Any],
    workspace_id: str,
    task_id: str,
    source_event_id: str,
    plan_doc: str,
    plan_sha256: str,
    record_title: str | None = None,
    record_phase: str | None = None,
) -> tuple[list[str], tuple[str, str, str] | None]:
    """Read-only C2 fingerprint verifier returning ``(errors, deployed_triple)``."""
    try:
        triple = _verify_issue_materialize_envelope_fingerprints(
            item=item,
            envelope=envelope,
            workspace_id=workspace_id,
            task_id=task_id,
            source_event_id=source_event_id,
            plan_doc=plan_doc,
            plan_sha256=plan_sha256,
            record_title=record_title,
            record_phase=record_phase,
            input_fingerprint=envelope.get("input_fingerprint", ""),
            before_fingerprint=envelope.get("before_fingerprint", ""),
            after_fingerprint=envelope.get("after_fingerprint", ""),
        )
        return [], triple
    except SplitOperationError as exc:
        return [str(exc)], None


def _check_issue_materialize_ledger_idempotency(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    workspace_id: str,
    task_id: str,
    source_event_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
    envelope: dict[str, Any],
    expected_task_payload: dict[str, Any],
    expected_ready_payload: dict[str, Any],
    ready_key: str,
    materialized_key: str,
    actor: str,
    target: str | None,
    effective_platform: str | None,
    effective_destination: str | None,
) -> dict[str, Any] | None:
    """Return the existing ledger row if the C2 operation is already applied exactly.

    Returns ``None`` if no ledger row exists. Raises ``SplitOperationError`` on
    any drift or missing promised DB artifact.
    """
    # The issue.materialized payload is the task payload plus the linked
    # plan.ready event id. The ready id is only known after the plan.ready
    # event is created, so the helper injects it from the persisted row.
    existing = get_split_operation(conn, operation_id)
    if existing is None:
        return None

    so = existing
    checks = [
        (so.contract_version == CONTRACT_VERSION, "contract_version"),
        (
            so.operation_kind == OPERATION_KIND_ISSUE_MATERIALIZE,
            "operation_kind",
        ),
        (so.workspace_id == workspace_id, "workspace_id"),
        (so.target_kind == TARGET_KIND_CHECKLIST_TASK, "target_kind"),
        (so.target_id == task_id, "target_id"),
        (
            so.source_kind == SOURCE_KIND_ISSUE_TRIAGED_EVENT,
            "source_kind",
        ),
        (so.source_id == source_event_id, "source_id"),
        (so.input_fingerprint == input_fingerprint, "input_fingerprint"),
        (so.before_fingerprint == before_fingerprint, "before_fingerprint"),
        (so.after_fingerprint == after_fingerprint, "after_fingerprint"),
        (so.status == STATUS_RECORD_APPLIED, "status"),
    ]
    for ok, field in checks:
        if not ok:
            raise SplitOperationError(
                f"ledger row for {operation_id} differs in {field}",
                REASON_OPERATION_CONFLICT,
            )

    for key in ("input_fingerprint", "before_fingerprint", "after_fingerprint"):
        if envelope.get(key) != getattr(so, key):
            raise SplitOperationError(
                f"deployed envelope {key} differs from ledger row",
                REASON_FINGERPRINT_DRIFT,
            )

    if so.record_event_id is None:
        raise SplitOperationError(
            f"ledger row for {operation_id} has no record_event_id",
            REASON_OPERATION_CONFLICT,
        )
    materialized_event = conn.execute(
        "SELECT * FROM events WHERE id = ?", (so.record_event_id,)
    ).fetchone()
    if materialized_event is None:
        raise SplitOperationError(
            f"ledger record_event_id {so.record_event_id} does not exist",
            REASON_OPERATION_CONFLICT,
        )
    if (
        materialized_event["event_type"] != "issue.materialized"
        or materialized_event["workspace_id"] != workspace_id
        or materialized_event["task_id"] != task_id
        or materialized_event["actor"] != actor
        or materialized_event["target"] != task_id
        or materialized_event["idempotency_key"] != materialized_key
    ):
        raise SplitOperationError(
            "issue.materialized event record metadata does not match expected intent",
            REASON_OPERATION_CONFLICT,
        )
    materialized_payload = json.loads(materialized_event["payload_json"])

    ready_event = conn.execute(
        "SELECT rowid, * FROM events WHERE id = ?",
        (materialized_payload.get("plan_ready_event_id"),),
    ).fetchone()
    if ready_event is None:
        raise SplitOperationError(
            "linked plan.ready event does not exist",
            REASON_OPERATION_CONFLICT,
        )
    if (
        ready_event["event_type"] != "plan.ready"
        or ready_event["workspace_id"] != workspace_id
        or ready_event["task_id"] != task_id
        or ready_event["actor"] != actor
        or ready_event["target"] != target
        or ready_event["idempotency_key"] != ready_key
    ):
        raise SplitOperationError(
            "plan.ready event record metadata does not match expected intent",
            REASON_OPERATION_CONFLICT,
        )
    ready_payload = json.loads(ready_event["payload_json"])
    # Independently derive the provenance link from the historical event store
    # cutoff (the bound ready event's rowid).  Do not trust the stored value.
    expected_supersedes_plan_ready_event_id = _prior_plan_ready_id_before_rowid(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        before_rowid=ready_event["rowid"],
    )
    expected_ready_payload = dict(expected_ready_payload)
    expected_ready_payload["supersedes_plan_ready_event_id"] = expected_supersedes_plan_ready_event_id
    # Treat a null value and an absent key as equivalent for the provenance link.
    normalized_ready_payload = dict(ready_payload)
    if "supersedes_plan_ready_event_id" not in normalized_ready_payload:
        normalized_ready_payload["supersedes_plan_ready_event_id"] = None
    if normalized_ready_payload != expected_ready_payload:
        raise SplitOperationError(
            "plan.ready event payload differs from expected intent",
            REASON_OPERATION_CONFLICT,
        )

    expected_materialized_payload = {
        **expected_task_payload,
        "plan_ready_event_id": ready_event["id"],
    }
    if materialized_payload != expected_materialized_payload:
        raise SplitOperationError(
            "issue.materialized event payload differs from expected intent",
            REASON_OPERATION_CONFLICT,
        )

    task = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()
    if task is None:
        raise SplitOperationError(
            f"task mirror for {workspace_id}/{task_id} does not exist",
            REASON_OPERATION_CONFLICT,
        )
    if (
        task["phase"] != expected_task_payload["phase"]
        or task["owner"]
        != (expected_task_payload.get("owner") if "owner" in expected_task_payload else None)
        or task["branch"]
        != (expected_task_payload.get("branch") if "branch" in expected_task_payload else None)
        or task["last_event_id"] != so.record_event_id
        or task["pr"] is not None
    ):
        raise SplitOperationError(
            "task mirror record columns differ from expected intent",
            REASON_OPERATION_CONFLICT,
        )
    task_payload = json.loads(task["payload_json"])
    if task_payload != expected_task_payload:
        raise SplitOperationError(
            "task mirror payload differs from expected intent",
            REASON_OPERATION_CONFLICT,
        )

    # Delivery immutable intent: event id, platform, destination, message_key,
    # rendered payload. Operational fields may legitimately advance.
    if effective_platform and effective_destination:
        from .policy import render_event

        rendered = render_event(
            conn,
            so.record_event_id,
            platform=effective_platform,
            destination=effective_destination,
        )
        if not rendered.supported:
            raise SplitOperationError(
                f"delivery expected but event {so.record_event_id} no longer renders: {rendered.reason}",
                REASON_OPERATION_CONFLICT,
            )
        if rendered.payload is None or rendered.message_key is None:
            raise SplitOperationError(
                "rendered delivery intent is incomplete",
                REASON_OPERATION_CONFLICT,
            )
        delivery_row = conn.execute(
            "SELECT * FROM deliveries WHERE message_key = ?",
            (rendered.message_key,),
        ).fetchone()
        if delivery_row is None:
            raise SplitOperationError(
                "promised delivery intent is missing",
                REASON_OPERATION_CONFLICT,
            )
        if (
            delivery_row["event_id"] != so.record_event_id
            or delivery_row["platform"] != effective_platform
            or delivery_row["destination"] != effective_destination
        ):
            raise SplitOperationError(
                "delivery row metadata differs from expected intent",
                REASON_OPERATION_CONFLICT,
            )
        delivery_payload = json.loads(delivery_row["payload_json"])
        if delivery_payload != rendered.payload:
            raise SplitOperationError(
                "delivery payload differs from expected intent",
                REASON_OPERATION_CONFLICT,
            )
    else:
        # No effective delivery configured: ensure no unexpected delivery exists
        # for this event. (A platform/destination that appears later is a conflict.)
        unexpected = conn.execute(
            "SELECT COUNT(*) FROM deliveries WHERE event_id = ?",
            (so.record_event_id,),
        ).fetchone()[0]
        if unexpected:
            raise SplitOperationError(
                "unexpected delivery exists for issue.materialized event",
                REASON_OPERATION_CONFLICT,
            )

    return row_to_dict(
        conn.execute(
            "SELECT * FROM split_operations WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
    )


def _check_ledger_idempotency(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    workspace_id: str,
    task_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
    envelope: dict[str, Any],
    expected_task_payload: dict[str, Any],
    expected_event_payload: dict[str, Any],
    ready_key: str,
    actor: str,
    target: str | None,
) -> dict[str, Any] | None:
    """Return the existing ledger row if the operation is already applied exactly.

    Returns ``None`` if no ledger row exists. Raises ``SplitOperationError`` on
    any drift or missing promised DB artifact, including any change to the
    record-only intent (owner/branch/actor/target/payload/idempotency key).
    """
    existing = get_split_operation(conn, operation_id)
    if existing is None:
        return None

    so = existing
    checks = [
        (so.contract_version == CONTRACT_VERSION, "contract_version"),
        (so.operation_kind == OPERATION_KIND_TASK_CREATE, "operation_kind"),
        (so.workspace_id == workspace_id, "workspace_id"),
        (so.target_kind == TARGET_KIND_CHECKLIST_TASK, "target_kind"),
        (so.target_id == task_id, "target_id"),
        (so.source_kind is None, "source_kind"),
        (so.source_id is None, "source_id"),
        (so.input_fingerprint == input_fingerprint, "input_fingerprint"),
        (so.before_fingerprint == before_fingerprint, "before_fingerprint"),
        (so.after_fingerprint == after_fingerprint, "after_fingerprint"),
        (so.status == STATUS_RECORD_APPLIED, "status"),
    ]
    for ok, field in checks:
        if not ok:
            raise SplitOperationError(
                f"ledger row for {operation_id} differs in {field}",
                REASON_OPERATION_CONFLICT,
            )

    # Envelope must still match exactly.
    for key in ("input_fingerprint", "before_fingerprint", "after_fingerprint"):
        if envelope.get(key) != getattr(so, key):
            raise SplitOperationError(
                f"deployed envelope {key} differs from ledger row",
                REASON_FINGERPRINT_DRIFT,
            )

    # The promised DB artifacts must exist and carry the exact record-only intent.
    if so.record_event_id is None:
        raise SplitOperationError(
            f"ledger row for {operation_id} has no record_event_id",
            REASON_OPERATION_CONFLICT,
        )
    event = conn.execute(
        "SELECT rowid, * FROM events WHERE id = ?", (so.record_event_id,)
    ).fetchone()
    if event is None:
        raise SplitOperationError(
            f"ledger record_event_id {so.record_event_id} does not exist",
            REASON_OPERATION_CONFLICT,
        )
    if (
        event["event_type"] != "plan.ready"
        or event["workspace_id"] != workspace_id
        or event["task_id"] != task_id
        or event["actor"] != actor
        or event["target"] != target
        or event["idempotency_key"] != ready_key
    ):
        raise SplitOperationError(
            "plan.ready event record metadata does not match the expected record intent",
            REASON_OPERATION_CONFLICT,
        )
    event_payload = json.loads(event["payload_json"])
    # The provenance link to the prior ready event reflects the event store
    # state at creation time; a later unrelated plan.ready must not break an
    # otherwise exact idempotent retry.  Derive the expected link independently
    # from the bound ready event's rowid and compare; do not copy the stored value.
    expected_supersedes_plan_ready_event_id = _prior_plan_ready_id_before_rowid(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        before_rowid=event["rowid"],
    )
    expected_event_payload = dict(expected_event_payload)
    expected_event_payload["supersedes_plan_ready_event_id"] = expected_supersedes_plan_ready_event_id
    # Treat a null value and an absent key as equivalent for the provenance link.
    normalized_event_payload = dict(event_payload)
    if "supersedes_plan_ready_event_id" not in normalized_event_payload:
        normalized_event_payload["supersedes_plan_ready_event_id"] = None
    if normalized_event_payload != expected_event_payload:
        raise SplitOperationError(
            "plan.ready event payload differs from expected record intent",
            REASON_OPERATION_CONFLICT,
        )

    task = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()
    if task is None:
        raise SplitOperationError(
            f"task mirror for {workspace_id}/{task_id} does not exist",
            REASON_OPERATION_CONFLICT,
        )
    if (
        task["phase"] != expected_task_payload["phase"]
        or task["owner"] != (expected_task_payload.get("owner") if "owner" in expected_task_payload else None)
        or task["branch"] != (expected_task_payload.get("branch") if "branch" in expected_task_payload else None)
        or task["last_event_id"] != so.record_event_id
        or task["pr"] is not None
    ):
        raise SplitOperationError(
            "task mirror record columns differ from expected record intent",
            REASON_OPERATION_CONFLICT,
        )
    task_payload = json.loads(task["payload_json"])
    if task_payload != expected_task_payload:
        raise SplitOperationError(
            "task mirror payload differs from expected record intent",
            REASON_OPERATION_CONFLICT,
        )

    return row_to_dict(conn.execute(
        "SELECT * FROM split_operations WHERE operation_id = ?",
        (operation_id,),
    ).fetchone())


def apply_task_create_record(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    plan_doc: str,
    title: str | None,
    phase: str,
    owner: str | None,
    branch: str | None,
    actor: str,
    target: str | None,
    payload: dict[str, Any] | None,
    operation_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
    idempotency_key: str | None = None,
    _inject_after_step: Callable[[str], None] | None = None,
) -> TaskCreateRecordResult:
    """Apply the record half of a task.create split operation inside one transaction.

    Verifies the deployed checklist envelope and fingerprints before any DB
    write. Uses a single SAVEPOINT so an injected failure after any DB step
    rolls back all effects.
    """
    (
        operation_id,
        workspace_id,
        input_fingerprint,
        before_fingerprint,
        after_fingerprint,
        task_id,
        plan_doc,
    ) = _validate_record_inputs(
        workspace_id=workspace_id,
        operation_id=operation_id,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
        task_id=task_id,
        plan_doc=plan_doc,
    )

    workspace = conn.execute("SELECT * FROM workspaces WHERE id = ?", (workspace_id,)).fetchone()
    if workspace is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    workspace = Workspace.from_row(workspace)

    plan_abs = _resolve_workspace_path(workspace, plan_doc)
    if not plan_abs.is_file():
        raise SplitOperationError(
            f"plan_doc does not exist at {plan_abs}",
            REASON_FILES_NOT_DEPLOYED,
        )
    plan_sha256 = compute_plan_sha256(plan_abs)

    item = _load_deployed_envelope(
        workspace=workspace,
        task_id=task_id,
        operation_id=operation_id,
    )
    envelope = item["split_operation"]

    _verify_envelope_shape(
        envelope=envelope,
        workspace_id=workspace_id,
        task_id=task_id,
        operation_id=operation_id,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
    )

    # A different operation already bound to this checklist target is a hard
    # conflict; we must not create a second ledger row for the same target.
    for existing_op in list_split_operations(
        conn,
        workspace_id=workspace_id,
        target_kind=TARGET_KIND_CHECKLIST_TASK,
        target_id=task_id,
    ):
        if existing_op.operation_id != operation_id:
            raise SplitOperationError(
                f"task {task_id} already has operation {existing_op.operation_id} in the ledger",
                REASON_OPERATION_CONFLICT,
            )

    deployed_title, deployed_phase, deployed_priority = _verify_envelope_fingerprints(
        item=item,
        envelope=envelope,
        workspace_id=workspace_id,
        task_id=task_id,
        plan_doc=plan_doc,
        plan_sha256=plan_sha256,
        record_title=title,
        record_phase=phase,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
    )

    # Build the exact record-only intent that will be persisted. The ledger
    # idempotency check compares every field, so the same operation with any
    # changed owner/branch/actor/target/payload/idempotency-key is a conflict.
    operation_meta = {
        "contract_version": CONTRACT_VERSION,
        "operation_id": operation_id,
        "operation_kind": OPERATION_KIND_TASK_CREATE,
        "input_fingerprint": input_fingerprint,
        "before_fingerprint": before_fingerprint,
        "after_fingerprint": after_fingerprint,
    }
    extra_payload = dict(payload or {})
    task_payload = {
        **extra_payload,
        "task_id": task_id,
        "title": deployed_title,
        "plan_doc": plan_doc,
        "absolute_plan_doc": str(plan_abs),
        "phase": deployed_phase,
        "status": deployed_phase,
        "priority": deployed_priority,
        "split_operation": operation_meta,
    }
    if branch:
        task_payload["branch"] = branch
    if owner:
        task_payload["owner"] = owner

    plan_content_hash = plan_sha256[:16]
    supersedes_plan_ready_event_id = _latest_prior_plan_ready_id(
        conn, workspace_id=workspace_id, task_id=task_id, exclude_operation_id=operation_id
    )
    event_payload = {
        **task_payload,
        "workspace_path": workspace.path,
        "current_branch": workspace.base_branch,
        "allocated_branch": branch,
        "status": "ready_for_worker" if deployed_phase in {"ready", "planned"} else deployed_phase,
        "plan_content_hash": plan_content_hash,
        "plan_sha256": plan_sha256,
        "supersedes_plan_ready_event_id": supersedes_plan_ready_event_id,
    }
    ready_key = (
        f"{workspace_id}:{task_id}:plan.ready:{operation_id}:"
        f"{idempotency_key or (plan_content_hash or 'nohash')}"
    )

    existing_ledger = _check_ledger_idempotency(
        conn,
        operation_id=operation_id,
        workspace_id=workspace_id,
        task_id=task_id,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
        envelope=envelope,
        expected_task_payload=task_payload,
        expected_event_payload=event_payload,
        ready_key=ready_key,
        actor=actor,
        target=target,
    )
    if existing_ledger is not None:
        event = conn.execute(
            "SELECT * FROM events WHERE id = ?",
            (existing_ledger["record_event_id"],),
        ).fetchone()
        task = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            (workspace_id, task_id),
        ).fetchone()
        return TaskCreateRecordResult(
            workspace=workspace,
            task=row_to_dict(task),
            event=row_to_dict(event),
            event_created=False,
            operation=existing_ledger,
        )

    now = utc_now()
    conn.execute("SAVEPOINT split_operation_apply")
    record_event_id: str | None = None
    try:
        ledger_row = insert_split_operation(
            conn,
            operation_id=operation_id,
            contract_version=CONTRACT_VERSION,
            operation_kind=OPERATION_KIND_TASK_CREATE,
            workspace_id=workspace_id,
            target_kind=TARGET_KIND_CHECKLIST_TASK,
            target_id=task_id,
            source_kind=None,
            source_id=None,
            input_fingerprint=input_fingerprint,
            before_fingerprint=before_fingerprint,
            after_fingerprint=after_fingerprint,
            status=STATUS_RECORD_APPLIED,
            created_at=now,
            updated_at=now,
        )
        if _inject_after_step:
            _inject_after_step("insert_ledger")

        task_row, _ = upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=deployed_phase,
            owner=owner,
            branch=branch,
            pr=None,
            payload=task_payload,
            commit=False,
        )
        if _inject_after_step:
            _inject_after_step("upsert_mirror_initial")

        event_result = append_event(
            conn,
            workspace_id=workspace_id,
            event_type="plan.ready",
            actor=actor,
            target=target,
            task_id=task_id,
            idempotency_key=ready_key,
            payload=event_payload,
            commit=False,
        )
        if not event_result.created:
            # A pre-existing event with this operation-bound idempotency key
            # already exists without a matching ledger row. Do not repair or
            # link partial state; fail closed and roll back the savepoint.
            raise SplitOperationError(
                f"plan.ready idempotency key {ready_key!r} already exists; "
                "refusing to link a pre-existing event to a new split operation",
                REASON_OPERATION_CONFLICT,
            )
        record_event_id = event_result.row["id"]
        if _inject_after_step:
            _inject_after_step("append_event")

        task_row, _ = upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=deployed_phase,
            owner=owner,
            branch=branch,
            pr=None,
            payload=task_payload,
            last_event_id=record_event_id,
            commit=False,
        )
        if _inject_after_step:
            _inject_after_step("upsert_mirror_final")

        update_split_operation_event(
            conn,
            operation_id=operation_id,
            event_id=record_event_id,
        )
        if _inject_after_step:
            _inject_after_step("link_ledger_event")

        conn.execute("RELEASE SAVEPOINT split_operation_apply")
        conn.commit()
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT split_operation_apply")
        conn.execute("RELEASE SAVEPOINT split_operation_apply")
        raise

    ledger_row = conn.execute(
        "SELECT * FROM split_operations WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()
    return TaskCreateRecordResult(
        workspace=workspace,
        task=row_to_dict(task_row),
        event=row_to_dict(event_result.row),
        event_created=event_result.created,
        operation=row_to_dict(ledger_row),
    )


# ---------------------------------------------------------------------------
# Issue materialize record-half
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IssueMaterializeRecordResult:
    workspace: Workspace
    task: dict[str, Any]
    plan_ready_event: dict[str, Any]
    event: dict[str, Any]
    event_created: bool
    operation: dict[str, Any]
    delivery: dict[str, Any] | None
    delivery_created: bool | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace.to_dict(),
            "task": self.task,
            "plan_ready_event": self.plan_ready_event,
            "event": self.event,
            "event_created": self.event_created,
            "operation": self.operation,
            "delivery": self.delivery,
            "delivery_created": self.delivery_created,
        }


def _validate_issue_materialize_record_inputs(
    *,
    workspace_id: str,
    operation_id: str,
    source_event_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
    task_id: str,
    plan_doc: str,
) -> tuple[str, str, str, str, str, str, str, str]:
    operation_id = validate_uuid(operation_id)
    source_event_id = validate_uuid(source_event_id)
    input_fingerprint = validate_sha256(input_fingerprint)
    before_fingerprint = validate_sha256(before_fingerprint)
    after_fingerprint = validate_sha256(after_fingerprint)
    if not workspace_id:
        raise SplitOperationError("workspace_id is required", REASON_VALIDATION_ERROR)
    if not task_id:
        raise SplitOperationError("task_id is required", REASON_VALIDATION_ERROR)
    plan_doc = validate_workspace_relative_path(plan_doc)
    return (
        operation_id,
        workspace_id,
        source_event_id,
        input_fingerprint,
        before_fingerprint,
        after_fingerprint,
        task_id,
        plan_doc,
    )


def apply_issue_materialize_record(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    source_event_id: str,
    task_id: str,
    plan_doc: str,
    operation_id: str,
    input_fingerprint: str,
    before_fingerprint: str,
    after_fingerprint: str,
    title: str | None = None,
    phase: str | None = None,
    owner: str | None = None,
    branch: str | None = None,
    actor: str = "operator",
    target: str | None = None,
    platform: str | None = None,
    destination: str | None = None,
    _inject_after_step: Callable[[str], None] | None = None,
) -> IssueMaterializeRecordResult:
    """Apply the record half of an issue.materialize split operation in one transaction.

    Verifies the accepted ``issue.triaged`` event, the deployed C2 envelope and
    all fingerprints before any DB write. Commits the ledger, task mirror,
    ``plan.ready``, ``issue.materialized``, mirror link, ledger event link and
    optional delivery together under a single savepoint.
    """
    (
        operation_id,
        workspace_id,
        source_event_id,
        input_fingerprint,
        before_fingerprint,
        after_fingerprint,
        task_id,
        plan_doc,
    ) = _validate_issue_materialize_record_inputs(
        workspace_id=workspace_id,
        operation_id=operation_id,
        source_event_id=source_event_id,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
        task_id=task_id,
        plan_doc=plan_doc,
    )

    workspace_row = conn.execute(
        "SELECT * FROM workspaces WHERE id = ?", (workspace_id,)
    ).fetchone()
    if workspace_row is None:
        raise ValueError(f"unknown workspace: {workspace_id}")
    workspace = Workspace.from_row(workspace_row)

    # Effective platform/destination is part of the immutable record intent.
    effective_platform = platform or workspace.default_bus
    effective_destination = destination or workspace.default_destination

    # Authority: load the accepted issue.triaged row from the DB.
    triage_event = conn.execute(
        "SELECT * FROM events WHERE id = ?", (source_event_id,)
    ).fetchone()
    if triage_event is None:
        raise SplitOperationError(
            f"issue.triaged event not found: {source_event_id}",
            REASON_OPERATION_CONFLICT,
        )
    if triage_event["workspace_id"] != workspace_id:
        raise SplitOperationError(
            f"event {source_event_id} belongs to workspace {triage_event['workspace_id']}, "
            f"not {workspace_id}",
            REASON_OPERATION_CONFLICT,
        )
    if triage_event["event_type"] != "issue.triaged":
        raise SplitOperationError(
            f"event {source_event_id} is {triage_event['event_type']}, not issue.triaged",
            REASON_OPERATION_CONFLICT,
        )
    triage_payload = json.loads(triage_event["payload_json"])
    if triage_payload.get("decision") != "accept":
        raise SplitOperationError(
            f"issue.triaged event {source_event_id} decision is "
            f"{triage_payload.get('decision')!r}; only accept can be materialized",
            REASON_OPERATION_CONFLICT,
        )
    triage_task_id = triage_payload.get("task_id")
    if triage_task_id is None:
        raise SplitOperationError(
            f"issue.triaged event {source_event_id} has no task_id",
            REASON_OPERATION_CONFLICT,
        )
    if task_id != triage_task_id:
        raise SplitOperationError(
            f"deployed task {task_id} does not match triage task {triage_task_id}",
            REASON_OPERATION_CONFLICT,
        )

    # The accept step must have created the DB task mirror.
    accepted_task = conn.execute(
        "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
        (workspace_id, task_id),
    ).fetchone()
    if accepted_task is None:
        raise SplitOperationError(
            f"task mirror not found for {workspace_id}/{task_id}; "
            "run `issue triage --decision accept` first",
            REASON_FILES_NOT_DEPLOYED,
        )

    plan_abs = _resolve_workspace_path(workspace, plan_doc)
    if not plan_abs.is_file():
        raise SplitOperationError(
            f"plan_doc does not exist at {plan_abs}",
            REASON_FILES_NOT_DEPLOYED,
        )
    plan_sha256 = compute_plan_sha256(plan_abs)

    item = _load_deployed_envelope(
        workspace=workspace,
        task_id=task_id,
        operation_id=operation_id,
    )
    envelope = item["split_operation"]

    if (
        envelope.get("source_kind") != SOURCE_KIND_ISSUE_TRIAGED_EVENT
        or envelope.get("source_id") != source_event_id
    ):
        raise SplitOperationError(
            f"deployed envelope source does not match triage event {source_event_id}",
            REASON_OPERATION_CONFLICT,
        )

    _verify_issue_materialize_envelope_shape(
        envelope=envelope,
        workspace_id=workspace_id,
        task_id=task_id,
        source_event_id=source_event_id,
        operation_id=operation_id,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
    )

    # A different operation already bound to this target or source is a hard
    # conflict; do not create a second ledger row.
    for existing_op in list_split_operations(
        conn,
        workspace_id=workspace_id,
        target_kind=TARGET_KIND_CHECKLIST_TASK,
        target_id=task_id,
    ):
        if existing_op.operation_id != operation_id:
            raise SplitOperationError(
                f"task {task_id} already has operation {existing_op.operation_id} in the ledger",
                REASON_OPERATION_CONFLICT,
            )
    for existing_op in list_split_operations(
        conn,
        workspace_id=workspace_id,
        source_kind=SOURCE_KIND_ISSUE_TRIAGED_EVENT,
        source_id=source_event_id,
    ):
        if existing_op.operation_id != operation_id:
            raise SplitOperationError(
                f"triage event {source_event_id} already has operation "
                f"{existing_op.operation_id} in the ledger",
                REASON_OPERATION_CONFLICT,
            )

    (
        deployed_title,
        deployed_phase,
        deployed_priority,
    ) = _verify_issue_materialize_envelope_fingerprints(
        item=item,
        envelope=envelope,
        workspace_id=workspace_id,
        task_id=task_id,
        source_event_id=source_event_id,
        plan_doc=plan_doc,
        plan_sha256=plan_sha256,
        record_title=title,
        record_phase=phase,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
    )

    # Build the exact record intent.
    operation_meta = {
        "contract_version": CONTRACT_VERSION,
        "operation_id": operation_id,
        "operation_kind": OPERATION_KIND_ISSUE_MATERIALIZE,
        "input_fingerprint": input_fingerprint,
        "before_fingerprint": before_fingerprint,
        "after_fingerprint": after_fingerprint,
    }
    task_payload: dict[str, Any] = {
        "task_id": task_id,
        "title": deployed_title,
        "plan_doc": plan_doc,
        "absolute_plan_doc": str(plan_abs),
        "phase": deployed_phase,
        "status": deployed_phase,
        "priority": deployed_priority,
        "source": "github_issue",
        "repo": triage_payload.get("repo") or "",
        "number": triage_payload.get("number"),
        "issue_url": triage_payload.get("url") or "",
        "content_trust": "untrusted",
        "spotted_event_id": triage_payload.get("source_event_id"),
        "triage_event_id": source_event_id,
        "split_operation": operation_meta,
    }
    if branch:
        task_payload["branch"] = branch
    if owner:
        task_payload["owner"] = owner

    plan_content_hash = plan_sha256[:16]
    supersedes_plan_ready_event_id = _latest_prior_plan_ready_id(
        conn, workspace_id=workspace_id, task_id=task_id, exclude_operation_id=operation_id
    )
    ready_payload = {
        **task_payload,
        "workspace_path": workspace.path,
        "current_branch": workspace.base_branch,
        "allocated_branch": branch,
        "status": "ready_for_worker" if deployed_phase in {"ready", "planned"} else deployed_phase,
        "plan_content_hash": plan_content_hash,
        "plan_sha256": plan_sha256,
        "supersedes_plan_ready_event_id": supersedes_plan_ready_event_id,
    }
    ready_key = (
        f"{workspace_id}:{task_id}:plan.ready:{operation_id}:"
        f"{source_event_id}:{plan_content_hash}"
    )
    materialized_key = (
        f"{workspace_id}:issue.materialized:{source_event_id}:"
        f"{operation_id}:{task_id}:{plan_doc}"
    )

    existing_ledger = _check_issue_materialize_ledger_idempotency(
        conn,
        operation_id=operation_id,
        workspace_id=workspace_id,
        task_id=task_id,
        source_event_id=source_event_id,
        input_fingerprint=input_fingerprint,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
        envelope=envelope,
        expected_task_payload=task_payload,
        expected_ready_payload=ready_payload,
        ready_key=ready_key,
        materialized_key=materialized_key,
        actor=actor,
        target=target,
        effective_platform=effective_platform,
        effective_destination=effective_destination,
    )
    if existing_ledger is not None:
        materialized_event = conn.execute(
            "SELECT * FROM events WHERE id = ?",
            (existing_ledger["record_event_id"],),
        ).fetchone()
        ready_event_id = json.loads(materialized_event["payload_json"]).get(
            "plan_ready_event_id"
        )
        ready_event = (
            conn.execute("SELECT * FROM events WHERE id = ?", (ready_event_id,)).fetchone()
            if ready_event_id
            else None
        )
        task = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            (workspace_id, task_id),
        ).fetchone()
        delivery: dict[str, Any] | None = None
        delivery_created: bool | None = None
        if effective_platform and effective_destination:
            from .policy import render_event

            rendered = render_event(
                conn,
                existing_ledger["record_event_id"],
                platform=effective_platform,
                destination=effective_destination,
            )
            if rendered.supported and rendered.message_key:
                row = conn.execute(
                    "SELECT * FROM deliveries WHERE message_key = ?",
                    (rendered.message_key,),
                ).fetchone()
                if row is not None:
                    delivery = row_to_dict(row)
                    delivery_created = False
        return IssueMaterializeRecordResult(
            workspace=workspace,
            task=row_to_dict(task),
            plan_ready_event=row_to_dict(ready_event) if ready_event else {},
            event=row_to_dict(materialized_event),
            event_created=False,
            operation=existing_ledger,
            delivery=delivery,
            delivery_created=delivery_created,
        )

    now = utc_now()
    conn.execute("SAVEPOINT issue_materialize_apply")
    record_event_id: str | None = None
    try:
        ledger_row = insert_split_operation(
            conn,
            operation_id=operation_id,
            contract_version=CONTRACT_VERSION,
            operation_kind=OPERATION_KIND_ISSUE_MATERIALIZE,
            workspace_id=workspace_id,
            target_kind=TARGET_KIND_CHECKLIST_TASK,
            target_id=task_id,
            source_kind=SOURCE_KIND_ISSUE_TRIAGED_EVENT,
            source_id=source_event_id,
            input_fingerprint=input_fingerprint,
            before_fingerprint=before_fingerprint,
            after_fingerprint=after_fingerprint,
            status=STATUS_RECORD_APPLIED,
            created_at=now,
            updated_at=now,
        )
        if _inject_after_step:
            _inject_after_step("insert_ledger")

        task_row, _ = upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=deployed_phase,
            owner=owner,
            branch=branch,
            pr=None,
            payload=task_payload,
            commit=False,
        )
        if _inject_after_step:
            _inject_after_step("upsert_mirror_initial")

        ready_result = append_event(
            conn,
            workspace_id=workspace_id,
            event_type="plan.ready",
            actor=actor,
            target=target,
            task_id=task_id,
            idempotency_key=ready_key,
            payload=ready_payload,
            commit=False,
        )
        if not ready_result.created:
            raise SplitOperationError(
                f"plan.ready idempotency key {ready_key!r} already exists; "
                "refusing to link a pre-existing event to a new split operation",
                REASON_OPERATION_CONFLICT,
            )
        ready_event_id = ready_result.row["id"]
        if _inject_after_step:
            _inject_after_step("append_plan_ready")

        materialized_payload = {**task_payload, "plan_ready_event_id": ready_event_id}
        materialized_result = append_event(
            conn,
            workspace_id=workspace_id,
            event_type="issue.materialized",
            actor=actor,
            target=task_id,
            task_id=task_id,
            idempotency_key=materialized_key,
            payload=materialized_payload,
            commit=False,
        )
        if not materialized_result.created:
            raise SplitOperationError(
                f"issue.materialized idempotency key {materialized_key!r} already exists; "
                "refusing to link a pre-existing event to a new split operation",
                REASON_OPERATION_CONFLICT,
            )
        record_event_id = materialized_result.row["id"]
        if _inject_after_step:
            _inject_after_step("append_materialized")

        task_row, _ = upsert_task_mirror(
            conn,
            workspace_id=workspace_id,
            task_id=task_id,
            phase=deployed_phase,
            owner=owner,
            branch=branch,
            pr=None,
            payload=task_payload,
            last_event_id=record_event_id,
            commit=False,
        )
        if _inject_after_step:
            _inject_after_step("upsert_mirror_final")

        update_split_operation_event(
            conn,
            operation_id=operation_id,
            event_id=record_event_id,
        )
        if _inject_after_step:
            _inject_after_step("link_ledger_event")

        delivery_result: dict[str, Any] | None = None
        delivery_created_flag: bool | None = None
        if effective_platform and effective_destination:
            from .policy import create_delivery_for_event

            policy_result = create_delivery_for_event(
                conn,
                record_event_id,
                platform=effective_platform,
                destination=effective_destination,
                commit=False,
            )
            if policy_result.supported:
                # No exact ledger existed when this transaction started. If the
                # delivery idempotency key already resolves to a persisted row,
                # refuse to link a pre-existing delivery to a new split
                # operation; rollback and leave the pre-existing row untouched.
                if not policy_result.created:
                    raise SplitOperationError(
                        "delivery idempotency collision without an exact ledger",
                        REASON_OPERATION_CONFLICT,
                    )
                delivery_result = policy_result.delivery
                delivery_created_flag = policy_result.created
            elif policy_result.delivery is not None:
                raise SplitOperationError(
                    "delivery exists for an unsupported render intent",
                    REASON_OPERATION_CONFLICT,
                )
        if _inject_after_step:
            _inject_after_step("create_delivery")

        conn.execute("RELEASE SAVEPOINT issue_materialize_apply")
        conn.commit()
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT issue_materialize_apply")
        conn.execute("RELEASE SAVEPOINT issue_materialize_apply")
        raise

    ledger_row = conn.execute(
        "SELECT * FROM split_operations WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()
    return IssueMaterializeRecordResult(
        workspace=workspace,
        task=row_to_dict(task_row),
        plan_ready_event=row_to_dict(ready_result.row),
        event=row_to_dict(materialized_result.row),
        event_created=materialized_result.created,
        operation=row_to_dict(ledger_row),
        delivery=delivery_result,
        delivery_created=delivery_created_flag,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
