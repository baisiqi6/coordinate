"""Coordinate-managed dependency update entry points (combined + file-only).

This module is the bounded mutation service behind the two new CLI commands
``coordinate task update-dependencies`` (same-host combined) and
``coordinate task update-dependencies-files`` (coding-host file-only).  It
deliberately contains no new authority, schema, event type, table, or generic
``update-item`` surface:

- The checklist ``dependencies`` field stays the single dependency authority;
  ``tasks.payload_json`` remains a queryable projection updated only by the
  existing targeted reconcile.
- Every checklist mutation goes through ``checklist_io.mutate_checklist``
  (resolver, per-file lock, current/candidate validation, runtime authority
  check, mode preservation, fsync, atomic replace).  Nothing here copies the
  resolver/validator/writer/lock.
- The combined command reuses ``HarnessAdapter.harnessctl_available()`` as a
  zero-mutation preflight, ``HarnessAdapter.refresh_state()`` and
  ``reconcile.reconcile_workspace(..., task_id=...)`` for the DB mirror half.
- ``update-dependencies-files`` never opens the Coordinate DB and deliberately
  skips the harnessctl preflight: the coding host does not need a harness
  runtime, and the caller owns the commit/deploy/refresh/reconcile boundary.

Desired-state idempotency: adding an already-present dependency or removing an
absent one is a no-op reported as ``already_satisfied``, so re-running the same
combined command after a file-half success / DB-half failure converges instead
of being blocked by a duplicate.
"""
from __future__ import annotations

import shlex
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from .checklist_io import mutate_checklist
from .db import Workspace, get_workspace
from .harness import HarnessAdapter
from .onboarding import _refuse_runtime_copy
from .reconcile import reconcile_workspace
from .split_operations import compute_task_item_fingerprint

# Stable machine-readable reasons raised by this service.
REASON_INVALID_INPUT = "invalid_input"
REASON_OVERLAP = "overlap"
REASON_TARGET_NOT_FOUND = "target_not_found"
REASON_DEPENDENCY_NOT_FOUND = "dependency_not_found"
REASON_SELF_DEPENDENCY = "self_dependency"
REASON_HARNESSCTL_UNAVAILABLE = "harnessctl_unavailable"
REASON_RUNTIME_COPY = "runtime_copy"
REASON_RELATIVE_PATH = "relative_path"
REASON_IO_ERROR = "io_error"


class DependencyUpdateError(ValueError):
    """A fail-closed dependency update rejection or failure.

    Covers every refusal before any mutation (invalid input, overlap, unknown
    target/dependency, self-dependency, relative paths, /opt runtime copy,
    missing harnessctl) and the ``REASON_IO_ERROR`` case where the atomic
    writer failed with an unknown commit status (the file may or may not have
    been replaced).  ``reason`` is a stable machine-readable classification
    string.
    """

    def __init__(self, message: str, reason: str):
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class DependencyRequestOutcome:
    """Per-request observable outcome (reviewer P2 constraint).

    Every requested add/remove is reported as ``applied`` or
    ``already_satisfied``; this is a report of what happened, not a new
    protocol entity.
    """

    dependency: str
    action: Literal["add", "remove"]
    outcome: Literal["applied", "already_satisfied"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "dependency": self.dependency,
            "action": self.action,
            "outcome": self.outcome,
        }


@dataclass(frozen=True)
class DependencyMutationResult:
    """Result of the checklist file-half mutation (shared by both commands).

    ``dependencies`` and the before/after fingerprints describe the **locked
    mutation snapshot** — the target item this mutation actually read and
    wrote under the checklist lock.  They are not a promise that nobody will
    modify the checklist again between the unlock and process exit; a later
    legal concurrent update wins on the authority and this result still
    honestly describes the snapshot it modified.
    """

    workspace_id: str
    task_id: str
    before_fingerprint: str
    after_fingerprint: str
    changed: bool
    outcomes: tuple[DependencyRequestOutcome, ...]
    dependencies: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "before_fingerprint": self.before_fingerprint,
            "after_fingerprint": self.after_fingerprint,
            "changed": self.changed,
            "requests": [outcome.to_dict() for outcome in self.outcomes],
            "dependencies": list(self.dependencies),
        }


@dataclass(frozen=True)
class TaskDependenciesRecovery:
    """Structured recovery for a combined half failure.

    The checklist file half already committed and remains the single
    authority; the DB mirror half (state refresh + targeted reconcile) failed.
    ``retry_argv`` re-runs the same combined command (idempotent, converges
    via desired-state semantics) and ``reconcile_argv`` runs the existing
    targeted reconcile to refresh only the target task mirror.
    """

    workspace_id: str
    task_id: str
    adds: tuple[str, ...]
    removes: tuple[str, ...]
    allow_runtime_copy: bool
    error_message: str = ""

    def retry_argv(self) -> list[str]:
        argv = [
            "coordinate",
            "task",
            "update-dependencies",
            self.workspace_id,
            "--task-id",
            self.task_id,
        ]
        for dependency in self.adds:
            argv += ["--add", dependency]
        for dependency in self.removes:
            argv += ["--remove", dependency]
        if self.allow_runtime_copy:
            argv.append("--allow-runtime-copy")
        return argv

    def reconcile_argv(self) -> list[str]:
        return [
            "coordinate",
            "reconcile",
            self.workspace_id,
            "--task-id",
            self.task_id,
        ]

    def to_dict(self) -> dict[str, Any]:
        retry = self.retry_argv()
        reconcile = self.reconcile_argv()
        return {
            "recovery_required": True,
            "checklist_committed": True,
            "retry_command": shlex.join(retry),
            "retry_argv": retry,
            "reconcile_command": shlex.join(reconcile),
            "reconcile_argv": reconcile,
            "error": self.error_message,
        }


class TaskDependenciesRecoveryError(ValueError):
    """The file half committed but the DB half failed.

    ``recovery`` carries the checklist-committed marker and the two recovery
    commands (same-command retry, targeted reconcile).
    """

    def __init__(self, message: str, recovery: TaskDependenciesRecovery):
        super().__init__(message)
        self.recovery = recovery


def _validate_dependency_inputs(
    task_id: str,
    add: list[str] | tuple[str, ...],
    remove: list[str] | tuple[str, ...],
) -> tuple[list[str], list[str]]:
    """Normalize inputs: dedupe preserving order, reject empty/overlap.

    Raises ``DependencyUpdateError`` before any file or DB work.
    """
    if not isinstance(task_id, str) or not task_id.strip():
        raise DependencyUpdateError("task_id is required", REASON_INVALID_INPUT)
    adds = [str(dep) for dep in add]
    removes = [str(dep) for dep in remove]
    for dep in adds + removes:
        if not dep.strip():
            raise DependencyUpdateError(
                "dependency ids must be non-empty strings",
                REASON_INVALID_INPUT,
            )
    if not adds and not removes:
        raise DependencyUpdateError(
            "update-dependencies requires at least one --add or --remove",
            REASON_INVALID_INPUT,
        )
    # Dedupe preserving the caller's order within each flag.
    adds = list(dict.fromkeys(adds))
    removes = list(dict.fromkeys(removes))
    overlap = sorted(set(adds) & set(removes))
    if overlap:
        raise DependencyUpdateError(
            f"dependency {overlap[0]!r} appears in both --add and --remove; "
            "refusing ambiguous input",
            REASON_OVERLAP,
        )
    return adds, removes


def _find_item(checklist: dict[str, Any], task_id: str) -> dict[str, Any] | None:
    items = checklist.get("items") if isinstance(checklist.get("items"), list) else None
    if not items:
        return None
    for item in items:
        if isinstance(item, dict) and item.get("id") == task_id:
            return item
    return None


def _require_absolute_path(value: str | Path, label: str) -> Path:
    """Fail closed when *value* is not an absolute path.

    Relative paths must never be interpreted against the process cwd (a
    file-only run from another directory could otherwise write the wrong
    checklist authority); external absolute harness roots remain supported.
    """
    path = Path(value)
    if not path.is_absolute():
        raise DependencyUpdateError(
            f"{label} must be an absolute path, got {value!r}; refusing to "
            "interpret it relative to the process working directory",
            REASON_RELATIVE_PATH,
        )
    return path


def _refuse_runtime_copy_guard(workspace: Workspace, *, allow_runtime_copy: bool) -> None:
    """Shared /opt runtime-copy guard with a command-neutral wire message.

    Reuses the existing ``onboarding._refuse_runtime_copy`` decision logic
    (startswith ``/opt/`` or exactly ``/opt``) but reports a message that is
    accurate for dependency updates instead of ``task create``.
    """
    try:
        _refuse_runtime_copy(workspace, allow_runtime_copy=allow_runtime_copy)
    except ValueError as exc:
        raise DependencyUpdateError(
            "refusing to update dependencies inside an /opt runtime deployment "
            "copy; run against the coding-host git checkout. Use "
            "--allow-runtime-copy only for explicit repair.",
            REASON_RUNTIME_COPY,
        ) from exc


def apply_dependency_mutation(
    *,
    workspace_path: str | Path,
    harness_root: str | Path,
    workspace_id: str,
    task_id: str,
    add: list[str] | tuple[str, ...] = (),
    remove: list[str] | tuple[str, ...] = (),
    allow_runtime_copy: bool = False,
    _lock_timeout: float = 30.0,
    _lock: Any = None,
) -> DependencyMutationResult:
    """Checklist file-half mutation: update an existing item's ``dependencies``.

    Reuses ``mutate_checklist`` (resolver -> lock -> validate current ->
    deepcopy -> callback -> validate candidate -> runtime problems -> atomic
    write).  Any rejection before the commit point leaves the original bytes
    untouched.  No DB connection is opened here: ``update-dependencies-files``
    calls this directly, so file-only runs are DB-free by construction.

    Service-boundary guards, all before any mutation: input validation,
    absolute-path validation for ``workspace_path``/``harness_root`` (relative
    paths are never resolved against the process cwd), and the /opt
    runtime-copy guard.

    Fingerprints are computed inside the mutation callback on the target
    item the callback actually sees (the same locked read ``mutate_checklist``
    validates), so they bind this mutation's real before/after projection and
    equal each other on a no-op; nothing here reads checklist bytes outside
    the lock.  ``dependencies`` and the fingerprints are the **locked mutation
    snapshot**: they describe the item this mutation read and wrote under the
    lock, not the checklist state after the lock is released (a later legal
    concurrent update wins on the authority, and the targeted reconcile
    re-reads the then-current canonical checklist).

    Desired-state idempotency: add-existing and remove-absent are no-ops
    reported as ``already_satisfied``; when every request is already
    satisfied the callback returns False and the file bytes are preserved.

    An ``OSError`` from the atomic writer is surfaced as ``DependencyUpdateError``
    with ``REASON_IO_ERROR``: the commit status may be unknown (the file may
    already have been replaced when a post-rename directory fsync fails), the
    DB half never runs, and re-running the same desired-state command is safe
    and converges.
    """
    adds, removes = _validate_dependency_inputs(task_id, add, remove)
    workspace_path = _require_absolute_path(workspace_path, "workspace_path")
    harness_root = _require_absolute_path(harness_root, "harness_root")
    _refuse_runtime_copy_guard(
        Workspace(
            id=workspace_id,
            name=workspace_id,
            path=str(workspace_path),
            harness_root=str(harness_root),
        ),
        allow_runtime_copy=allow_runtime_copy,
    )

    outcomes: list[DependencyRequestOutcome] = []
    final_dependencies: list[str] = []
    before_fingerprint = ""
    after_fingerprint = ""

    def callback(candidate: dict[str, Any]) -> bool:
        nonlocal final_dependencies, before_fingerprint, after_fingerprint
        target = _find_item(candidate, task_id)
        if target is None:
            raise DependencyUpdateError(
                f"target task {task_id!r} does not exist in the checklist; "
                "nothing written",
                REASON_TARGET_NOT_FOUND,
            )
        item_ids = {
            item.get("id")
            for item in candidate.get("items", [])
            if isinstance(item, dict) and item.get("id")
        }
        current = list(target.get("dependencies") or [])
        # Bound to the item this callback actually sees under the lock.
        before_fingerprint = compute_task_item_fingerprint(
            item=target, task_id=task_id
        )
        updated = list(current)
        for dependency in removes:
            if dependency in updated:
                updated = [dep for dep in updated if dep != dependency]
                outcomes.append(
                    DependencyRequestOutcome(dependency, "remove", "applied")
                )
            else:
                outcomes.append(
                    DependencyRequestOutcome(dependency, "remove", "already_satisfied")
                )
        for dependency in adds:
            if dependency == task_id:
                raise DependencyUpdateError(
                    f"task {task_id!r} cannot depend on itself; nothing written",
                    REASON_SELF_DEPENDENCY,
                )
            if dependency not in item_ids:
                raise DependencyUpdateError(
                    f"dependency {dependency!r} does not exist in the checklist; "
                    "nothing written",
                    REASON_DEPENDENCY_NOT_FOUND,
                )
            if dependency in updated:
                outcomes.append(
                    DependencyRequestOutcome(dependency, "add", "already_satisfied")
                )
            else:
                updated.append(dependency)
                outcomes.append(
                    DependencyRequestOutcome(dependency, "add", "applied")
                )
        changed = any(outcome.outcome == "applied" for outcome in outcomes)
        if changed:
            target["dependencies"] = updated
        after_fingerprint = compute_task_item_fingerprint(
            item=target, task_id=task_id
        )
        final_dependencies = updated
        return changed

    try:
        mutate_checklist(harness_root, callback, lock_timeout=_lock_timeout, _lock=_lock)
    except OSError as exc:
        raise DependencyUpdateError(
            f"checklist write failed with unknown commit status (the file may "
            f"or may not have been replaced): {exc}. The DB half was not run; "
            "re-running the same desired-state command is safe and converges.",
            REASON_IO_ERROR,
        ) from exc

    return DependencyMutationResult(
        workspace_id=workspace_id,
        task_id=task_id,
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
        changed=any(outcome.outcome == "applied" for outcome in outcomes),
        outcomes=tuple(outcomes),
        dependencies=tuple(final_dependencies),
    )


def update_task_dependencies(
    conn: sqlite3.Connection,
    *,
    workspace_id: str,
    task_id: str,
    add: list[str] | tuple[str, ...] = (),
    remove: list[str] | tuple[str, ...] = (),
    allow_runtime_copy: bool = False,
    _lock_timeout: float = 30.0,
    _lock: Any = None,
    _adapter: HarnessAdapter | None = None,
) -> dict[str, Any]:
    """Combined same-host dependency update: preflight, file-first, mirror.

    Order of operations:
    1. workspace lookup + /opt runtime-copy guard (zero mutation on refusal);
    2. ``harnessctl_available()`` preflight BEFORE any file mutation — a
       workspace without a harness runtime fails closed with zero mutation
       instead of committing the checklist and mislabeling a normal gap as
       recovery;
    3. checklist file-half mutation (atomic, idempotent);
    4. ``refresh_state()`` then targeted ``reconcile_workspace(task_id=...)``
       so only the target task mirror follows the authority (no full
       reconcile, no new event type: ``task_mirror.updated`` /
       ``reconciliation.completed`` already provide the evidence).

    When the file half commits but the DB half fails, raises
    ``TaskDependenciesRecoveryError`` with the checklist-committed marker and
    both recovery commands; re-running the same command converges.  The
    recovery text notes that a harness-state refresh failure itself must be
    fixed before re-running (the old state is then refused by the digest
    freshness guard rather than reused).
    """
    workspace = get_workspace(conn, workspace_id)
    if workspace is None:
        raise DependencyUpdateError(
            f"unknown workspace: {workspace_id}", REASON_INVALID_INPUT
        )
    # Validate inputs before the preflight so malformed invocations fail fast
    # with zero side effects; the file mutation itself re-validates.
    _validate_dependency_inputs(task_id, add, remove)
    _require_absolute_path(workspace.path, "workspace.path")
    _require_absolute_path(workspace.harness_root, "workspace.harness_root")
    _refuse_runtime_copy_guard(workspace, allow_runtime_copy=allow_runtime_copy)
    adapter = _adapter or HarnessAdapter(workspace)
    if not adapter.harnessctl_available():
        raise DependencyUpdateError(
            f"harnessctl is not available for workspace {workspace_id!r}; "
            "refusing to mutate the checklist without the harness runtime "
            "(zero mutation). Register the workspace with --harnessctl-path or "
            "place harnessctl under the workspace/harness scripts directory. "
            "For a coding-host file-only update use "
            "`coordinate task update-dependencies-files`.",
            REASON_HARNESSCTL_UNAVAILABLE,
        )

    files = apply_dependency_mutation(
        workspace_path=workspace.path,
        harness_root=workspace.harness_root,
        workspace_id=workspace_id,
        task_id=task_id,
        add=add,
        remove=remove,
        allow_runtime_copy=allow_runtime_copy,
        _lock_timeout=_lock_timeout,
        _lock=_lock,
    )

    try:
        adapter.refresh_state()
        reconciliation = reconcile_workspace(
            conn,
            workspace,
            refresh=False,
            adapter=adapter,
            task_id=task_id,
        )
    except Exception as exc:
        recovery = TaskDependenciesRecovery(
            workspace_id=workspace_id,
            task_id=task_id,
            adds=tuple(_dedupe_order(add)),
            removes=tuple(_dedupe_order(remove)),
            allow_runtime_copy=allow_runtime_copy,
            error_message=str(exc),
        )
        raise TaskDependenciesRecoveryError(
            f"checklist dependencies committed for task {task_id!r} but the "
            "state refresh / targeted reconcile failed; the checklist remains "
            "the single authority. Complete the DB half with the same command "
            "(idempotent) or `coordinate reconcile "
            f"{workspace_id} --task-id {task_id}`. If the harness state refresh "
            "itself failed, fix that cause first, then re-run the same "
            "desired-state command.",
            recovery=recovery,
        ) from exc

    # Flat result: every fact appears exactly once (file-half fields plus the
    # reconciliation report; no duplicated requests/dependencies copies).
    result = files.to_dict()
    result["reconciliation"] = reconciliation.to_dict()
    return result


def _dedupe_order(values: list[str] | tuple[str, ...]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values))
