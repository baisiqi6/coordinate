"""Versioned, digest-bound ExecutionContext for managed runtime jobs.

Coordinate owns serialization and validation. MultiNexus owns strict consumption.
No foreign-host path is ever normalized through the local ``pathlib.Path.resolve()``.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from coordinate.db import Workspace, WorkspaceHostProfile

CONTRACT_VERSION = 1
MAX_SCOPE_LEN = 256
MAX_LEGACY_SCOPES = 10

# Channel/thread/request/task scope ids are opaque to Coordinate; we only bound
# length and characters so they cannot carry injection or traversal payloads.
_SAFE_SCOPE_RE = re.compile(r"^[A-Za-z0-9_.:/-]+$")

# Context id is a stable SHA-256 digest with a lowercase 64-hex body.
_CONTEXT_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

# Host-absolute path forms: POSIX ``/...`` or Windows ``C:\...`` / UNC ``\\...``.
_WINDOWS_ABS_RE = re.compile(r"^([A-Za-z]:[\\/]|\\{2})")

_V1_SNAPSHOT_KEYS = frozenset({
    "assigned_agent",
    "branch",
    "contract_version",
    "context_id",
    "harness_root",
    "host_id",
    "job_id",
    "legacy_scope_ids",
    "log_handle",
    "session_scope_id",
    "task_id",
    "worktree_path",
    "workspace_id",
    "workspace_path",
})

_LOG_HANDLE_KEYS = frozenset({"kind", "job_id", "logs_path"})


class ContextError(ValueError):
    """Raised when a context snapshot is missing, malformed, or conflicts with authority."""


@dataclass(frozen=True)
class ExecutionContextV1:
    """Immutable v1 execution context for one managed job."""

    context_id: str
    job_id: str
    workspace_id: str
    task_id: str | None
    assigned_agent: str
    host_id: str
    workspace_path: str
    worktree_path: str
    harness_root: str
    branch: str | None
    session_scope_id: str
    legacy_scope_ids: tuple[str, ...]
    log_handle: Mapping[str, Any]

    def __post_init__(self) -> None:
        # Freeze the mutable log_handle dict so digest binding is protected.
        if not isinstance(self.log_handle, MappingProxyType):
            object.__setattr__(self, "log_handle", MappingProxyType(dict(self.log_handle)))

    def to_dict(self) -> dict[str, Any]:
        """Full public snapshot, including the self-referential context_id."""
        return {
            "contract_version": CONTRACT_VERSION,
            "context_id": self.context_id,
            "job_id": self.job_id,
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "assigned_agent": self.assigned_agent,
            "host_id": self.host_id,
            "workspace_path": self.workspace_path,
            "worktree_path": self.worktree_path,
            "harness_root": self.harness_root,
            "branch": self.branch,
            "session_scope_id": self.session_scope_id,
            "legacy_scope_ids": list(self.legacy_scope_ids),
            "log_handle": dict(self.log_handle),
        }

    def canonical_snapshot_dict(self) -> dict[str, Any]:
        """Canonical v1 fields excluding context_id; used for digest and equality."""
        return {
            "assigned_agent": self.assigned_agent,
            "branch": self.branch,
            "contract_version": CONTRACT_VERSION,
            "harness_root": self.harness_root,
            "host_id": self.host_id,
            "job_id": self.job_id,
            "legacy_scope_ids": list(self.legacy_scope_ids),
            "log_handle": dict(self.log_handle),
            "session_scope_id": self.session_scope_id,
            "task_id": self.task_id,
            "worktree_path": self.worktree_path,
            "workspace_id": self.workspace_id,
            "workspace_path": self.workspace_path,
        }


def _canonical_json(value: dict[str, Any]) -> str:
    """Deterministic JSON used for digests and byte-identical fixtures."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _compute_context_id(canonical: dict[str, Any]) -> str:
    data = _canonical_json(canonical).encode("utf-8")
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def _isabs(path: str) -> bool:
    return path.startswith("/") or _WINDOWS_ABS_RE.match(path) is not None


def _has_traversal(segments: list[str]) -> bool:
    """Return True if any segment is a path traversal component."""
    return any(seg in {"..", "."} for seg in segments)


def _validate_path(value: Any, label: str) -> str:
    """Validate a host-absolute authority path.

    Rejects non-strings, empty strings, NUL/newline, relative paths, and
    traversal segments. The path is authority data, not a local filesystem path,
    so this is purely lexical.
    """
    if not isinstance(value, str) or not value:
        raise ContextError(f"{label} is required")
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ContextError(f"{label} contains NUL or newline")
    if not _isabs(value):
        raise ContextError(f"{label} must be absolute: {value!r}")
    segments = value.replace("\\", "/").split("/")
    if _has_traversal(segments):
        raise ContextError(f"{label} contains traversal: {value!r}")
    return value


def _host_separator(host_root: str) -> str:
    """Infer host-native separator from an already-host-native root path."""
    if "\\" in host_root or (len(host_root) >= 2 and host_root[1] == ":"):
        return "\\"
    return "/"


def _join_host_path(host_root: str, relative_path: str) -> str:
    """Pure string/segment join; never calls local Path.resolve()."""
    if not relative_path:
        return host_root
    sep = _host_separator(host_root)
    rel = relative_path.replace("/", sep).replace("\\", sep).strip(sep)
    if _has_traversal(rel.split(sep)):
        raise ContextError(f"relative path contains traversal: {relative_path!r}")
    return host_root.rstrip(sep) + sep + rel


def _map_foreign_path(
    control_root: str,
    host_root: str,
    path_text: str,
) -> str:
    """Map a control-plane path to a host-native path.

    The control-plane path must be absolute and must lie under the control
    workspace root. Relative paths and absolute paths outside the workspace are
    rejected so they cannot become an adapter cwd or log handle.
    """
    if not path_text:
        return path_text

    if "\x00" in path_text or "\n" in path_text or "\r" in path_text:
        raise ContextError("path contains NUL or newline")
    if not _isabs(path_text):
        raise ContextError(f"path must be absolute under workspace: {path_text!r}")

    control_root = control_root.rstrip("/")
    normalized = path_text.replace("\\", "/")
    segments = normalized.split("/")
    if _has_traversal(segments):
        raise ContextError(f"path contains traversal: {path_text!r}")

    if normalized == control_root:
        return host_root
    prefix = control_root + "/"
    if normalized.startswith(prefix):
        rel = normalized[len(prefix):]
        return _join_host_path(host_root, rel)

    raise ContextError(
        f"path is outside control workspace {control_root!r}: {path_text!r}"
    )


def _validate_scope(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ContextError(f"{label} is required")
    if len(value) > MAX_SCOPE_LEN or not _SAFE_SCOPE_RE.match(value):
        raise ContextError(f"{label} contains unsafe characters or is too long: {value!r}")
    return value


def _parse_origin_scope(
    origin: dict[str, Any],
    workspace_id: str,
    task_id: str | None,
) -> tuple[str, tuple[str, ...]]:
    """Return (session_scope_id, legacy_scope_ids) from bridge origin.

    For a task job the primary scope is always Coordinate-canonical and bridge
    input may not override it. For a non-task job the bridge must supply a
    bounded non-empty channel/thread/request scope.
    """
    if task_id:
        session_scope_id = f"task:{workspace_id}:{task_id}"
    else:
        raw = origin.get("session_scope_id")
        session_scope_id = _validate_scope(raw, "origin.session_scope_id")

    legacy: list[str] = []
    raw_legacy = origin.get("legacy_scope_ids")
    if raw_legacy is not None:
        if not isinstance(raw_legacy, (list, tuple)):
            raise ContextError("origin.legacy_scope_ids must be a list")
        if len(raw_legacy) > MAX_LEGACY_SCOPES:
            raise ContextError(f"origin.legacy_scope_ids exceeds {MAX_LEGACY_SCOPES}")
        seen: set[str] = {session_scope_id}
        for item in raw_legacy:
            scoped = _validate_scope(item, "origin.legacy_scope_ids item")
            if scoped in seen:
                continue
            seen.add(scoped)
            legacy.append(scoped)

    return session_scope_id, tuple(legacy)


def resolve_execution_context_v1(
    *,
    job_id: str,
    workspace: Workspace,
    task: dict[str, Any] | None,
    assigned_agent: str,
    host_id: str,
    profile: WorkspaceHostProfile,
    origin: dict[str, Any],
    job_branch: str | None = None,
    job_worktree_path: str | None = None,
    job_logs_path: str | None = None,
) -> ExecutionContextV1:
    """Build the authoritative v1 ExecutionContext for a managed job."""
    if not isinstance(job_id, str) or not job_id:
        raise ContextError("job_id is required")
    if not isinstance(assigned_agent, str) or not assigned_agent:
        raise ContextError("assigned_agent is required")
    if not host_id or not host_id.strip():
        raise ContextError("agent host_id is required")
    if not profile.workspace_path or not profile.workspace_path.strip():
        raise ContextError("workspace host profile workspace_path is required")

    workspace_id = workspace.id
    task_id = None
    if task is not None:
        raw_task_id = task.get("task_id")
        if not isinstance(raw_task_id, str) or not raw_task_id:
            raise ContextError("task mirror has no task_id")
        task_id = raw_task_id

    if not isinstance(origin, dict):
        raise ContextError("origin must be a dict")

    session_scope_id, legacy_scope_ids = _parse_origin_scope(origin, workspace_id, task_id)

    branch_candidates = [
        job_branch,
        task.get("branch") if task else None,
        workspace.base_branch,
    ]
    branch = None
    for candidate in branch_candidates:
        if isinstance(candidate, str) and candidate.strip():
            branch = candidate.strip()
            break

    workspace_path = _validate_path(profile.workspace_path, "workspace_path")
    if job_worktree_path is not None:
        worktree_path = _map_foreign_path(workspace.path, workspace_path, job_worktree_path)
    else:
        worktree_path = workspace_path
    if not worktree_path:
        worktree_path = workspace_path
    worktree_path = _validate_path(worktree_path, "worktree_path")

    if profile.harness_root:
        harness_root = _validate_path(profile.harness_root, "harness_root")
    else:
        harness_root = _map_foreign_path(workspace.path, workspace_path, workspace.harness_root)
        if not _isabs(harness_root):
            harness_root = _join_host_path(workspace_path, harness_root)
        harness_root = _validate_path(harness_root, "harness_root")

    logs_path = None
    if job_logs_path is not None:
        logs_path = _map_foreign_path(workspace.path, workspace_path, job_logs_path)
        logs_path = _validate_path(logs_path, "logs_path")

    log_handle = MappingProxyType({
        "kind": "coordinate_job",
        "job_id": job_id,
        "logs_path": logs_path,
    })

    ctx = ExecutionContextV1(
        context_id="",  # computed below
        job_id=job_id,
        workspace_id=workspace_id,
        task_id=task_id,
        assigned_agent=assigned_agent,
        host_id=host_id,
        workspace_path=workspace_path,
        worktree_path=worktree_path,
        harness_root=harness_root,
        branch=branch,
        session_scope_id=session_scope_id,
        legacy_scope_ids=legacy_scope_ids,
        log_handle=log_handle,
    )
    context_id = _compute_context_id(ctx.canonical_snapshot_dict())
    return ExecutionContextV1(
        context_id=context_id,
        job_id=ctx.job_id,
        workspace_id=ctx.workspace_id,
        task_id=ctx.task_id,
        assigned_agent=ctx.assigned_agent,
        host_id=ctx.host_id,
        workspace_path=ctx.workspace_path,
        worktree_path=ctx.worktree_path,
        harness_root=ctx.harness_root,
        branch=ctx.branch,
        session_scope_id=ctx.session_scope_id,
        legacy_scope_ids=ctx.legacy_scope_ids,
        log_handle=ctx.log_handle,
    )


def _validate_string_or_none(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContextError(f"{label} must be a string or null")
    if not value.strip():
        raise ContextError(f"{label} must be a non-empty string or null")
    return value


def _validate_required_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ContextError(f"{label} is required")
    if not value.strip():
        raise ContextError(f"{label} must be a non-empty string")
    return value


def _validate_log_handle(value: Any, *, expected_job_id: str) -> MappingProxyType[str, Any]:
    if not isinstance(value, dict):
        raise ContextError("log_handle must be an object")
    if set(value.keys()) != _LOG_HANDLE_KEYS:
        raise ContextError(
            f"log_handle has incorrect keys: expected {_LOG_HANDLE_KEYS}, got {set(value.keys())}"
        )
    if value.get("kind") != "coordinate_job":
        raise ContextError(f"log_handle.kind must be 'coordinate_job': {value.get('kind')!r}")
    job_id = value.get("job_id")
    if not isinstance(job_id, str) or not job_id:
        raise ContextError("log_handle.job_id is required")
    if job_id != expected_job_id:
        raise ContextError(
            f"log_handle.job_id mismatch: {job_id!r} != {expected_job_id!r}"
        )
    logs_path = value.get("logs_path")
    if logs_path is not None and (not isinstance(logs_path, str) or not logs_path):
        raise ContextError("log_handle.logs_path must be a non-empty string or null")
    if logs_path is not None:
        _validate_path(logs_path, "log_handle.logs_path")
    return MappingProxyType({
        "kind": "coordinate_job",
        "job_id": job_id,
        "logs_path": logs_path,
    })


def validate_execution_context_snapshot(
    data: dict[str, Any],
    *,
    job_id: str | None = None,
    workspace_id: str | None = None,
    task_id: str | None = None,
    assigned_agent: str | None = None,
    host_id: str | None = None,
) -> ExecutionContextV1:
    """Parse and cryptographically validate a stored v1 snapshot.

    Raises ContextError if the snapshot is malformed, the digest does not match,
    or any supplied identity field conflicts.
    """
    if not isinstance(data, dict):
        raise ContextError("execution_context must be an object")
    if set(data.keys()) != _V1_SNAPSHOT_KEYS:
        raise ContextError(
            f"execution_context has incorrect keys: expected {_V1_SNAPSHOT_KEYS}, got {set(data.keys())}"
        )
    if data.get("contract_version") != CONTRACT_VERSION:
        raise ContextError("execution_context contract_version must be 1")

    supplied_id = data.get("context_id")
    if not isinstance(supplied_id, str) or not _CONTEXT_ID_RE.match(supplied_id):
        raise ContextError("execution_context.context_id must be sha256:<64-hex>")

    # Validate scalar fields strictly (no silent coercion).
    parsed_job_id = _validate_required_string(data.get("job_id"), "execution_context.job_id")
    parsed_workspace_id = _validate_required_string(data.get("workspace_id"), "execution_context.workspace_id")
    parsed_assigned_agent = _validate_required_string(data.get("assigned_agent"), "execution_context.assigned_agent")
    parsed_host_id = _validate_required_string(data.get("host_id"), "execution_context.host_id")
    parsed_workspace_path = _validate_path(data.get("workspace_path"), "execution_context.workspace_path")
    parsed_worktree_path = _validate_path(data.get("worktree_path"), "execution_context.worktree_path")
    parsed_harness_root = _validate_path(data.get("harness_root"), "execution_context.harness_root")
    parsed_session_scope_id = _validate_scope(data.get("session_scope_id"), "execution_context.session_scope_id")
    parsed_branch = _validate_string_or_none(data.get("branch"), "execution_context.branch")
    parsed_task_id = _validate_string_or_none(data.get("task_id"), "execution_context.task_id")

    raw_legacy = data.get("legacy_scope_ids")
    # The JSON v1 contract requires a unique list; reject wrong containers
    # and any duplicate (including duplicates of the primary scope).
    if not isinstance(raw_legacy, list):
        raise ContextError("execution_context.legacy_scope_ids must be a list")
    if len(raw_legacy) > MAX_LEGACY_SCOPES:
        raise ContextError(f"execution_context.legacy_scope_ids exceeds {MAX_LEGACY_SCOPES}")
    seen: set[str] = set()
    legacy_scope_ids: list[str] = []
    for item in raw_legacy:
        scoped = _validate_scope(item, "execution_context.legacy_scope_ids item")
        if scoped in seen or scoped == parsed_session_scope_id:
            raise ContextError(f"execution_context.legacy_scope_ids contains duplicate: {scoped!r}")
        seen.add(scoped)
        legacy_scope_ids.append(scoped)

    log_handle = _validate_log_handle(data.get("log_handle"), expected_job_id=parsed_job_id)

    # Reconstruct without trusting the supplied context_id, then recompute.
    ctx = ExecutionContextV1(
        context_id="",
        job_id=parsed_job_id,
        workspace_id=parsed_workspace_id,
        task_id=parsed_task_id,
        assigned_agent=parsed_assigned_agent,
        host_id=parsed_host_id,
        workspace_path=parsed_workspace_path,
        worktree_path=parsed_worktree_path,
        harness_root=parsed_harness_root,
        branch=parsed_branch,
        session_scope_id=parsed_session_scope_id,
        legacy_scope_ids=tuple(legacy_scope_ids),
        log_handle=log_handle,
    )

    expected_id = _compute_context_id(ctx.canonical_snapshot_dict())
    if expected_id != supplied_id:
        raise ContextError(
            f"execution_context digest mismatch: expected {expected_id}, got {supplied_id}"
        )

    # Return the parsed snapshot with the validated context_id bound.
    ctx = ExecutionContextV1(
        context_id=supplied_id,
        job_id=ctx.job_id,
        workspace_id=ctx.workspace_id,
        task_id=ctx.task_id,
        assigned_agent=ctx.assigned_agent,
        host_id=ctx.host_id,
        workspace_path=ctx.workspace_path,
        worktree_path=ctx.worktree_path,
        harness_root=ctx.harness_root,
        branch=ctx.branch,
        session_scope_id=ctx.session_scope_id,
        legacy_scope_ids=ctx.legacy_scope_ids,
        log_handle=ctx.log_handle,
    )

    if job_id is not None and ctx.job_id != job_id:
        raise ContextError(f"execution_context job_id mismatch: {ctx.job_id} != {job_id}")
    if workspace_id is not None and ctx.workspace_id != workspace_id:
        raise ContextError(
            f"execution_context workspace_id mismatch: {ctx.workspace_id} != {workspace_id}"
        )
    if assigned_agent is not None and ctx.assigned_agent != assigned_agent:
        raise ContextError(
            f"execution_context assigned_agent mismatch: {ctx.assigned_agent} != {assigned_agent}"
        )
    if host_id is not None and ctx.host_id != host_id:
        raise ContextError(f"execution_context host_id mismatch: {ctx.host_id} != {host_id}")
    if task_id is not None and ctx.task_id != task_id:
        raise ContextError(f"execution_context task_id mismatch: {ctx.task_id} != {task_id}")

    return ctx


def context_matches_origin(
    ctx: ExecutionContextV1,
    origin: dict[str, Any],
    workspace_id: str,
    task_id: str | None,
) -> bool:
    """True when the context's scope fields match the bridge origin."""
    try:
        session_scope_id, legacy_scope_ids = _parse_origin_scope(origin, workspace_id, task_id)
    except ContextError:
        return False
    return ctx.session_scope_id == session_scope_id and ctx.legacy_scope_ids == legacy_scope_ids


def execution_context_dict_matches(
    snapshot: dict[str, Any],
    other: dict[str, Any],
) -> bool:
    """Compare two v1 snapshots ignoring the self-referential context_id."""
    a = validate_execution_context_snapshot(snapshot).canonical_snapshot_dict()
    b = validate_execution_context_snapshot(other).canonical_snapshot_dict()
    return a == b
