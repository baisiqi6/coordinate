"""Sole managed checklist file I/O boundary for Coordinate.

Owns the Coordinate-local resolver (old/new filename), raw-bytes read + JSON
parse + full contract validation, the per-checklist lock, the unique-temp
atomic writer, the callback-style mutation pipeline, checklist SHA-256 /
freshness helpers, the plan-locator Coordinate adapter, and the initial
phase projection.

Deliberately contains no DB, event, task-lifecycle, Discord, or deployment
logic; ``split_operations``, ``transitions``, ``completion``, ``harness`` and
friends consume this boundary instead of re-implementing readers/writers.
Dependency direction is one-way: this module imports only ``checklist_contract``
and the standard library.
"""
from __future__ import annotations

import errno
import hashlib
import json
import os
import stat as stat_module
import tempfile
import time
import unicodedata
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal

from .checklist_contract import validate_checklist

CHECKLIST_NEW_NAME = "harness-checklist.json"
CHECKLIST_LEGACY_NAME = "mvp-checklist.json"

# Stable machine-readable reasons raised by this boundary.
REASON_CHECKLIST_MISSING = "checklist_missing"
REASON_DUAL_AUTHORITY = "dual_authority"
REASON_VALIDATION_ERROR = "validation_error"
REASON_LOCK_TIMEOUT = "lock_timeout"
REASON_PHASE_NOT_CREATABLE = "phase_not_creatable"

# Lifecycle/terminal phases that create must never fabricate.  Create only
# produces todo/todo; these states are reached through the dedicated lifecycle
# commands (assign/accept/handoff/blocker/unblock/review/closeout/mark-done).
LIFECYCLE_PHASES = frozenset({
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
    "unblocked",
})
TERMINAL_PHASES = frozenset({"released", "closed", "done"})
# Planning-safe phases that map directly to todo/todo.
SAFE_PLANNING_PHASES = frozenset({"ready", "planned", "todo"})


class ChecklistError(ValueError):
    """A checklist authority/validation failure.

    ``reason`` is a stable machine-readable classification string.
    """

    def __init__(self, message: str, reason: str):
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class ResolvedChecklist:
    """The single active checklist file for a purpose.

    Carries only the actual path and whether it uses the new or legacy
    filename; it is not a new persistent entity.
    """

    path: Path
    kind: Literal["new", "legacy"]


def checklist_candidates(harness_root: str | Path) -> dict[str, Path]:
    """Both possible checklist filenames; used by doctor for diagnosis only."""
    root = Path(harness_root)
    return {
        "new": root / CHECKLIST_NEW_NAME,
        "legacy": root / CHECKLIST_LEGACY_NAME,
    }


def resolve_checklist(
    harness_root: str | Path,
    *,
    purpose: Literal["read", "mutation", "migrate"] = "read",
) -> ResolvedChecklist:
    """Resolve the single active checklist file.

    Purpose semantics follow the U1 contract matrix: read/mutation accept
    new-only or legacy-only; none and dual authority fail closed. Only an
    explicit init entry point may create a new checklist, so ``migrate`` is
    reserved for the runtime's migrate-checklist command (never Coordinate).
    """
    if purpose not in {"read", "mutation", "migrate"}:
        raise ValueError(f"unknown checklist purpose: {purpose!r}")
    root = Path(harness_root)
    candidates = checklist_candidates(root)
    has_new = candidates["new"].is_file()
    has_legacy = candidates["legacy"].is_file()

    if has_new and has_legacy:
        raise ChecklistError(
            f"dual checklist authority: both {CHECKLIST_NEW_NAME} and "
            f"{CHECKLIST_LEGACY_NAME} exist under {root}; refusing to pick "
            "either. Keep exactly one authority before mutating.",
            REASON_DUAL_AUTHORITY,
        )
    if purpose == "migrate":
        if has_new:
            raise ChecklistError(
                f"cannot migrate: {CHECKLIST_NEW_NAME} already exists; "
                "migration only accepts a legacy-only checklist",
                REASON_DUAL_AUTHORITY,
            )
        if not has_legacy:
            raise ChecklistError(
                f"cannot migrate: {CHECKLIST_LEGACY_NAME} does not exist",
                REASON_CHECKLIST_MISSING,
            )
        return ResolvedChecklist(candidates["legacy"], "legacy")
    if has_new:
        return ResolvedChecklist(candidates["new"], "new")
    if has_legacy:
        return ResolvedChecklist(candidates["legacy"], "legacy")
    raise ChecklistError(
        f"no checklist found: neither {CHECKLIST_NEW_NAME} nor "
        f"{CHECKLIST_LEGACY_NAME} exists under {root}. Initialize the harness "
        "first (workspace init-harness).",
        REASON_CHECKLIST_MISSING,
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Raw read + parse + full validation
# ---------------------------------------------------------------------------


def read_checklist_bytes(
    harness_root: str | Path,
    *,
    purpose: Literal["read", "mutation"] = "read",
) -> tuple[bytes, ResolvedChecklist]:
    """Return the raw checklist bytes and the resolved path.

    Raises ``ChecklistError`` on none/both; JSON/validation failures are left
    to the caller's parse step so read-only consumers can distinguish an
    unparseable checklist from a missing one.
    """
    resolved = resolve_checklist(harness_root, purpose=purpose)
    return resolved.path.read_bytes(), resolved


def load_checklist(
    harness_root: str | Path,
    *,
    purpose: Literal["read", "mutation"] = "read",
    resolved: ResolvedChecklist | None = None,
) -> tuple[dict[str, Any], ResolvedChecklist]:
    """Read, parse, and fully validate the current checklist.

    Validation failures fail closed: a malformed current checklist is refused
    by every writer and is surfaced (not silently repaired) to readers.
    """
    resolved = resolved or resolve_checklist(harness_root, purpose=purpose)
    try:
        raw = resolved.path.read_bytes()
    except OSError as exc:
        raise ChecklistError(
            f"checklist {resolved.path} cannot be read: {exc}",
            REASON_VALIDATION_ERROR,
        ) from exc
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ChecklistError(
            f"checklist {resolved.path} is not valid JSON: {exc}",
            REASON_VALIDATION_ERROR,
        ) from exc
    if not isinstance(data, dict):
        raise ChecklistError(
            f"checklist {resolved.path} root must be a JSON object",
            REASON_VALIDATION_ERROR,
        )
    errors, _ = validate_checklist(data)
    if errors:
        raise ChecklistError(
            "current checklist is invalid; refusing to use it: "
            f"{resolved.path}\n" + "\n".join(f"  - {error}" for error in errors[:8]),
            REASON_VALIDATION_ERROR,
        )
    return data, resolved


# ---------------------------------------------------------------------------
# Plan-locator Coordinate adapter
# ---------------------------------------------------------------------------

# Safe item id rule shared with the managed runtime: a non-empty single safe
# path component (no separators, not '.'/'..', no Unicode C* characters).


def safe_item_id_problem(item_id: Any) -> str | None:
    if not isinstance(item_id, str):
        return f"item id must be a string, got {type(item_id).__name__}"
    if not item_id.strip():
        return "item id must be a non-empty string"
    if item_id in (".", ".."):
        return f"item id {item_id!r} is not a safe path component"
    if "/" in item_id or "\\" in item_id:
        return f"item id {item_id!r} contains a path separator"
    for char in item_id:
        if unicodedata.category(char).startswith("C"):
            return (
                f"item id {item_id!r} contains a control/format/surrogate "
                f"character (U+{ord(char):04X})"
            )
    return None


def item_plan_locator_fields(item: dict[str, Any]) -> list[tuple[str, str]]:
    """Non-empty plan_path / artifacts.plan locators on an item."""
    fields: list[tuple[str, str]] = []
    for key, value in (
        ("plan_path", item.get("plan_path")),
        ("artifacts.plan", (item.get("artifacts") or {}).get("plan")),
    ):
        if isinstance(value, str) and value.strip():
            fields.append((key, value.strip()))
    return fields


def checklist_runtime_problems(checklist: dict[str, Any]) -> list[str]:
    """Shared runtime authority check for every checklist item.

    Coordinate-managed items must carry a single non-conflicting plan locator:
    safe item ids, string-or-null locator fields, no lexical '..', and no
    dual-locator conflict. This is the Coordinate adapter of the U1 runtime
    check; it lives here (the boundary), never in the schema validator.
    """
    problems: list[str] = []
    for item in checklist.get("items", []):
        if not isinstance(item, dict):
            continue
        id_problem = safe_item_id_problem(item.get("id"))
        if id_problem:
            problems.append(id_problem)
        plan_path = item.get("plan_path")
        if plan_path is not None and not isinstance(plan_path, str):
            problems.append(
                f"item {item.get('id')!r} plan_path must be a string or null, "
                f"got {type(plan_path).__name__}"
            )
        artifacts = item.get("artifacts")
        if isinstance(artifacts, dict) and artifacts.get("plan") is not None:
            artifacts_plan = artifacts["plan"]
            if not isinstance(artifacts_plan, str):
                problems.append(
                    f"item {item.get('id')!r} artifacts.plan must be a string or null, "
                    f"got {type(artifacts_plan).__name__}"
                )
        fields = item_plan_locator_fields(item)
        norms: set[str] = set()
        for key, raw in fields:
            path = Path(raw)
            if ".." in path.parts:
                problems.append(
                    f"item {item.get('id')!r} {key} locator contains '..': {raw!r}"
                )
                continue
            norms.add(os.path.normpath(str(path)))
        if len(norms) > 1:
            details = "; ".join(f"{key}={raw!r}" for key, raw in fields)
            problems.append(
                f"item {item.get('id')!r} has conflicting plan locators: {details}"
            )
    return problems


# ---------------------------------------------------------------------------
# Initial phase projection
# ---------------------------------------------------------------------------


def initial_projection(phase: str) -> tuple[str, str]:
    """Map a create-time phase to the single initial (coarse, workflow) pair.

    Create only ever produces ``(todo, todo)``. Reserved lifecycle and
    terminal phases fail closed: they must be reached through the dedicated
    lifecycle commands, never fabricated by a create/init/materialize.
    Arbitrary project planning labels (e.g. ``phase-8``) are accepted as
    todo/todo; the original label survives only in item ``phase`` metadata.
    """
    if not isinstance(phase, str) or not phase.strip():
        raise ChecklistError(
            "phase is required for create", REASON_PHASE_NOT_CREATABLE
        )
    if phase != phase.strip():
        # Surrounding whitespace must not smuggle reserved lifecycle/terminal
        # phases past the check ("done ", " running", ...).
        raise ChecklistError(
            f"phase must not have leading/trailing whitespace, got {phase!r}",
            REASON_PHASE_NOT_CREATABLE,
        )
    if phase in SAFE_PLANNING_PHASES:
        return "todo", "todo"
    if phase in LIFECYCLE_PHASES:
        raise ChecklistError(
            f"phase {phase!r} cannot be created; it must be reached through "
            "the lifecycle commands (assign/accept/handoff/blocker/unblock/"
            "review/closeout)",
            REASON_PHASE_NOT_CREATABLE,
        )
    if phase == "blocked":
        raise ChecklistError(
            "phase 'blocked' cannot be created; create the item as todo and "
            "then raise the blocker through the blocker lifecycle command",
            REASON_PHASE_NOT_CREATABLE,
        )
    if phase in TERMINAL_PHASES:
        raise ChecklistError(
            f"phase {phase!r} cannot be created; create must not fabricate "
            "terminal history",
            REASON_PHASE_NOT_CREATABLE,
        )
    # Arbitrary planning label: accepted as a fresh todo item.
    return "todo", "todo"


def reconstruct_projection(phase: str) -> tuple[str, str]:
    """Historical creation-time projection for read-only proof verification.

    Used ONLY by ``reconstruct_creation_time_checklist_item`` to reproduce the
    exact item an already-deployed (possibly pre-U2) item had at creation, so
    after-fingerprint proofs do not false-positive on items whose create-time
    phase predates the initial projection contract. Never used for authoring.
    """
    if phase in {"done", "closed", "released"}:
        workflow = "closed" if phase in {"done", "closed"} else "released"
        return "done", workflow
    if phase == "blocked":
        return "blocked", "blocked"
    if phase in LIFECYCLE_PHASES or phase == "assigned":
        return "doing", phase
    return "todo", "todo"


# ---------------------------------------------------------------------------
# Per-checklist lock (moved from split_operations)
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
                raise ChecklistError(
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
# Atomic writer (unique temp, fsync, mode preservation, parent fsync)
# ---------------------------------------------------------------------------

# errno values that unambiguously mean "this platform cannot open/fsync a
# directory fd". Ordinary I/O errors (EACCES, EIO, EBADF, ENOENT, ...) must
# NOT be treated as unsupported: they propagate.
_DIR_FSYNC_UNSUPPORTED_ERRNOS = frozenset(
    candidate
    for candidate in (
        getattr(errno, "ENOTSUP", None),
        getattr(errno, "EOPNOTSUPP", None),
        getattr(errno, "EINVAL", None),
    )
    if candidate is not None
)


def _fsync_dir(directory: Path) -> None:
    """Fsync a directory so a rename is durable.

    Both the open and the fsync stage apply the same policy: errno values
    that mean "directory fds are unsupported on this platform" hit the
    controlled fallback; ordinary I/O errors (EACCES, EIO, ...) propagate.
    The directory fd is always closed.
    """
    try:
        dir_fd = os.open(directory, os.O_RDONLY)
    except OSError as exc:
        if exc.errno in _DIR_FSYNC_UNSUPPORTED_ERRNOS:
            return  # controlled fallback: platform cannot open a directory fd
        raise
    try:
        try:
            os.fsync(dir_fd)
        except OSError as exc:
            if exc.errno in _DIR_FSYNC_UNSUPPORTED_ERRNOS:
                return  # controlled fallback: platform cannot fsync a dir fd
            raise
    finally:
        os.close(dir_fd)


def atomic_write_bytes(path: Path, data: bytes, *, mode: int | None = None) -> None:
    """Crash-safe single-writer write: unique temp, flush+fsync, mode
    preserved, os.replace, parent fsync, temp cleanup on failure.

    Before the commit point (os.replace) any failure leaves the original
    bytes untouched.
    """
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)
    if mode is None and path.exists():
        mode = stat_module.S_IMODE(path.stat().st_mode)
    fd: int | None = None
    tmp_path: Path | None = None
    try:
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=str(directory)
        )
        tmp_path = Path(tmp_name)
        with os.fdopen(fd, "wb") as handle:
            fd = None
            handle.write(data)
            handle.flush()
            if mode is not None:
                # Apply mode before fsync so a single fsync covers data and
                # mode metadata (no second fsync needed).
                os.chmod(tmp_path, mode)
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        tmp_path = None
        _fsync_dir(directory)
    finally:
        if fd is not None:
            os.close(fd)
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except OSError:
                pass


def atomic_write_json(path: Path, data: Any) -> None:
    """Atomic JSON writer for checklist authority and derived state."""
    body = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    atomic_write_bytes(path, body.encode("utf-8"))


# ---------------------------------------------------------------------------
# Init authority resolution (single boundary; onboarding consumes only this)
# ---------------------------------------------------------------------------


def resolve_checklist_for_init(harness_root: str | Path) -> tuple[ResolvedChecklist, bool]:
    """Resolve the single init authority decision: none/new-only/legacy-only/both.

    The init entry points (and the empty-checklist helper) all consume this so
    the authority matrix lives in exactly one place: both fail closed with
    ``dual_authority``; a single existing candidate (new or legacy) must be a
    regular, readable file that passes full schema validation; the none case
    returns the new filename with ``create_new=True``.

    Returns ``(resolved, create_new)`` where ``resolved`` is a
    ``ResolvedChecklist`` (reused model; nothing new is invented).
    """
    root = Path(harness_root)
    candidates = checklist_candidates(root)
    new_path = candidates["new"]
    legacy_path = candidates["legacy"]
    # lexists catches every filesystem entry, including directories and broken
    # symlinks, so a non-regular candidate can never be silently skipped over.
    has_new = os.path.lexists(new_path)
    has_legacy = os.path.lexists(legacy_path)
    if has_new and has_legacy:
        raise ChecklistError(
            f"dual checklist authority under {root}: both "
            f"{CHECKLIST_NEW_NAME} and {CHECKLIST_LEGACY_NAME} exist; refuse to "
            "init over it. Keep exactly one authority first.",
            REASON_DUAL_AUTHORITY,
        )
    if has_new:
        resolved = ResolvedChecklist(new_path, "new")
    elif has_legacy:
        resolved = ResolvedChecklist(legacy_path, "legacy")
    else:
        return ResolvedChecklist(new_path, "new"), True
    if not resolved.path.is_file():
        raise ChecklistError(
            f"checklist candidate {resolved.path} exists but is not a regular "
            "file; refusing to init over it",
            REASON_VALIDATION_ERROR,
        )
    # An existing candidate must be readable and schema-valid before anything
    # else is created around it (fail closed, zero mutation).
    load_checklist(root, purpose="read", resolved=resolved)
    return resolved, False


def create_empty_checklist(
    harness_root: str | Path,
    *,
    project: str,
    harness_root_rel: str,
    updated_at: str | None = None,
) -> Path:
    """Create a new empty checklist through the unified validator + atomic writer.

    Only the init entry points call this, and only after the none-case
    preflight. The helper itself delegates to the same
    ``resolve_checklist_for_init`` boundary and writes only in the none case:
    any existing authority (new-only, legacy-only, or both — including a
    directory or broken symlink) is refused by that boundary with a stable
    reason, so calling it directly can never manufacture a second authority.
    The payload is validated with the same ``validate_checklist`` every writer
    uses and committed with ``atomic_write_json`` — init never writes the
    authority with a bare ``write_text``.

    ``harness_root_rel`` is the serialized root locator stored in the
    checklist's ``harness_root`` field: workspace-relative for in-workspace
    minimal roots, or an absolute serialized locator for external minimal
    roots. Returns the created path.
    """
    root = Path(harness_root)
    resolved, create_new = resolve_checklist_for_init(root)
    if not create_new:
        raise ChecklistError(
            f"cannot create empty checklist: {resolved.kind} checklist "
            f"{resolved.path} already exists under {root}; a legacy-only "
            "harness is reused, never shadowed by a new checklist",
            REASON_VALIDATION_ERROR,
        )
    data = {
        "project": project,
        "harness_root": harness_root_rel,
        "version": 1,
        "updated_at": updated_at or _today_utc(),
        "items": [],
    }
    errors, _ = validate_checklist(data)
    if errors:
        raise ChecklistError(
            "refusing to create an invalid empty checklist:\n"
            + "\n".join(f"  - {error}" for error in errors),
            REASON_VALIDATION_ERROR,
        )
    atomic_write_json(resolved.path, data)
    return resolved.path


# ---------------------------------------------------------------------------
# Mutation pipeline
# ---------------------------------------------------------------------------


def mutate_checklist(
    harness_root: str | Path,
    callback: Callable[[dict[str, Any]], bool],
    *,
    purpose: Literal["mutation"] = "mutation",
    lock_timeout: float = 30.0,
    _lock: ChecklistLock | None = None,
) -> tuple[dict[str, Any], bool]:
    """Single mutation pipeline: resolve -> lock -> validate current ->
    deepcopy -> callback -> validate candidate -> atomic commit.

    The callback mutates the candidate dict in place and returns ``True`` when
    a change was made (``False`` leaves the file untouched, preserving bytes
    and mtime for idempotent retries). Any failure before the commit point
    leaves the original bytes untouched.

    Returns ``(candidate, changed)``.
    """
    resolved = resolve_checklist(harness_root, purpose=purpose)
    lock = _lock or ChecklistLock(resolved.path, timeout=lock_timeout)
    with lock:
        current, _ = load_checklist(harness_root, purpose=purpose, resolved=resolved)
        candidate = deepcopy(current)
        changed = callback(candidate)
        if not changed:
            return candidate, False
        candidate["updated_at"] = _today_utc()
        errors, _ = validate_checklist(candidate)
        if errors:
            raise ChecklistError(
                "candidate checklist is invalid after mutation; nothing "
                f"written: {resolved.path}\n"
                + "\n".join(f"  - {error}" for error in errors[:8]),
                REASON_VALIDATION_ERROR,
            )
        problems = checklist_runtime_problems(candidate)
        if problems:
            raise ChecklistError(
                "candidate checklist has runtime authority problems; nothing "
                f"written: {resolved.path}\n"
                + "\n".join(f"  - {problem}" for problem in problems),
                REASON_VALIDATION_ERROR,
            )
        mode = (
            stat_module.S_IMODE(resolved.path.stat().st_mode)
            if resolved.path.exists()
            else None
        )
        body = json.dumps(candidate, ensure_ascii=False, indent=2) + "\n"
        atomic_write_bytes(resolved.path, body.encode("utf-8"), mode=mode)
        return candidate, True
