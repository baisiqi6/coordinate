"""Host-scoped normalized worktree resource identity.

Input is the already host-resolved ``host_id + worktree_path`` from P9-1. This
module performs **lexical** normalization only: no ``realpath``, no filesystem
probe, no symlink/junction/network inference, and no cwd/env dependency.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


RESOURCE_CONTRACT_VERSION = 1
MAX_PATH_LEN = 4096
MAX_HOST_ID_LEN = 64

_RESOURCE_KEY_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class ResourceIdentityError(ValueError):
    """Raised when a resource identity or stored snapshot is invalid."""


# Reject control characters (including NUL) and empty/relative/too-long paths.
# POSIX: require leading slash.
# Windows drive: require [A-Za-z]: followed by separator or end.
# Windows UNC: require \\\\
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


@dataclass(frozen=True)
class WorktreeResource:
    host_id: str
    normalized_path: str

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "contract_version": RESOURCE_CONTRACT_VERSION,
            "resource_kind": "worktree",
            "host_id": self.host_id,
            "normalized_path": self.normalized_path,
        }


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _validate_host_id(host_id: str) -> str:
    if not isinstance(host_id, str):
        raise ResourceIdentityError("host_id must be a string")
    if not host_id:
        raise ResourceIdentityError("host_id is required")
    if host_id != host_id.strip():
        raise ResourceIdentityError("host_id must not have surrounding whitespace")
    if len(host_id) > MAX_HOST_ID_LEN:
        raise ResourceIdentityError(f"host_id exceeds {MAX_HOST_ID_LEN} characters")
    if _CONTROL_RE.search(host_id):
        raise ResourceIdentityError("host_id contains control characters")
    return host_id


def _is_windows_drive(path: str) -> bool:
    if len(path) < 2:
        return False
    return path[0].isalpha() and path[1] == ":"


def _is_windows_unc(path: str) -> bool:
    return len(path) >= 2 and path[:2] in ("\\\\", "//")


def _has_trailing_slash(path: str) -> bool:
    return len(path) > 1 and path[-1] in ("/", "\\")


def _has_traversal_segments(path: str) -> bool:
    """True when any ``/`` or ``\\`` separated segment is ``.`` or ``..``."""
    return any(seg in {".", ".."} for seg in path.replace("\\", "/").split("/"))


def _path_flavour(normalized: str) -> str:
    """Classify a normalized host-native path flavour: ``posix``/``drive``/``unc``."""
    if normalized.startswith("\\\\"):
        return "unc"
    if len(normalized) >= 2 and normalized[1] == ":":
        return "drive"
    return "posix"


def _is_filesystem_root(normalized: str) -> bool:
    """True when a normalized path is a POSIX root, drive root, or UNC share root."""
    if normalized == "/":
        return True
    if len(normalized) == 3 and normalized[1] == ":" and normalized[2] == "\\":
        return True
    if normalized.startswith("\\\\"):
        non_empty = [part for part in normalized.split("\\") if part]
        return len(non_empty) <= 2
    return False


def normalize_control_path_separators(path: str) -> str:
    """Canonicalize control-plane separators to ``/`` for lexical comparison.

    Control roots and submitted paths may use either separator regardless of
    the control host; every control-root/raw comparison must go through this
    so the classification in ``classify_worktree_raw_path`` and the mapping in
    ``execution_context._map_foreign_path`` cannot drift.
    """
    return path.replace("\\", "/")


def classify_worktree_raw_path(control_root: str, raw_path: str) -> str:
    """Lexically classify a submitted worktree path as ``control`` or ``host_native``.

    Paths equal to or under the control workspace root keep the legacy control
    branch (resolved on the control host and mapped onto the canonical host
    workspace); every other path is a host-native sibling candidate. The prefix
    test is segment-aware and separator-symmetric (``/`` and ``\\`` compare
    equal), so a sibling directory sharing the control-root prefix
    (``/a/b-other`` vs ``/a/b``) is never misclassified.
    """
    control = normalize_control_path_separators(control_root).rstrip("/")
    raw = normalize_control_path_separators(raw_path)
    if raw == control:
        return "control"
    prefix = control + "/"
    if raw.startswith(prefix):
        return "control"
    return "host_native"


def is_strict_descendant(candidate: str, root: str) -> bool:
    """Pure lexical strict-descendant test on normalized host-native paths.

    Both inputs MUST already be normalized via ``normalize_worktree_path``.
    The root itself is never its own descendant, and different path flavours
    never compare as contained because the separator-aware prefix differs.
    """
    if candidate == root:
        return False
    sep = "\\" if _path_flavour(root) in {"drive", "unc"} else "/"
    return candidate.startswith(root.rstrip(sep) + sep)


def normalize_host_native_worktree_path(path: str) -> str:
    """Reject relative/empty/control/traversal forms, then normalize.

    The host-native branch of worktree resolution rejects ``.``/``..``
    segments outright (``normalize_worktree_path`` would silently collapse
    them) before applying the shared resource identity normalization.
    Raises ``ResourceIdentityError`` on any invalid form.
    """
    if not isinstance(path, str) or not path:
        raise ResourceIdentityError("path is required")
    if _CONTROL_RE.search(path):
        raise ResourceIdentityError("path contains control characters")
    if _has_traversal_segments(path):
        raise ResourceIdentityError("path contains traversal components")
    return normalize_worktree_path(path)


def validate_worktree_root(root: str, *, workspace_path: str) -> str:
    """Validate and normalize one allowlisted sibling worktree root.

    Rejects relative, control/NUL/newline, traversal-bearing, and
    filesystem/drive/share-root paths; roots whose path flavour differs from
    the profile's canonical ``workspace_path``; and roots that equal the
    canonical workspace or are an ancestor of it (only canonical descendants
    and true siblings are allowed). Returns the normalized root; raises
    ``ResourceIdentityError`` otherwise.
    """
    normalized = normalize_host_native_worktree_path(root)
    if _is_filesystem_root(normalized):
        raise ResourceIdentityError(
            f"root must not be a filesystem/drive/share root: {root!r}"
        )
    normalized_workspace = normalize_worktree_path(workspace_path)
    if _path_flavour(normalized) != _path_flavour(normalized_workspace):
        raise ResourceIdentityError(
            f"root path flavour must match workspace_path: {root!r}"
        )
    if normalized == normalized_workspace or is_strict_descendant(
        normalized_workspace, normalized
    ):
        raise ResourceIdentityError(
            f"root must not be the canonical workspace_path or an ancestor: {root!r}"
        )
    return normalized


def resolve_allowlisted_worktree_path(path: str, roots: Iterable[str]) -> str:
    """Normalize a host-native worktree candidate and require containment.

    Returns the normalized path only when it is a strict descendant of at
    least one allowlisted root; raises ``ResourceIdentityError`` otherwise.
    ``roots`` must already be normalized and validated.
    """
    normalized = normalize_host_native_worktree_path(path)
    for root in roots:
        if is_strict_descendant(normalized, root):
            return normalized
    raise ResourceIdentityError(
        f"path is outside every configured worktree root: {path!r}"
    )


def normalize_worktree_root_list(
    items: Iterable[Any],
    *,
    workspace_path: str,
    entry_label: str,
    dedupe: bool,
) -> list[str]:
    """Validate and normalize a worktree-root list with a SINGLE shared loop.

    Used by both the profile write path (raw input) and the stored-read path.
    Every item goes through the same ``validate_worktree_root``; the ``dedupe``
    parameter expresses the asymmetry:
    - ``dedupe=True`` (write input): silently keeps the first occurrence,
      order-stable, non-canonical spellings are normalized away;
    - ``dedupe=False`` (stored-canonical read): every entry must already be
      canonical and duplicates fail closed.
    Raises ``ValueError`` on any invalid entry.
    """
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if not isinstance(item, str):
            raise ValueError(f"{entry_label} entries must be strings")
        try:
            normalized = validate_worktree_root(item, workspace_path=workspace_path)
        except ResourceIdentityError as exc:
            raise ValueError(f"invalid {entry_label}: {exc}") from exc
        if not dedupe and normalized != item:
            raise ValueError(
                f"{entry_label} entry is not canonical: {item!r} != {normalized!r}"
            )
        if normalized in seen:
            if not dedupe:
                raise ValueError(f"{entry_label} contains duplicate: {item!r}")
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def parse_normalized_worktree_roots(
    raw: str | None,
    *,
    workspace_path: str,
) -> tuple[str, ...]:
    """Parse stored ``worktree_roots_json``; fail closed on malformed state.

    The stored value must be a JSON array of already-canonical, deduplicated,
    non-root host-native path strings whose path flavour matches the row's
    ``workspace_path``. Stored roots are re-validated through the same
    ``validate_worktree_root`` used at write time, so corruption or manual
    mutation that produces a cross-flavour or over-wide profile fails closed on
    read. Raises ``ValueError`` otherwise.
    """
    if raw is None:
        return ()
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"worktree_roots_json is invalid JSON: {exc}") from exc
    if not isinstance(decoded, list):
        raise ValueError("worktree_roots_json must be a JSON array of strings")
    return tuple(
        normalize_worktree_root_list(
            decoded,
            workspace_path=workspace_path,
            entry_label="worktree_roots_json",
            dedupe=False,
        )
    )


def check_worktree_containment_policy(
    *,
    worktree_path: str,
    canonical_workspace_path: str,
    worktree_roots: Iterable[str],
) -> str:
    """Claim-time policy gate: stored path must be canonical-or-roots contained.

    Pure lexical check of an already-stored normalized worktree path against
    the CURRENT canonical workspace checkout and allowlisted roots. The
    canonical checkout accepts equality or any strict descendant (the control
    branch maps control-root paths onto canonical descendants); allowlisted
    roots accept strict descendants only. Fails closed by raising
    ``ResourceIdentityError``; never probes the filesystem and never
    re-derives the snapshot/digest.
    """
    normalized = normalize_worktree_path(worktree_path)
    canonical = normalize_worktree_path(canonical_workspace_path)
    if normalized == canonical or is_strict_descendant(normalized, canonical):
        return normalized
    for root in worktree_roots:
        if is_strict_descendant(normalized, root):
            return normalized
    raise ResourceIdentityError(
        f"worktree path {worktree_path!r} is not contained in the current "
        "canonical checkout or any allowlisted worktree root"
    )


def normalize_worktree_path(path: str) -> str:
    """Normalize a host-resolved worktree path to a host-scoped lexical identity.

    POSIX: require absolute ``/``, apply ``posixpath.normpath``, Unicode NFC,
    preserve case, preserve root semantics.

    Windows drive/UNC: accept either separator, apply ``ntpath.normpath``,
    canonicalize separators to ``\\``, Unicode NFC plus ``casefold``, and
    preserve drive/UNC root semantics.

    Rejects relative, empty, control/NUL-bearing, and over-4096-character paths.
    """
    if not isinstance(path, str):
        raise ResourceIdentityError("path must be a string")
    path = path.strip()
    if not path:
        raise ResourceIdentityError("path is required")
    if len(path) > MAX_PATH_LEN:
        raise ResourceIdentityError(f"path exceeds {MAX_PATH_LEN} characters")
    if _CONTROL_RE.search(path):
        raise ResourceIdentityError("path contains control characters")

    if _is_windows_unc(path):
        import ntpath

        # Normalize with ntpath, then canonicalize separators to backslash.
        normalized = ntpath.normpath(path)
        if normalized in (".", ""):
            raise ResourceIdentityError("UNC path collapsed to relative")
        normalized = normalized.replace("/", "\\")
        normalized = unicodedata.normalize("NFC", normalized).casefold()
        return normalized

    if _is_windows_drive(path):
        import ntpath

        if len(path) == 2 or (len(path) > 2 and path[2] not in ("\\", "/")):
            raise ResourceIdentityError("Windows drive path must be absolute")
        normalized = ntpath.normpath(path)
        if normalized in (".", ""):
            raise ResourceIdentityError("drive path collapsed to relative")
        normalized = normalized.replace("/", "\\")
        # Ensure drive letter is lowercase for canonical identity.
        drive = normalized[0].lower()
        rest = normalized[2:]
        normalized = drive + ":" + rest
        normalized = unicodedata.normalize("NFC", normalized).casefold()
        return normalized

    # POSIX: require absolute.
    if not path.startswith("/"):
        raise ResourceIdentityError("POSIX path must be absolute")

    import posixpath

    normalized = posixpath.normpath(path)
    if normalized == ".":
        raise ResourceIdentityError("path collapsed to relative")
    # Preserve root ``/``.
    if normalized == "":
        normalized = "/"
    normalized = unicodedata.normalize("NFC", normalized)
    return normalized


def build_worktree_resource(host_id: str, worktree_path: str) -> WorktreeResource:
    """Build a normalized worktree resource identity."""
    host_id = _validate_host_id(host_id)
    normalized_path = normalize_worktree_path(worktree_path)
    return WorktreeResource(host_id=host_id, normalized_path=normalized_path)


def compute_resource_key(resource: WorktreeResource) -> str:
    """Return ``sha256:<digest>`` for the canonical resource object."""
    canonical = _canonical_json(resource.canonical_dict())
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def _resource_key_canonical_dict(resource: dict[str, Any]) -> dict[str, Any]:
    keys = {"contract_version", "resource_kind", "host_id", "normalized_path"}
    if set(resource.keys()) != keys:
        raise ResourceIdentityError(
            f"resource object has incorrect fields: {sorted(resource.keys())}"
        )
    if resource["contract_version"] != RESOURCE_CONTRACT_VERSION:
        raise ResourceIdentityError("resource contract_version must be 1")
    if resource["resource_kind"] != "worktree":
        raise ResourceIdentityError("resource_kind must be 'worktree'")
    return {k: resource[k] for k in sorted(keys)}


def validate_resource_key_matches(resource: dict[str, Any], resource_key: str) -> dict[str, Any]:
    """Validate a stored resource object against its digest.

    Rejects non-canonical paths, malformed host_id, and malformed digests.
    Raises ``ResourceIdentityError`` on malformed stored state.
    """
    if not isinstance(resource_key, str):
        raise ResourceIdentityError("resource_key must be a string")
    if not _RESOURCE_KEY_RE.match(resource_key):
        raise ResourceIdentityError("resource_key must be sha256:<64 lowercase hex>")
    canonical_dict = _resource_key_canonical_dict(resource)
    _validate_host_id(canonical_dict["host_id"])
    normalized = normalize_worktree_path(canonical_dict["normalized_path"])
    if normalized != canonical_dict["normalized_path"]:
        raise ResourceIdentityError(
            f"normalized_path is not canonical: {canonical_dict['normalized_path']!r} != {normalized!r}"
        )
    canonical = _canonical_json(canonical_dict)
    expected = f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"
    if resource_key != expected:
        raise ResourceIdentityError(f"resource digest mismatch: expected {expected}, got {resource_key}")
    return canonical_dict


def redacted_resource_evidence(resource: WorktreeResource) -> dict[str, Any]:
    """Redacted evidence suitable for events: only the resource key and kind."""
    return {
        "resource_kind": "worktree",
        "resource_key": compute_resource_key(resource),
    }


# Convenience alias used by callers that prefer a plain function signature.
resolve_resource = build_worktree_resource
