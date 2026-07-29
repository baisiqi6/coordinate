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
from typing import Any


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
