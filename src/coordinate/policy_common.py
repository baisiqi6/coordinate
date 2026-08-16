"""Shared server-local auth policy primitives (R2A runtime HTTP + R3 remote MCP).

Both adapters enforce the same fail-closed file and credential rules, so the
strict-file read, digest shape check, digest derivation and timing-safe
comparison live here once. Each adapter keeps its own policy schema and
principal model; this module never interprets policy JSON beyond the shared
file/digest rules.

- ``read_strict_policy_file``: regular file only, never group/world writable,
  read as UTF-8. Missing/unreadable files, non-regular files and bad modes all
  raise ``AuthPolicyError`` so the server fails closed on startup.
- Digests are 64 lowercase hex chars; plaintext tokens are never accepted.
- ``constant_time_digest_equals`` is the only credential comparison used.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import stat
from pathlib import Path

_TOKEN_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


class AuthPolicyError(ValueError):
    """Strict policy file is invalid; the server must fail closed on startup."""


def is_valid_token_digest(value: str) -> bool:
    """Return True only for the accepted 64-lowercase-hex digest shape."""
    return bool(_TOKEN_DIGEST_RE.match(value))


def digest_token(token: str) -> str:
    """Derive the stored digest form of a presented token."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def constant_time_digest_equals(expected: str, supplied: str) -> bool:
    """Timing-safe comparison for digest equality checks."""
    return hmac.compare_digest(expected, supplied)


def read_strict_policy_file(path: str) -> str:
    """Read a server-local policy file with fail-closed stat/read checks.

    Raises ``AuthPolicyError`` when the path is missing, is not a regular
    file, is group/world writable, is not owned by ``root`` or the current
    effective uid (POSIX only; the check is skipped on platforms without
    ``os.geteuid``), or cannot be decoded as UTF-8.
    """
    policy_path = Path(path).expanduser()
    try:
        file_stat = policy_path.stat()
    except OSError as exc:
        raise AuthPolicyError(f"cannot read auth policy: {exc}") from exc
    if not stat.S_ISREG(file_stat.st_mode):
        raise AuthPolicyError("auth policy must be a regular file")
    if hasattr(os, "geteuid"):
        # A root-owned policy readable by the service group is a valid
        # deployment shape, so both root and the effective uid are allowed.
        euid = os.geteuid()
        if file_stat.st_uid != 0 and file_stat.st_uid != euid:
            raise AuthPolicyError(
                "auth policy file must be owned by root or the current user"
            )
    if stat.S_IMODE(file_stat.st_mode) & 0o022:
        raise AuthPolicyError("auth policy file must not be group or world writable")
    try:
        return policy_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise AuthPolicyError(f"cannot read auth policy: {exc}") from exc
