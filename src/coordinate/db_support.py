"""Pure, dependency-free helpers shared by coordinate.db and coordinate.job_repository.

This module intentionally does NOT import coordinate.db so that job_repository.py
can depend on it without creating an import cycle.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    """Strict UTC timestamp string with microsecond truncated and Z suffix."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_dumps(value: dict[str, Any] | None) -> str:
    """Canonical compact JSON for SQLite payload/result columns."""
    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _absolute_path(path: str | Path, *, base: str | Path | None = None) -> str:
    """Normalize a control-plane path relative to an optional base.

    This resolves paths on the control host. It must NOT be used for mapping
    paths onto a foreign execution host; use the pure-segment foreign path
    helpers in execution_context.py for that case.
    """
    expanded = Path(path).expanduser()
    if base is not None:
        if not expanded.is_absolute():
            expanded = Path(base).expanduser() / expanded
    return str(expanded.resolve())
