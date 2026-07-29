"""Shared CLI support helpers used by ``coordinate.cli`` and future registrars.

This module owns the generic persistence connection context manager and JSON
printing semantics that previously lived only in ``coordinate.cli``. It must not
import ``coordinate.cli`` or any domain registrar; dependents import it instead.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from .db import initialize


DEFAULT_DB_PATH = "~/.local/share/coordinate/coordinator.sqlite3"


@contextmanager
def open_connection(
    args: Any,
) -> Generator[Any, None, None]:
    """Open a database connection from ``args.db`` and close it on exit."""
    conn = initialize(Path(args.db).expanduser())
    try:
        yield conn
    finally:
        conn.close()


def print_json(value: Any) -> None:
    """Print ``value`` as UTF-8 JSON with two-space indentation and sorted keys."""
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
