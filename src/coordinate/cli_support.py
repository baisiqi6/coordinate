"""Shared CLI support helpers used by ``coordinate.cli`` and future registrars.

This module owns the generic persistence connection context manager and JSON
printing semantics that previously lived only in ``coordinate.cli``. It must not
import ``coordinate.cli`` or any domain registrar; dependents import it instead.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from .db import (
    ReadOnlyConnectionError,
    assert_schema_compatible,
    connect_readonly,
    initialize,
)


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


@contextmanager
def open_readonly_connection(
    args: Any,
) -> Generator[Any, None, None]:
    """Open a strict read-only registry connection from ``args.db``.

    Existing-only ``mode=ro`` + ``query_only=ON`` + exact schema gate: the DB
    is never created, migrated, or touched. Common failures (missing DB,
    legacy/unknown schema) surface as ValueError so ``main()`` prints a clean
    ``error:`` line and exits 1; other DB-layer failures are wrapped the same
    way instead of leaking a traceback.
    """
    conn = connect_readonly(Path(args.db).expanduser())
    try:
        assert_schema_compatible(conn)
        yield conn
    except sqlite3.Error as exc:
        raise ReadOnlyConnectionError(
            f"read-only registry query failed: {exc}"
        ) from exc
    finally:
        conn.close()


def print_json(value: Any) -> None:
    """Print ``value`` as UTF-8 JSON with two-space indentation and sorted keys."""
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
