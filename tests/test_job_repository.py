"""Tests for the job_repository extraction and cycle-free import boundary."""
from __future__ import annotations

import importlib
import sys
import unittest


class JobRepositoryImportTests(unittest.TestCase):
    def test_db_re_exports_job_repository_symbols(self) -> None:
        import coordinate.db as db
        import coordinate.job_repository as job_repository

        for name in (
            "create_job",
            "get_job",
            "list_jobs",
            "mark_job_started",
            "mark_job_completed",
            "mark_job_cancelled",
        ):
            self.assertIs(
                getattr(db, name),
                getattr(job_repository, name),
                f"coordinate.db.{name} is not coordinate.job_repository.{name}",
            )

    def test_cold_import_job_repository_then_db(self) -> None:
        # Force a fresh import order by clearing cached modules, then restore
        # the previous state so later tests are not affected.
        saved = {
            mod: sys.modules[mod]
            for mod in list(sys.modules)
            if mod.startswith("coordinate")
        }
        for mod in list(saved):
            del sys.modules[mod]
        try:
            importlib.import_module("coordinate.job_repository")
            importlib.import_module("coordinate.db")
        finally:
            sys.modules.update(saved)

    def test_cold_import_db_then_job_repository(self) -> None:
        saved = {
            mod: sys.modules[mod]
            for mod in list(sys.modules)
            if mod.startswith("coordinate")
        }
        for mod in list(saved):
            del sys.modules[mod]
        try:
            importlib.import_module("coordinate.db")
            importlib.import_module("coordinate.job_repository")
        finally:
            sys.modules.update(saved)

    def test_job_repository_does_not_import_db(self) -> None:
        import coordinate.job_repository as job_repository

        self.assertNotIn("coordinate.db", job_repository.__dict__)
        # The module's globals will not contain a reference to db either.
        self.assertIsNone(getattr(job_repository, "db", None))
