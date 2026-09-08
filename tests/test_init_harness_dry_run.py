"""Real CLI regression: preview must not consume files or task identity."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from coordinate.db import initialize, upsert_workspace
from coordinate.onboarding import init_file_harness
from tests.fixtures.runtime_template import make_template_source


class InitHarnessDryRunTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.plan = self.workspace / "plan.md"
        self.plan.write_text("# Original plan\n", encoding="utf-8")
        self.db = self.root / "registry.sqlite3"
        conn = initialize(self.db)
        upsert_workspace(conn, workspace_id="demo", name="Demo", path=self.workspace,
                         harness_root=self.workspace / "harness", base_branch="none")
        conn.close()
        self.args = ["workspace", "init-harness", "demo", "--mode", "minimal",
                     "--root", "harness", "--task-id", "first", "--plan-doc", "plan.md"]

    def snapshot(self):
        return {str(p.relative_to(self.root)): (
            p.stat().st_mode, p.stat().st_mtime_ns,
            hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None,
        ) for p in [self.root, *self.root.rglob("*")]}

    def run_cli(self, args, expected=0, db=None):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
        result = subprocess.run([sys.executable, "-m", "coordinate", "--db", str(db or self.db), *args],
                                capture_output=True, text=True, env=env, timeout=30)
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        return json.loads(result.stdout) if result.stdout.strip() else {}

    def test_repeated_minimal_preview_is_readonly_then_actual_initializes(self):
        before = self.snapshot()
        for _ in range(2):
            result = self.run_cli([*self.args, "--dry-run"])["result"]
            self.assertEqual(self.snapshot(), before)
            self.assertTrue(result["dry_run"])
            self.assertNotIn("event_created", result)
            self.assertNotIn("event", result)
            self.assertNotIn("task", result)
            self.assertIn(str(self.workspace / "harness/harness-checklist.json"), result["files"])
        result = self.run_cli(self.args)["result"]
        self.assertTrue(result["event_created"])
        self.assertEqual(result["task"]["task_id"], "first")
        conn = sqlite3.connect(self.db)
        self.addCleanup(conn.close)
        self.assertEqual(conn.execute("SELECT count(*) FROM tasks").fetchone()[0], 1)
        self.assertEqual(self.plan.read_text(), "# Original plan\n")

    def test_service_preview_accepts_readonly_connection(self):
        conn = sqlite3.connect(f"{self.db.as_uri()}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        before = self.snapshot()
        result = init_file_harness(conn, workspace_id="demo", root="harness", task_id="first",
                                   plan_doc="plan.md", dry_run=True).to_dict()
        self.assertTrue(result["dry_run"])
        self.assertEqual(self.snapshot(), before)

    def test_missing_database_not_created_in_either_mode(self):
        missing = self.root / "missing/registry.sqlite3"
        before = self.snapshot()
        for mode in ["minimal", "full"]:
            self.run_cli([*self.args, "--mode", mode, "--source", str(self.root / "template"), "--dry-run"], expected=1, db=missing)
            self.assertEqual(self.snapshot(), before)

    def test_old_schema_not_migrated(self):
        old = self.root / "old.sqlite3"
        conn = sqlite3.connect(old)
        conn.execute("PRAGMA user_version = 1")
        conn.close()
        before = self.snapshot()
        self.run_cli([*self.args, "--dry-run"], expected=1, db=old)
        self.assertEqual(self.snapshot(), before)

    def test_required_arguments_are_checked_before_missing_database(self):
        missing = self.root / "missing.sqlite3"
        before = self.snapshot()
        env = os.environ.copy()
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
        result = subprocess.run([sys.executable, "-m", "coordinate", "--db", str(missing),
                                 "workspace", "init-harness", "demo", "--dry-run"],
                                capture_output=True, text=True, env=env, timeout=30)
        self.assertEqual(result.returncode, 1)
        self.assertIn("--root is required", result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_bad_plan_and_status_leave_no_changes(self):
        before = self.snapshot()
        for extra in [["--plan-doc", "missing.md"], ["--plan-doc", "../outside.md"], ["--status", "done"]]:
            self.run_cli([*self.args, *extra, "--dry-run"], expected=1)
            self.assertEqual(self.snapshot(), before)

    def test_existing_task_preview_rejects_without_changes(self):
        self.run_cli(self.args)
        before = self.snapshot()
        self.run_cli([*self.args, "--dry-run"], expected=1)
        self.assertEqual(self.snapshot(), before)

    def test_legacy_checklist_preview_and_real_init(self):
        harness = self.workspace / "harness"
        harness.mkdir()
        legacy = harness / "mvp-checklist.json"
        legacy.write_text(json.dumps({"project": "demo", "harness_root": "harness",
                                     "version": 1, "updated_at": "2026-09-07", "items": []}))
        before = self.snapshot()
        result = self.run_cli([*self.args, "--dry-run"])["result"]
        self.assertEqual(self.snapshot(), before)
        self.assertIn(str(legacy), result["files"])
        self.run_cli(self.args)
        self.assertFalse((harness / "harness-checklist.json").exists())
        self.assertEqual(json.loads(legacy.read_text())["items"][0]["id"], "first")

    def test_dual_authority_rejects_preview_without_changes(self):
        harness = self.workspace / "harness"
        harness.mkdir()
        for name in ["mvp-checklist.json", "harness-checklist.json"]:
            (harness / name).write_text("{}")
        before = self.snapshot()
        self.run_cli([*self.args, "--dry-run"], expected=1)
        self.assertEqual(self.snapshot(), before)

    def test_full_cli_preview_is_readonly_then_actual_initializes(self):
        source = make_template_source(self.root / "template")
        args = ["workspace", "init-harness", "demo", "--mode", "full", "--source", str(source)]
        before = self.snapshot()
        self.run_cli([*args, "--dry-run"])
        self.assertEqual(self.snapshot(), before)
        self.run_cli(args)
        self.assertTrue((self.workspace / "scripts/harness/harnessctl").exists())
