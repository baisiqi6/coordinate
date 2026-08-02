"""Boundary and behavior tests for the P9-0A2b planning CLI extraction."""

from __future__ import annotations

import ast
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import argparse
import sqlite3

import coordinate.cli
import coordinate.planning_cli
from coordinate.cli import build_parser, main
from coordinate.db import append_event, initialize, upsert_task_mirror, upsert_workspace
from coordinate.planning_cli import (
    handle_plan_revise,
    handle_task_handoff,
    register_operator_command,
    register_planning_commands,
)


SRC_PATH = Path(__file__).resolve().parents[1] / "src"
CLI_PATH = SRC_PATH / "coordinate" / "cli.py"
PLANNING_CLI_PATH = SRC_PATH / "coordinate" / "planning_cli.py"

MOVED_HANDLER_NAMES = frozenset(
    [
        "handle_event_append",
        "handle_event_list",
        "handle_task_create",
        "handle_task_create_files",
        "handle_task_create_record",
        "handle_task_handoff",
        "handle_plan_review_request",
        "handle_plan_revise",
        "handle_plan_approve",
        "handle_plan_reject",
        "handle_operator_pending",
    ]
)

PLANNING_LEAF_PATHS = {
    "event append",
    "event list",
    "task create",
    "task create-files",
    "task create-record",
    "task handoff",
    "plan review-request",
    "plan revise",
    "plan approve",
    "plan reject",
    "operator pending",
}


class PlanningModuleBoundaryTests(unittest.TestCase):
    """Module-import and facade identity checks."""

    def test_root_handler_aliases_are_identical_to_planning_handlers(self) -> None:
        for name in MOVED_HANDLER_NAMES:
            with self.subTest(handler=name):
                self.assertIs(
                    getattr(coordinate.cli, name),
                    getattr(coordinate.planning_cli, name),
                )

    def test_planning_cli_does_not_import_cli(self) -> None:
        script = """
import sys
import coordinate.planning_cli
if 'coordinate.cli' in sys.modules:
    raise SystemExit('planning_cli imported coordinate.cli')
print('ok')
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [sys.executable, "-c", script],
                cwd=tmpdir,
                env={
                    "PYTHONPATH": str(SRC_PATH),
                    "PATH": os.environ.get("PATH", ""),
                },
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )
        self.assertIn("ok", result.stdout)

    def test_import_orders_succeed(self) -> None:
        orders = [
            ["coordinate.cli", "coordinate.cli_support", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.pr_cli"],
            ["coordinate.cli_support", "coordinate.cli", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.pr_cli"],
            ["coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.cli", "coordinate.cli_support", "coordinate.pr_cli"],
            ["coordinate.planning_cli", "coordinate.pr_cli", "coordinate.cli", "coordinate.cli_support", "coordinate.workspace_cli"],
            ["coordinate.cli_support", "coordinate.pr_cli", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.cli"],
        ]
        for order in orders:
            script = "; ".join(f"import {name}" for name in order) + "; print('ok')"
            with self.subTest(order=order):
                with tempfile.TemporaryDirectory() as tmpdir:
                    result = subprocess.run(
                        [sys.executable, "-c", script],
                        cwd=tmpdir,
                        env={
                            "PYTHONPATH": str(SRC_PATH),
                            "PATH": os.environ.get("PATH", ""),
                        },
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        check=True,
                    )
                self.assertIn("ok", result.stdout)

    def test_root_no_longer_defines_moved_handlers(self) -> None:
        source = CLI_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        defined = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
        for name in MOVED_HANDLER_NAMES:
            with self.subTest(handler=name):
                self.assertNotIn(name, defined)

    def test_root_no_longer_imports_planning_only_services(self) -> None:
        source = CLI_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        forbidden = {
            "create_plan_task",
            "create_plan_task_files",
            "create_plan_task_record",
            "approve_plan",
            "reject_plan",
            "review_request_plan",
            "list_pending_actions",
            "pending_snapshot_metadata",
            "prepare_handoff",
            "append_event",
            "list_events",
        }
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                imported.update(alias.name for alias in node.names)
        for name in forbidden:
            with self.subTest(name=name):
                self.assertNotIn(name, imported)


class PlanningRegistrationTests(unittest.TestCase):
    """Registrar behavior and canonical parser positions."""

    def _fresh_subcommands(self):
        parser = argparse.ArgumentParser(prog="coordinate")
        return parser, parser.add_subparsers(dest="command")

    def _leaf_handlers(self, parser: argparse.ArgumentParser) -> dict[str, object]:
        result: dict[str, object] = {}

        def walk(p: argparse.ArgumentParser, path: list[str]) -> None:
            subparsers = [a for a in p._actions if isinstance(a, argparse._SubParsersAction)]
            if not subparsers:
                if path:
                    result[" ".join(path)] = p.get_default("handler")
                return
            for name, child in subparsers[0].choices.items():
                walk(child, path + [name])

        walk(parser, [])
        return result

    def test_planning_registrars_add_expected_commands(self) -> None:
        parser, subcommands = self._fresh_subcommands()
        register_planning_commands(subcommands)
        register_operator_command(subcommands)
        handlers = self._leaf_handlers(parser)
        for path in PLANNING_LEAF_PATHS:
            with self.subTest(path=path):
                self.assertIn(path, handlers)
                self.assertEqual(handlers[path].__module__, "coordinate.planning_cli")

    def test_full_parser_preserves_top_level_order(self) -> None:
        parser = build_parser()
        subparsers = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)][0]
        commands = list(subparsers.choices.keys())
        expected = [
            "workspace",
            "state",
            "event",
            "task",
            "plan",
            "runner",
            "reconcile",
            "branch",
            "pr",
            "ci",
            "review",
            "merge",
            "issue",
            "job",
            "delivery",
            "policy",
            "worker",
            "runtime",
            "assignment",
            "operator",
            "serve",
        ]
        self.assertEqual(commands, expected)

    def test_register_planning_commands_called_once(self) -> None:
        parser, subcommands = self._fresh_subcommands()
        register_planning_commands(subcommands)
        subparsers = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)][0]
        self.assertEqual(
            sorted({"event", "task", "plan"} & set(subparsers.choices)),
            ["event", "plan", "task"],
        )

    def test_all_planning_leaves_point_to_moved_handlers(self) -> None:
        parser = build_parser()
        handlers = self._leaf_handlers(parser)
        planning_handlers = {p: h for p, h in handlers.items() if p in PLANNING_LEAF_PATHS}
        self.assertEqual(len(planning_handlers), 11)
        for path, handler in planning_handlers.items():
            with self.subTest(path=path):
                self.assertEqual(handler.__module__, "coordinate.planning_cli")

    def test_no_unapproved_leaf_uses_planning_cli(self) -> None:
        parser = build_parser()
        handlers = self._leaf_handlers(parser)
        for path, handler in handlers.items():
            if handler.__module__ == "coordinate.planning_cli":
                with self.subTest(path=path):
                    self.assertIn(path, PLANNING_LEAF_PATHS)


class TaskHandoffPathTests(unittest.TestCase):
    """Invariant: moving the handler must not change the derived repository root."""

    def test_handle_task_handoff_derives_same_repository_root(self) -> None:
        cli_root = Path(coordinate.cli.__file__).resolve().parents[2]
        planning_root = Path(coordinate.planning_cli.__file__).resolve().parents[2]
        self.assertEqual(cli_root, planning_root)
        self.assertTrue((planning_root / "src" / "coordinate").exists())

    def test_handle_task_handoff_passes_repository_root_to_prepare_handoff(self) -> None:
        expected_root = str(Path(coordinate.planning_cli.__file__).resolve().parents[2])
        conn = Mock()
        result = Mock()
        result.workspace.path = "/tmp/demo"
        result.bootstrap_text = ""
        result.bootstrap_recommended_path = "docs/bootstrap.md"
        result.to_dict.return_value = {"workspace": {"id": "demo"}}

        args = SimpleNamespace(
            db=":memory:",
            workspace_id="demo",
            task_id="t1",
            role="worker",
            required_scope="implementation plan",
            actor="operator",
            idempotency_key=None,
            write_bootstrap=True,
            target_agent=None,
            review_type="code",
        )

        with patch("coordinate.planning_cli.prepare_handoff", return_value=result) as mock:
            with patch("coordinate.planning_cli._conn") as mock_conn:
                mock_conn.return_value.__enter__ = Mock(return_value=conn)
                mock_conn.return_value.__exit__ = Mock(return_value=False)
                handle_task_handoff(args)

        mock_conn.assert_called_once_with(args)
        mock.assert_called_once()
        call_kwargs = mock.call_args.kwargs
        self.assertEqual(call_kwargs["coordinator_path"], expected_root)


class PlanningBehaviorTests(unittest.TestCase):
    """Representative success/error behavior preserved after the move."""

    def run_cli(self, *args):
        code, stdout, _ = self.run_cli_raw(*args)
        return code, json.loads(stdout)

    def run_cli_raw(self, *args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(list(args))
        return code, stdout.getvalue(), stderr.getvalue()

    def _setup_workspace(self, tmp, db_path):
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
        )

    def test_event_append_success_and_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(tmp, db_path)
            code, payload = self.run_cli(
                "--db", db_path,
                "event", "append", "plan.ready",
                "--workspace-id", "demo",
                "--actor", "operator",
                "--payload-json", '{"task_id": "t1"}',
            )
            self.assertEqual(code, 0)
            self.assertTrue(payload["created"])
            self.assertEqual(payload["event"]["event_type"], "plan.ready")

            list_code, list_payload = self.run_cli("--db", db_path, "event", "list", "--workspace-id", "demo")
            self.assertEqual(list_code, 0)
            self.assertEqual(len(list_payload["events"]), 1)

    def test_event_append_invalid_payload_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, stderr = self.run_cli_raw(
                "--db", db_path,
                "event", "append", "plan.ready",
                "--payload-json", "not-json",
            )
            self.assertEqual(code, 1)
            self.assertIn("invalid --payload-json", stderr)

    def test_event_append_non_object_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, stderr = self.run_cli_raw(
                "--db", db_path,
                "event", "append", "plan.ready",
                "--payload-json", "[]",
            )
            self.assertEqual(code, 1)
            self.assertIn("must decode to an object", stderr)

    def test_task_create_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(
                json.dumps({"project": "demo", "harness_root": ".", "version": 1, "updated_at": "2026-07-13", "items": []}),
                encoding="utf-8",
            )
            self._setup_workspace(tmp, db_path)

            code, payload = self.run_cli(
                "--db", db_path,
                "task", "create", "demo",
                "--task-id", "phase-001",
                "--plan-doc", "plan.md",
                "--title", "Phase 001",
            )
            self.assertEqual(code, 0)
            self.assertEqual(payload["result"]["task"]["task_id"], "phase-001")

    def test_task_create_invalid_payload_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, stderr = self.run_cli_raw(
                "--db", db_path,
                "task", "create", "demo",
                "--task-id", "phase-001",
                "--plan-doc", "plan.md",
                "--payload-json", "bad",
            )
            self.assertEqual(code, 1)
            self.assertIn("invalid --payload-json", stderr)

    def test_task_create_non_object_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, stderr = self.run_cli_raw(
                "--db", db_path,
                "task", "create", "demo",
                "--task-id", "phase-001",
                "--plan-doc", "plan.md",
                "--payload-json", "\"string\"",
            )
            self.assertEqual(code, 1)
            self.assertIn("must decode to an object", stderr)

    def test_task_create_files_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(
                json.dumps({"project": "demo", "harness_root": ".", "version": 1, "updated_at": "2026-07-13", "items": []}),
                encoding="utf-8",
            )
            operation_id = str(uuid.uuid4())
            code, payload = self.run_cli(
                "--db", ":memory:",
                "task", "create-files",
                "--workspace-path", tmp,
                "--harness-root", tmp,
                "--workspace-id", "demo",
                "--operation-id", operation_id,
                "--task-id", "phase-001",
                "--plan-doc", "plan.md",
                "--title", "Phase 001",
            )
            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["checklist_changed"])
            self.assertEqual(payload["result"]["operation_id"], operation_id)
            self.assertEqual(payload["result"]["contract_version"], 1)
            self.assertIsNotNone(payload["result"]["input_fingerprint"])

    def test_task_create_record_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            checklist = Path(tmp) / "mvp-checklist.json"
            checklist.write_text(
                json.dumps({"project": "demo", "harness_root": ".", "version": 1, "updated_at": "2026-07-13", "items": []}),
                encoding="utf-8",
            )
            self._setup_workspace(tmp, db_path)

            operation_id = str(uuid.uuid4())
            _, files_payload = self.run_cli(
                "--db", ":memory:",
                "task", "create-files",
                "--workspace-path", tmp,
                "--harness-root", tmp,
                "--workspace-id", "demo",
                "--operation-id", operation_id,
                "--task-id", "phase-001",
                "--plan-doc", "plan.md",
                "--title", "Phase 001",
            )
            result = files_payload["result"]

            code, payload = self.run_cli(
                "--db", db_path,
                "task", "create-record", "demo",
                "--operation-id", operation_id,
                "--input-fingerprint", result["input_fingerprint"],
                "--before-fingerprint", result["before_fingerprint"],
                "--after-fingerprint", result["after_fingerprint"],
                "--task-id", "phase-001",
                "--plan-doc", "plan.md",
                "--title", "Phase 001",
            )
            self.assertEqual(code, 0)
            self.assertEqual(payload["result"]["task"]["task_id"], "phase-001")
            self.assertEqual(payload["result"]["event"]["event_type"], "plan.ready")
            self.assertIsNotNone(payload["result"]["operation"]["record_event_id"])
            checklist_payload = json.loads(checklist.read_text(encoding="utf-8"))
            self.assertEqual(checklist_payload["items"][0]["id"], "phase-001")

    def test_task_handoff_gate_error_unknown_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, stderr = self.run_cli_raw(
                "--db", db_path,
                "task", "handoff", "demo",
                "--task-id", "phase-001",
                "--role", "worker",
            )
            self.assertEqual(code, 1)
            self.assertIn("unknown workspace", stderr)

    def test_plan_review_request_gate_error_unknown_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, stderr = self.run_cli_raw(
                "--db", db_path,
                "plan", "review-request", "demo",
                "--task-id", "phase-001",
            )
            self.assertEqual(code, 1)
            self.assertIn("unknown workspace", stderr)

    def test_plan_approve_gate_error_unknown_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, stderr = self.run_cli_raw(
                "--db", db_path,
                "plan", "approve", "demo",
                "--task-id", "phase-001",
                "--scope", "implementation plan",
            )
            self.assertEqual(code, 1)
            self.assertIn("unknown workspace", stderr)

    def test_plan_reject_gate_error_unknown_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, stderr = self.run_cli_raw(
                "--db", db_path,
                "plan", "reject", "demo",
                "--task-id", "phase-001",
                "--scope", "implementation plan",
                "--reason", "incomplete",
            )
            self.assertEqual(code, 1)
            self.assertIn("unknown workspace", stderr)

    def test_operator_pending_reports_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            conn = initialize(db_path)
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=tmp,
                harness_root=tmp,
            )
            upsert_task_mirror(
                conn,
                workspace_id="demo",
                task_id="t1",
                phase="running",
                owner="mac-codex",
                branch=None,
                pr=None,
                payload={},
            )
            append_event(
                conn,
                workspace_id="demo",
                event_type="plan.ready",
                actor="operator",
                target="worker",
                task_id="t1",
                idempotency_key="demo:t1:plan.ready",
                payload={"task_id": "t1"},
            )
            conn.close()

            code, payload = self.run_cli("--db", db_path, "operator", "pending", "demo")
            self.assertEqual(code, 0)
            self.assertIn("pending_actions", payload)
            self.assertIn("snapshot", payload)
            self.assertIsNotNone(payload["snapshot"]["task_mirror_updated_at"])


class PlanReviseTests(unittest.TestCase):
    """Behavior of the plan revise entry (no-operation revision branch)."""

    def run_cli(self, *args):
        code, stdout, _ = self.run_cli_raw(*args)
        return code, json.loads(stdout)

    def run_cli_raw(self, *args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(list(args))
        return code, stdout.getvalue(), stderr.getvalue()

    def _setup_workspace(self, tmp, db_path):
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp,
            "--harness-root", tmp,
        )

    def _create_split_task(self, tmp, db_path, task_id="phase-001"):
        """File half (create-files) + DB half (create-record); return (plan, operation_id, record_payload)."""
        plan = Path(tmp) / "plan.md"
        plan.write_text("# Plan v1\n", encoding="utf-8")
        checklist = Path(tmp) / "mvp-checklist.json"
        checklist.write_text(
            json.dumps({"project": "demo", "harness_root": ".", "version": 1, "updated_at": "2026-07-13", "items": []}),
            encoding="utf-8",
        )
        operation_id = str(uuid.uuid4())
        _, files_payload = self.run_cli(
            "--db", ":memory:",
            "task", "create-files",
            "--workspace-path", tmp,
            "--harness-root", tmp,
            "--workspace-id", "demo",
            "--operation-id", operation_id,
            "--task-id", task_id,
            "--plan-doc", "plan.md",
            "--title", "Phase 001",
        )
        files = files_payload["result"]
        code, payload = self.run_cli(
            "--db", db_path,
            "task", "create-record", "demo",
            "--operation-id", operation_id,
            "--input-fingerprint", files["input_fingerprint"],
            "--before-fingerprint", files["before_fingerprint"],
            "--after-fingerprint", files["after_fingerprint"],
            "--task-id", task_id,
            "--plan-doc", "plan.md",
            "--title", "Phase 001",
        )
        self.assertEqual(code, 0)
        return plan, operation_id, payload

    def _plan_ready_events(self, db_path):
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT id, payload_json FROM events "
            "WHERE event_type = 'plan.ready' ORDER BY rowid"
        ).fetchall()
        conn.close()
        return [{"id": r[0], "payload_json": r[1]} for r in rows]

    def test_plan_revise_parser_defaults_and_help(self) -> None:
        parser = build_parser()
        ns = parser.parse_args([
            "plan", "revise", "demo",
            "--task-id", "t1",
            "--plan-doc", "plan.md",
        ])
        self.assertEqual(ns.plan_command, "revise")
        self.assertEqual(ns.handler, handle_plan_revise)
        self.assertEqual(ns.workspace_id, "demo")
        self.assertEqual(ns.task_id, "t1")
        self.assertEqual(ns.plan_doc, "plan.md")
        self.assertIsNone(ns.title)
        self.assertIsNone(ns.owner)
        self.assertIsNone(ns.branch)
        self.assertEqual(ns.phase, "ready")
        self.assertEqual(ns.actor, "operator")
        self.assertEqual(ns.target, "worker")
        self.assertIsNone(ns.payload_json)
        self.assertIsNone(ns.idempotency_key)

        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as cm:
                parser.parse_args(["plan", "revise", "--help"])
            self.assertEqual(cm.exception.code, 0)

    def test_plan_revise_split_task_supersedes_and_preserves_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(tmp, db_path)
            plan, operation_id, first = self._create_split_task(tmp, db_path)
            self.assertTrue(first["result"]["event_created"])

            plan.write_text("# Plan v2 revised\n", encoding="utf-8")
            code, payload = self.run_cli(
                "--db", db_path,
                "plan", "revise", "demo",
                "--task-id", "phase-001",
                "--plan-doc", "plan.md",
                "--title", "Phase 001 (revised)",
                "--owner", "mac-codex",
            )
            self.assertEqual(code, 0)
            self.assertEqual(payload["result"]["event"]["event_type"], "plan.ready")
            self.assertTrue(payload["result"]["event_created"])

            events = self._plan_ready_events(db_path)
            self.assertEqual(len(events), 2)
            first_payload = json.loads(events[0]["payload_json"])
            second_payload = json.loads(events[1]["payload_json"])
            self.assertIsNone(first_payload["supersedes_plan_ready_event_id"])
            self.assertEqual(
                second_payload["supersedes_plan_ready_event_id"],
                events[0]["id"],
            )
            self.assertNotEqual(
                first_payload["plan_sha256"],
                second_payload["plan_sha256"],
            )

            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT payload_json FROM tasks "
                "WHERE workspace_id = 'demo' AND task_id = 'phase-001'"
            ).fetchone()
            conn.close()
            mirror = json.loads(row[0])
            self.assertEqual(mirror["split_operation"]["operation_id"], operation_id)
            self.assertEqual(mirror["split_operation"]["operation_kind"], "task.create")
            self.assertEqual(mirror["title"], "Phase 001 (revised)")

    def test_plan_revise_same_revision_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(tmp, db_path)
            plan, _, _ = self._create_split_task(tmp, db_path)

            plan.write_text("# Plan v2 revised\n", encoding="utf-8")
            args = [
                "--db", db_path,
                "plan", "revise", "demo",
                "--task-id", "phase-001",
                "--plan-doc", "plan.md",
            ]
            code, payload = self.run_cli(*args)
            self.assertEqual(code, 0)
            self.assertTrue(payload["result"]["event_created"])
            self.assertEqual(len(self._plan_ready_events(db_path)), 2)

            code, payload = self.run_cli(*args)
            self.assertEqual(code, 0)
            self.assertFalse(payload["result"]["event_created"])
            events = self._plan_ready_events(db_path)
            self.assertEqual(len(events), 2)

    def test_plan_revise_fail_closed_zero_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            plan = Path(tmp) / "plan.md"
            plan.write_text("# Plan\n", encoding="utf-8")
            self._setup_workspace(tmp, db_path)

            cases = [
                ("invalid json", ["demo", "--task-id", "t1", "--plan-doc", "plan.md", "--payload-json", "not-json"], "invalid --payload-json"),
                ("non-object", ["demo", "--task-id", "t1", "--plan-doc", "plan.md", "--payload-json", "[]"], "must decode to an object"),
                ("missing plan", ["demo", "--task-id", "t1", "--plan-doc", "does-not-exist.md"], "plan_doc is not a regular readable file"),
                ("unknown workspace", ["ghost", "--task-id", "t1", "--plan-doc", "plan.md"], "unknown workspace"),
            ]
            for name, argv, needle in cases:
                with self.subTest(case=name):
                    code, stdout, stderr = self.run_cli_raw(
                        "--db", db_path, "plan", "revise", *argv
                    )
                    self.assertEqual(code, 1)
                    self.assertIn(needle, stdout + stderr)
                    conn = sqlite3.connect(db_path)
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], 0)
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 0)
                    conn.close()


if __name__ == "__main__":
    unittest.main()
