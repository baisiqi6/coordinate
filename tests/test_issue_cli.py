"""Boundary and delegation tests for the extracted issue CLI owner."""
from __future__ import annotations

import argparse
import ast
import contextlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import coordinate.cli
import coordinate.issue_cli
from coordinate.cli import build_parser, main


SRC_PATH = Path(__file__).resolve().parents[1] / "src"

def _run_subprocess_script(script: str, env: dict[str, str] | None = None) -> str:
    base_env = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(SRC_PATH),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    if env:
        base_env.update(env)
    result = subprocess.run(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=base_env,
        check=True,
    )
    return result.stdout


class IssueCLIOwnershipTests(unittest.TestCase):
    """Tests that ownership moved cleanly from coordinate.cli to coordinate.issue_cli."""

    def test_root_aliases_are_object_identical_to_issue_cli_handlers(self) -> None:
        self.assertIs(coordinate.cli.handle_issue_scan, coordinate.issue_cli.handle_issue_scan)
        self.assertIs(coordinate.cli.handle_issue_triage, coordinate.issue_cli.handle_issue_triage)
        self.assertIs(coordinate.cli.handle_issue_materialize, coordinate.issue_cli.handle_issue_materialize)
        self.assertIs(
            coordinate.cli.handle_issue_materialize_files,
            coordinate.issue_cli.handle_issue_materialize_files,
        )
        self.assertIs(
            coordinate.cli.handle_issue_materialize_record,
            coordinate.issue_cli.handle_issue_materialize_record,
        )

    def test_issue_cli_does_not_import_root_cli(self) -> None:
        script = """
import sys
import coordinate.issue_cli
if 'coordinate.cli' in sys.modules:
    raise SystemExit('issue_cli imported coordinate.cli')
print('ok')
"""
        self.assertIn("ok", _run_subprocess_script(script))

    def test_clean_import_orders_succeed(self) -> None:
        orders = [
            ["coordinate.cli", "coordinate.cli_support", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.pr_cli", "coordinate.issue_cli"],
            ["coordinate.cli_support", "coordinate.cli", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.issue_cli", "coordinate.pr_cli"],
            ["coordinate.issue_cli", "coordinate.cli_support", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.pr_cli", "coordinate.cli"],
            ["coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.issue_cli", "coordinate.cli", "coordinate.cli_support", "coordinate.pr_cli"],
            ["coordinate.planning_cli", "coordinate.pr_cli", "coordinate.issue_cli", "coordinate.cli", "coordinate.cli_support", "coordinate.workspace_cli"],
            ["coordinate.cli_support", "coordinate.pr_cli", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.issue_cli", "coordinate.cli"],
        ]
        for order in orders:
            script = "; ".join(f"import {name}" for name in order) + "; print('ok')"
            with self.subTest(order=order):
                self.assertIn("ok", _run_subprocess_script(script))

    def test_issue_parser_position_after_merge_before_job(self) -> None:
        parser = build_parser()
        subparsers = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
        self.assertEqual(len(subparsers), 1)
        names = list(subparsers[0].choices.keys())
        self.assertGreater(names.index("issue"), names.index("merge"))
        self.assertLess(names.index("issue"), names.index("job"))

    def test_registrar_preserves_five_leaf_order_and_ownership(self) -> None:
        parser = build_parser()
        issue_parser = parser._subparsers._group_actions[0].choices["issue"]
        sub = issue_parser._subparsers._group_actions[0]
        names = list(sub.choices.keys())
        self.assertEqual(names, ["scan", "triage", "materialize", "materialize-files", "materialize-record"])
        expected = {
            "scan": coordinate.issue_cli.handle_issue_scan,
            "triage": coordinate.issue_cli.handle_issue_triage,
            "materialize": coordinate.issue_cli.handle_issue_materialize,
            "materialize-files": coordinate.issue_cli.handle_issue_materialize_files,
            "materialize-record": coordinate.issue_cli.handle_issue_materialize_record,
        }
        for name, handler in expected.items():
            leaf = sub.choices[name]
            self.assertIs(leaf.get_default("handler"), handler)

    def test_no_unapproved_leaf_owned_by_issue_cli(self) -> None:
        parser = build_parser()
        approved = {
            "issue scan",
            "issue triage",
            "issue materialize",
            "issue materialize-files",
            "issue materialize-record",
        }

        def walk(p, path):
            sub = next((a for a in p._actions if isinstance(a, argparse._SubParsersAction)), None)
            if sub is None:
                full = " ".join(path)
                handler = p.get_default("handler")
                if handler is not None and getattr(handler, "__module__", "") == "coordinate.issue_cli":
                    self.assertIn(full, approved)
                return
            for name, child in sub.choices.items():
                walk(child, path + [name])

        walk(parser, [])

    def test_root_aliases_point_to_issue_cli_module(self) -> None:
        for name in (
            "handle_issue_scan",
            "handle_issue_triage",
            "handle_issue_materialize",
            "handle_issue_materialize_files",
            "handle_issue_materialize_record",
        ):
            obj = getattr(coordinate.cli, name)
            self.assertEqual(obj.__module__, "coordinate.issue_cli")

    def test_root_source_has_no_moved_issue_handler_definitions(self) -> None:
        cli_source = (SRC_PATH / "coordinate" / "cli.py").read_text()
        cli_module = ast.parse(cli_source)
        defined = {node.name for node in ast.walk(cli_module) if isinstance(node, ast.FunctionDef)}
        for name in (
            "handle_issue_scan",
            "handle_issue_triage",
            "handle_issue_materialize",
            "handle_issue_materialize_files",
            "handle_issue_materialize_record",
        ):
            self.assertNotIn(name, defined)

    def test_root_has_no_issue_only_service_imports(self) -> None:
        issue_only = {
            "IssueTriageError",
            "materialize_issue",
            "materialize_issue_files",
            "materialize_issue_record",
            "scan_github_issues",
            "scan_github_issues_via_event_cli",
            "triage_issue",
        }
        for name in issue_only:
            self.assertNotIn(name, coordinate.cli.__dict__)


class IssueCLIDelegationTests(unittest.TestCase):
    """Tests that handlers delegate to the correct service seams and stay isolated."""

    def run_cli(self, *args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(list(args))
        return code, json.loads(stdout.getvalue()) if stdout.getvalue() else None, stdout.getvalue(), stderr.getvalue()

    def _args(self, **kwargs):
        return SimpleNamespace(**kwargs)

    def test_scan_with_event_cli_path_does_not_open_local_db(self) -> None:
        result = Mock()
        result.to_dict.return_value = {"scanned": True}
        event_cli_called = []
        db_called = []

        def fake_scan_via_event_cli(**kwargs):
            event_cli_called.append(kwargs)
            return result

        def fake_scan_github_issues(*args, **kwargs):
            db_called.append((args, kwargs))
            return result

        with patch("coordinate.issue_cli.scan_github_issues_via_event_cli", fake_scan_via_event_cli):
            with patch("coordinate.issue_cli.scan_github_issues", fake_scan_github_issues):
                with patch("coordinate.issue_cli._conn") as mock_conn:
                    code = coordinate.issue_cli.handle_issue_scan(
                        self._args(
                            workspace_id="demo",
                            repo="acme/repo",
                            label="bug",
                            limit=10,
                            actor="github",
                            event_cli_path="/fake/coord-ssh",
                        )
                    )
        self.assertEqual(code, 0)
        self.assertEqual(len(event_cli_called), 1)
        self.assertEqual(event_cli_called[0]["event_cli_path"], "/fake/coord-ssh")
        self.assertEqual(db_called, [])
        mock_conn.assert_not_called()

    def test_scan_without_event_cli_path_uses_db_seam(self) -> None:
        result = Mock()
        result.to_dict.return_value = {"created": 1, "existing": 0}
        conn = Mock()
        scan_called = []

        def fake_scan(conn_arg, **kwargs):
            scan_called.append((conn_arg, kwargs))
            return result

        @contextlib.contextmanager
        def fake_conn(args):
            yield conn

        with patch("coordinate.issue_cli.scan_github_issues", fake_scan):
            with patch("coordinate.issue_cli._conn", fake_conn):
                code = coordinate.issue_cli.handle_issue_scan(
                    self._args(
                        workspace_id="demo",
                        repo="acme/repo",
                        label="bug",
                        limit=10,
                        actor="github",
                        event_cli_path=None,
                        db=":memory:",
                    )
                )
        self.assertEqual(code, 0)
        self.assertEqual(len(scan_called), 1)
        self.assertIs(scan_called[0][0], conn)

    def test_triage_error_prints_json_and_returns_one(self) -> None:
        @contextlib.contextmanager
        def fake_conn(args):
            yield Mock()

        with patch("coordinate.issue_cli._conn", fake_conn):
            with patch(
                "coordinate.issue_cli.triage_issue",
                side_effect=coordinate.issues.IssueTriageError("bad decision"),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    code = coordinate.issue_cli.handle_issue_triage(
                        self._args(
                            workspace_id="demo",
                            event_id="evt-1",
                            decision="accept",
                            task_id=None,
                            title=None,
                            owner=None,
                            phase="phase-8",
                            actor="operator",
                            reason=None,
                            platform=None,
                            destination=None,
                            db=":memory:",
                        )
                    )
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(stdout.getvalue()), {"error": {"message": "bad decision"}})

    def test_triage_success_prints_result(self) -> None:
        result = Mock()
        result.to_dict.return_value = {"triage": "accepted"}

        @contextlib.contextmanager
        def fake_conn(args):
            yield Mock()

        with patch("coordinate.issue_cli._conn", fake_conn):
            with patch("coordinate.issue_cli.triage_issue", return_value=result):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    code = coordinate.issue_cli.handle_issue_triage(
                        self._args(
                            workspace_id="demo",
                            event_id="evt-1",
                            decision="accept",
                            task_id=None,
                            title=None,
                            owner=None,
                            phase="phase-8",
                            actor="operator",
                            reason=None,
                            platform=None,
                            destination=None,
                            db=":memory:",
                        )
                    )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {"result": {"triage": "accepted"}})

    def test_materialize_success(self) -> None:
        result = Mock()
        result.to_dict.return_value = {"materialized": True}

        @contextlib.contextmanager
        def fake_conn(args):
            yield Mock()

        with patch("coordinate.issue_cli._conn", fake_conn):
            with patch("coordinate.issue_cli.materialize_issue", return_value=result):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    code = coordinate.issue_cli.handle_issue_materialize(
                        self._args(
                            workspace_id="demo",
                            event_id="evt-1",
                            plan_doc="plan.md",
                            task_id=None,
                            title=None,
                            owner=None,
                            branch=None,
                            phase="ready",
                            actor="operator",
                            platform=None,
                            destination=None,
                            allow_runtime_copy=False,
                            db=":memory:",
                        )
                    )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {"result": {"materialized": True}})

    def test_materialize_files_opt_guard_blocks(self) -> None:
        with patch(
            "coordinate.issue_cli.materialize_issue_files",
            side_effect=coordinate.issues.IssueTriageError("/opt runtime copy guard"),
        ):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = coordinate.issue_cli.handle_issue_materialize_files(
                    self._args(
                        workspace_path="/opt/demo",
                        harness_root="/opt/demo",
                        workspace_id="demo",
                        operation_id="12345678-1234-1234-1234-123456789abc",
                        event_id="22345678-1234-1234-1234-123456789abc",
                        task_id="task-1",
                        plan_doc="plan.md",
                        title=None,
                        phase="ready",
                        priority="p1",
                        allow_runtime_copy=False,
                    )
                )
        self.assertEqual(code, 1)
        self.assertIn("/opt runtime copy guard", json.loads(stdout.getvalue())["error"]["message"])

    def test_materialize_files_success(self) -> None:
        result = Mock()
        result.to_dict.return_value = {"files": "synced"}
        called = []

        def fake_files(**kwargs):
            called.append(kwargs)
            return result

        with patch("coordinate.issue_cli.materialize_issue_files", fake_files):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = coordinate.issue_cli.handle_issue_materialize_files(
                    self._args(
                        workspace_path="/tmp/demo",
                        harness_root="/tmp/demo",
                        workspace_id="demo",
                        operation_id="12345678-1234-1234-1234-123456789abc",
                        event_id="22345678-1234-1234-1234-123456789abc",
                        task_id="task-1",
                        plan_doc="plan.md",
                        title=None,
                        phase="ready",
                        priority="p1",
                        allow_runtime_copy=False,
                    )
                )
        self.assertEqual(code, 0)
        self.assertEqual(len(called), 1)
        self.assertEqual(called[0]["workspace_path"], "/tmp/demo")
        self.assertEqual(called[0]["workspace_id"], "demo")
        self.assertEqual(called[0]["operation_id"], "12345678-1234-1234-1234-123456789abc")
        self.assertEqual(called[0]["event_id"], "22345678-1234-1234-1234-123456789abc")
        self.assertEqual(json.loads(stdout.getvalue()), {"result": {"files": "synced"}})

    def test_materialize_record_error(self) -> None:
        @contextlib.contextmanager
        def fake_conn(args):
            yield Mock()

        with patch("coordinate.issue_cli._conn", fake_conn):
            with patch(
                "coordinate.issue_cli.materialize_issue_record",
                side_effect=coordinate.issues.IssueTriageError("missing event"),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    code = coordinate.issue_cli.handle_issue_materialize_record(
                        self._args(
                            workspace_id="demo",
                            event_id="evt-1",
                            plan_doc="plan.md",
                            operation_id="12345678-1234-1234-1234-123456789abc",
                            input_fingerprint="a" * 64,
                            before_fingerprint="b" * 64,
                            after_fingerprint="c" * 64,
                            task_id=None,
                            title=None,
                            owner=None,
                            branch=None,
                            phase=None,
                            actor="operator",
                            platform=None,
                            destination=None,
                            db=":memory:",
                        )
                    )
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(stdout.getvalue()), {"error": {"message": "missing event"}})

    def test_materialize_record_success(self) -> None:
        result = Mock()
        result.to_dict.return_value = {"record": "written"}
        called = []

        @contextlib.contextmanager
        def fake_conn(args):
            yield Mock()

        def fake_record(conn, **kwargs):
            called.append(kwargs)
            return result

        with patch("coordinate.issue_cli._conn", fake_conn):
            with patch("coordinate.issue_cli.materialize_issue_record", fake_record):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    code = coordinate.issue_cli.handle_issue_materialize_record(
                        self._args(
                            workspace_id="demo",
                            event_id="evt-1",
                            plan_doc="plan.md",
                            operation_id="12345678-1234-1234-1234-123456789abc",
                            input_fingerprint="a" * 64,
                            before_fingerprint="b" * 64,
                            after_fingerprint="c" * 64,
                            task_id=None,
                            title=None,
                            owner=None,
                            branch=None,
                            phase=None,
                            actor="operator",
                            platform=None,
                            destination=None,
                            db=":memory:",
                        )
                    )
        self.assertEqual(code, 0)
        self.assertEqual(len(called), 1)
        self.assertEqual(called[0]["operation_id"], "12345678-1234-1234-1234-123456789abc")
        self.assertEqual(called[0]["input_fingerprint"], "a" * 64)
        self.assertEqual(json.loads(stdout.getvalue()), {"result": {"record": "written"}})

    def test_materialize_files_split_error_includes_reason(self) -> None:
        with patch(
            "coordinate.issue_cli.materialize_issue_files",
            side_effect=coordinate.issues.IssueTriageError(
                "missing plan", reason="files_not_deployed"
            ),
        ):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = coordinate.issue_cli.handle_issue_materialize_files(
                    self._args(
                        workspace_path="/tmp/demo",
                        harness_root="/tmp/demo",
                        workspace_id="demo",
                        operation_id="12345678-1234-1234-1234-123456789abc",
                        event_id="22345678-1234-1234-1234-123456789abc",
                        task_id="task-1",
                        plan_doc="plan.md",
                        title=None,
                        phase="ready",
                        priority="p1",
                        allow_runtime_copy=False,
                    )
                )
        self.assertEqual(code, 1)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {"error": {"message": "missing plan", "reason": "files_not_deployed"}},
        )

    def test_materialize_record_split_error_includes_reason(self) -> None:
        @contextlib.contextmanager
        def fake_conn(args):
            yield Mock()

        with patch("coordinate.issue_cli._conn", fake_conn):
            with patch(
                "coordinate.issue_cli.materialize_issue_record",
                side_effect=coordinate.issues.IssueTriageError(
                    "envelope drift", reason="fingerprint_drift"
                ),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    code = coordinate.issue_cli.handle_issue_materialize_record(
                        self._args(
                            workspace_id="demo",
                            event_id="evt-1",
                            plan_doc="plan.md",
                            operation_id="12345678-1234-1234-1234-123456789abc",
                            input_fingerprint="a" * 64,
                            before_fingerprint="b" * 64,
                            after_fingerprint="c" * 64,
                            task_id=None,
                            title=None,
                            owner=None,
                            branch=None,
                            phase=None,
                            actor="operator",
                            platform=None,
                            destination=None,
                            db=":memory:",
                        )
                    )
        self.assertEqual(code, 1)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {"error": {"message": "envelope drift", "reason": "fingerprint_drift"}},
        )

    def test_triage_error_without_reason_keeps_message_only(self) -> None:
        with patch(
            "coordinate.issue_cli.triage_issue",
            side_effect=coordinate.issues.IssueTriageError("bad decision"),
        ):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = coordinate.issue_cli.handle_issue_triage(
                    self._args(
                        workspace_id="demo",
                        event_id="evt-1",
                        decision="accept",
                        task_id=None,
                        title=None,
                        owner=None,
                        phase="phase-8",
                        actor="operator",
                        reason=None,
                        platform=None,
                        destination=None,
                        db=":memory:",
                    )
                )
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(stdout.getvalue()), {"error": {"message": "bad decision"}})

    def test_materialize_combined_error_does_not_include_reason(self) -> None:
        """Legacy combined `issue materialize` must keep the historical error shape."""
        @contextlib.contextmanager
        def fake_conn(args):
            yield Mock()

        with patch("coordinate.issue_cli._conn", fake_conn):
            with patch(
                "coordinate.issue_cli.materialize_issue",
                side_effect=coordinate.issues.IssueTriageError(
                    "bad plan", reason="files_not_deployed"
                ),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    code = coordinate.issue_cli.handle_issue_materialize(
                        self._args(
                            workspace_id="demo",
                            event_id="evt-1",
                            plan_doc="plan.md",
                            task_id=None,
                            title=None,
                            owner=None,
                            branch=None,
                            phase="ready",
                            actor="operator",
                            platform=None,
                            destination=None,
                            allow_runtime_copy=False,
                            db=":memory:",
                        )
                    )
        self.assertEqual(code, 1)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {"error": {"message": "bad plan"}},
        )

    def test_materialize_files_opt_guard_cli_reason_validation_error(self) -> None:
        """Real host-aware files path: /opt guard surfaces validation_error with reason."""
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = coordinate.issue_cli.handle_issue_materialize_files(
                self._args(
                    workspace_path="/opt/demo",
                    harness_root="/opt/demo",
                    workspace_id="demo",
                    operation_id="12345678-1234-1234-1234-123456789abc",
                    event_id="22345678-1234-1234-1234-123456789abc",
                    task_id="task-1",
                    plan_doc="plan.md",
                    title=None,
                    phase="ready",
                    priority="p1",
                    allow_runtime_copy=False,
                )
            )
        self.assertEqual(code, 1)
        error = json.loads(stdout.getvalue())["error"]
        self.assertIn("runtime deployment copy", error["message"])
        self.assertEqual(error["reason"], "validation_error")

    def test_materialize_files_missing_plan_cli_reason_files_not_deployed(self) -> None:
        """Real host-aware files path: missing plan file surfaces files_not_deployed."""
        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(tmp, ignore_errors=True))
        harness_root = Path(tmp) / "docs"
        harness_root.mkdir()
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = coordinate.issue_cli.handle_issue_materialize_files(
                self._args(
                    workspace_path=tmp,
                    harness_root=str(harness_root),
                    workspace_id="demo",
                    operation_id="12345678-1234-1234-1234-123456789abc",
                    event_id="22345678-1234-1234-1234-123456789abc",
                    task_id="task-1",
                    plan_doc="plans/missing.md",
                    title=None,
                    phase="ready",
                    priority="p1",
                    allow_runtime_copy=False,
                )
            )
        self.assertEqual(code, 1)
        error = json.loads(stdout.getvalue())["error"]
        self.assertIn("plan_doc does not exist", error["message"])
        self.assertEqual(error["reason"], "files_not_deployed")

    def test_materialize_files_invalid_operation_id_cli_reason_validation_error(self) -> None:
        """Real host-aware files path: invalid UUID argument surfaces validation_error."""
        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(tmp, ignore_errors=True))
        harness_root = Path(tmp) / "docs"
        harness_root.mkdir()
        plan = Path(tmp) / "plans" / "foo.md"
        plan.parent.mkdir(parents=True)
        plan.write_text("# Plan\n", encoding="utf-8")
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = coordinate.issue_cli.handle_issue_materialize_files(
                self._args(
                    workspace_path=tmp,
                    harness_root=str(harness_root),
                    workspace_id="demo",
                    operation_id="not-a-uuid",
                    event_id="22345678-1234-1234-1234-123456789abc",
                    task_id="task-1",
                    plan_doc="plans/foo.md",
                    title=None,
                    phase="ready",
                    priority="p1",
                    allow_runtime_copy=False,
                )
            )
        self.assertEqual(code, 1)
        error = json.loads(stdout.getvalue())["error"]
        self.assertIn("operation id", error["message"])
        self.assertEqual(error["reason"], "validation_error")


class IssueCLIIntegrationSmokeTests(unittest.TestCase):
    """Light integration smoke through the public CLI entry point."""

    def run_cli(self, *args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(list(args))
        return code, stdout.getvalue(), stderr.getvalue()

    def test_scan_help_lists_event_cli_path(self) -> None:
        parser = build_parser()
        issue_parser = parser._subparsers._group_actions[0].choices["issue"]
        scan_parser = issue_parser._subparsers._group_actions[0].choices["scan"]
        help_text = scan_parser.format_help()
        self.assertIn("--event-cli-path", help_text)

    def test_materialize_files_help_lists_allow_runtime_copy(self) -> None:
        parser = build_parser()
        issue_parser = parser._subparsers._group_actions[0].choices["issue"]
        files_parser = issue_parser._subparsers._group_actions[0].choices["materialize-files"]
        help_text = files_parser.format_help()
        self.assertIn("--allow-runtime-copy", help_text)

    def test_materialize_files_help_lists_workspace_operation_event(self) -> None:
        parser = build_parser()
        issue_parser = parser._subparsers._group_actions[0].choices["issue"]
        files_parser = issue_parser._subparsers._group_actions[0].choices["materialize-files"]
        help_text = files_parser.format_help()
        self.assertIn("--workspace-id", help_text)
        self.assertIn("--operation-id", help_text)
        self.assertIn("--event-id", help_text)

    def test_materialize_record_help_lists_operation_and_fingerprints(self) -> None:
        parser = build_parser()
        issue_parser = parser._subparsers._group_actions[0].choices["issue"]
        record_parser = issue_parser._subparsers._group_actions[0].choices["materialize-record"]
        help_text = record_parser.format_help()
        self.assertIn("--operation-id", help_text)
        self.assertIn("--input-fingerprint", help_text)
        self.assertIn("--before-fingerprint", help_text)
        self.assertIn("--after-fingerprint", help_text)


if __name__ == "__main__":
    unittest.main()
