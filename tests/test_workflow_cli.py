"""Boundary tests for the workflow control CLI extraction (P9-0A4b)."""
from __future__ import annotations

import argparse
import ast
import contextlib
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import coordinate.cli
import coordinate.completion_cli
import coordinate.workflow_cli
from coordinate.cli import build_parser


SRC_PATH = Path(__file__).resolve().parents[1] / "src"


# SHA-256 of the canonical AST projection for each of the 12 moved functions.
# Generated once from the reviewed start 4526d09 using the accepted projection:
# preserve node types, non-empty fields, scalar values, contexts, and list order
# while dropping only None/empty list/tuple fields.
_CANONICAL_AST_HASHES = {
    "handle_branch_allocate": "8439aff3858a033ade5c50038851b886b522ba663cc00295f8a5c894be82b1e0",
    "handle_ci_check": "b8d47ef70afb0f863cfd617ffd0f255de772ffc293c08fcaa105e2805aca1c3c",
    "handle_review_check": "63f4d093b9dab623dde9c3697157bd70933aad06d1a64f8c3b154b287883bf89",
    "handle_merge_gate": "d1dc4486d1160e3636dbbd3f908de758c56e59c33438ba3ea9c30226f5857ea6",
    "handle_assignment_request": "e186254f57d77fadb4a76acbc241513eeb6df9a2b08b3bc71294b0f88f123554",
    "handle_assignment_accept": "2e2d7641a7b691facff73c8f98820695b7ffe6488e04f5d2baa057efec85e616",
    "handle_assignment_handoff": "6c8f5a8e3bb482dab3bb110f6216eafa61168050bc8a42979b17b6f78fb20f98",
    "handle_assignment_blocker": "5e692869bb9ad87bd6c0099a85b201cfbe711163968b494fab271357e2ea3b72",
    "handle_assignment_unblock": "73c59722e4c49205d9056e7a775bb3149e7177b929a0aa86dd014e447afb4d0c",
    "handle_assignment_closeout": "6dc9a56745ea1d437edf53740136ed88c11f630f28100a4956de4d5817de35af",
    "handle_assignment_review_result": "e6fd179a7a4c0ef1eef6a21240518dfbe9cc8d12e86878de20a3bebdc521473d",
    "handle_assignment_mark_done": "686a82cccdcaba4f82c1de7dc92d2df2d4919f6f122a5f3166799cd3ffc4dcb7",
}

_WORKFLOW_HANDLER_NAMES = list(_CANONICAL_AST_HASHES.keys())
_REGISTRAR_NAMES = [
    "register_branch_command",
    "register_forge_commands",
    "register_assignment_commands",
]


def _capture_json() -> tuple[list[object], contextlib._GeneratorContextManager]:
    captured: list[object] = []

    def capture(obj: object) -> None:
        captured.append(obj)

    return captured, patch("coordinate.workflow_cli._print_json", side_effect=capture)


def _mock_conn() -> contextlib._GeneratorContextManager:
    conn = Mock()
    mock_cm = Mock(
        return_value=Mock(
            __enter__=Mock(return_value=conn),
            __exit__=Mock(return_value=False),
        )
    )
    return patch("coordinate.workflow_cli._conn", mock_cm)


class WorkflowCLIOwnershipTests(unittest.TestCase):
    """Ownership and alias tests for the P9-0A4b extraction."""

    def test_all_moved_handlers_are_object_identical_between_root_and_workflow_cli(self) -> None:
        for name in _WORKFLOW_HANDLER_NAMES:
            with self.subTest(name=name):
                self.assertIs(
                    getattr(coordinate.cli, name),
                    getattr(coordinate.workflow_cli, name),
                )

    def test_all_registrars_are_object_identical_between_root_and_workflow_cli(self) -> None:
        for name in _REGISTRAR_NAMES:
            with self.subTest(name=name):
                self.assertIs(
                    getattr(coordinate.cli, name),
                    getattr(coordinate.workflow_cli, name),
                )

    def test_root_has_no_moved_function_definitions(self) -> None:
        source = Path(coordinate.cli.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        root_function_names = {
            node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
        }
        for name in _WORKFLOW_HANDLER_NAMES:
            with self.subTest(name=name):
                self.assertNotIn(name, root_function_names)
        self.assertIn("main", root_function_names)
        self.assertIn("build_parser", root_function_names)
        self.assertIn("handle_serve", root_function_names)

    def test_root_retains_global_exception_dispatch(self) -> None:
        source = Path(coordinate.cli.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        main = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "main"
        )
        try_node = next(node for node in main.body if isinstance(node, ast.Try))
        handler = try_node.handlers[0]
        self.assertIsInstance(handler.type, ast.Tuple)
        self.assertEqual(
            [item.id for item in handler.type.elts if isinstance(item, ast.Name)],
            [
                "HarnessError",
                "JobError",
                "BusError",
                "PolicyError",
                "ValueError",
                "KeyError",
            ],
        )

    def test_root_retains_other_registrar_aliases(self) -> None:
        for name in (
            "register_pr_commands",
            "register_issue_commands",
            "register_job_commands",
            "register_delivery_commands",
            "register_runtime_commands",
            "register_completion_commands",
            "register_workspace_commands",
            "register_planning_commands",
            "register_operator_command",
        ):
            with self.subTest(name=name):
                self.assertTrue(hasattr(coordinate.cli, name))
                self.assertTrue(callable(getattr(coordinate.cli, name)))

    def test_workflow_cli_exports_all_moved_names(self) -> None:
        for name in _WORKFLOW_HANDLER_NAMES:
            with self.subTest(name=name):
                self.assertTrue(
                    hasattr(coordinate.workflow_cli, name),
                    f"workflow_cli must export {name}",
                )
                self.assertTrue(
                    callable(getattr(coordinate.workflow_cli, name)),
                    f"{name} must be callable",
                )


class WorkflowCLIBackedgeTests(unittest.TestCase):
    """Import-direction tests for the P9-0A4b extraction."""

    def _source(self, module_name: str) -> str:
        module = sys.modules[module_name]
        return Path(module.__file__).read_text(encoding="utf-8")

    def test_workflow_cli_does_not_import_root_or_peer_registrars(self) -> None:
        source = self._source("coordinate.workflow_cli")
        forbidden = [
            "from .cli ",
            "from .pr_cli",
            "from .issue_cli",
            "from .execution_cli",
            "from .delivery_cli",
            "from .runtime_cli",
            "import coordinate.cli",
        ]
        for fragment in forbidden:
            with self.subTest(fragment=fragment):
                self.assertNotIn(fragment, source)

    def test_completion_cli_does_not_import_workflow_cli(self) -> None:
        source = self._source("coordinate.completion_cli")
        self.assertNotIn("workflow_cli", source)

    def test_lower_cli_modules_do_not_import_workflow_cli(self) -> None:
        for module_name in (
            "coordinate.pr_cli",
            "coordinate.issue_cli",
            "coordinate.execution_cli",
            "coordinate.delivery_cli",
        ):
            with self.subTest(module=module_name):
                source = self._source(module_name)
                self.assertNotIn("workflow_cli", source)

    def test_completion_workflow_root_import_orders_succeed_fresh(self) -> None:
        orders = [
            ["coordinate.completion_cli", "coordinate.workflow_cli", "coordinate.cli"],
            ["coordinate.workflow_cli", "coordinate.completion_cli", "coordinate.cli"],
            ["coordinate.cli", "coordinate.workflow_cli", "coordinate.completion_cli"],
        ]
        env = {
            "PYTHONPATH": str(SRC_PATH),
            "PATH": os.environ.get("PATH", ""),
        }
        for order in orders:
            script = "; ".join(f"import {name}" for name in order) + "; print('ok')"
            with self.subTest(order=order), tempfile.TemporaryDirectory() as tmpdir:
                result = subprocess.run(
                    [sys.executable, "-c", script],
                    cwd=tmpdir,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                )
            self.assertEqual(result.stdout.strip(), "ok")


class WorkflowCLIRegistrationTests(unittest.TestCase):
    """Parser registration tests for workflow leaves."""

    def _top_level_command_names(self) -> list[str]:
        parser = build_parser()
        subparser_action = next(
            a for a in parser._actions
            if isinstance(a, argparse._SubParsersAction)
        )
        return list(subparser_action.choices.keys())

    def _build_parser_registrar_calls(self) -> list[str]:
        source = Path(coordinate.cli.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        func = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "build_parser":
                func = node
                break
        assert func is not None
        calls: list[str] = []
        for node in ast.walk(func):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    calls.append(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    calls.append(node.func.attr)
        return calls

    def test_top_level_command_order(self) -> None:
        names = self._top_level_command_names()
        self.assertEqual(
            names,
            [
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
            ],
        )

    def test_registrar_calls_at_exact_seams(self) -> None:
        calls = self._build_parser_registrar_calls()
        expected_order = [
            "register_workspace_commands",
            "register_planning_commands",
            "register_runner_commands",
            "register_reconcile_command",
            "register_branch_command",
            "register_pr_commands",
            "register_forge_commands",
            "register_issue_commands",
            "register_job_commands",
            "register_delivery_commands",
            "register_runtime_commands",
            "register_assignment_commands",
            "register_operator_command",
        ]
        filtered = [c for c in calls if c in expected_order]
        self.assertEqual(filtered, expected_order)

    def test_assignment_leaves_in_exact_order(self) -> None:
        parser = build_parser()
        assignment_action = next(
            a for a in parser._actions
            if isinstance(a, argparse._SubParsersAction) and a.dest == "command"
        )
        assignment_parser = assignment_action.choices["assignment"]
        sub = next(
            a for a in assignment_parser._actions
            if isinstance(a, argparse._SubParsersAction)
        )
        names = list(sub.choices.keys())
        self.assertEqual(
            names,
            [
                "request",
                "accept",
                "handoff",
                "blocker",
                "unblock",
                "closeout",
                "review-result",
                "mark-done",
                "mark-done-prepare",
                "mark-done-preflight",
                "mark-done-claim",
                "mark-done-apply",
                "mark-done-files",
                "mark-done-record",
            ],
        )

    def test_workflow_assignment_leaves_point_to_workflow_cli(self) -> None:
        parser = build_parser()
        assignment_action = next(
            a for a in parser._actions
            if isinstance(a, argparse._SubParsersAction) and a.dest == "command"
        )
        assignment_parser = assignment_action.choices["assignment"]
        sub = next(
            a for a in assignment_parser._actions
            if isinstance(a, argparse._SubParsersAction)
        )
        choices = dict(sub.choices)
        for name in [
            "request",
            "accept",
            "handoff",
            "blocker",
            "unblock",
            "closeout",
            "review-result",
            "mark-done",
        ]:
            with self.subTest(name=name):
                handler = choices[name]._defaults["handler"]
                self.assertEqual(handler.__module__, "coordinate.workflow_cli")

    def test_receipt_leaves_still_owned_by_completion_cli(self) -> None:
        parser = build_parser()
        assignment_action = next(
            a for a in parser._actions
            if isinstance(a, argparse._SubParsersAction) and a.dest == "command"
        )
        assignment_parser = assignment_action.choices["assignment"]
        sub = next(
            a for a in assignment_parser._actions
            if isinstance(a, argparse._SubParsersAction)
        )
        choices = dict(sub.choices)
        for name in [
            "mark-done-prepare",
            "mark-done-preflight",
            "mark-done-claim",
            "mark-done-apply",
            "mark-done-files",
            "mark-done-record",
        ]:
            with self.subTest(name=name):
                handler = choices[name]._defaults["handler"]
                self.assertEqual(handler.__module__, "coordinate.completion_cli")


class WorkflowCLIBodyProofTests(unittest.TestCase):
    """Canonical AST body proofs for the 12 moved functions."""

    @staticmethod
    def _canonicalize(node: ast.AST) -> object:
        if isinstance(node, ast.AST):
            result: dict[str, object] = {"_type": type(node).__name__}
            for field, value in ast.iter_fields(node):
                if value is None:
                    continue
                if isinstance(value, (list, tuple)) and not value:
                    continue
                result[field] = WorkflowCLIBodyProofTests._canonicalize(value)
            return result
        if isinstance(node, (list, tuple)):
            return [WorkflowCLIBodyProofTests._canonicalize(item) for item in node]
        if isinstance(node, (str, int, float, bool)):
            return node
        if node is None:
            return None
        return repr(node)

    def test_moved_function_bodies_match_canonical_hashes(self) -> None:
        source = Path(coordinate.workflow_cli.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        functions = {
            node.name: node for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        for name, expected_hash in _CANONICAL_AST_HASHES.items():
            with self.subTest(name=name):
                func = functions[name]
                canon = self._canonicalize(func)
                payload = json.dumps(
                    canon, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
                actual_hash = hashlib.sha256(payload).hexdigest()
                self.assertEqual(
                    actual_hash,
                    expected_hash,
                    f"Canonical AST projection for {name} changed",
                )


class WorkflowCLIBranchForgeDelegationTests(unittest.TestCase):
    """Mocked service delegation and envelope tests for branch/CI/review/merge."""

    def test_branch_allocate_delegates_and_prints_json(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", owner="alice", actor="operator"
        )
        result = SimpleNamespace(
            workspace_id="ws1", task_id="t1", branch="feature/t1",
            owner="alice", event_created=True, existing=False,
        )
        captured, capture_ctx = _capture_json()
        with capture_ctx:
            with _mock_conn() as mock_conn:
                with patch(
                    "coordinate.workflow_cli.allocate_branch",
                    return_value=result,
                ) as mock_allocate:
                    code = coordinate.workflow_cli.handle_branch_allocate(args)
        self.assertEqual(code, 0)
        mock_allocate.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws1",
            task_id="t1",
            owner="alice",
            actor="operator",
        )
        self.assertEqual(captured[-1]["branch"], "feature/t1")

    def test_branch_allocate_value_error_returns_json_error(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", owner="alice", actor="operator"
        )
        captured, capture_ctx = _capture_json()
        with capture_ctx:
            with _mock_conn():
                with patch(
                    "coordinate.workflow_cli.allocate_branch",
                    side_effect=ValueError("bad branch"),
                ):
                    code = coordinate.workflow_cli.handle_branch_allocate(args)
        self.assertEqual(code, 1)
        self.assertEqual(captured[-1], {"error": {"message": "bad branch"}})

    def test_ci_check_delegates_and_prints_dict(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", pr_url=None, branch=None, actor="operator"
        )
        result = Mock(to_dict=Mock(return_value={"ok": True}))
        captured, capture_ctx = _capture_json()
        with capture_ctx:
            with _mock_conn() as mock_conn:
                with patch(
                    "coordinate.workflow_cli.check_ci",
                    return_value=result,
                ) as mock_check:
                    code = coordinate.workflow_cli.handle_ci_check(args)
        self.assertEqual(code, 0)
        mock_check.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws1",
            task_id="t1",
            pr_url=None,
            branch=None,
            actor="operator",
        )
        self.assertEqual(captured[-1], {"ok": True})

    def test_review_check_value_error_returns_json_error(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", pr_url=None, branch=None, actor="operator"
        )
        captured, capture_ctx = _capture_json()
        with capture_ctx:
            with _mock_conn():
                with patch(
                    "coordinate.workflow_cli.check_pr_review",
                    side_effect=ValueError("no pr"),
                ):
                    code = coordinate.workflow_cli.handle_review_check(args)
        self.assertEqual(code, 1)
        self.assertEqual(captured[-1], {"error": {"message": "no pr"}})

    def test_merge_gate_delegates_with_required_args(self) -> None:
        args = SimpleNamespace(workspace_id="ws1", task_id="t1")
        result = Mock(to_dict=Mock(return_value={"ready": True}))
        captured, capture_ctx = _capture_json()
        with capture_ctx:
            with _mock_conn() as mock_conn:
                with patch(
                    "coordinate.workflow_cli.check_merge_gate",
                    return_value=result,
                ) as mock_check:
                    code = coordinate.workflow_cli.handle_merge_gate(args)
        self.assertEqual(code, 0)
        mock_check.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws1",
            task_id="t1",
        )
        self.assertEqual(captured[-1], {"ready": True})


class WorkflowCLIAssignmentDelegationTests(unittest.TestCase):
    """Mocked service delegation and envelope tests for assignment workflow."""

    def _make_assignment_result(self, mutation: Mock | None = None, mutation_failed: bool = False) -> Mock:
        event = {"event_type": "harness.mutation_failed" if mutation_failed else "assignment.accepted"}
        result = Mock(
            mutation=mutation,
            event=event,
            event_created=True,
            delivery=None,
            delivery_created=False,
        )
        return result

    def test_assignment_request_delegates_with_all_args(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", owner="alice", session="s1",
            actor="operator", branch="main", platform="discord", destination="#ch",
            idempotency_hint="hint",
        )
        mutation = Mock(to_dict=Mock(return_value={"id": "m1"}))
        result = self._make_assignment_result(mutation=mutation)
        captured, capture_ctx = _capture_json()
        with capture_ctx:
            with _mock_conn() as mock_conn:
                with patch(
                    "coordinate.workflow_cli.request_assignment",
                    return_value=result,
                ) as mock_request:
                    code = coordinate.workflow_cli.handle_assignment_request(args)
        self.assertEqual(code, 0)
        mock_request.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws1",
            task_id="t1",
            owner="alice",
            session="s1",
            actor="operator",
            branch="main",
            platform="discord",
            destination="#ch",
            idempotency_hint="hint",
        )
        self.assertEqual(captured[-1]["result"]["mutation"], {"id": "m1"})

    def test_assignment_request_mutation_failure_returns_one(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", owner="alice", session="s1",
            actor="operator", branch=None, platform=None, destination=None,
            idempotency_hint=None,
        )
        result = self._make_assignment_result(mutation_failed=True)
        with _capture_json()[1]:
            with _mock_conn():
                with patch(
                    "coordinate.workflow_cli.request_assignment",
                    return_value=result,
                ):
                    code = coordinate.workflow_cli.handle_assignment_request(args)
        self.assertEqual(code, 1)

    def test_assignment_accept_includes_bootstrap_output(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", owner="alice", session="s1",
            actor=None, branch=None, idempotency_hint=None,
        )
        mutation = Mock(to_dict=Mock(return_value={"id": "m1"}))
        result = self._make_assignment_result(mutation=mutation)
        bootstrap = {
            "bootstrap_text": "bt",
            "bootstrap_path": "bp",
            "event_id": "e1",
            "execution_profile": "ep",
        }
        captured, capture_ctx = _capture_json()
        with capture_ctx:
            with _mock_conn() as mock_conn:
                with patch(
                    "coordinate.workflow_cli.accept_task",
                    return_value=result,
                ) as mock_accept:
                    with patch(
                        "coordinate.workflow_cli.latest_prepared_handoff_bootstrap",
                        return_value=bootstrap,
                    ) as mock_bootstrap:
                        code = coordinate.workflow_cli.handle_assignment_accept(args)
        self.assertEqual(code, 0)
        mock_accept.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws1",
            task_id="t1",
            owner="alice",
            session="s1",
            actor=None,
            branch=None,
            idempotency_hint=None,
        )
        mock_bootstrap.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws1",
            task_id="t1",
            target_agent="alice",
        )
        self.assertEqual(captured[-1]["result"]["bootstrap_text"], "bt")

    def test_assignment_handoff_delegates_and_prints_result(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", target="bob", actor="operator",
            reason="handoff", idempotency_hint=None,
        )
        mutation = Mock(to_dict=Mock(return_value={"id": "m1"}))
        result = self._make_assignment_result(mutation=mutation)
        captured, capture_ctx = _capture_json()
        with capture_ctx:
            with _mock_conn() as mock_conn:
                with patch(
                    "coordinate.workflow_cli.handoff_task",
                    return_value=result,
                ) as mock_handoff:
                    code = coordinate.workflow_cli.handle_assignment_handoff(args)
        self.assertEqual(code, 0)
        mock_handoff.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws1",
            task_id="t1",
            target="bob",
            actor="operator",
            reason="handoff",
            idempotency_hint=None,
        )
        self.assertEqual(captured[-1]["result"]["event"]["event_type"], "assignment.accepted")

    def test_assignment_blocker_mutation_failure_returns_one(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", actor="operator",
            reason="blocked", idempotency_hint=None,
        )
        mutation = Mock(to_dict=Mock(return_value={"id": "m1"}))
        result = self._make_assignment_result(mutation=mutation, mutation_failed=True)
        with _capture_json()[1]:
            with _mock_conn():
                with patch(
                    "coordinate.workflow_cli.blocker_task",
                    return_value=result,
                ):
                    code = coordinate.workflow_cli.handle_assignment_blocker(args)
        self.assertEqual(code, 1)

    def test_assignment_unblock_passes_force_and_decision(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", actor="operator", decision="approve",
            force=True, reason="ok", idempotency_hint="hint",
        )
        mutation = Mock(to_dict=Mock(return_value={"id": "m1"}))
        result = self._make_assignment_result(mutation=mutation)
        with _capture_json()[1]:
            with _mock_conn() as mock_conn:
                with patch(
                    "coordinate.workflow_cli.unblock_task",
                    return_value=result,
                ) as mock_unblock:
                    code = coordinate.workflow_cli.handle_assignment_unblock(args)
        self.assertEqual(code, 0)
        mock_unblock.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws1",
            task_id="t1",
            actor="operator",
            decision="approve",
            force=True,
            reason="ok",
            idempotency_hint="hint",
        )

    def test_assignment_closeout_passes_self_test_evidence(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", reviewer="rev",
            self_test_evidence="evidence", actor="operator", idempotency_hint=None,
        )
        mutation = Mock(to_dict=Mock(return_value={"id": "m1"}))
        result = self._make_assignment_result(mutation=mutation)
        with _capture_json()[1]:
            with _mock_conn() as mock_conn:
                with patch(
                    "coordinate.workflow_cli.closeout_task",
                    return_value=result,
                ) as mock_closeout:
                    code = coordinate.workflow_cli.handle_assignment_closeout(args)
        self.assertEqual(code, 0)
        mock_closeout.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws1",
            task_id="t1",
            reviewer="rev",
            actor="operator",
            idempotency_hint=None,
            self_test_evidence="evidence",
        )

    def test_assignment_closeout_empty_self_test_becomes_none(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", reviewer="rev",
            self_test_evidence="", actor="operator", idempotency_hint=None,
        )
        mutation = Mock(to_dict=Mock(return_value={"id": "m1"}))
        result = self._make_assignment_result(mutation=mutation)
        with _capture_json()[1]:
            with _mock_conn():
                with patch(
                    "coordinate.workflow_cli.closeout_task",
                    return_value=result,
                ) as mock_closeout:
                    code = coordinate.workflow_cli.handle_assignment_closeout(args)
        self.assertEqual(code, 0)
        call_kwargs = mock_closeout.call_args.kwargs
        self.assertIsNone(call_kwargs["self_test_evidence"])

    def test_assignment_review_result_passes_summary(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", reviewer="rev", decision="approve",
            summary="looks good", actor="operator", idempotency_hint=None,
        )
        mutation = Mock(to_dict=Mock(return_value={"id": "m1"}))
        result = self._make_assignment_result(mutation=mutation)
        with _capture_json()[1]:
            with _mock_conn() as mock_conn:
                with patch(
                    "coordinate.workflow_cli.review_result_task",
                    return_value=result,
                ) as mock_review:
                    code = coordinate.workflow_cli.handle_assignment_review_result(args)
        self.assertEqual(code, 0)
        mock_review.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws1",
            task_id="t1",
            reviewer="rev",
            decision="approve",
            actor="operator",
            summary="looks good",
            idempotency_hint=None,
        )

    def test_assignment_mark_done_gate_failure_returns_one(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", actor="operator",
            idempotency_hint=None, verification=None,
        )
        result = Mock(
            to_dict=Mock(return_value={"ok": False}),
            gate=Mock(passed=False),
            event={"event_type": "task.done"},
        )
        with _capture_json()[1]:
            with _mock_conn():
                with patch(
                    "coordinate.workflow_cli.mark_done_task",
                    return_value=result,
                ):
                    code = coordinate.workflow_cli.handle_assignment_mark_done(args)
        self.assertEqual(code, 1)

    def test_assignment_mark_done_mutation_failure_returns_one(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", actor="operator",
            idempotency_hint=None, verification=None,
        )
        result = Mock(
            to_dict=Mock(return_value={"ok": True}),
            gate=None,
            event={"event_type": "harness.mutation_failed"},
        )
        with _capture_json()[1]:
            with _mock_conn():
                with patch(
                    "coordinate.workflow_cli.mark_done_task",
                    return_value=result,
                ):
                    code = coordinate.workflow_cli.handle_assignment_mark_done(args)
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
