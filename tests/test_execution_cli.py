"""Boundary tests for the P9-0A3a runner/job/runtime CLI extraction."""
from __future__ import annotations

import argparse
import ast
import contextlib
import hashlib
import io
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
import coordinate.execution_cli
from coordinate.cli import build_parser
from coordinate.execution_cli import (
    handle_job_cancel,
    handle_job_create,
    handle_job_list,
    handle_job_pump,
    handle_job_retry,
    handle_job_run,
    handle_runner_add,
    handle_runner_example,
    handle_runner_examples,
    handle_runner_list,
    handle_runtime_agent_deactivate,
    handle_runtime_agent_heartbeat,
    handle_runtime_agent_register,
    handle_runtime_executor_list,
    handle_runtime_executor_show,
    handle_runtime_executor_sync,
    handle_runtime_job_claim,
    handle_runtime_job_lease_reap,
    handle_runtime_job_lease_renew,
    handle_runtime_job_progress,
    handle_runtime_job_report,
    handle_runtime_request_submit,
    register_job_commands,
    register_runner_commands,
    register_runtime_commands,
)


SRC_PATH = Path(__file__).resolve().parents[1] / "src"
CLI_PATH = SRC_PATH / "coordinate" / "cli.py"
EXECUTION_CLI_PATH = SRC_PATH / "coordinate" / "execution_cli.py"

# Stable AST SHA-256 constants for the 25 handlers (16 original + 3 P9-2A
# executor leaves + 3 P9-3A capacity leaves + 2 P9-3B lease leaves + P9-3C1
# deactivate),
# computed from the reviewed extraction.
# AST body SHA-256 constants are computed with a canonical projection of
# each handler's body statements. The projection drops None and empty
# list/tuple fields, which normalizes the known differences between Python
# minor versions (e.g. 3.12 emits empty Call.args/keywords,
# Try.orelse/finalbody, FunctionDef.type_params, while 3.14 omits them).
# It does not claim absolute future-proof stability for any change Python
# may make to the ast module.
HANDLER_AST_SHA256 = {
    "handle_runner_add": "84435cf407af16254852a6f90540cb5681b790bce9b71eccf51723e51e08e37c",
    "handle_runner_list": "6a06f803dcb855791cbfd96ecc1b7d1225d0b5e422e7a69347293ca1d8c71a9c",
    "handle_runner_examples": "8decdb4327264e2cb078333a912bc1e4fed3124ac40dae5f6da75796bdbf04c2",
    "handle_runner_example": "91970aad177c6f481375922e32a357e3ab00ec608da5cd1dcacfaf5c20fdfa10",
    "handle_job_create": "184071998ec43787351f547296657f6335cfe8f17fda74a2b21504e90b19140b",
    "handle_job_list": "801854dd43ce78329a21fdb0c06d8d7ac391ddd670f6685f6d1493f2d291c14f",
    "handle_job_run": "3a2500484960d9e7a81cb308ea5d6c3a4accc584be8ac3f3c88a57f9570dee04",
    "handle_job_cancel": "02599956d5b9149a00d6baf13c3a048fb7808dbf947ad7d158477cc4fa8d9d5c",
    "handle_job_retry": "6b74bec9f7e92a49db898e804567c122613bdf4c2401e0f422907be5eb963bf8",
    "handle_job_pump": "08a21020de142ade58c9bce54c073f839f44e967f07e135e0de7eb1b2777f36f",
    "handle_runtime_agent_register": "9a05752e2bde77effd439bae23b5851486ada59031e8dcfa7754f6c992e65148",
    "handle_runtime_agent_heartbeat": "1cbd60a94c19847006e2c2e71d5b9ae1bcb80b746ec34e2d792bff6abf8f4b81",
    "handle_runtime_agent_deactivate": "168c0c311de1e73fdcb4a10b4c6a70a59256fef71111504cde98c6c0b8e52853",
    "handle_runtime_request_submit": "25cc9c0e0a1869bccc3e6a83fd9dcabe9c9c13441b715d9c6e59f6c0dc36ec44",
    "handle_runtime_job_claim": "4dd9979b6a936c28ce4226ced3261bbfac28db2ee0edc5e634650c6caffae522",
    "handle_runtime_job_report": "e9b85eebe2da7201d6bf7618d6b0b26fa5ef37fb19162cad8ac67c55a7ab7c4c",
    "handle_runtime_job_progress": "172394f318c1b1fe316765c509f45f55e0696c296bda6d948e2e843f2f1b54ec",
    "handle_runtime_job_lease_renew": "405718b41ca70339b4cbc8fd23ecb36c7082f4ccf56fd6f527cb710c281c912f",
    "handle_runtime_job_lease_reap": "36a408e42fc91dae2b9e2970576a31f60dc42fe1c48cb4b4c3f6dfcbd3b13500",
    "handle_runtime_executor_sync": "d75b94e24cbb1b93a504820ba3f5a7d5930262fc0c7474d936750aba7c5ad0fd",
    "handle_runtime_executor_list": "ce022aad1386da7efd14a20a7adca10163e5cdfd5a14110c1c9033f040711c73",
    "handle_runtime_executor_show": "fc0e0ebf932ebd49a7dbb32669a5def5cdc9ac8e3330220873e3ab37642c8487",
    "handle_runtime_capacity_sync": "5655f5afc2b2967b07863e0a64243e559ab8f024a5de0dde491bb374f93515dc",
    "handle_runtime_capacity_list": "b6ebd588e47d7b188a5ff150bd92a77710588349c484f23270fd59fe29f99628",
    "handle_runtime_capacity_show": "833f6b226738acc5555ef9a28eb2f4dc253b35e7d029d25fd096ff903c241807",
}


def _canonical_projection(node: object) -> object:
    """Return a normalized JSON-serializable projection of an AST node.

    Drops None and empty list/tuple fields. This eliminates the known
    differences between Python 3.12 and 3.14 ast default values, such as
    empty Call.args/keywords, Try.orelse/finalbody, and
    FunctionDef.type_params. It does not guarantee immunity to arbitrary
    future ast module changes.
    """
    if isinstance(node, ast.AST):
        projected: dict[str, object] = {}
        for name, value in ast.iter_fields(node):
            if value is None:
                continue
            if isinstance(value, (list, tuple)) and len(value) == 0:
                continue
            projected[name] = _canonical_projection(value)
        return {type(node).__name__: projected}
    if isinstance(node, (list, tuple)):
        return [_canonical_projection(item) for item in node]
    return node


def _handler_ast_sha256(func: object) -> str:
    """Compute a SHA-256 of a handler's body statements via canonical projection."""
    source = Path(func.__code__.co_filename).read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func.__name__:
            body_proj = [_canonical_projection(stmt) for stmt in node.body]
            body_json = json.dumps(
                body_proj,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            return hashlib.sha256(body_json.encode("utf-8")).hexdigest()
    raise AssertionError(f"Handler {func.__name__} not found in source")


class ExecutionCLIOwnershipTests(unittest.TestCase):
    """Tests that handlers moved to execution_cli remain reachable from root."""

    def test_root_aliases_are_identical_to_execution_cli_handlers(self) -> None:
        pairs = [
            (coordinate.cli.handle_runner_add, handle_runner_add),
            (coordinate.cli.handle_runner_list, handle_runner_list),
            (coordinate.cli.handle_runner_examples, handle_runner_examples),
            (coordinate.cli.handle_runner_example, handle_runner_example),
            (coordinate.cli.handle_job_create, handle_job_create),
            (coordinate.cli.handle_job_list, handle_job_list),
            (coordinate.cli.handle_job_run, handle_job_run),
            (coordinate.cli.handle_job_cancel, handle_job_cancel),
            (coordinate.cli.handle_job_retry, handle_job_retry),
            (coordinate.cli.handle_job_pump, handle_job_pump),
            (coordinate.cli.handle_runtime_agent_register, handle_runtime_agent_register),
            (coordinate.cli.handle_runtime_agent_heartbeat, handle_runtime_agent_heartbeat),
            (coordinate.cli.handle_runtime_request_submit, handle_runtime_request_submit),
            (coordinate.cli.handle_runtime_job_claim, handle_runtime_job_claim),
            (coordinate.cli.handle_runtime_job_report, handle_runtime_job_report),
            (coordinate.cli.handle_runtime_job_progress, handle_runtime_job_progress),
            (coordinate.cli.handle_runtime_executor_sync, handle_runtime_executor_sync),
            (coordinate.cli.handle_runtime_executor_list, handle_runtime_executor_list),
            (coordinate.cli.handle_runtime_executor_show, handle_runtime_executor_show),
        ]
        for root_alias, execution_handler in pairs:
            self.assertIs(root_alias, execution_handler)

    def test_root_source_has_no_moved_handler_definitions(self) -> None:
        source = CLI_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        defined = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
        moved = set(HANDLER_AST_SHA256.keys())
        self.assertEqual(
            defined & moved,
            set(),
            f"Root CLI still defines moved handlers: {defined & moved}",
        )

    def test_root_source_has_no_execution_only_service_imports(self) -> None:
        source = CLI_PATH.read_text(encoding="utf-8")
        execution_only = {
            "create_job",
            "list_jobs",
            "list_runner_profiles",
            "upsert_runner_profile",
            "cancel_job",
            "pump_jobs",
            "retry_job",
            "run_job",
            "get_runner_profile_example",
            "list_runner_profile_examples",
            "claim_job",
            "runtime_claim_job",
            "heartbeat_agent",
            "register_agent",
            "record_job_progress",
            "report_job_result",
            "submit_request",
        }
        found = set()
        tree = ast.parse(source)
        for node in tree.body:
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name in execution_only or alias.asname in execution_only:
                        found.add(alias.asname or alias.name)
        self.assertEqual(
            found,
            set(),
            f"Root CLI still imports execution-only services: {found}",
        )

    def test_root_retains_job_error_dispatch(self) -> None:
        source = CLI_PATH.read_text(encoding="utf-8")
        self.assertIn("from .jobs import JobError", source)
        self.assertIn("except (HarnessError, JobError, BusError, PolicyError, ValueError, KeyError)", source)

    def test_execution_cli_does_not_import_root(self) -> None:
        source = EXECUTION_CLI_PATH.read_text(encoding="utf-8")
        self.assertNotIn("from .cli import", source)
        self.assertNotIn("import coordinate.cli", source)

    def test_execution_cli_does_not_import_delivery_or_worker_services(self) -> None:
        source = EXECUTION_CLI_PATH.read_text(encoding="utf-8")
        forbidden = {
            "create_delivery",
            "list_deliveries",
            "send_delivery",
            "pump_deliveries",
            "recover_sending_deliveries",
            "run_delivery_worker",
            "create_delivery_for_event",
            "create_deliveries_for_event",
            "pump_events",
            "render_event",
        }
        found = set()
        tree = ast.parse(source)
        for node in tree.body:
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    name = alias.asname or alias.name
                    if name in forbidden:
                        found.add(name)
        self.assertEqual(found, set(), f"execution_cli imports delivery/worker services: {found}")

    def test_import_orders_succeed(self) -> None:
        orders = [
            ["coordinate.cli_support", "coordinate.execution_cli", "coordinate.cli"],
            ["coordinate.execution_cli", "coordinate.cli_support", "coordinate.cli"],
            ["coordinate.cli", "coordinate.cli_support", "coordinate.execution_cli"],
            ["coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.issue_cli", "coordinate.execution_cli", "coordinate.cli"],
            ["coordinate.execution_cli", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.issue_cli", "coordinate.cli"],
        ]
        for order in orders:
            script = "; ".join(f"import {name}" for name in order) + "; print('ok')"
            with self.subTest(order=order):
                with tempfile.TemporaryDirectory() as tmpdir:
                    result = subprocess.run(
                        [sys.executable, "-c", script],
                        cwd=tmpdir,
                        env={"PYTHONPATH": str(SRC_PATH), "PATH": os.environ.get("PATH", "")},
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        check=True,
                    )
                self.assertIn("ok", result.stdout)


class ExecutionCLIRegistrationTests(unittest.TestCase):
    """Tests that registrars preserve parser positions and leaf ownership."""

    def _leaf_handlers(self) -> dict[str, str]:
        parser = build_parser()
        result: dict[str, str] = {}

        def walk(p, path):
            subparser_actions = [a for a in p._actions if isinstance(a, argparse._SubParsersAction)]
            if not subparser_actions:
                if path:
                    handler = getattr(p, "_defaults", {}).get("handler")
                    if handler is not None:
                        result[" ".join(path)] = f"{handler.__module__}.{handler.__qualname__}"
                return
            for name, child in subparser_actions[0].choices.items():
                walk(child, path + [name])

        walk(parser, [])
        return result

    def test_runner_job_runtime_positions_unchanged(self) -> None:
        parser = build_parser()
        subparser_actions = [a for a in parser._actions if type(a).__name__ == "_SubParsersAction"]
        self.assertEqual(len(subparser_actions), 1)
        top = list(subparser_actions[0].choices.keys())
        self.assertEqual(
            top,
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

    def test_execution_cli_leaf_ownership(self):
        leaf_handlers = self._leaf_handlers()
        execution_leaves = {
            path for path, owner in leaf_handlers.items()
            if owner.startswith("coordinate.execution_cli.")
        }
        self.assertEqual(
            execution_leaves,
            {
                "runner add",
                "runner list",
                "runner examples",
                "runner example",
                "job create",
                "job list",
                "job run",
                "job cancel",
                "job retry",
                "job pump",
                "runtime agent register",
                "runtime agent heartbeat",
                "runtime agent deactivate",
                "runtime request submit",
                "runtime job claim",
                "runtime job report",
                "runtime job progress",
                "runtime job lease renew",
                "runtime job lease reap",
                "runtime executor sync",
                "runtime executor list",
                "runtime executor show",
                "runtime capacity sync",
                "runtime capacity list",
                "runtime capacity show",
            },
        )

    def test_runtime_executor_leaf_ownership(self):
        leaf_handlers = self._leaf_handlers()
        runtime_executor_leaves = {
            path for path, owner in leaf_handlers.items()
            if owner.startswith("coordinate.execution_cli.") and path.startswith("runtime executor ")
        }
        self.assertEqual(
            runtime_executor_leaves,
            {
                "runtime executor sync",
                "runtime executor list",
                "runtime executor show",
            },
        )

    def test_all_25_leaf_handlers_match_stable_ast_hashes(self) -> None:
        for name, expected_sha in HANDLER_AST_SHA256.items():
            func = getattr(coordinate.execution_cli, name)
            actual_sha = _handler_ast_sha256(func)
            self.assertEqual(
                actual_sha,
                expected_sha,
                f"Handler {name} AST drifted from reviewed baseline",
            )

    def test_registrars_add_exact_command_tree(self) -> None:
        parser = argparse.ArgumentParser(prog="test")
        subcommands = parser.add_subparsers(dest="command")
        register_runner_commands(subcommands)
        register_job_commands(subcommands)
        register_runtime_commands(subcommands)

        top = list(subcommands.choices.keys())
        self.assertEqual(top, ["runner", "job", "runtime"])

        runner = subcommands.choices["runner"]
        runner_leaves = list(runner._subparsers._group_actions[0].choices.keys())
        self.assertEqual(runner_leaves, ["add", "list", "examples", "example"])

        job = subcommands.choices["job"]
        job_leaves = list(job._subparsers._group_actions[0].choices.keys())
        self.assertEqual(job_leaves, ["create", "list", "run", "cancel", "retry", "pump"])

        runtime = subcommands.choices["runtime"]
        runtime_leaves = list(runtime._subparsers._group_actions[0].choices.keys())
        self.assertEqual(runtime_leaves, ["agent", "request", "job", "executor", "capacity"])


class ExecutionCLIDelegationTests(unittest.TestCase):
    """Tests that moved handlers delegate to mocked seams exactly as before."""

    def _args(self, **kwargs) -> SimpleNamespace:
        return SimpleNamespace(**kwargs)

    def _capture(self, func, args) -> tuple[int, str]:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = func(args)
        return code, stdout.getvalue()

    def _capture_json(self, func, args) -> tuple[int, dict]:
        code, stdout = self._capture(func, args)
        return code, json.loads(stdout)

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.upsert_runner_profile")
    def test_runner_add_invalid_json(self, mock_upsert, mock_conn) -> None:
        args = self._args(id="r1", name=None, runner_type="subprocess", command="echo", working_directory_strategy="current_dir", supports_stream_attach=False, env_json="not-json", db=":memory:")
        code, stdout = self._capture(handle_runner_add, args)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        mock_upsert.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.upsert_runner_profile")
    def test_runner_add_non_object_env(self, mock_upsert, mock_conn) -> None:
        args = self._args(id="r1", name=None, runner_type="subprocess", command="echo", working_directory_strategy="current_dir", supports_stream_attach=False, env_json='["x"]', db=":memory:")
        code, stdout = self._capture(handle_runner_add, args)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        mock_upsert.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.upsert_runner_profile")
    def test_runner_add_delegation(self, mock_upsert, mock_conn) -> None:
        profile = Mock()
        profile.to_dict.return_value = {"id": "r1"}
        mock_upsert.return_value = profile
        args = self._args(id="r1", name="Runner", runner_type="subprocess", command="echo", working_directory_strategy="current_dir", supports_stream_attach=True, env_json='{"x": 1}', db=":memory:")
        code, payload = self._capture_json(handle_runner_add, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"runner_profile": {"id": "r1"}})
        mock_upsert.assert_called_once()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.list_runner_profiles")
    def test_runner_list_delegation(self, mock_list, mock_conn) -> None:
        profile = Mock()
        profile.to_dict.return_value = {"id": "r1"}
        mock_list.return_value = [profile]
        args = self._args(db=":memory:")
        code, payload = self._capture_json(handle_runner_list, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"runner_profiles": [{"id": "r1"}]})

    @patch("coordinate.execution_cli.list_runner_profile_examples")
    def test_runner_examples_delegation(self, mock_list) -> None:
        mock_list.return_value = [{"id": "ex1"}]
        args = self._args(db=":memory:")
        code, payload = self._capture_json(handle_runner_examples, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"runner_profile_examples": [{"id": "ex1"}]})

    @patch("coordinate.execution_cli.get_runner_profile_example")
    def test_runner_example_delegation(self, mock_get) -> None:
        mock_get.return_value = {"id": "ex1"}
        args = self._args(id="ex1", db=":memory:")
        code, payload = self._capture_json(handle_runner_example, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"runner_profile_example": {"id": "ex1"}})

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.create_job")
    def test_job_create_invalid_json(self, mock_create, mock_conn) -> None:
        args = self._args(workspace_id="ws", task_id=None, runner_profile_id="r1", prompt_path=None, branch=None, worktree_path=None, terminal_session_id=None, logs_path=None, result_path=None, timeout_seconds=None, payload_json="not-json", db=":memory:")
        code, stdout = self._capture(handle_job_create, args)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        mock_create.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.create_job")
    def test_job_create_non_object_payload(self, mock_create, mock_conn) -> None:
        args = self._args(workspace_id="ws", task_id=None, runner_profile_id="r1", prompt_path=None, branch=None, worktree_path=None, terminal_session_id=None, logs_path=None, result_path=None, timeout_seconds=None, payload_json='["x"]', db=":memory:")
        code, stdout = self._capture(handle_job_create, args)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        mock_create.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.row_to_dict")
    @patch("coordinate.execution_cli.create_job")
    def test_job_create_result_path_merge(self, mock_create, mock_row_to_dict, mock_conn) -> None:
        mock_row_to_dict.return_value = {"id": "j1"}
        mock_create.return_value = Mock()
        args = self._args(workspace_id="ws", task_id="t1", runner_profile_id="r1", prompt_path=None, branch=None, worktree_path=None, terminal_session_id=None, logs_path=None, result_path="/out.json", timeout_seconds=None, payload_json='{"x": 1}', db=":memory:")
        code, payload = self._capture_json(handle_job_create, args)
        self.assertEqual(code, 0)
        mock_create.assert_called_once()
        passed_payload = mock_create.call_args.kwargs["payload"]
        self.assertEqual(passed_payload["result_path"], "/out.json")

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.list_jobs")
    def test_job_list_delegation(self, mock_list, mock_conn) -> None:
        mock_list.return_value = [{"id": "j1"}]
        args = self._args(workspace_id="ws", status=None, db=":memory:")
        code, payload = self._capture_json(handle_job_list, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"jobs": [{"id": "j1"}]})
        mock_list.assert_called_once_with(mock_conn.return_value.__enter__.return_value, workspace_id="ws", status=None)

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.run_job")
    def test_job_run_delegation(self, mock_run, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "j1"}
        mock_run.return_value = result
        args = self._args(job_id="j1", db=":memory:")
        code, payload = self._capture_json(handle_job_run, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"result": {"id": "j1"}})

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.cancel_job")
    def test_job_cancel_delegation(self, mock_cancel, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "j1"}
        mock_cancel.return_value = result
        args = self._args(job_id="j1", reason="stop", db=":memory:")
        code, payload = self._capture_json(handle_job_cancel, args)
        self.assertEqual(code, 0)
        mock_cancel.assert_called_once_with(mock_conn.return_value.__enter__.return_value, "j1", reason="stop")

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.retry_job")
    def test_job_retry_delegation(self, mock_retry, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "j1"}
        mock_retry.return_value = result
        args = self._args(job_id="j1", reason="again", db=":memory:")
        code, payload = self._capture_json(handle_job_retry, args)
        self.assertEqual(code, 0)
        mock_retry.assert_called_once_with(mock_conn.return_value.__enter__.return_value, "j1", reason="again")

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.pump_jobs")
    def test_job_pump_delegation(self, mock_pump, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"pumped": 1}
        mock_pump.return_value = result
        args = self._args(workspace_id="ws", limit=5, db=":memory:")
        code, payload = self._capture_json(handle_job_pump, args)
        self.assertEqual(code, 0)
        mock_pump.assert_called_once_with(mock_conn.return_value.__enter__.return_value, workspace_id="ws", limit=5)

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.register_agent")
    def test_runtime_agent_register_capabilities_json(self, mock_register, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "a1"}
        mock_register.return_value = result
        args = self._args(agent_id="a1", host_id="h1", client_type="agentd", capabilities_json='{"gpu": true}', actor="runtime", db=":memory:")
        code, payload = self._capture_json(handle_runtime_agent_register, args)
        self.assertEqual(code, 0)
        mock_register.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            agent_id="a1",
            host_id="h1",
            capabilities={"gpu": True},
            client_type="agentd",
            actor="runtime",
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.heartbeat_agent")
    def test_runtime_agent_heartbeat_delegation(self, mock_heartbeat, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "a1"}
        mock_heartbeat.return_value = result
        args = self._args(agent_id="a1", host_id="h1", actor="runtime", db=":memory:")
        code, payload = self._capture_json(handle_runtime_agent_heartbeat, args)
        self.assertEqual(code, 0)
        mock_heartbeat.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            agent_id="a1",
            host_id="h1",
            actor="runtime",
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.deactivate_agent")
    def test_runtime_agent_deactivate_delegation(self, mock_deactivate, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"blocked": False, "deactivated": True}
        mock_deactivate.return_value = result
        args = self._args(
            agent_id="a1",
            host_id="h1",
            reason="planned maintenance",
            actor="operator",
            dry_run=False,
            db=":memory:",
        )
        code, payload = self._capture_json(handle_runtime_agent_deactivate, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload["result"]["deactivated"], True)
        mock_deactivate.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            agent_id="a1",
            host_id="h1",
            reason="planned maintenance",
            actor="operator",
            dry_run=False,
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.deactivate_agent")
    def test_runtime_agent_deactivate_blocker_returns_one(
        self, mock_deactivate, mock_conn
    ) -> None:
        result = Mock()
        result.to_dict.return_value = {"blocked": True, "deactivated": False}
        mock_deactivate.return_value = result
        args = self._args(
            agent_id="a1",
            host_id="h1",
            reason="planned maintenance",
            actor="operator",
            dry_run=True,
            db=":memory:",
        )
        code, _ = self._capture_json(handle_runtime_agent_deactivate, args)
        self.assertEqual(code, 1)

    @patch("coordinate.execution_cli._conn")
    def test_runtime_agent_deactivate_invalid_shape_precedes_connection(
        self, mock_conn
    ) -> None:
        args = self._args(
            agent_id=" ",
            host_id="h1",
            reason="planned maintenance",
            actor="operator",
            dry_run=False,
            db=":memory:",
        )
        with self.assertRaisesRegex(ValueError, "agent_id"):
            handle_runtime_agent_deactivate(args)
        mock_conn.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_runtime_request_submit_json_parsing(self, mock_submit, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "r1"}
        mock_submit.return_value = result
        args = self._args(
            workspace_id="ws",
            target_agent="a1",
            route_capabilities=None,
            route_definition=None,
            preferred_host=None,
            override_agent=None,
            override_reason=None,
            worktree_path="/control/ws/e1",
            prompt="go",
            origin_json='{"src": "x"}',
            reply_json='{"dst": "y"}',
            actor="bridge",
            task_id="t1",
            idempotency_key="k1",
            db=":memory:",
        )
        code, payload = self._capture_json(handle_runtime_request_submit, args)
        self.assertEqual(code, 0)
        mock_submit.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws",
            target_agent="a1",
            prompt="go",
            origin={"src": "x"},
            reply={"dst": "y"},
            actor="bridge",
            task_id="t1",
            idempotency_key="k1",
            routing_request=None,
            worktree_path="/control/ws/e1",
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.runtime_claim_job")
    def test_runtime_job_claim_delegation(self, mock_claim, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"claimed": True}
        mock_claim.return_value = result
        args = self._args(
            agent_id="a1",
            recoverable=True,
            recovery_reason="operator confirmed prior process stopped via tooling",
            prior_process_stopped=True,
            reap_mode="none",
            reap_reason="scoped claim",
            db=":memory:",
        )
        code, payload = self._capture_json(handle_runtime_job_claim, args)
        self.assertEqual(code, 0)
        mock_claim.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            agent_id="a1",
            recoverable=True,
            recovery_reason="operator confirmed prior process stopped via tooling",
            prior_process_stopped=True,
            reap_mode="none",
            reap_reason="scoped claim",
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.report_job_result")
    def test_runtime_job_report_result_json(self, mock_report, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "j1"}
        mock_report.return_value = result
        args = self._args(job_id="j1", agent_id="a1", status="done", result_json='{"ok": true}', actor="runtime", attempt_token=3, db=":memory:")
        code, payload = self._capture_json(handle_runtime_job_report, args)
        self.assertEqual(code, 0)
        mock_report.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            job_id="j1",
            agent_id="a1",
            status="done",
            result={"ok": True},
            actor="runtime",
            attempt_token=3,
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.report_job_result")
    def test_runtime_job_report_forwards_lease_id_when_present(self, mock_report, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "j1"}
        mock_report.return_value = result
        args = self._args(
            job_id="j1",
            agent_id="a1",
            status="done",
            result_json='{"ok": true}',
            actor="runtime",
            attempt_token=3,
            lease_id="lease-123",
            db=":memory:",
        )
        code, payload = self._capture_json(handle_runtime_job_report, args)
        self.assertEqual(code, 0)
        mock_report.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            job_id="j1",
            agent_id="a1",
            status="done",
            result={"ok": True},
            actor="runtime",
            attempt_token=3,
            lease_id="lease-123",
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.report_job_result")
    def test_runtime_job_report_omits_lease_id_when_absent(self, mock_report, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "j1"}
        mock_report.return_value = result
        args = self._args(
            job_id="j1",
            agent_id="a1",
            status="done",
            result_json='{"ok": true}',
            actor="runtime",
            attempt_token=3,
            db=":memory:",
        )
        code, payload = self._capture_json(handle_runtime_job_report, args)
        self.assertEqual(code, 0)
        mock_report.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            job_id="j1",
            agent_id="a1",
            status="done",
            result={"ok": True},
            actor="runtime",
            attempt_token=3,
        )
        self.assertNotIn("lease_id", mock_report.call_args.kwargs)

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.record_job_progress")
    def test_runtime_job_progress_delegation(self, mock_record, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "j1"}
        mock_record.return_value = result
        args = self._args(job_id="j1", agent_id="a1", stage="run", summary="ok", session_id="s1", actor="runtime", attempt_token=3, db=":memory:")
        code, payload = self._capture_json(handle_runtime_job_progress, args)
        self.assertEqual(code, 0)
        mock_record.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            job_id="j1",
            agent_id="a1",
            stage="run",
            summary="ok",
            session_id="s1",
            actor="runtime",
            attempt_token=3,
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.record_job_progress")
    def test_runtime_job_progress_forwards_lease_id_when_present(self, mock_record, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "j1"}
        mock_record.return_value = result
        args = self._args(
            job_id="j1",
            agent_id="a1",
            stage="run",
            summary="ok",
            session_id="s1",
            actor="runtime",
            attempt_token=3,
            lease_id="lease-123",
            db=":memory:",
        )
        code, payload = self._capture_json(handle_runtime_job_progress, args)
        self.assertEqual(code, 0)
        mock_record.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            job_id="j1",
            agent_id="a1",
            stage="run",
            summary="ok",
            session_id="s1",
            actor="runtime",
            attempt_token=3,
            lease_id="lease-123",
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.record_job_progress")
    def test_runtime_job_progress_omits_lease_id_when_absent(self, mock_record, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "j1"}
        mock_record.return_value = result
        args = self._args(
            job_id="j1",
            agent_id="a1",
            stage="run",
            summary="ok",
            session_id="s1",
            actor="runtime",
            attempt_token=3,
            db=":memory:",
        )
        code, payload = self._capture_json(handle_runtime_job_progress, args)
        self.assertEqual(code, 0)
        mock_record.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            job_id="j1",
            agent_id="a1",
            stage="run",
            summary="ok",
            session_id="s1",
            actor="runtime",
            attempt_token=3,
        )
        self.assertNotIn("lease_id", mock_record.call_args.kwargs)

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.renew_managed_lease")
    def test_runtime_job_lease_renew_delegation(self, mock_renew, mock_conn) -> None:
        mock_renew.return_value = {"lease_id": "lease-123", "expires_at": "2026-07-14T12:00:00Z"}
        args = self._args(
            job_id="j1",
            agent_id="a1",
            attempt_token=3,
            lease_id="lease-123",
            actor="runtime",
            db=":memory:",
        )
        code, payload = self._capture_json(handle_runtime_job_lease_renew, args)
        self.assertEqual(code, 0)
        mock_renew.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            lease_id="lease-123",
            job_id="j1",
            attempt_token=3,
            agent_id="a1",
        )
        self.assertEqual(payload, {"result": {"lease_id": "lease-123", "expires_at": "2026-07-14T12:00:00Z"}})

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.reap_due_leases")
    def test_runtime_job_lease_reap_delegation(self, mock_reap, mock_conn) -> None:
        mock_reap.return_value = {"reaped": 2, "skipped": 0}
        args = self._args(actor="runtime", batch_size=50, db=":memory:")
        code, payload = self._capture_json(handle_runtime_job_lease_reap, args)
        self.assertEqual(code, 0)
        mock_reap.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            actor="runtime",
            batch_size=50,
        )
        self.assertEqual(payload, {"result": {"reaped": 2, "skipped": 0}})

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.reap_exact_lease")
    def test_runtime_job_lease_reap_exact_delegation(
        self, mock_exact, mock_conn
    ) -> None:
        mock_exact.return_value = {"mode": "exact", "reaped_count": 1}
        args = self._args(
            actor="operator",
            batch_size=None,
            lease_id="lease-1",
            job_id="job-1",
            db=":memory:",
        )
        code, payload = self._capture_json(handle_runtime_job_lease_reap, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload["result"]["mode"], "exact")
        mock_exact.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            lease_id="lease-1",
            job_id="job-1",
            actor="operator",
        )

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.reap_exact_lease")
    @patch("coordinate.execution_cli.reap_due_leases")
    def test_runtime_job_lease_reap_partial_and_mixed_precede_connection(
        self, mock_global, mock_exact, mock_conn
    ) -> None:
        cases = (
            self._args(
                actor="runtime",
                batch_size=None,
                lease_id="lease-1",
                job_id=None,
                db=":memory:",
            ),
            self._args(
                actor="runtime",
                batch_size=None,
                lease_id=None,
                job_id="job-1",
                db=":memory:",
            ),
            self._args(
                actor="runtime",
                batch_size=3,
                lease_id="lease-1",
                job_id="job-1",
                db=":memory:",
            ),
        )
        for args in cases:
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(handle_runtime_job_lease_reap(args), 1)
        mock_conn.assert_not_called()
        mock_global.assert_not_called()
        mock_exact.assert_not_called()


class RuntimeRequestSubmitCLITests(unittest.TestCase):
    """P9-2B: direct parser/handler tests for runtime request submit."""

    def _args(self, **kwargs) -> SimpleNamespace:
        return SimpleNamespace(**kwargs)

    def _capture(self, func, args) -> tuple[int, str]:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = func(args)
        return code, stdout.getvalue()

    def _capture_json(self, func, args) -> tuple[int, dict]:
        code, stdout = self._capture(func, args)
        return code, json.loads(stdout)

    def _parse(self, argv: list[str]) -> argparse.Namespace:
        parser = build_parser()
        return parser.parse_args(argv)

    def test_exact_with_route_capability_rejected(self):
        with self.assertRaises(SystemExit):
            self._parse([
                "runtime", "request", "submit", "demo",
                "--target-agent", "a1",
                "--route-capability", "coding",
                "--prompt", "go",
                "--origin-json", "{}",
                "--reply-json", "{}",
            ])

    def test_exact_parser_accepts_worktree_path(self):
        args = self._parse([
            "runtime", "request", "submit", "demo",
            "--target-agent", "a1",
            "--worktree-path", "/control/ws/e1",
            "--prompt", "go",
            "--origin-json", "{}",
            "--reply-json", "{}",
        ])
        self.assertEqual(args.worktree_path, "/control/ws/e1")

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_routed_handler_rejects_worktree_path(self, mock_submit, mock_conn):
        args = self._args(
            workspace_id="demo",
            target_agent=None,
            route_capabilities=["coding"],
            route_definition=None,
            preferred_host=None,
            override_agent=None,
            override_reason=None,
            worktree_path="/control/ws/e1",
            prompt="go",
            origin_json="{}",
            reply_json="{}",
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            code, stdout = self._capture(handle_runtime_request_submit, args)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        self.assertEqual(
            stderr.getvalue(),
            "error: --worktree-path requires --target-agent\n",
        )
        mock_submit.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_exact_handler_rejects_route_definition(self, mock_submit, mock_conn):
        args = self._args(
            workspace_id="demo",
            target_agent="a1",
            route_capabilities=None,
            route_definition="coder",
            preferred_host=None,
            override_agent=None,
            override_reason=None,
            prompt="go",
            origin_json="{}",
            reply_json="{}",
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, stdout = self._capture(handle_runtime_request_submit, args)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        mock_submit.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_exact_handler_rejects_preferred_host(self, mock_submit, mock_conn):
        args = self._args(
            workspace_id="demo",
            target_agent="a1",
            route_capabilities=None,
            route_definition=None,
            preferred_host="mac",
            override_agent=None,
            override_reason=None,
            prompt="go",
            origin_json="{}",
            reply_json="{}",
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, stdout = self._capture(handle_runtime_request_submit, args)
        self.assertEqual(code, 1)
        mock_submit.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_exact_handler_rejects_override_agent(self, mock_submit, mock_conn):
        args = self._args(
            workspace_id="demo",
            target_agent="a1",
            route_capabilities=None,
            route_definition=None,
            preferred_host=None,
            override_agent="a1",
            override_reason=None,
            prompt="go",
            origin_json="{}",
            reply_json="{}",
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, stdout = self._capture(handle_runtime_request_submit, args)
        self.assertEqual(code, 1)
        mock_submit.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_exact_handler_rejects_override_reason(self, mock_submit, mock_conn):
        args = self._args(
            workspace_id="demo",
            target_agent="a1",
            route_capabilities=None,
            route_definition=None,
            preferred_host=None,
            override_agent=None,
            override_reason="reason",
            prompt="go",
            origin_json="{}",
            reply_json="{}",
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, stdout = self._capture(handle_runtime_request_submit, args)
        self.assertEqual(code, 1)
        mock_submit.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_exact_handler_rejects_route_capabilities(self, mock_submit, mock_conn):
        args = self._args(
            workspace_id="demo",
            target_agent="a1",
            route_capabilities=["coding"],
            route_definition=None,
            preferred_host=None,
            override_agent=None,
            override_reason=None,
            prompt="go",
            origin_json="{}",
            reply_json="{}",
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, stdout = self._capture(handle_runtime_request_submit, args)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        mock_submit.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_routed_handler_rejects_override_id_only(self, mock_submit, mock_conn):
        args = self._args(
            workspace_id="demo",
            target_agent=None,
            route_capabilities=["coding"],
            route_definition=None,
            preferred_host=None,
            override_agent="a1",
            override_reason=None,
            prompt="go",
            origin_json="{}",
            reply_json="{}",
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, stdout = self._capture(handle_runtime_request_submit, args)
        self.assertEqual(code, 1)
        mock_submit.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_routed_handler_rejects_override_reason_only(self, mock_submit, mock_conn):
        args = self._args(
            workspace_id="demo",
            target_agent=None,
            route_capabilities=["coding"],
            route_definition=None,
            preferred_host=None,
            override_agent=None,
            override_reason="reason",
            prompt="go",
            origin_json="{}",
            reply_json="{}",
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, stdout = self._capture(handle_runtime_request_submit, args)
        self.assertEqual(code, 1)
        mock_submit.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_routed_handler_rejects_blank_override_reason(self, mock_submit, mock_conn):
        args = self._args(
            workspace_id="demo",
            target_agent=None,
            route_capabilities=["coding"],
            route_definition=None,
            preferred_host=None,
            override_agent="a1",
            override_reason="   ",
            prompt="go",
            origin_json="{}",
            reply_json="{}",
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, stdout = self._capture(handle_runtime_request_submit, args)
        self.assertEqual(code, 1)
        mock_submit.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_routed_handler_rejects_control_char_override_reason(self, mock_submit, mock_conn):
        args = self._args(
            workspace_id="demo",
            target_agent=None,
            route_capabilities=["coding"],
            route_definition=None,
            preferred_host=None,
            override_agent="a1",
            override_reason="bad\x00reason",
            prompt="go",
            origin_json="{}",
            reply_json="{}",
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, stdout = self._capture(handle_runtime_request_submit, args)
        self.assertEqual(code, 1)
        mock_submit.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_routed_handler_rejects_overlong_override_reason(self, mock_submit, mock_conn):
        args = self._args(
            workspace_id="demo",
            target_agent=None,
            route_capabilities=["coding"],
            route_definition=None,
            preferred_host=None,
            override_agent="a1",
            override_reason="x" * 513,
            prompt="go",
            origin_json="{}",
            reply_json="{}",
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, stdout = self._capture(handle_runtime_request_submit, args)
        self.assertEqual(code, 1)
        mock_submit.assert_not_called()

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_routed_handler_normalizes_capabilities(self, mock_submit, mock_conn):
        result = Mock()
        result.to_dict.return_value = {"id": "r1"}
        mock_submit.return_value = result
        args = self._args(
            workspace_id="demo",
            target_agent=None,
            route_capabilities=["review", "coding", "review"],
            route_definition=None,
            preferred_host=None,
            override_agent=None,
            override_reason=None,
            prompt="go",
            origin_json='{"platform": "discord", "destination": "ch", "message_id": "m1"}',
            reply_json='{"platform": "discord", "destination": "ch"}',
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, _ = self._capture_json(handle_runtime_request_submit, args)
        self.assertEqual(code, 0)
        mock_submit.assert_called_once()
        passed_routing = mock_submit.call_args.kwargs["routing_request"]
        self.assertEqual(passed_routing.required_capabilities, ("coding", "review"))

    @patch("coordinate.execution_cli._conn")
    @patch("coordinate.execution_cli.submit_request")
    def test_valid_routed_outputs_json(self, mock_submit, mock_conn):
        result = Mock()
        result.to_dict.return_value = {"id": "r1"}
        mock_submit.return_value = result
        args = self._args(
            workspace_id="demo",
            target_agent=None,
            route_capabilities=["coding"],
            route_definition=None,
            preferred_host=None,
            override_agent=None,
            override_reason=None,
            prompt="go",
            origin_json='{"platform": "discord", "destination": "ch", "message_id": "m1"}',
            reply_json='{"platform": "discord", "destination": "ch"}',
            actor="bridge",
            task_id=None,
            idempotency_key=None,
            db=":memory:",
        )
        code, payload = self._capture_json(handle_runtime_request_submit, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"result": {"id": "r1"}})
        mock_submit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
