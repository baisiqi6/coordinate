"""Boundary tests for the P9-0A3b delivery, policy, and worker CLI extraction."""
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
import coordinate.delivery_cli
from coordinate.cli import build_parser
from coordinate.delivery_cli import (
    handle_delivery_create,
    handle_delivery_list,
    handle_delivery_pump,
    handle_delivery_recover_sending,
    handle_delivery_send,
    handle_policy_create_deliveries,
    handle_policy_create_delivery,
    handle_policy_pump_events,
    handle_policy_render_event,
    handle_worker_delivery,
    register_delivery_commands,
)


SRC_PATH = Path(__file__).resolve().parents[1] / "src"
CLI_PATH = SRC_PATH / "coordinate" / "cli.py"
DELIVERY_CLI_PATH = SRC_PATH / "coordinate" / "delivery_cli.py"

# Stable AST SHA-256 constants for the 10 handlers, computed once from the
# reviewed start commit 533ffcb1be17c6a26e8d5acf31e9c3c05da1ef63.
# The projection drops None and empty list/tuple fields to normalize Python
# minor-version AST differences. It does not claim absolute future-proof
# stability for arbitrary future ast module changes.
HANDLER_AST_SHA256 = {
    "handle_delivery_create": "139578e77baa1f34f6db8a2a9105a27678c085900a986a73dfe60edd40bb44eb",
    "handle_delivery_list": "4f5c248dcb8b295399b69389112623f64ce8b68f41b51592fa41c531bef0e348",
    "handle_delivery_send": "774936201a3488b5c1668dd07b1fab8777ebba1a84a40fee531ebb56f4eb1b7f",
    "handle_delivery_pump": "5995f68b824ef73b9fff9a997511d008671e40749ea70ea25f2da48832b18df1",
    "handle_delivery_recover_sending": "184c52a6fb8b85ba93b16c748a350cfdc7dccc5bc2e96452e3b2deb3f008a71b",
    "handle_policy_render_event": "b51e0e50f6ef6cc347925c86d1fd00fa6aeb0f12dd67451a1a61fa7d0a2d1309",
    "handle_policy_create_delivery": "e6525645594f6015d0a8e9213727390397d1c84e22cbc8a6ff28af911236dc41",
    "handle_policy_create_deliveries": "f71d4e147c3aac437e05792f67718e03c114844bb9ae886f6fb624aa86bc7224",
    "handle_policy_pump_events": "48aed563193cfa8bd311e2441fab475fa3e125da781f59f5c2b18f7105fe39fe",
    "handle_worker_delivery": "414fd67ffc56f6fdb19cc4e8d504ffa36038aae37dc9647bcd57a07939f54952",
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


class DeliveryCLIOwnershipTests(unittest.TestCase):
    """Tests that handlers moved to delivery_cli remain reachable from root."""

    def test_root_aliases_are_identical_to_delivery_cli_handlers(self) -> None:
        pairs = [
            (coordinate.cli.handle_delivery_create, handle_delivery_create),
            (coordinate.cli.handle_delivery_list, handle_delivery_list),
            (coordinate.cli.handle_delivery_send, handle_delivery_send),
            (coordinate.cli.handle_delivery_pump, handle_delivery_pump),
            (coordinate.cli.handle_delivery_recover_sending, handle_delivery_recover_sending),
            (coordinate.cli.handle_policy_render_event, handle_policy_render_event),
            (coordinate.cli.handle_policy_create_delivery, handle_policy_create_delivery),
            (coordinate.cli.handle_policy_create_deliveries, handle_policy_create_deliveries),
            (coordinate.cli.handle_policy_pump_events, handle_policy_pump_events),
            (coordinate.cli.handle_worker_delivery, handle_worker_delivery),
        ]
        for root_alias, delivery_handler in pairs:
            self.assertIs(root_alias, delivery_handler)

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

    def test_root_source_has_no_delivery_only_service_imports(self) -> None:
        source = CLI_PATH.read_text(encoding="utf-8")
        delivery_only = {
            "create_delivery",
            "list_deliveries",
            "recover_sending_deliveries",
            "send_delivery",
            "pump_deliveries",
            "create_delivery_for_event",
            "create_deliveries_for_event",
            "pump_events",
            "render_event",
            "run_delivery_worker",
        }
        found = set()
        tree = ast.parse(source)
        for node in tree.body:
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name in delivery_only or alias.asname in delivery_only:
                        found.add(alias.asname or alias.name)
        self.assertEqual(
            found,
            set(),
            f"Root CLI still imports delivery-only services: {found}",
        )

    def test_root_retains_bus_and_policy_error_dispatch(self) -> None:
        source = CLI_PATH.read_text(encoding="utf-8")
        self.assertIn("from .bus import BusError", source)
        self.assertIn("from .policy import PolicyError", source)
        self.assertIn("except (HarnessError, JobError, BusError, PolicyError, ValueError, KeyError)", source)

    def test_delivery_cli_does_not_import_root(self) -> None:
        source = DELIVERY_CLI_PATH.read_text(encoding="utf-8")
        self.assertNotIn("from .cli import", source)
        self.assertNotIn("import coordinate.cli", source)

    def test_delivery_cli_does_not_import_execution_or_workflow_modules(self) -> None:
        source = DELIVERY_CLI_PATH.read_text(encoding="utf-8")
        forbidden = {
            "execution_cli",
            "workspace_cli",
            "planning_cli",
            "issue_cli",
            "pr_cli",
            "completion",
            "transitions",
        }
        found = set()
        tree = ast.parse(source)
        for node in tree.body:
            if isinstance(node, ast.ImportFrom):
                if node.module in forbidden:
                    found.add(node.module)
        self.assertEqual(found, set(), f"delivery_cli imports execution/workflow modules: {found}")

    def test_import_orders_succeed(self) -> None:
        orders = [
            ["coordinate.cli_support", "coordinate.delivery_cli", "coordinate.cli"],
            ["coordinate.delivery_cli", "coordinate.cli_support", "coordinate.cli"],
            ["coordinate.cli", "coordinate.cli_support", "coordinate.delivery_cli"],
            ["coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.issue_cli", "coordinate.execution_cli", "coordinate.delivery_cli", "coordinate.cli"],
            ["coordinate.delivery_cli", "coordinate.workspace_cli", "coordinate.planning_cli", "coordinate.issue_cli", "coordinate.execution_cli", "coordinate.cli"],
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


class DeliveryCLIRegistrationTests(unittest.TestCase):
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

    def test_delivery_policy_worker_positions_unchanged(self) -> None:
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

    def test_only_10_leaves_owned_by_delivery_cli(self) -> None:
        leaf_handlers = self._leaf_handlers()
        delivery_leaves = {
            path for path, owner in leaf_handlers.items()
            if owner.startswith("coordinate.delivery_cli.")
        }
        self.assertEqual(
            delivery_leaves,
            {
                "delivery create",
                "delivery list",
                "delivery send",
                "delivery pump",
                "delivery recover-sending",
                "policy render-event",
                "policy create-delivery",
                "policy create-deliveries",
                "policy pump-events",
                "worker delivery",
            },
        )

    def test_all_10_leaf_handlers_match_stable_ast_hashes(self) -> None:
        for name, expected_sha in HANDLER_AST_SHA256.items():
            func = getattr(coordinate.delivery_cli, name)
            actual_sha = _handler_ast_sha256(func)
            self.assertEqual(
                actual_sha,
                expected_sha,
                f"Handler {name} AST drifted from reviewed baseline",
            )

    def test_registrar_adds_exact_command_tree(self) -> None:
        parser = argparse.ArgumentParser(prog="test")
        subcommands = parser.add_subparsers(dest="command")
        register_delivery_commands(subcommands)

        top = list(subcommands.choices.keys())
        self.assertEqual(top, ["delivery", "policy", "worker"])

        delivery = subcommands.choices["delivery"]
        delivery_leaves = list(delivery._subparsers._group_actions[0].choices.keys())
        self.assertEqual(delivery_leaves, ["create", "list", "send", "pump", "recover-sending"])

        policy = subcommands.choices["policy"]
        policy_leaves = list(policy._subparsers._group_actions[0].choices.keys())
        self.assertEqual(policy_leaves, ["render-event", "create-delivery", "create-deliveries", "pump-events"])

        worker = subcommands.choices["worker"]
        worker_leaves = list(worker._subparsers._group_actions[0].choices.keys())
        self.assertEqual(worker_leaves, ["delivery"])


class DeliveryCLIDelegationTests(unittest.TestCase):
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

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.row_to_dict")
    @patch("coordinate.delivery_cli.create_delivery")
    def test_delivery_create_invalid_json(self, mock_create, mock_row_to_dict, mock_conn) -> None:
        stderr = io.StringIO()
        args = self._args(event_id="e1", platform="discord", destination="#ch", message_key="k", payload_json="not-json", db=":memory:")
        with contextlib.redirect_stderr(stderr):
            code, stdout = self._capture(handle_delivery_create, args)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        self.assertIn("invalid --payload-json", stderr.getvalue())
        mock_create.assert_not_called()

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.row_to_dict")
    @patch("coordinate.delivery_cli.create_delivery")
    def test_delivery_create_non_object_payload(self, mock_create, mock_row_to_dict, mock_conn) -> None:
        stderr = io.StringIO()
        args = self._args(event_id="e1", platform="discord", destination="#ch", message_key="k", payload_json='["x"]', db=":memory:")
        with contextlib.redirect_stderr(stderr):
            code, stdout = self._capture(handle_delivery_create, args)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        self.assertIn("must decode to an object", stderr.getvalue())
        mock_create.assert_not_called()

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.row_to_dict")
    @patch("coordinate.delivery_cli.create_delivery")
    def test_delivery_create_delegation(self, mock_create, mock_row_to_dict, mock_conn) -> None:
        row = Mock()
        mock_row_to_dict.return_value = {"id": "d1"}
        mock_create.return_value = (row, True)
        args = self._args(event_id="e1", platform="discord", destination="#ch", message_key="k", payload_json='{"x": 1}', db=":memory:")
        code, payload = self._capture_json(handle_delivery_create, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"created": True, "delivery": {"id": "d1"}})
        mock_create.assert_called_once()
        passed_payload = mock_create.call_args.kwargs["payload"]
        self.assertEqual(passed_payload, {"x": 1})

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.row_to_dict")
    @patch("coordinate.delivery_cli.list_deliveries")
    def test_delivery_list_delegation(self, mock_list, mock_row_to_dict, mock_conn) -> None:
        mock_row_to_dict.return_value = {"id": "d1"}
        mock_list.return_value = [Mock()]
        args = self._args(status=None, platform=None, delivery_type=None, db=":memory:")
        code, payload = self._capture_json(handle_delivery_list, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"deliveries": [{"id": "d1"}]})
        mock_list.assert_called_once_with(mock_conn.return_value.__enter__.return_value, status=None, platform=None, delivery_type=None)

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.send_delivery")
    def test_delivery_send_delegation_stderr(self, mock_send, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "d1"}
        mock_send.return_value = result
        args = self._args(delivery_id="d1", db=":memory:")
        code, payload = self._capture_json(handle_delivery_send, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"result": {"id": "d1"}})
        mock_send.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            "d1",
            output_stream=sys.stderr,
        )

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.pump_deliveries")
    def test_delivery_pump_delegation_stderr_recovery(self, mock_pump, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"pumped": 1}
        mock_pump.return_value = result
        args = self._args(platform="discord", limit=5, recover_sending=True, db=":memory:")
        code, payload = self._capture_json(handle_delivery_pump, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"result": {"pumped": 1}})
        mock_pump.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            platform="discord",
            limit=5,
            output_stream=sys.stderr,
            recover_sending=True,
        )

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.row_to_dict")
    @patch("coordinate.delivery_cli.recover_sending_deliveries")
    def test_delivery_recover_sending_delegation(self, mock_recover, mock_row_to_dict, mock_conn) -> None:
        mock_row_to_dict.return_value = {"id": "d1"}
        mock_recover.return_value = [Mock()]
        args = self._args(platform="discord", db=":memory:")
        code, payload = self._capture_json(handle_delivery_recover_sending, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"recovered": 1, "deliveries": [{"id": "d1"}]})
        mock_recover.assert_called_once_with(mock_conn.return_value.__enter__.return_value, platform="discord")

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.render_event")
    def test_policy_render_event_delegation(self, mock_render, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "e1"}
        mock_render.return_value = result
        args = self._args(event_id="e1", platform="discord", destination="#ch", db=":memory:")
        code, payload = self._capture_json(handle_policy_render_event, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"result": {"id": "e1"}})
        mock_render.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            "e1",
            platform="discord",
            destination="#ch",
        )

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.create_delivery_for_event")
    def test_policy_create_delivery_delegation(self, mock_create, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "d1"}
        mock_create.return_value = result
        args = self._args(event_id="e1", platform="discord", destination="#ch", db=":memory:")
        code, payload = self._capture_json(handle_policy_create_delivery, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"result": {"id": "d1"}})
        mock_create.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            "e1",
            platform="discord",
            destination="#ch",
        )

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.create_deliveries_for_event")
    def test_policy_create_deliveries_delegation(self, mock_create, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"id": "d1"}
        mock_create.return_value = [result]
        args = self._args(event_id="e1", platform="discord", destination="#ch", db=":memory:")
        code, payload = self._capture_json(handle_policy_create_deliveries, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"results": [{"id": "d1"}]})
        mock_create.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            "e1",
            platform="discord",
            destination="#ch",
        )

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.pump_events")
    def test_policy_pump_events_delegation(self, mock_pump, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"created": 1}
        mock_pump.return_value = result
        args = self._args(
            workspace_id="ws",
            platform="discord",
            destination="#ch",
            limit=10,
            task_id="t1",
            event_type="evt",
            allow_backfill=True,
            db=":memory:",
        )
        code, payload = self._capture_json(handle_policy_pump_events, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"result": {"created": 1}})
        mock_pump.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws",
            platform="discord",
            destination="#ch",
            limit=10,
            task_id="t1",
            event_type="evt",
            allow_backfill=True,
        )

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.run_delivery_worker")
    def test_worker_delivery_once_maps_to_max_iterations_one(self, mock_run, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"pumped": 1}
        mock_run.return_value = result
        args = self._args(once=True, platform="discord", limit=5, interval=1.0, max_iterations=99, recover_sending=True, db=":memory:")
        code, payload = self._capture_json(handle_worker_delivery, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"result": {"pumped": 1}})
        mock_run.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            platform="discord",
            limit=5,
            interval=1.0,
            max_iterations=1,
            output_stream=sys.stderr,
            recover_sending=True,
        )

    @patch("coordinate.delivery_cli._conn")
    @patch("coordinate.delivery_cli.run_delivery_worker")
    def test_worker_delivery_forwards_max_iterations_when_not_once(self, mock_run, mock_conn) -> None:
        result = Mock()
        result.to_dict.return_value = {"pumped": 2}
        mock_run.return_value = result
        args = self._args(once=False, platform="discord", limit=5, interval=2.0, max_iterations=7, recover_sending=False, db=":memory:")
        code, payload = self._capture_json(handle_worker_delivery, args)
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"result": {"pumped": 2}})
        mock_run.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            platform="discord",
            limit=5,
            interval=2.0,
            max_iterations=7,
            output_stream=sys.stderr,
            recover_sending=False,
        )


if __name__ == "__main__":
    unittest.main()
