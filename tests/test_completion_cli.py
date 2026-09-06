"""Boundary tests for the receipt-aware completion CLI extraction (P9-0A4a)."""
from __future__ import annotations

import argparse
import ast
import contextlib
import json
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import coordinate.cli
import coordinate.completion_cli
from coordinate.cli import build_parser
from coordinate.completion import CompletionReceiptError
from coordinate.completion_cli import (
    _build_mark_done_event_cli_argv,
    _remote_verification_error,
    _run_mark_done_files_receipt,
    _run_remote_cli_json,
    handle_assignment_mark_done_apply,
    handle_assignment_mark_done_claim,
    handle_assignment_mark_done_files,
    handle_assignment_mark_done_prepare,
    handle_assignment_mark_done_preflight,
    handle_assignment_mark_done_record,
)


# SHA-256 of the canonical AST projection for each of the 14 moved functions.
# Generated once from the reviewed start cfcb56f using the accepted projection:
# preserve node types, non-empty fields, scalar values, contexts, and list order
# while dropping only None/empty list/tuple fields.
# SHA-256 of the canonical AST projection for each of the 14 moved functions.
# Generated once from the reviewed start cfcb56f using the accepted projection:
# preserve node types, non-empty fields, scalar values, contexts, and list order
# while dropping only None/empty list/tuple fields.
_CANONICAL_AST_HASHES = {
    "handle_assignment_mark_done_files": "2a6757856947bd780d6585429c5936147f6bdb64be863eedd6314664bacf4936",
    "_run_mark_done_files_receipt": "3f9a491312cfbfd196b8224ba9a6d7cbf1a2856863889f0e2b19fd4a4ba09b77",
    "handle_assignment_mark_done_record": "0cdda74743e8dcaff82a576e997466f81154b6856015052682d70f2a2e0375eb",
    "_stamp_repair_verification": "9ad52e462de15491a642a6d92047c2178b93f397c813b6597f1b1ad56ffd058c",
    "handle_assignment_mark_done_prepare": "1b396f711bdb812076ff45f867cf5be816ab221e713207cc0eea0c93b280ffaa",
    "handle_assignment_mark_done_preflight": "918ae1c1a16a70c1eb4aa4c27d9b1c8a117a4f431ccb2e870d2d6acfe836870a",
    "handle_assignment_mark_done_claim": "42efbce4e9b38f68caa3126078a81733e33e9f11ad1bc4e16005b6978fdc6a67",
    "handle_assignment_mark_done_apply": "18e0e016dfa021a80358ff5e90a2099225c54bcab5f9964db05a72bbd0d412b9",
    "_lookup_receipt_for_preflight": "fac49d447daf8d23ad8cf551096b16043188d5a115bdad5a8e0b73c8a92ad7f9",
    "_build_mark_done_event_cli_argv": "47f045dc9b4a5dc12044cbe1a57f1aa0d5917f16ee47a5e94d07d4de31924255",
    "_forward_mark_done_preflight": "db6211708e649bd4dba437452489be3aa3403d10d8856b90fd5b60402b7ff89f",
    "_forward_mark_done_claim": "5b7929133000289b48e4db08421a18c09f24f5bee0fa0effb2d9ee03e40362aa",
    "_forward_mark_done_apply": "2195f07c11e7ed67648d511d6f729fe59e0d5f1ce210d0c3a4430bf3943762e3",
    "_run_remote_cli_json": "9ba4940aa7e2985a188eb59bdea8283f54bfe2637e6fd8d0f85df2a3f0250d4c",
}

# S4-D legitimately changed two of the 14 functions to derive the authoritative
# receipt state (consumed > applied > claimed > authorized) and to stop expired
# authorizations from regressing terminal receipts.  The historical constants
# above are preserved unchanged; the current hashes below express the delta.
_S4D_CURRENT_HASHES = dict(_CANONICAL_AST_HASHES)
_S4D_CURRENT_HASHES.update({
    "_lookup_receipt_for_preflight": "6c31d4cfc1274bbb8db5715a52993799c98ff0fb794a813a0d5ad614d412d803",
    "handle_assignment_mark_done_preflight": "2d350d46056c7ab685032afeaa3714011cdcaa978c9bbc79aa8d908df0e143e7",
})
_S4D_CHANGED_FUNCTIONS = frozenset({
    "_lookup_receipt_for_preflight",
    "handle_assignment_mark_done_preflight",
})

# Docstrings introduced by S4-D for the two changed functions.  The strict
# node-level rewind validates these exact texts before removing them.
_LOOKUP_S4D_DOCSTRING = (
    'Derive the authoritative receipt state from its event chain.\n\n    '
    'Precedence is ``consumed > applied > claimed > authorized``.  Partial,\n    '
    'duplicate, or inconsistent chains fail closed.  All immutable links\n    '
    '(workspace, task, actor, fingerprints, required task.done) are verified.\n    '
)
_PREFLIGHT_S4D_DOCSTRING = 'Read-only: return the authoritative receipt binding for the host.'

# R5B deltas: the remote verification gate, the transport dispatchers and the
# shared-domain delegation.  The S4-D docstrings are preserved as constants so
# the R5B rewind can restore them exactly.
_HANDLER_S4D_DOCSTRING = (
    'Coding-host mark-done-files handler.\n\n    Two paths:\n\n    - **Normal '
    '(receipt)**: requires ``--receipt`` and ``--event-cli-path``.\n      Before '
    'any canonical file mutation, forwards a read-only\n      ``mark-done-preflight`` '
    'and then an atomic ``mark-done-claim`` to the\n      control-plane coord CLI. '
    'The remote re-derives workspace/task/expiry\n      from the ledger; the host '
    'never trusts its own claims.\n    - **Repair-only**: requires non-empty '
    '``--repair-reason``. Bypasses the\n      receipt protocol and stamps '
    '``repair_only=true`` plus the reason into\n      the result for audit.\n\n    '
    'No path mutates the file before the receipt is claimed. Without either a\n    '
    'receipt+remote-CLI pair or an explicit repair reason, the command fails\n    '
    'closed.\n    '
)
_HANDLER_R5B_DOCSTRING = (
    'Coding-host mark-done-files handler.\n\n    Two paths:\n\n    - **Normal '
    '(receipt)**: requires ``--receipt`` and one remote\n      verification transport '
    '— ``--event-cli-path`` or the narrow Remote\n      MCP pair '
    '``--event-mcp-url``/``--event-mcp-token-env``. Before any\n      canonical file '
    'mutation, forwards a read-only receipt preflight and\n      then an atomic receipt '
    'claim to the remote transport. The remote\n      re-derives workspace/task/expiry '
    'from the ledger; the host never\n      trusts its own claims.\n    - **Repair-only**: '
    'requires non-empty ``--repair-reason``. Bypasses the\n      receipt protocol and '
    'stamps ``repair_only=true`` plus the reason into\n      the result for audit.\n\n    '
    'No path mutates the file before the receipt is claimed. Without either a\n    '
    'receipt+remote-transport pair or an explicit repair reason, the command\n    '
    'fails closed.\n    '
)
_LOOKUP_R5B_DOCSTRING = (
    'CLI-facing alias of the shared domain receipt-state derivation.\n\n    '
    'The chain-derivation state machine lives once in\n    '
    '``completion.lookup_receipt_for_preflight`` and is shared with the MCP\n    '
    'facade; this alias keeps the CLI call sites and the P9/S4-D ownership\n    '
    'contract unchanged.\n    '
)
# The S4-D no-remote message restored by the R5B rewind.
_S4D_FILES_NO_REMOTE_MESSAGE = (
    "normal mark-done-files path requires --event-cli-path to "
    "verify/claim the receipt online before file mutation"
)

# R5B legitimately changed three of the 14 functions: the mark-done-files
# handler gained the remote-transport gate, the receipt orchestration
# dispatches through _remote_preflight/_remote_claim/_remote_apply, and the
# preflight lookup became a one-line delegation to the shared domain helper.
_R5B_CURRENT_HASHES = dict(_S4D_CURRENT_HASHES)
_R5B_CURRENT_HASHES.update({
    "handle_assignment_mark_done_files": "83dba60fc3a03bf96c18c5cacda12577e0131546ede999bfc05a9429bb5039d1",
    "_run_mark_done_files_receipt": "d1c38718e0525e0c000f0d21b7524835795aaf88a721fdf2143d0eddba196749",
    "_lookup_receipt_for_preflight": "2d567dda8bf38af903b642ec5aba9b960773cd702445b95909b220faf6e19544",
})
_R5B_CHANGED_FUNCTIONS = frozenset({
    "handle_assignment_mark_done_files",
    "_run_mark_done_files_receipt",
    "_lookup_receipt_for_preflight",
})


class CompletionCLIOwnershipTests(unittest.TestCase):
    """Ownership and alias tests for the P9-0A4a extraction."""

    def test_all_moved_names_are_object_identical_between_root_and_completion_cli(self) -> None:
        names = [
            "register_completion_commands",
            "handle_assignment_mark_done_prepare",
            "handle_assignment_mark_done_preflight",
            "handle_assignment_mark_done_claim",
            "handle_assignment_mark_done_apply",
            "handle_assignment_mark_done_files",
            "handle_assignment_mark_done_record",
            "_run_mark_done_files_receipt",
            "_stamp_repair_verification",
            "_lookup_receipt_for_preflight",
            "_build_mark_done_event_cli_argv",
            "_forward_mark_done_preflight",
            "_forward_mark_done_claim",
            "_forward_mark_done_apply",
            "_run_remote_cli_json",
        ]
        for name in names:
            with self.subTest(name=name):
                self.assertIs(
                    getattr(coordinate.cli, name),
                    getattr(coordinate.completion_cli, name),
                )

    def test_root_has_no_moved_function_definitions(self) -> None:
        """The functions must be owned by completion_cli, not defined in cli.py."""
        source = Path(coordinate.cli.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        root_function_names = {
            node.name for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        for name in _CANONICAL_AST_HASHES:
            with self.subTest(name=name):
                self.assertNotIn(name, root_function_names)

    def test_root_retains_legacy_mark_done_and_workflow_handlers(self) -> None:
        import coordinate.workflow_cli
        self.assertIs(
            coordinate.cli.handle_assignment_mark_done,
            coordinate.workflow_cli.handle_assignment_mark_done,
        )
        self.assertIs(
            coordinate.cli.handle_assignment_request,
            coordinate.workflow_cli.handle_assignment_request,
        )
        self.assertIs(
            coordinate.cli.handle_assignment_closeout,
            coordinate.workflow_cli.handle_assignment_closeout,
        )
        self.assertIs(
            coordinate.cli.handle_assignment_mark_done_files,
            coordinate.completion_cli.handle_assignment_mark_done_files,
        )

    def test_completion_cli_exports_all_fourteen_functions(self) -> None:
        for name in _CANONICAL_AST_HASHES:
            with self.subTest(name=name):
                self.assertTrue(
                    hasattr(coordinate.completion_cli, name),
                    f"completion_cli must export {name}",
                )
                self.assertTrue(
                    callable(getattr(coordinate.completion_cli, name)),
                    f"{name} must be callable",
                )


class CompletionCLIRegistrationTests(unittest.TestCase):
    """Parser registration tests for receipt-aware leaves."""

    def _assignment_leaves(self) -> list[tuple[str, object]]:
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
        return list(sub.choices.items())

    def test_register_completion_commands_adds_exactly_six_leaves(self) -> None:
        leaves = self._assignment_leaves()
        receipt_leaves = [name for name, _ in leaves if name.startswith("mark-done-")]
        self.assertEqual(
            receipt_leaves,
            [
                "mark-done-prepare",
                "mark-done-preflight",
                "mark-done-claim",
                "mark-done-apply",
                "mark-done-files",
                "mark-done-record",
            ],
        )

    def test_six_leaves_are_ordered_immediately_after_legacy_mark_done(self) -> None:
        leaves = self._assignment_leaves()
        names = [name for name, _ in leaves]
        mark_done_index = names.index("mark-done")
        self.assertEqual(
            names[mark_done_index : mark_done_index + 7],
            [
                "mark-done",
                "mark-done-prepare",
                "mark-done-preflight",
                "mark-done-claim",
                "mark-done-apply",
                "mark-done-files",
                "mark-done-record",
            ],
        )

    def test_legacy_mark_done_handler_retained(self) -> None:
        leaves = self._assignment_leaves()
        handler = dict(leaves)["mark-done"]._defaults["handler"]
        self.assertIs(handler, coordinate.cli.handle_assignment_mark_done)

    def test_receipt_leaves_point_to_completion_cli(self) -> None:
        leaves = self._assignment_leaves()
        choices = dict(leaves)
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


class CompletionCLIBodyProofTests(unittest.TestCase):
    """Canonical AST body proofs for the 14 moved functions."""

    @staticmethod
    def _canonicalize(node: ast.AST) -> object:
        if isinstance(node, ast.AST):
            result: dict[str, object] = {"_type": type(node).__name__}
            for field, value in ast.iter_fields(node):
                if value is None:
                    continue
                if isinstance(value, (list, tuple)) and not value:
                    continue
                result[field] = CompletionCLIBodyProofTests._canonicalize(value)
            return result
        if isinstance(node, (list, tuple)):
            return [CompletionCLIBodyProofTests._canonicalize(item) for item in node]
        if isinstance(node, (str, int, float, bool)):
            return node
        if node is None:
            return None
        return repr(node)

    def test_moved_function_bodies_match_canonical_hashes(self) -> None:
        source = Path(coordinate.completion_cli.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        functions = {
            node.name: node for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        for name, expected_hash in _R5B_CURRENT_HASHES.items():
            with self.subTest(name=name):
                func = functions[name]
                canon = self._canonicalize(func)
                payload = json.dumps(
                    canon, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
                import hashlib
                actual_hash = hashlib.sha256(payload).hexdigest()
                self.assertEqual(
                    actual_hash,
                    expected_hash,
                    f"Canonical AST projection for {name} changed",
                )

    @staticmethod
    def _is_docstring(stmt: ast.stmt, text: str) -> bool:
        return (
            isinstance(stmt, ast.Expr)
            and isinstance(stmt.value, ast.Constant)
            and isinstance(stmt.value.value, str)
            and stmt.value.value == text
        )

    @staticmethod
    def _is_import_from(stmt: ast.stmt, module: str, level: int, names: list[str]) -> bool:
        return (
            isinstance(stmt, ast.ImportFrom)
            and stmt.module == module
            and stmt.level == level
            and [a.name for a in stmt.names] == names
        )

    @staticmethod
    def _is_assign_to(stmt: ast.stmt, name: str) -> bool:
        return (
            isinstance(stmt, ast.Assign)
            and len(stmt.targets) == 1
            and isinstance(stmt.targets[0], ast.Name)
            and stmt.targets[0].id == name
        )

    @staticmethod
    def _is_annassign_to(stmt: ast.stmt, name: str) -> bool:
        return (
            isinstance(stmt, ast.AnnAssign)
            and isinstance(stmt.target, ast.Name)
            and stmt.target.id == name
        )

    @staticmethod
    def _assert_lookup_shape(body: list[ast.stmt]) -> None:
        """Validate the exact S4-D body shape before rewinding."""
        if len(body) != 19:
            raise AssertionError(
                f"_lookup_receipt_for_preflight body length {len(body)} != 19"
            )
        if not CompletionCLIBodyProofTests._is_docstring(body[0], _LOOKUP_S4D_DOCSTRING):
            raise AssertionError("stmt 0: expected S4-D docstring")
        if not CompletionCLIBodyProofTests._is_import_from(
            body[1], 'db', 1, ['get_event', 'row_to_dict']
        ):
            raise AssertionError("stmt 1: expected 'from .db import get_event, row_to_dict'")
        if not CompletionCLIBodyProofTests._is_assign_to(body[2], 'rows'):
            raise AssertionError("stmt 2: expected rows assignment")
        if not CompletionCLIBodyProofTests._is_assign_to(body[3], 'events'):
            raise AssertionError("stmt 3: expected events assignment")
        if not isinstance(body[4], ast.Expr):
            raise AssertionError("stmt 4: expected events.sort call")
        if not isinstance(body[5], ast.If):
            raise AssertionError("stmt 5: expected if not events")
        if not isinstance(body[6], ast.If):
            raise AssertionError("stmt 6: expected first-event type check")
        if not CompletionCLIBodyProofTests._is_assign_to(body[7], 'status_order'):
            raise AssertionError("stmt 7: expected status_order assignment")
        expected_annassigns = [
            'seen', 'workspace_id', 'task_id', 'actor', 'state',
            'authorized_harness_fingerprint', 'claimed_expected_after',
            'applied_after',
        ]
        for offset, name in enumerate(expected_annassigns, start=8):
            if not CompletionCLIBodyProofTests._is_annassign_to(body[offset], name):
                raise AssertionError(f"stmt {offset}: expected annassign {name}")
        if not isinstance(body[16], ast.For):
            raise AssertionError("stmt 16: expected for event in events")
        if not CompletionCLIBodyProofTests._is_assign_to(body[17], 'authorized_payload'):
            raise AssertionError("stmt 17: expected authorized_payload assignment")
        if not isinstance(body[18], ast.Return):
            raise AssertionError("stmt 18: expected final return")

    @staticmethod
    def _rewind_lookup(func: ast.FunctionDef) -> None:
        """Rewind _lookup_receipt_for_preflight from S4-D to P9-0A4a in place.

        No historical body is assembled.  We validate the existing body, mutate
        the existing ImportFrom aliases, then construct only the two new local
        expressions required (``row = latest_event(...)`` and the final return).
        """
        CompletionCLIBodyProofTests._assert_lookup_shape(func.body)
        # Mutate the existing ImportFrom: get_event -> latest_event.
        func.body[1].names = [
            ast.alias(name='latest_event'),
            ast.alias(name='row_to_dict'),
        ]
        # Construct the minimal row assignment expression.
        row_assign = ast.Assign(
            targets=[ast.Name(id='row', ctx=ast.Store())],
            value=ast.Call(
                func=ast.Name(id='latest_event', ctx=ast.Load()),
                args=[ast.Name(id='conn', ctx=ast.Load())],
                keywords=[
                    ast.keyword(
                        arg='event_type',
                        value=ast.Constant(value='completion.authorized'),
                    ),
                    ast.keyword(
                        arg='payload_key',
                        value=ast.Constant(value='receipt_id'),
                    ),
                    ast.keyword(
                        arg='payload_value',
                        value=ast.Name(id='receipt_id', ctx=ast.Load()),
                    ),
                ],
            ),
        )
        ret = ast.Return(
            value=ast.IfExp(
                test=ast.Compare(
                    left=ast.Name(id='row', ctx=ast.Load()),
                    ops=[ast.IsNot()],
                    comparators=[ast.Constant(value=None)],
                ),
                body=ast.Call(
                    func=ast.Name(id='row_to_dict', ctx=ast.Load()),
                    args=[ast.Name(id='row', ctx=ast.Load())],
                    keywords=[],
                ),
                orelse=ast.Constant(value=None),
            ),
        )
        # Drop the S4-D docstring and every chain-only statement.
        func.body = [func.body[1], row_assign, ret]
        ast.fix_missing_locations(func)

    @staticmethod
    def _assert_preflight_shape(body: list[ast.stmt]) -> None:
        """Validate the exact S4-D body shape before rewinding."""
        if len(body) != 15:
            raise AssertionError(
                f"handle_assignment_mark_done_preflight body length {len(body)} != 15"
            )
        if not CompletionCLIBodyProofTests._is_docstring(body[0], _PREFLIGHT_S4D_DOCSTRING):
            raise AssertionError("stmt 0: expected preflight docstring")
        # stmt 1: with _conn(args) as conn: state = _lookup_receipt_for_preflight(...)
        if not isinstance(body[1], ast.With):
            raise AssertionError("stmt 1: expected with _conn(args) as conn")
        with_stmt = body[1]
        if len(with_stmt.items) != 1:
            raise AssertionError("stmt 1: expected single withitem")
        if not (
            isinstance(with_stmt.items[0].context_expr, ast.Call)
            and isinstance(with_stmt.items[0].context_expr.func, ast.Name)
            and with_stmt.items[0].context_expr.func.id == '_conn'
            and isinstance(with_stmt.items[0].optional_vars, ast.Name)
            and with_stmt.items[0].optional_vars.id == 'conn'
        ):
            raise AssertionError("stmt 1: expected _conn(args) as conn")
        if len(with_stmt.body) != 1:
            raise AssertionError("stmt 1: expected single assignment in with body")
        inner = with_stmt.body[0]
        if not (
            isinstance(inner, ast.Assign)
            and len(inner.targets) == 1
            and isinstance(inner.targets[0], ast.Name)
            and inner.targets[0].id == 'state'
            and isinstance(inner.value, ast.Call)
            and isinstance(inner.value.func, ast.Name)
            and inner.value.func.id == '_lookup_receipt_for_preflight'
        ):
            raise AssertionError(
                "stmt 1: expected state = _lookup_receipt_for_preflight(...)"
            )
        # stmt 2: if state is None: ...
        if not (
            isinstance(body[2], ast.If)
            and isinstance(body[2].test, ast.Compare)
            and isinstance(body[2].test.left, ast.Name)
            and body[2].test.left.id == 'state'
        ):
            raise AssertionError("stmt 2: expected if state is None")
        # stmt 3: if state.get("broken"): ...
        if not (
            isinstance(body[3], ast.If)
            and isinstance(body[3].test, ast.Call)
            and isinstance(body[3].test.func, ast.Attribute)
            and body[3].test.func.attr == 'get'
            and isinstance(body[3].test.func.value, ast.Name)
            and body[3].test.func.value.id == 'state'
            and len(body[3].test.args) == 1
            and isinstance(body[3].test.args[0], ast.Constant)
            and body[3].test.args[0].value == 'broken'
        ):
            raise AssertionError("stmt 3: expected if state.get('broken')")
        # stmts 4-6: workspace_id/task_id/expires_at = state.get(...)
        for i, name in [(4, 'workspace_id'), (5, 'task_id'), (6, 'expires_at')]:
            if not (
                CompletionCLIBodyProofTests._is_assign_to(body[i], name)
                and isinstance(body[i].value, ast.Call)
                and isinstance(body[i].value.func, ast.Attribute)
                and body[i].value.func.attr == 'get'
                and isinstance(body[i].value.func.value, ast.Name)
                and body[i].value.func.value.id == 'state'
                and len(body[i].value.args) == 1
                and isinstance(body[i].value.args[0], ast.Constant)
                and body[i].value.args[0].value == name
            ):
                raise AssertionError(f"stmt {i}: expected {name} = state.get('{name}')")
        # stmt 7: status = state.get("status")
        if not (
            CompletionCLIBodyProofTests._is_assign_to(body[7], 'status')
            and isinstance(body[7].value, ast.Call)
            and isinstance(body[7].value.func, ast.Attribute)
            and body[7].value.func.attr == 'get'
            and isinstance(body[7].value.func.value, ast.Name)
            and body[7].value.func.value.id == 'state'
            and len(body[7].value.args) == 1
            and isinstance(body[7].value.args[0], ast.Constant)
            and body[7].value.args[0].value == 'status'
        ):
            raise AssertionError("stmt 7: expected status = state.get('status')")
        # stmt 8: expired = False
        if not CompletionCLIBodyProofTests._is_assign_to(body[8], 'expired'):
            raise AssertionError("stmt 8: expected expired = False")
        # stmt 9: if status == STATUS_AUTHORIZED and expires_at:
        if not (
            isinstance(body[9], ast.If)
            and isinstance(body[9].test, ast.BoolOp)
            and isinstance(body[9].test.op, ast.And)
            and len(body[9].test.values) == 2
        ):
            raise AssertionError(
                "stmt 9: expected if status == STATUS_AUTHORIZED and expires_at"
            )
        # stmts 10-12: if blocks
        for i in [10, 11, 12]:
            if not isinstance(body[i], ast.If):
                raise AssertionError(f"stmt {i}: expected if statement")
        # stmt 13: final _print_json ok result
        if not (
            isinstance(body[13], ast.Expr)
            and isinstance(body[13].value, ast.Call)
            and isinstance(body[13].value.func, ast.Name)
            and body[13].value.func.id == '_print_json'
        ):
            raise AssertionError("stmt 13: expected final _print_json call")
        # stmt 14: return 0
        if not isinstance(body[14], ast.Return):
            raise AssertionError("stmt 14: expected return 0")

    @staticmethod
    def _rewind_preflight(func: ast.FunctionDef) -> None:
        """Rewind handle_assignment_mark_done_preflight from S4-D to P9-0A4a.

        We validate the existing body, then rename/remove/rewrite only the
        specific nodes that express the S4-D delta.  No historical body is
        substituted.
        """
        CompletionCLIBodyProofTests._assert_preflight_shape(func.body)
        body = func.body
        # Rename state -> receipt in the with assignment and the is-None check.
        with_assign = body[1].body[0]
        with_assign.targets[0].id = 'receipt'
        body[2].test.left.id = 'receipt'
        # Construct the one local expression needed: payload = receipt.get("payload") or {}.
        payload_assign = ast.Assign(
            targets=[ast.Name(id='payload', ctx=ast.Store())],
            value=ast.BoolOp(
                op=ast.Or(),
                values=[
                    ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id='receipt', ctx=ast.Load()),
                            attr='get',
                            ctx=ast.Load(),
                        ),
                        args=[ast.Constant(value='payload')],
                        keywords=[],
                    ),
                    ast.Dict(keys=[], values=[]),
                ],
            ),
        )
        # Redirect workspace_id/task_id/expires_at to payload.
        for i in [4, 5, 6]:
            body[i].value.func.value.id = 'payload'
        # Replace the expiry guard with just `expires_at`.
        body[9].test = body[9].test.values[1]
        # Rewrite the final ok result: remove terminal_event_id and read
        # status/issued_at/actor from payload.
        result_dict = body[13].value.args[0].values[0]
        keys = result_dict.keys
        values = result_dict.values
        terminal_idx = next(
            i for i, k in enumerate(keys)
            if isinstance(k, ast.Constant) and k.value == 'terminal_event_id'
        )
        keys.pop(terminal_idx)
        values.pop(terminal_idx)
        for key_name in ['status', 'issued_at', 'actor']:
            idx = next(
                i for i, k in enumerate(keys)
                if isinstance(k, ast.Constant) and k.value == key_name
            )
            if key_name == 'status':
                values[idx] = ast.Call(
                    func=ast.Attribute(
                        value=ast.Name(id='payload', ctx=ast.Load()),
                        attr='get',
                        ctx=ast.Load(),
                    ),
                    args=[ast.Constant(value='status')],
                    keywords=[],
                )
            else:
                values[idx].func.value.id = 'payload'
                values[idx].args[0].value = key_name
        new_body = [
            body[0],   # docstring
            body[1],   # with receipt = ...
            body[2],   # if receipt is None
            payload_assign,
            body[4],   # workspace_id = payload.get(...)
            body[5],   # task_id = payload.get(...)
            body[6],   # expires_at = payload.get(...)
            body[8],   # expired = False
            body[9],   # if expires_at
            body[10],  # workspace mismatch
            body[11],  # task mismatch
            body[12],  # if expired
            body[13],  # final ok
            body[14],  # return 0
        ]
        func.body = new_body
        ast.fix_missing_locations(func)

    @staticmethod
    def _is_remote_dispatch(stmt: ast.stmt) -> str | None:
        """Return the dispatched remote helper name or None."""
        if (
            isinstance(stmt, ast.Assign)
            and len(stmt.targets) == 1
            and isinstance(stmt.value, ast.Call)
            and isinstance(stmt.value.func, ast.Name)
            and stmt.value.func.id
            in ("_remote_preflight", "_remote_claim", "_remote_apply")
        ):
            return stmt.value.func.id
        return None

    @staticmethod
    def _assert_files_handler_shape(body: list[ast.stmt]) -> None:
        """Validate the exact R5B mark-done-files handler body before rewinding."""
        if len(body) != 10:
            raise AssertionError(
                f"handle_assignment_mark_done_files body length {len(body)} != 10"
            )
        if not CompletionCLIBodyProofTests._is_docstring(body[0], _HANDLER_R5B_DOCSTRING):
            raise AssertionError("stmt 0: expected R5B docstring")
        if not CompletionCLIBodyProofTests._is_assign_to(body[1], "receipt_id"):
            raise AssertionError("stmt 1: expected receipt_id assignment")
        if not CompletionCLIBodyProofTests._is_assign_to(body[2], "repair_reason"):
            raise AssertionError("stmt 2: expected repair_reason assignment")
        for idx in (3, 4):
            if not isinstance(body[idx], ast.If):
                raise AssertionError(f"stmt {idx}: expected if statement")
        if not CompletionCLIBodyProofTests._is_assign_to(body[5], "remote_error"):
            raise AssertionError("stmt 5: expected remote_error assignment")
        gate = body[6]
        if not isinstance(gate, ast.If):
            raise AssertionError("stmt 6: expected remote gate if")
        if not (
            isinstance(gate.test, ast.Compare)
            and isinstance(gate.test.left, ast.Name)
            and gate.test.left.id == "remote_error"
            and len(gate.test.ops) == 1
            and isinstance(gate.test.ops[0], ast.IsNot)
        ):
            raise AssertionError("stmt 6: expected remote_error is not None test")
        if len(gate.body) != 3:
            raise AssertionError("stmt 6: expected three inner statements")
        unpack = gate.body[0]
        if not (
            isinstance(unpack, ast.Assign)
            and len(unpack.targets) == 1
            and isinstance(unpack.targets[0], ast.Tuple)
            and len(unpack.targets[0].elts) == 2
            and all(isinstance(t, ast.Name) for t in unpack.targets[0].elts)
            and [t.id for t in unpack.targets[0].elts] == ["message", "reason"]
            and isinstance(unpack.value, ast.Name)
            and unpack.value.id == "remote_error"
        ):
            raise AssertionError("stmt 6 inner: expected message, reason = remote_error")
        print_call = gate.body[1]
        if not (
            isinstance(print_call, ast.Expr)
            and isinstance(print_call.value, ast.Call)
            and isinstance(print_call.value.func, ast.Name)
            and print_call.value.func.id == "_print_json"
            and len(print_call.value.args) == 1
            and isinstance(print_call.value.args[0], ast.Dict)
        ):
            raise AssertionError("stmt 6 inner: expected _print_json({...})")
        outer = print_call.value.args[0]
        if not (
            len(outer.keys) == 1
            and isinstance(outer.keys[0], ast.Constant)
            and outer.keys[0].value == "error"
            and len(outer.values) == 1
            and isinstance(outer.values[0], ast.Dict)
        ):
            raise AssertionError("stmt 6 inner: expected {'error': {...}}")
        inner = outer.values[0]
        if not (
            len(inner.keys) == 2
            and all(isinstance(k, ast.Constant) for k in inner.keys)
            and [k.value for k in inner.keys] == ["message", "reason"]
            and all(isinstance(v, ast.Name) for v in inner.values)
            and [v.id for v in inner.values] == ["message", "reason"]
        ):
            raise AssertionError("stmt 6 inner: expected message/reason names")
        if not isinstance(gate.body[2], ast.Return):
            raise AssertionError("stmt 6 inner: expected return 1")
        if not isinstance(body[7], ast.Try):
            raise AssertionError("stmt 7: expected try")
        if not (
            isinstance(body[8], ast.Expr)
            and isinstance(body[8].value, ast.Call)
            and isinstance(body[8].value.func, ast.Name)
            and body[8].value.func.id == "_print_json"
        ):
            raise AssertionError("stmt 8: expected final _print_json call")
        if not isinstance(body[9], ast.Return):
            raise AssertionError("stmt 9: expected return 0")

    @staticmethod
    def _rewind_files_handler(func: ast.FunctionDef) -> None:
        """Rewind the R5B remote gate to the S4-D no-remote check in place."""
        CompletionCLIBodyProofTests._assert_files_handler_shape(func.body)
        body = func.body
        gate = body[6]
        # Restore the S4-D gate test: receipt_id and not args.event_cli_path.
        gate.test = ast.BoolOp(
            op=ast.And(),
            values=[
                ast.Name(id="receipt_id", ctx=ast.Load()),
                ast.UnaryOp(
                    op=ast.Not(),
                    operand=ast.Attribute(
                        value=ast.Name(id="args", ctx=ast.Load()),
                        attr="event_cli_path",
                        ctx=ast.Load(),
                    ),
                ),
            ],
        )
        # Drop the unpack; the print dict becomes the S4-D constants.
        gate.body = gate.body[1:]
        inner = gate.body[0].value.args[0].values[0]
        for idx, key_node in enumerate(inner.keys):
            if key_node.value == "message":
                inner.values[idx] = ast.Constant(value=_S4D_FILES_NO_REMOTE_MESSAGE)
            else:
                inner.values[idx] = ast.Constant(value="no_remote_verification_path")
        body.pop(5)
        body[0] = ast.Expr(ast.Constant(value=_HANDLER_S4D_DOCSTRING))
        ast.fix_missing_locations(func)

    @staticmethod
    def _assert_files_receipt_shape(body: list[ast.stmt]) -> None:
        """Validate the exact R5B orchestration dispatches before rewinding."""
        found: dict[str, ast.Assign] = {}
        for stmt in body:
            name = CompletionCLIBodyProofTests._is_remote_dispatch(stmt)
            if name is None:
                continue
            if name in found:
                raise AssertionError(f"duplicate {name} dispatch")
            found[name] = stmt
        if set(found) != {"_remote_preflight", "_remote_claim", "_remote_apply"}:
            raise AssertionError(
                f"expected exactly the three remote dispatches, got {sorted(found)}"
            )
        for name, stmt in found.items():
            call = stmt.value
            if not (
                call.args
                and isinstance(call.args[0], ast.Name)
                and call.args[0].id == "args"
            ):
                raise AssertionError(
                    f"{name} must take args as its first positional argument"
                )
        expected_kw = {
            "_remote_preflight": ["receipt_id", "workspace_id", "task_id"],
            "_remote_claim": [
                "receipt_id", "workspace_id", "task_id",
                "before_fingerprint", "expected_after_fingerprint",
            ],
            "_remote_apply": ["receipt_id", "workspace_id", "task_id", "after_fingerprint"],
        }
        for name, stmt in found.items():
            kw = [k.arg for k in stmt.value.keywords]
            if kw != expected_kw[name]:
                raise AssertionError(f"{name} keywords {kw} != {expected_kw[name]}")
        indexes = {name: body.index(stmt) for name, stmt in found.items()}
        if not (
            indexes["_remote_preflight"]
            < indexes["_remote_claim"]
            < indexes["_remote_apply"]
        ):
            raise AssertionError("dispatches must appear preflight < claim < apply")

    @staticmethod
    def _rewind_files_receipt(func: ast.FunctionDef) -> None:
        """Rewind the three R5B dispatches to the S4-D --event-cli-path
        forwards in place (renaming the helper and restoring the args.actor
        keyword the CLI path forwards)."""
        CompletionCLIBodyProofTests._assert_files_receipt_shape(func.body)
        event_cli_path = ast.Attribute(
            value=ast.Name(id="args", ctx=ast.Load()),
            attr="event_cli_path",
            ctx=ast.Load(),
        )
        actor = ast.Attribute(
            value=ast.Name(id="args", ctx=ast.Load()),
            attr="actor",
            ctx=ast.Load(),
        )
        renames = {
            "_remote_preflight": ("_forward_mark_done_preflight", ()),
            "_remote_claim": ("_forward_mark_done_claim", ("actor",)),
            "_remote_apply": ("_forward_mark_done_apply", ("actor",)),
        }
        for stmt in func.body:
            name = CompletionCLIBodyProofTests._is_remote_dispatch(stmt)
            if name is None:
                continue
            call = stmt.value
            new_name, insert_kw = renames[name]
            call.func.id = new_name
            call.args[0] = event_cli_path
            for kw_name in insert_kw:
                # The S4-D forward passes actor=args.actor after task_id.
                call.keywords.insert(
                    3, ast.keyword(arg=kw_name, value=actor)
                )
        ast.fix_missing_locations(func)

    @staticmethod
    def _assert_lookup_delegation_shape(func: ast.FunctionDef) -> None:
        """Validate the exact R5B delegation body before rewinding."""
        body = func.body
        if len(body) != 2:
            raise AssertionError(
                f"_lookup_receipt_for_preflight body length {len(body)} != 2"
            )
        if not CompletionCLIBodyProofTests._is_docstring(body[0], _LOOKUP_R5B_DOCSTRING):
            raise AssertionError("stmt 0: expected R5B delegation docstring")
        ret = body[1]
        if not (
            isinstance(ret, ast.Return)
            and isinstance(ret.value, ast.Call)
            and isinstance(ret.value.func, ast.Name)
            and ret.value.func.id == "lookup_receipt_for_preflight"
            and len(ret.value.args) == 2
            and isinstance(ret.value.args[0], ast.Name)
            and ret.value.args[0].id == "conn"
            and isinstance(ret.value.args[1], ast.Name)
            and ret.value.args[1].id == "receipt_id"
            and not ret.value.keywords
        ):
            raise AssertionError(
                "stmt 1: expected return lookup_receipt_for_preflight(conn, receipt_id)"
            )

    @staticmethod
    def _rewind_lookup_delegation(func: ast.FunctionDef) -> None:
        """Rewind the R5B delegation to the full S4-D body.

        The S4-D body is taken from the shared domain helper
        ``completion.lookup_receipt_for_preflight`` (renamed to the CLI
        private name); the hash comparison then proves the domain body is
        exactly the reviewed S4-D body, so the CLI shim cannot drift from
        the domain state machine.
        """
        CompletionCLIBodyProofTests._assert_lookup_delegation_shape(func)
        import coordinate.completion

        source = Path(coordinate.completion.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        domain = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            and node.name == "lookup_receipt_for_preflight"
        )
        domain.name = "_lookup_receipt_for_preflight"
        func.body = domain.body
        func.args = domain.args
        func.returns = domain.returns
        func.decorator_list = []
        ast.fix_missing_locations(func)

    @staticmethod
    def _rewind_r5b_delta(tree: ast.AST) -> ast.AST:
        """Return a copy of *tree* with the R5B semantic delta removed.

        The delta touches exactly three functions:

        * ``handle_assignment_mark_done_files`` gained the
          ``_remote_verification_error`` gate (CLI-path/MCP-pair validation);
        * ``_run_mark_done_files_receipt`` dispatches through
          ``_remote_preflight/_remote_claim/_remote_apply`` instead of the
          fixed ``--event-cli-path`` forwards;
        * ``_lookup_receipt_for_preflight`` became a one-line delegation to
          the shared domain helper ``completion.lookup_receipt_for_preflight``.

        This rewriter validates the exact R5B shapes and rewinds node by
        node; the S4-D lookup body is reconstructed from the shared domain
        helper and verified by hash equality with the S4-D constant.
        """
        import copy

        rewound = copy.deepcopy(tree)
        function_nodes = {
            node.name: node for node in ast.walk(rewound)
            if isinstance(node, ast.FunctionDef)
        }
        CompletionCLIBodyProofTests._rewind_files_handler(
            function_nodes["handle_assignment_mark_done_files"]
        )
        CompletionCLIBodyProofTests._rewind_files_receipt(
            function_nodes["_run_mark_done_files_receipt"]
        )
        CompletionCLIBodyProofTests._rewind_lookup_delegation(
            function_nodes["_lookup_receipt_for_preflight"]
        )
        return rewound

    @staticmethod
    def _rewind_s4d_delta(tree: ast.AST) -> ast.AST:
        """Return a copy of *tree* with the S4-D semantic delta removed.

        The delta affects exactly two functions:

        * ``_lookup_receipt_for_preflight`` was extended from a simple
          ``latest_event`` lookup to a full receipt chain derivation.
        * ``handle_assignment_mark_done_preflight`` was changed to read from
          the derived ``state`` dict, handle broken chains, and only treat
          expiry as invalid for an unused ``authorized`` receipt.

        This rewriter operates node-by-node on the current FunctionDefs:
        it validates the exact S4-D shape, reuses existing nodes where
        possible, and raises AssertionError for any unrecognized statement.
        No historical FunctionDef, full body list, or source constant is
        reconstructed.
        """
        import copy

        rewound = copy.deepcopy(tree)
        function_nodes = {
            node.name: node for node in ast.walk(rewound)
            if isinstance(node, ast.FunctionDef)
        }
        CompletionCLIBodyProofTests._rewind_lookup(
            function_nodes['_lookup_receipt_for_preflight']
        )
        CompletionCLIBodyProofTests._rewind_preflight(
            function_nodes['handle_assignment_mark_done_preflight']
        )
        return rewound

    def _hash_function(self, func: ast.FunctionDef) -> str:
        canon = self._canonicalize(func)
        payload = json.dumps(
            canon, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        import hashlib
        return hashlib.sha256(payload).hexdigest()

    def test_s4d_delta_proof_against_rewound_hashes(self) -> None:
        """Prove that removing the S4-D delta (after the R5B rewind) recovers
        the exact P9-0A4a hashes."""
        source = Path(coordinate.completion_cli.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        functions = {
            node.name: node for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        # First: only the S4-D and R5B deltas differ from P9 in the current AST.
        current_changed = {
            name for name in _CANONICAL_AST_HASHES
            if self._hash_function(functions[name]) != _CANONICAL_AST_HASHES[name]
        }
        self.assertEqual(
            current_changed,
            set(_S4D_CHANGED_FUNCTIONS) | set(_R5B_CHANGED_FUNCTIONS),
            "Only the S4-D and R5B deltas should differ from the P9-0A4a baseline",
        )
        # Second: rewind the R5B delta and prove the S4-D hashes are recovered.
        rewound_r5b = self._rewind_r5b_delta(tree)
        rewound_r5b_functions = {
            node.name: node for node in ast.walk(rewound_r5b)
            if isinstance(node, ast.FunctionDef)
        }
        for name in _CANONICAL_AST_HASHES:
            with self.subTest(name=name, projection="r5b-rewound"):
                self.assertEqual(
                    self._hash_function(rewound_r5b_functions[name]),
                    _S4D_CURRENT_HASHES[name],
                    f"R5B-rewound AST projection for {name} must equal the S4-D current hash",
                )
        # Third: rewind the S4-D delta on the R5B-rewound tree.
        rewound = self._rewind_s4d_delta(rewound_r5b)
        rewound_functions = {
            node.name: node for node in ast.walk(rewound)
            if isinstance(node, ast.FunctionDef)
        }
        for name in _CANONICAL_AST_HASHES:
            with self.subTest(name=name, projection="rewound"):
                self.assertEqual(
                    self._hash_function(rewound_functions[name]),
                    _CANONICAL_AST_HASHES[name],
                    f"Rewound AST projection for {name} must equal P9-0A4a baseline",
                )
        for name in _S4D_CHANGED_FUNCTIONS | _R5B_CHANGED_FUNCTIONS:
            self.assertIn(name, _CANONICAL_AST_HASHES)
            self.assertIn(name, _R5B_CURRENT_HASHES)

    def test_r5b_delta_proof_against_rewound_hashes(self) -> None:
        """Prove that removing the R5B delta recovers the exact S4-D hashes."""
        source = Path(coordinate.completion_cli.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        functions = {
            node.name: node for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        current_changed = {
            name for name in _S4D_CURRENT_HASHES
            if self._hash_function(functions[name]) != _S4D_CURRENT_HASHES[name]
        }
        self.assertEqual(
            current_changed,
            set(_R5B_CHANGED_FUNCTIONS),
            "Only the R5B delta should differ from the S4-D baseline",
        )
        rewound = self._rewind_r5b_delta(tree)
        rewound_functions = {
            node.name: node for node in ast.walk(rewound)
            if isinstance(node, ast.FunctionDef)
        }
        for name in _S4D_CURRENT_HASHES:
            with self.subTest(name=name, projection="r5b-rewound"):
                self.assertEqual(
                    self._hash_function(rewound_functions[name]),
                    _S4D_CURRENT_HASHES[name],
                    f"R5B-rewound AST projection for {name} must equal the S4-D current hash",
                )

    def _load_completion_cli_ast(self) -> ast.AST:
        source = Path(coordinate.completion_cli.__file__).read_text(encoding="utf-8")
        return ast.parse(source)

    def _find_function(self, tree: ast.AST, name: str) -> ast.FunctionDef:
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return node
        raise AssertionError(f"function {name} not found in AST")

    def test_rewind_lookup_raises_on_unexpected_statement(self) -> None:
        """Injecting an unrelated statement must fail shape validation, not be ignored."""
        import copy
        tree = self._rewind_r5b_delta(self._load_completion_cli_ast())
        bad_tree = copy.deepcopy(tree)
        func = self._find_function(bad_tree, '_lookup_receipt_for_preflight')
        func.body.insert(1, ast.Pass())
        with self.assertRaises(AssertionError):
            self._rewind_s4d_delta(bad_tree)

    def test_rewind_preflight_raises_on_unexpected_statement(self) -> None:
        """Injecting an unrelated statement must fail shape validation, not be ignored."""
        import copy
        tree = self._rewind_r5b_delta(self._load_completion_cli_ast())
        bad_tree = copy.deepcopy(tree)
        func = self._find_function(bad_tree, 'handle_assignment_mark_done_preflight')
        func.body.insert(2, ast.Pass())
        with self.assertRaises(AssertionError):
            self._rewind_s4d_delta(bad_tree)

    def test_rewind_files_handler_raises_on_unexpected_statement(self) -> None:
        """The R5B handler rewind validates its exact shape before rewinding."""
        import copy
        tree = self._load_completion_cli_ast()
        bad_tree = copy.deepcopy(tree)
        func = self._find_function(bad_tree, 'handle_assignment_mark_done_files')
        func.body.insert(5, ast.Pass())
        with self.assertRaises(AssertionError):
            self._rewind_r5b_delta(bad_tree)

    def test_rewind_files_receipt_raises_on_unexpected_dispatch(self) -> None:
        """The R5B orchestration rewind refuses a renamed/extra dispatch."""
        import copy
        tree = self._load_completion_cli_ast()
        bad_tree = copy.deepcopy(tree)
        func = self._find_function(bad_tree, '_run_mark_done_files_receipt')
        for stmt in func.body:
            if CompletionCLIBodyProofTests._is_remote_dispatch(stmt) == "_remote_claim":
                stmt.value.func.id = "_remote_sneak"
        with self.assertRaises(AssertionError):
            self._rewind_r5b_delta(bad_tree)

    def test_rewind_lookup_delegation_raises_on_unexpected_statement(self) -> None:
        """The R5B delegation rewind validates its exact shape before rewinding."""
        import copy
        tree = self._load_completion_cli_ast()
        bad_tree = copy.deepcopy(tree)
        func = self._find_function(bad_tree, '_lookup_receipt_for_preflight')
        func.body.insert(1, ast.Pass())
        with self.assertRaises(AssertionError):
            self._rewind_r5b_delta(bad_tree)


class CompletionCLIRemoteProcessTests(unittest.TestCase):
    """Mocked remote coord CLI JSON failure contract."""

    def _run(self, returncode: int, stdout: str, stderr: str = "") -> None:
        completed = Mock(returncode=returncode, stdout=stdout, stderr=stderr)
        with patch("subprocess.run", return_value=completed) as mock_run:
            with self.assertRaises(CompletionReceiptError) as ctx:
                _run_remote_cli_json("/cli", ["/cli", "assignment", "mark-done-preflight", "r1"], "preflight")
        mock_run.assert_called_once_with(
            ["/cli", "assignment", "mark-done-preflight", "r1"],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        return ctx.exception

    def test_remote_success_returns_result_object(self) -> None:
        completed = Mock(returncode=0, stdout=json.dumps({"result": {"ok": True}}), stderr="")
        with patch("subprocess.run", return_value=completed):
            result = _run_remote_cli_json("/cli", ["/cli", "x"], "op")
        self.assertEqual(result, {"ok": True})

    def test_remote_nonzero_with_error_json_uses_error_reason_and_message(self) -> None:
        stdout = json.dumps({"error": {"reason": "bad_thing", "message": "explained"}})
        exc = self._run(1, stdout, "")
        self.assertEqual(exc.reason, "bad_thing")
        self.assertIn("explained", str(exc))

    def test_remote_nonzero_with_false_result_uses_payload_reason(self) -> None:
        stdout = json.dumps({"result": {"ok": False, "reason": "rejected", "message": "nope"}})
        exc = self._run(1, stdout, "")
        self.assertEqual(exc.reason, "rejected")
        self.assertIn("nope", str(exc))

    def test_remote_stderr_only_uses_returncode_and_op_failed_reason(self) -> None:
        exc = self._run(7, "", "")
        self.assertEqual(exc.reason, "preflight_failed")
        self.assertIn("7", str(exc))

    def test_remote_empty_stdout_raises_invalid_json(self) -> None:
        exc = self._run(0, "")
        self.assertEqual(exc.reason, "preflight_invalid_json")

    def test_remote_invalid_json_raises_invalid_json(self) -> None:
        exc = self._run(0, "not json")
        self.assertEqual(exc.reason, "preflight_invalid_json")

    def test_remote_missing_result_object_raises_invalid_json(self) -> None:
        exc = self._run(0, json.dumps({"other": {}}))
        self.assertEqual(exc.reason, "preflight_invalid_json")


class CompletionCLIFilesOrderTests(unittest.TestCase):
    """Two-phase order and safety invariants for mark-done-files."""

    def _make_args(self, **overrides) -> SimpleNamespace:
        defaults = {
            "workspace_path": "/tmp/ws",
            "harness_root": "/tmp/harness",
            "task_id": "t1",
            "workspace_id": "ws1",
            "actor": "operator",
            "verification": None,
            "receipt": "r1",
            "event_cli_path": "/cli",
            "event_mcp_url": None,
            "event_mcp_token_env": None,
            "repair_reason": None,
            "allow_runtime_copy": False,
        }
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def test_files_path_runs_preflight_claim_write_apply_in_order(self) -> None:
        args = self._make_args()
        calls = []

        def fake_preflight(path, *, receipt_id, workspace_id, task_id):
            calls.append("preflight")
            self.assertEqual(path, "/cli")
            return {"ok": True, "workspace_id": "ws1", "task_id": "t1"}

        def fake_claim(path, *, receipt_id, workspace_id, task_id, actor, before_fingerprint, expected_after_fingerprint):
            calls.append("claim")
            return {
                "receipt_id": receipt_id,
                "before_fingerprint": before_fingerprint,
                "expected_after_fingerprint": expected_after_fingerprint,
            }

        def fake_apply(path, *, receipt_id, workspace_id, task_id, actor, after_fingerprint):
            calls.append("apply")
            return {"ok": True}

        result_mock = Mock(after_fingerprint="fp-after", to_dict=Mock(return_value={"ok": True}))

        def fake_mark_done_files(*_args, **_kwargs):
            calls.append("write")
            return result_mock

        with patch("coordinate.completion_cli._forward_mark_done_preflight", fake_preflight):
            with patch("coordinate.completion_cli._forward_mark_done_claim", fake_claim):
                with patch("coordinate.completion_cli._forward_mark_done_apply", fake_apply):
                    with patch(
                        "coordinate.completion_cli.compute_mark_done_fingerprints",
                        return_value=Mock(before_fingerprint="fp-before", after_fingerprint="fp-after"),
                    ):
                        with patch(
                            "coordinate.completion_cli.mark_done_files",
                            side_effect=fake_mark_done_files,
                        ) as mock_write:
                            _run_mark_done_files_receipt(args, "r1")

        self.assertEqual(calls, ["preflight", "claim", "write", "apply"])
        mock_write.assert_called_once()
        call_kwargs = mock_write.call_args.kwargs
        self.assertEqual(call_kwargs["receipt"].receipt_id, "r1")

    def test_no_local_write_before_successful_claim(self) -> None:
        args = self._make_args()

        def fake_preflight(*_args, **_kwargs):
            return {"ok": True, "workspace_id": "ws1", "task_id": "t1"}

        def fake_claim(*_args, **_kwargs):
            # Return missing required fields -> fails before local write
            return {"receipt_id": "r1", "before_fingerprint": "fp-before"}

        with patch("coordinate.completion_cli._forward_mark_done_preflight", fake_preflight):
            with patch("coordinate.completion_cli._forward_mark_done_claim", fake_claim):
                with patch("coordinate.completion_cli.compute_mark_done_fingerprints") as mock_fps:
                    with patch("coordinate.completion_cli.mark_done_files") as mock_write:
                        with self.assertRaises(CompletionReceiptError) as ctx:
                            _run_mark_done_files_receipt(args, "r1")
                        self.assertEqual(ctx.exception.reason, "invalid_claim_result")
                        mock_fps.assert_called_once()
                        mock_write.assert_not_called()

    def test_repair_path_bypasses_receipt_and_calls_mark_done_files(self) -> None:
        args = self._make_args(receipt=None, repair_reason="reconcile drift", event_cli_path=None)
        result_mock = Mock(to_dict=Mock(return_value={"ok": True}))
        with patch("coordinate.completion_cli.mark_done_files", return_value=result_mock) as mock_write:
            code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 0)
        mock_write.assert_called_once_with(
            workspace_path="/tmp/ws",
            harness_root="/tmp/harness",
            task_id="t1",
            actor="operator",
            verification=None,
            allow_runtime_copy=False,
            repair_reason="reconcile drift",
        )


class CompletionCLIMCPTransportTests(unittest.TestCase):
    """R5B: mark-done-files through the narrow Remote MCP transport."""

    def _make_args(self, **overrides) -> SimpleNamespace:
        defaults = {
            "workspace_path": "/tmp/ws",
            "harness_root": "/tmp/harness",
            "task_id": "t1",
            "workspace_id": "ws1",
            "actor": "operator",
            "verification": None,
            "receipt": "r1",
            "event_cli_path": None,
            "event_mcp_url": "https://coord.example/mcp",
            "event_mcp_token_env": "R5B_TEST_TOKEN",
            "repair_reason": None,
            "allow_runtime_copy": False,
        }
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def _capture_json(self) -> tuple[list[object], unittest.mock._patch]:
        captured: list[object] = []
        return captured, patch(
            "coordinate.completion_cli._print_json", side_effect=captured.append,
        )

    # -- option validation --------------------------------------------------

    def test_remote_verification_error_variants(self):
        cases = [
            # (args overrides, expected reason or None)
            ({"event_cli_path": "/cli", "event_mcp_url": "https://x/mcp"}, "conflicting_remote"),
            ({"event_cli_path": "/cli", "event_mcp_token_env": "T"}, "conflicting_remote"),
            ({"event_mcp_url": None}, "incomplete_mcp_pair"),
            ({"event_mcp_token_env": None}, "incomplete_mcp_pair"),
            ({"event_mcp_url": "https://x/mcp", "workspace_id": None}, "missing_workspace_id"),
            ({"event_mcp_url": None, "event_mcp_token_env": None, "event_cli_path": None}, "no_remote_verification_path"),
            ({}, None),
            ({"receipt": None}, None),
        ]
        for overrides, expected in cases:
            with self.subTest(overrides=overrides):
                args = self._make_args(**overrides)
                result = _remote_verification_error(args)
                if expected is None:
                    self.assertIsNone(result)
                else:
                    self.assertIsNotNone(result)
                    self.assertEqual(result[1], expected)

    def test_handler_rejects_incomplete_mcp_pair(self):
        captured, capture = self._capture_json()
        args = self._make_args(event_mcp_url=None)
        with capture:
            with patch("coordinate.completion_cli.mark_done_files") as mock_write:
                code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 1)
        self.assertEqual(captured[0]["error"]["reason"], "incomplete_mcp_pair")
        mock_write.assert_not_called()

    def test_handler_rejects_conflicting_remotes(self):
        captured, capture = self._capture_json()
        args = self._make_args(event_cli_path="/cli")
        with capture:
            with patch("coordinate.completion_cli.mark_done_files") as mock_write:
                code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 1)
        self.assertEqual(captured[0]["error"]["reason"], "conflicting_remote")
        mock_write.assert_not_called()

    def test_handler_rejects_mcp_mode_without_workspace_id(self):
        captured, capture = self._capture_json()
        args = self._make_args(workspace_id=None)
        with capture:
            with patch("coordinate.completion_cli.mark_done_files") as mock_write:
                code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 1)
        self.assertEqual(captured[0]["error"]["reason"], "missing_workspace_id")
        mock_write.assert_not_called()

    def test_handler_repair_path_ignores_valid_remote_flags(self):
        args = self._make_args(receipt=None, repair_reason="drift")
        result_mock = Mock(to_dict=Mock(return_value={"ok": True}))
        with patch("coordinate.completion_cli.mark_done_files", return_value=result_mock) as mock_write:
            code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 0)
        mock_write.assert_called_once()

    # -- transport orchestration -------------------------------------------

    def _fake_transport(self, *, preflight_result=None, preflight_error=None,
                        claim_result=None, claim_error=None,
                        apply_result=None, apply_error=None):
        calls: list[str] = []

        class _Fake:
            def __init__(self, *, url, token_env):
                self.url = url
                self.token_env = token_env

            def preflight(self, *, receipt_id, workspace_id, **kwargs):
                calls.append("preflight")
                if preflight_error is not None:
                    raise preflight_error
                return preflight_result or {
                    "ok": True, "workspace_id": "ws1", "task_id": "t1",
                }

            def claim(self, **kwargs):
                calls.append("claim")
                if claim_error is not None:
                    raise claim_error
                return claim_result or {
                    "receipt_id": kwargs["receipt_id"],
                    "before_fingerprint": kwargs["before_fingerprint"],
                    "expected_after_fingerprint": kwargs["expected_after_fingerprint"],
                }

            def apply(self, **kwargs):
                calls.append("apply")
                if apply_error is not None:
                    raise apply_error
                return apply_result or {"ok": True}

        return _Fake, calls

    def _patch_transport(self, fake_cls):
        return patch(
            "coordinate.completion_mcp_transport.CompletionMCPTransport",
            fake_cls,
        )

    def test_mcp_preflight_failure_no_file_mutation(self):
        fake, calls = self._fake_transport(
            preflight_error=CompletionReceiptError("nope", reason="unknown_receipt"),
        )
        captured, capture = self._capture_json()
        args = self._make_args()
        with capture:
            with self._patch_transport(fake):
                with patch("coordinate.completion_cli.mark_done_files") as mock_write:
                    code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 1)
        self.assertEqual(captured[0]["error"]["reason"], "unknown_receipt")
        self.assertEqual(calls, ["preflight"])
        mock_write.assert_not_called()

    def test_mcp_full_flow_order_and_result(self):
        fake, calls = self._fake_transport()
        captured, capture = self._capture_json()
        args = self._make_args()
        result_mock = Mock(
            after_fingerprint="fp-after",
            to_dict=Mock(return_value={"ok": True, "task_id": "t1"}),
        )
        with capture:
            with self._patch_transport(fake):
                with patch(
                    "coordinate.completion_cli.compute_mark_done_fingerprints",
                    return_value=Mock(before_fingerprint="fp-before", after_fingerprint="fp-after"),
                ):
                    with patch(
                        "coordinate.completion_cli.mark_done_files",
                        side_effect=lambda *a, **k: calls.append("write") or result_mock,
                    ) as mock_write:
                        code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 0)
        self.assertEqual(calls, ["preflight", "claim", "write", "apply"])
        mock_write.assert_called_once()
        call_kwargs = mock_write.call_args.kwargs
        self.assertEqual(call_kwargs["receipt"].receipt_id, "r1")
        self.assertEqual(call_kwargs["receipt"].before_fingerprint, "fp-before")
        self.assertEqual(call_kwargs["receipt"].after_fingerprint, "fp-after")
        self.assertIn("claim", captured[0]["result"])

    def test_mcp_apply_failure_preserves_claimed_and_local_write(self):
        """Apply failure after the local write keeps the claimed state and the
        already-written canonical files (recoverable, same as the CLI path)."""
        fake, calls = self._fake_transport(
            apply_error=CompletionReceiptError("boom", reason="transport_failed"),
        )
        captured, capture = self._capture_json()
        args = self._make_args()
        result_mock = Mock(
            after_fingerprint="fp-after",
            to_dict=Mock(return_value={"ok": True}),
        )
        with capture:
            with self._patch_transport(fake):
                with patch(
                    "coordinate.completion_cli.compute_mark_done_fingerprints",
                    return_value=Mock(before_fingerprint="fp-before", after_fingerprint="fp-after"),
                ):
                    with patch(
                        "coordinate.completion_cli.mark_done_files",
                        side_effect=lambda *a, **k: calls.append("write") or result_mock,
                    ) as mock_write:
                        code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 1)
        self.assertEqual(captured[0]["error"]["reason"], "transport_failed")
        # preflight -> claim -> local write happened -> apply attempted.
        self.assertEqual(calls, ["preflight", "claim", "write", "apply"])
        mock_write.assert_called_once()

    def test_mcp_token_missing_fails_before_any_call(self):
        captured, capture = self._capture_json()
        args = self._make_args(event_mcp_token_env="R5B_UNSET_TOKEN_ENV")
        with capture:
            with patch("coordinate.completion_cli.mark_done_files") as mock_write:
                with patch.dict("os.environ", {}, clear=False):
                    os.environ.pop("R5B_UNSET_TOKEN_ENV", None)
                    code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 1)
        self.assertEqual(captured[0]["error"]["reason"], "token_missing")
        mock_write.assert_not_called()

    def test_mcp_preflight_failure_leaves_file_bytes_unchanged(self):
        """Real harness files: a remote preflight failure fails closed BEFORE
        any canonical write (bytes identical, no receipt metadata)."""
        import shutil
        import tempfile

        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: shutil.rmtree(tmp, ignore_errors=True))
        checklist_path = tmp / "harness-checklist.json"
        checklist_bytes = json.dumps({
            "project": "demo",
            "harness_root": ".",
            "version": 1,
            "updated_at": "2026-08-11",
            "items": [{
                "id": "t1",
                "title": "Task",
                "status": "doing",
                "priority": "p1",
                "verification": "",
                "workflow": {"status": "review_approved", "branch": "feat-x"},
                "owner": None,
                "selected_in_session": None,
            }],
        }, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
        checklist_path.write_bytes(checklist_bytes)

        fake, calls = self._fake_transport(
            preflight_error=CompletionReceiptError("nope", reason="unknown_receipt"),
        )
        captured, capture = self._capture_json()
        args = self._make_args(
            workspace_path=str(tmp / "ws"),
            harness_root=str(tmp),
            receipt="r1",
        )
        with capture:
            with self._patch_transport(fake):
                code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 1)
        self.assertEqual(captured[0]["error"]["reason"], "unknown_receipt")
        self.assertEqual(calls, ["preflight"])
        self.assertEqual(checklist_path.read_bytes(), checklist_bytes)
        data = json.loads(checklist_path.read_text(encoding="utf-8"))
        self.assertNotIn("completion_receipt", data["items"][0])

    def test_mcp_token_never_leaks_to_stdout_or_stderr(self):
        """Real transport with a real token: a transport failure's stdout and
        stderr never contain the token literal."""
        import io as _io
        from contextlib import redirect_stderr, redirect_stdout

        os.environ["R5B_LEAK_PROBE_TOKEN"] = "r5b-leak-probe-secret"
        self.addCleanup(os.environ.pop, "R5B_LEAK_PROBE_TOKEN", None)
        captured, capture = self._capture_json()
        args = self._make_args(
            event_mcp_url="http://127.0.0.1:1/mcp",
            event_mcp_token_env="R5B_LEAK_PROBE_TOKEN",
        )
        stdout = _io.StringIO()
        stderr = _io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            with capture:
                code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 1)
        import importlib.util
        expected_reason = (
            "transport_failed"
            if importlib.util.find_spec("mcp") is not None
            else "mcp_transport_unavailable"
        )
        self.assertEqual(captured[0]["error"]["reason"], expected_reason)
        self.assertNotIn("r5b-leak-probe-secret", stdout.getvalue())
        self.assertNotIn("r5b-leak-probe-secret", stderr.getvalue())
        self.assertNotIn("r5b-leak-probe-secret", json.dumps(captured[0]))

    def test_mcp_invalid_url_fails_before_token_and_write(self):
        """C1: a plaintext non-loopback endpoint URL is refused at the
        transport boundary with the static invalid_url reason, before any
        token read or canonical write."""
        os.environ["R5B_URL_PROBE_TOKEN"] = "url-probe-secret"
        self.addCleanup(os.environ.pop, "R5B_URL_PROBE_TOKEN", None)
        captured, capture = self._capture_json()
        args = self._make_args(
            event_mcp_url="http://example.com/mcp",
            event_mcp_token_env="R5B_URL_PROBE_TOKEN",
        )
        with capture:
            with patch("coordinate.completion_cli.mark_done_files") as mock_write:
                code = handle_assignment_mark_done_files(args)
        self.assertEqual(code, 1)
        self.assertEqual(captured[0]["error"]["reason"], "invalid_url")
        self.assertEqual(
            captured[0]["error"]["message"], "invalid Remote MCP endpoint URL",
        )
        self.assertNotIn("url-probe-secret", json.dumps(captured[0]))
        mock_write.assert_not_called()

    def test_help_shows_mcp_options(self):
        import io
        from contextlib import redirect_stdout

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
        files_parser = sub.choices["mark-done-files"]
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            files_parser.print_help()
        help_text = buffer.getvalue()
        self.assertIn("--event-mcp-url", help_text)
        self.assertIn("--event-mcp-token-env", help_text)
        self.assertIn("--event-cli-path", help_text)


class CompletionCLIDelegationTests(unittest.TestCase):
    """Service delegation and envelope tests for prepare/claim/apply/record."""

    def _capture_json(self) -> tuple[list[object], contextlib._GeneratorContextManager]:
        captured: list[object] = []
        def capture(obj):
            captured.append(obj)
        return captured, patch("coordinate.completion_cli._print_json", side_effect=capture)

    def test_prepare_delegates_to_prepare_completion_receipt(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", actor="operator",
            authorized_actor="alice", ttl_seconds=300,
        )
        receipt_mock = Mock(to_dict=Mock(return_value={"receipt_id": "r1"}))
        captured, capture_ctx = self._capture_json()
        with capture_ctx:
            with patch("coordinate.completion_cli._conn") as mock_conn:
                with patch(
                    "coordinate.completion_cli.prepare_completion_receipt",
                    return_value=receipt_mock,
                ) as mock_prepare:
                    code = handle_assignment_mark_done_prepare(args)
        self.assertEqual(code, 0)
        mock_prepare.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            workspace_id="ws1",
            task_id="t1",
            requester="operator",
            authorized_actor="alice",
            ttl_seconds=300,
        )
        self.assertEqual(captured[-1], {"result": {"receipt_id": "r1"}})

    def test_preflight_returns_authoritative_fields(self) -> None:
        args = SimpleNamespace(receipt_id="r1", workspace_id="ws1", task_id="t1")
        receipt = {
            "workspace_id": "ws1",
            "task_id": "t1",
            "status": "authorized",
            "issued_at": "2026-01-01T00:00:00Z",
            "expires_at": "2099-01-01T00:00:00Z",
            "actor": "operator",
        }
        captured, capture_ctx = self._capture_json()
        with capture_ctx:
            with patch("coordinate.completion_cli._conn") as mock_conn:
                with patch(
                    "coordinate.completion_cli._lookup_receipt_for_preflight",
                    return_value=receipt,
                ) as mock_lookup:
                    code = handle_assignment_mark_done_preflight(args)
        self.assertEqual(code, 0)
        mock_lookup.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            "r1",
        )
        self.assertEqual(captured[-1]["result"]["ok"], True)
        self.assertEqual(captured[-1]["result"]["workspace_id"], "ws1")

    def test_claim_delegates_to_claim_completion_receipt(self) -> None:
        args = SimpleNamespace(
            receipt_id="r1", workspace_id="ws1", task_id="t1", actor="operator",
            before_fingerprint="fp-before", expected_after_fingerprint="fp-after",
        )
        result_mock = Mock(to_dict=Mock(return_value={"ok": True}))
        captured, capture_ctx = self._capture_json()
        with capture_ctx:
            with patch("coordinate.completion_cli._conn") as mock_conn:
                with patch(
                    "coordinate.completion_cli.claim_completion_receipt",
                    return_value=result_mock,
                ) as mock_claim:
                    code = handle_assignment_mark_done_claim(args)
        self.assertEqual(code, 0)
        mock_claim.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            receipt_id="r1",
            workspace_id="ws1",
            task_id="t1",
            actor="operator",
            before_fingerprint="fp-before",
            expected_after_fingerprint="fp-after",
        )

    def test_apply_delegates_to_apply_completion_receipt(self) -> None:
        args = SimpleNamespace(
            receipt_id="r1", workspace_id="ws1", task_id="t1", actor="operator",
            after_fingerprint="fp-after",
        )
        result_mock = Mock(to_dict=Mock(return_value={"ok": True}))
        captured, capture_ctx = self._capture_json()
        with capture_ctx:
            with patch("coordinate.completion_cli._conn") as mock_conn:
                with patch(
                    "coordinate.completion_cli.apply_completion_receipt",
                    return_value=result_mock,
                ) as mock_apply:
                    code = handle_assignment_mark_done_apply(args)
        self.assertEqual(code, 0)
        mock_apply.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            receipt_id="r1",
            workspace_id="ws1",
            task_id="t1",
            actor="operator",
            after_fingerprint="fp-after",
        )

    def test_record_consumes_receipt(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id="t1", receipt="r1", actor="operator",
            verification="v1", idempotency_hint="hint", repair_reason=None,
        )
        result_mock = Mock(to_dict=Mock(return_value={"consumed": True}))
        captured, capture_ctx = self._capture_json()
        with capture_ctx:
            with patch("coordinate.completion_cli._conn") as mock_conn:
                with patch(
                    "coordinate.completion_cli.consume_completion_receipt",
                    return_value=result_mock,
                ) as mock_consume:
                    code = handle_assignment_mark_done_record(args)
        self.assertEqual(code, 0)
        mock_consume.assert_called_once_with(
            mock_conn.return_value.__enter__.return_value,
            receipt_id="r1",
            actor="operator",
            verification="v1",
        )
        self.assertEqual(captured[-1], {"result": {"consumed": True}})

    def test_record_repair_path_requires_task_id(self) -> None:
        args = SimpleNamespace(
            workspace_id="ws1", task_id=None, receipt=None, actor="operator",
            verification=None, idempotency_hint=None, repair_reason="reconcile",
        )
        captured, capture_ctx = self._capture_json()
        with capture_ctx:
            with patch("coordinate.completion_cli._conn"):
                with patch("coordinate.completion_cli.mark_done_record") as mock_record:
                    code = handle_assignment_mark_done_record(args)
        self.assertEqual(code, 1)
        self.assertEqual(captured[-1]["error"]["reason"], "missing_task_id")
        mock_record.assert_not_called()

    def test_build_event_cli_argv_uses_python_for_py_wrapper(self) -> None:
        import sys
        argv = _build_mark_done_event_cli_argv("/tmp/cli.py", ["a", "b"])
        self.assertEqual(argv[0], sys.executable)
        self.assertEqual(argv[1], "/tmp/cli.py")

    def test_build_event_cli_argv_passes_binary_through(self) -> None:
        argv = _build_mark_done_event_cli_argv("/tmp/cli", ["a", "b"])
        self.assertEqual(argv, ["/tmp/cli", "a", "b"])


if __name__ == "__main__":
    unittest.main()
