"""Boundary tests for P9-0A2a workspace/state/reconcile CLI extraction."""
from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import coordinate.cli
import coordinate.cli_support
import coordinate.pr_cli
import coordinate.workspace_cli
from coordinate.cli import main


# P9-0A2a migrated exactly these 11 handlers to coordinate.workspace_cli.
MIGRATED_HANDLERS = [
    "handle_workspace_add",
    "handle_workspace_list",
    "handle_workspace_audit",
    "handle_workspace_doctor",
    "handle_workspace_init_harness",
    "handle_workspace_agent_add",
    "handle_workspace_agent_sync",
    "handle_workspace_host_profile_set",
    "handle_workspace_host_profile_list",
    "handle_state",
    "handle_reconcile",
]

MIGRATED_LEAF_PATHS = [
    "workspace add",
    "workspace list",
    "workspace audit",
    "workspace doctor",
    "workspace init-harness",
    "workspace agent add",
    "workspace agent sync",
    "workspace host-profile set",
    "workspace host-profile list",
    "state",
    "reconcile",
]

# S4-B1 added a new workspace_cli leaf that is intentionally owned there.
S4B1_WORKSPACE_LEAF_PATHS = {"workspace agent remove-override"}

SRC_PATH = Path(__file__).resolve().parents[1] / "src"


class WorkspaceCliOwnershipTests(unittest.TestCase):
    """Tests proving ownership, aliases, and import direction."""

    def test_root_handler_aliases_are_identical_to_workspace_cli(self) -> None:
        for name in MIGRATED_HANDLERS:
            root_attr = getattr(coordinate.cli, name, None)
            owner_attr = getattr(coordinate.workspace_cli, name, None)
            self.assertIsNotNone(owner_attr, f"{name} missing from workspace_cli")
            self.assertIs(
                root_attr,
                owner_attr,
                f"coordinate.cli.{name} must be the same object as coordinate.workspace_cli.{name}",
            )
            self.assertTrue(callable(root_attr))

    def test_root_module_no_longer_defines_moved_handlers(self) -> None:
        import inspect

        root_source = inspect.getsource(coordinate.cli)
        for name in MIGRATED_HANDLERS:
            self.assertNotIn(
                f"def {name}",
                root_source,
                f"Root CLI must not define {name} after extraction",
            )

    def test_root_module_no_longer_imports_workspace_only_services(self) -> None:
        import inspect

        root_source = inspect.getsource(coordinate.cli)
        workspace_only = [
            "audit_workspace",
            "diagnose_workspace",
            "parse_agents_toml",
            "init_file_harness",
            "init_full_harness",
            "reconcile_workspace",
            "HarnessAdapter",
            "upsert_workspace",
            "list_workspaces",
            "list_workspace_host_profiles",
            "set_workspace_agent",
            "sync_workspace_agents",
            "upsert_workspace_host_profile",
        ]
        for name in workspace_only:
            self.assertNotIn(
                name,
                root_source,
                f"Root CLI must not import {name} after extraction",
            )

    def test_workspace_cli_does_not_import_root_cli(self) -> None:
        script = """
import sys
import coordinate.workspace_cli
if 'coordinate.cli' in sys.modules:
    raise SystemExit('workspace_cli imported coordinate.cli')
print('ok')
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=SRC_PATH,
            env={"PYTHONPATH": str(SRC_PATH), "PATH": os.environ.get("PATH", "")},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        self.assertIn("ok", result.stdout)

    def test_import_orders_succeed(self) -> None:
        modules = [
            "coordinate.cli",
            "coordinate.workspace_cli",
            "coordinate.cli_support",
            "coordinate.pr_cli",
        ]
        # Rotate through several orderings.
        for offset in range(len(modules)):
            order = modules[offset:] + modules[:offset]
            script = "; ".join(f"import {name}" for name in order) + "; print('ok')"
            with self.subTest(order=order):
                result = subprocess.run(
                    [sys.executable, "-c", script],
                    cwd=SRC_PATH,
                    env={"PYTHONPATH": str(SRC_PATH), "PATH": os.environ.get("PATH", "")},
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                )
                self.assertIn("ok", result.stdout)


class WorkspaceCliRegistrationTests(unittest.TestCase):
    """Tests proving registration preserves position and leaf ownership."""

    def _run_cli(self, *args: str) -> tuple[int, dict, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                code = main(list(args))
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else 1
        raw_out = stdout.getvalue()
        payload = json.loads(raw_out) if raw_out.strip() else {}
        return code, payload, stderr.getvalue()

    def test_register_workspace_commands_adds_workspace_and_state(self) -> None:
        parser = coordinate.workspace_cli.argparse.ArgumentParser(prog="test")
        subcommands = parser.add_subparsers(dest="command")
        coordinate.workspace_cli.register_workspace_commands(subcommands)
        choices = list(subcommands.choices.keys())
        self.assertEqual(choices, ["workspace", "state"])
        workspace_parser = subcommands.choices["workspace"]
        subparser_actions = [
            a for a in workspace_parser._actions
            if isinstance(a, coordinate.workspace_cli.argparse._SubParsersAction)
        ]
        self.assertEqual(len(subparser_actions), 1)
        self.assertIn("add", subparser_actions[0].choices)

    def test_register_reconcile_command_adds_reconcile_once(self) -> None:
        parser = coordinate.workspace_cli.argparse.ArgumentParser(prog="test")
        subcommands = parser.add_subparsers(dest="command")
        coordinate.workspace_cli.register_workspace_commands(subcommands)
        subcommands.add_parser("event", help="placeholder")
        coordinate.workspace_cli.register_reconcile_command(subcommands)
        choices = list(subcommands.choices.keys())
        self.assertEqual(choices, ["workspace", "state", "event", "reconcile"])

    def test_build_parser_leaves_point_to_workspace_cli(self) -> None:
        parser = coordinate.cli.build_parser()
        workspace_cli_leaves = set(MIGRATED_LEAF_PATHS) | S4B1_WORKSPACE_LEAF_PATHS
        expected_handlers = dict(zip(MIGRATED_LEAF_PATHS, MIGRATED_HANDLERS))
        expected_handlers["workspace agent remove-override"] = "handle_workspace_agent_remove_override"

        def traverse(p: coordinate.workspace_cli.argparse.ArgumentParser, path: list[str]) -> None:
            subparser_actions = [
                a for a in p._actions
                if isinstance(a, coordinate.workspace_cli.argparse._SubParsersAction)
            ]
            if not subparser_actions:
                leaf_path = " ".join(path)
                handler = p._defaults.get("handler")
                self.assertIsNotNone(handler, f"{leaf_path} has no handler")
                if leaf_path in workspace_cli_leaves:
                    self.assertEqual(
                        f"{handler.__module__}.{handler.__qualname__}",
                        f"coordinate.workspace_cli.{expected_handlers[leaf_path]}",
                    )
                else:
                    self.assertNotEqual(
                        handler.__module__,
                        "coordinate.workspace_cli",
                        f"{leaf_path} must not be owned by workspace_cli",
                    )
                return
            for name, child in subparser_actions[0].choices.items():
                traverse(child, path + [name])

        traverse(parser, [])


class WorkspaceCliBehaviorTests(unittest.TestCase):
    """Tests proving behavior is preserved after moving handlers."""

    def _run_cli(self, *args: str) -> tuple[int, dict, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                code = main(list(args))
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else 1
        raw_out = stdout.getvalue()
        payload = json.loads(raw_out) if raw_out.strip() else {}
        return code, payload, stderr.getvalue()

    def test_workspace_add_and_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            add_code, add_payload, _ = self._run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
                "--default-bus", "discord",
                "--default-destination", "#general",
            )
            list_code, list_payload, _ = self._run_cli("--db", db_path, "workspace", "list")

            self.assertEqual(add_code, 0)
            self.assertEqual(add_payload["workspace"]["id"], "demo")
            self.assertEqual(list_code, 0)
            self.assertEqual(list_payload["workspaces"][0]["default_bus"], "discord")

    def test_doctor_unknown_workspace_returns_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, stderr = self._run_cli("--db", db_path, "workspace", "doctor", "missing")
            self.assertEqual(code, 1)
            self.assertIn("unknown workspace", stderr)

    def test_state_unknown_workspace_returns_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, stderr = self._run_cli("--db", db_path, "state", "missing")
            self.assertEqual(code, 1)
            self.assertIn("unknown workspace", stderr)

    def test_reconcile_unknown_workspace_returns_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, stderr = self._run_cli("--db", db_path, "reconcile", "missing")
            self.assertEqual(code, 1)
            self.assertIn("unknown workspace", stderr)

    def test_host_profile_invalid_metadata_json_returns_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )
            code, _, stderr = self._run_cli(
                "--db", db_path,
                "workspace", "host-profile", "set", "demo",
                "--host-id", "mac",
                "--workspace-path", tmp,
                "--metadata-json", "not-json",
            )
            self.assertEqual(code, 1)
            self.assertIn("error:", stderr)

    def test_init_harness_minimal_missing_root_returns_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )
            code, _, stderr = self._run_cli(
                "--db", db_path,
                "workspace", "init-harness", "demo",
                "--task-id", "t1",
                "--plan-doc", "plan.md",
            )
            self.assertEqual(code, 1)
            self.assertIn("--root is required", stderr)

    def test_agent_sync_invalid_toml_returns_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )
            source = Path(tmp) / "agents.toml"
            source.write_text("not valid toml", encoding="utf-8")
            code, _, stderr = self._run_cli(
                "--db", db_path,
                "workspace", "agent", "sync", "demo",
                "--source", str(source),
            )
            self.assertEqual(code, 1)
            self.assertIn("error:", stderr)

    def test_host_profile_list_empty_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )
            code, payload, _ = self._run_cli(
                "--db", db_path,
                "workspace", "host-profile", "list", "demo",
            )
            self.assertEqual(code, 0)
            self.assertEqual(payload["profiles"], [])

    def test_agent_add_requires_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )
            code, _, stderr = self._run_cli(
                "--db", db_path,
                "workspace", "agent", "add", "demo",
                "--name", "mac-claude",
                "--discord-user-id", "123",
            )
            self.assertNotEqual(code, 0)
            self.assertIn("the following arguments are required: --reason", stderr)

    def test_agent_add_and_remove_override_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )
            add_code, add_payload, _ = self._run_cli(
                "--db", db_path,
                "workspace", "agent", "add", "demo",
                "--name", "mac-claude",
                "--discord-user-id", "123",
                "--reason", "testing",
                "--actor", "operator",
            )
            self.assertEqual(add_code, 0)
            self.assertEqual(add_payload["status"], "registered")

            remove_code, remove_payload, _ = self._run_cli(
                "--db", db_path,
                "workspace", "agent", "remove-override", "demo",
                "--name", "mac-claude",
                "--reason", "done",
            )
            self.assertEqual(remove_code, 0)
            self.assertEqual(remove_payload["status"], "removed")

    def test_agent_sync_requires_replace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._run_cli(
                "--db", db_path,
                "workspace", "add", "demo",
                "--path", tmp,
                "--harness-root", tmp,
            )
            source = Path(tmp) / "agents.toml"
            source.write_text(
                '[registry]\nid = "s"\nversion = 1\n\n[[agents]]\nid = "mac-claude"\ndiscord_user_id = 123\n',
                encoding="utf-8",
            )
            code, _, stderr = self._run_cli(
                "--db", db_path,
                "workspace", "agent", "sync", "demo",
                "--source", str(source),
            )
            self.assertEqual(code, 1)
            self.assertIn("--replace is required", stderr)




class WorktreeRootsCliTests(unittest.TestCase):
    """Issue #18: workspace host-profile set sibling worktree-root flags."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = str(Path(self.tmp.name) / "coordinator.sqlite3")
        code, _, _ = self._run_cli(
            "--db", self.db_path,
            "workspace", "add", "demo",
            "--path", self.tmp.name,
            "--harness-root", self.tmp.name,
        )
        self.assertEqual(code, 0)

    def _run_cli(self, *args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                code = main(list(args))
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else 1
        raw_out = stdout.getvalue()
        payload = json.loads(raw_out) if raw_out.strip() else {}
        return code, payload, stderr.getvalue()

    def _set_profile(self, *extra):
        return self._run_cli(
            "--db", self.db_path,
            "workspace", "host-profile", "set", "demo",
            "--host-id", "mac",
            "--workspace-path", "/host/ws",
            "--harness-root", "/host/harness",
            *extra,
        )

    def test_repeat_worktree_root_flags_replace_allowlist(self):
        code, payload, _ = self._set_profile(
            "--worktree-root", "/host/worktrees/a",
            "--worktree-root", "/host/worktrees/b",
        )
        self.assertEqual(code, 0)
        self.assertEqual(
            payload["result"]["worktree_roots"],
            ["/host/worktrees/a", "/host/worktrees/b"],
        )

    def test_omitted_flags_preserve_roots(self):
        self._set_profile("--worktree-root", "/host/worktrees/a")
        code, payload, _ = self._set_profile()
        self.assertEqual(code, 0)
        self.assertEqual(payload["result"]["worktree_roots"], ["/host/worktrees/a"])

    def test_clear_worktree_roots_flag_clears(self):
        self._set_profile("--worktree-root", "/host/worktrees/a")
        code, payload, _ = self._set_profile("--clear-worktree-roots")
        self.assertEqual(code, 0)
        self.assertEqual(payload["result"]["worktree_roots"], [])

    def test_clear_and_root_are_mutually_exclusive(self):
        code, _, stderr = self._set_profile(
            "--worktree-root", "/host/worktrees/a",
            "--clear-worktree-roots",
        )
        self.assertEqual(code, 2)
        self.assertIn("not allowed with argument", stderr)

    def test_invalid_root_returns_error(self):
        code, _, stderr = self._set_profile("--worktree-root", "relative/root")
        self.assertEqual(code, 1)
        self.assertIn("error:", stderr)
        self.assertIn("invalid worktree root", stderr)

    def test_list_roundtrips_roots(self):
        self._set_profile(
            "--worktree-root", "/host/worktrees/a",
            "--worktree-root", "/host/worktrees/b",
        )
        code, payload, _ = self._run_cli(
            "--db", self.db_path,
            "workspace", "host-profile", "list", "demo",
        )
        self.assertEqual(code, 0)
        self.assertEqual(
            payload["profiles"][0]["worktree_roots"],
            ["/host/worktrees/a", "/host/worktrees/b"],
        )

    def test_fresh_profile_without_flags_has_empty_roots(self):
        code, payload, _ = self._set_profile()
        self.assertEqual(code, 0)
        self.assertEqual(payload["result"]["worktree_roots"], [])

class ReadOnlyRegistryCliTests(unittest.TestCase):
    """T6/T7: workspace list / host-profile list run strictly read-only:
    missing/legacy/unknown DBs fail closed with zero mutation; current-schema
    queries keep the exact output contract and never touch the DB file."""

    def _run_cli(self, *args: str) -> tuple[int, dict, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                code = main(list(args))
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else 1
        raw_out = stdout.getvalue()
        payload = json.loads(raw_out) if raw_out.strip() else {}
        return code, payload, stderr.getvalue()

    def _snapshot(self, db_path: str) -> tuple[bytes | None, int | None]:
        p = Path(db_path)
        if not p.exists():
            return None, None
        return p.read_bytes(), p.stat().st_mtime_ns

    def _assert_sidecars_absent(self, db_path: str) -> None:
        for suffix in ("-journal", "-wal", "-shm"):
            self.assertFalse(Path(f"{db_path}{suffix}").exists(), f"unexpected sidecar {suffix}")

    def _downgrade_to_v15(self, db_path: str) -> None:
        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE task_usage_warning_policies")
        conn.execute("DROP TABLE job_attempt_usage")
        conn.execute("PRAGMA user_version = 15")
        conn.commit()
        conn.close()

    def _assert_fail_closed(
        self,
        db_path: str,
        stderr: str,
        before: tuple[bytes | None, int | None],
    ) -> None:
        """Assert the CLI action failed closed: clean error, and the baseline
        captured BEFORE the action is byte-identical afterwards (bytes are the
        primary zero-write evidence; mtime is a same-platform auxiliary)."""
        self.assertIn("error:", stderr)
        self.assertNotIn("Traceback", stderr)
        self.assertIsNotNone(before[0])
        after_bytes, after_mtime = self._snapshot(db_path)
        self.assertEqual(after_bytes, before[0], "DB bytes changed")
        self.assertEqual(after_mtime, before[1], "DB mtime changed")
        self._assert_sidecars_absent(db_path)

    def test_workspace_list_missing_db_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, payload, stderr = self._run_cli("--db", db_path, "workspace", "list")
            self.assertEqual(code, 1)
            self.assertEqual(payload, {})
            self.assertIn("error:", stderr)
            self.assertFalse(Path(db_path).exists())
            self._assert_sidecars_absent(db_path)

    def test_workspace_list_legacy_schema_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            from coordinate.db import initialize
            conn = initialize(db_path)
            conn.close()
            self._downgrade_to_v15(db_path)
            before = self._snapshot(db_path)
            code, payload, stderr = self._run_cli("--db", db_path, "workspace", "list")
            self.assertEqual(code, 1)
            self.assertEqual(payload, {})
            self.assertIn("schema version 15", stderr)
            self._assert_fail_closed(db_path, stderr, before)

    def test_workspace_list_unknown_schema_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            from coordinate.db import initialize
            conn = initialize(db_path)
            conn.execute("PRAGMA user_version = 17")
            conn.commit()
            conn.close()
            before = self._snapshot(db_path)
            code, payload, stderr = self._run_cli("--db", db_path, "workspace", "list")
            self.assertEqual(code, 1)
            self.assertEqual(payload, {})
            self.assertIn("schema version 17", stderr)
            self._assert_fail_closed(db_path, stderr, before)

    def test_workspace_list_current_schema_zero_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, payload, _ = self._run_cli(
                "--db", db_path, "workspace", "add", "demo",
                "--path", tmp, "--harness-root", tmp,
            )
            self.assertEqual(code, 0)
            before = self._snapshot(db_path)
            code, payload, stderr = self._run_cli("--db", db_path, "workspace", "list")
            self.assertEqual(code, 0)
            self.assertEqual(payload["workspaces"][0]["id"], "demo")
            self.assertEqual(stderr, "")
            after_bytes, after_mtime = self._snapshot(db_path)
            self.assertEqual(after_bytes, before[0], "DB bytes changed")
            self.assertEqual(after_mtime, before[1], "DB mtime changed")
            self._assert_sidecars_absent(db_path)

    def test_host_profile_list_missing_db_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, payload, stderr = self._run_cli(
                "--db", db_path, "workspace", "host-profile", "list", "demo"
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload, {})
            self.assertIn("error:", stderr)
            self.assertFalse(Path(db_path).exists())
            self._assert_sidecars_absent(db_path)

    def test_host_profile_list_legacy_schema_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            from coordinate.db import initialize
            conn = initialize(db_path)
            conn.close()
            self._downgrade_to_v15(db_path)
            before = self._snapshot(db_path)
            code, payload, stderr = self._run_cli(
                "--db", db_path, "workspace", "host-profile", "list", "demo"
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload, {})
            self.assertIn("schema version 15", stderr)
            self._assert_fail_closed(db_path, stderr, before)

    def test_host_profile_list_unknown_schema_fails_closed(self) -> None:
        """F3: host-profile list on an unknown/future schema fails closed with
        zero mutation (bytes before == bytes after, no new sidecars)."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            from coordinate.db import initialize
            conn = initialize(db_path)
            conn.execute("PRAGMA user_version = 17")
            conn.commit()
            conn.close()
            before = self._snapshot(db_path)
            code, payload, stderr = self._run_cli(
                "--db", db_path, "workspace", "host-profile", "list", "demo"
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload, {})
            self.assertIn("schema version 17", stderr)
            self._assert_fail_closed(db_path, stderr, before)

    def test_host_profile_list_current_schema_zero_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, _, _ = self._run_cli(
                "--db", db_path, "workspace", "add", "demo",
                "--path", tmp, "--harness-root", tmp,
            )
            self.assertEqual(code, 0)
            code, _, _ = self._run_cli(
                "--db", db_path, "workspace", "host-profile", "set", "demo",
                "--host-id", "mac", "--workspace-path", tmp,
            )
            self.assertEqual(code, 0)
            before = self._snapshot(db_path)
            code, payload, stderr = self._run_cli(
                "--db", db_path, "workspace", "host-profile", "list", "demo"
            )
            self.assertEqual(code, 0)
            self.assertEqual(payload["profiles"][0]["host_id"], "mac")
            self.assertEqual(stderr, "")
            after_bytes, after_mtime = self._snapshot(db_path)
            self.assertEqual(after_bytes, before[0], "DB bytes changed")
            self.assertEqual(after_mtime, before[1], "DB mtime changed")
            self._assert_sidecars_absent(db_path)


if __name__ == "__main__":
    unittest.main()
