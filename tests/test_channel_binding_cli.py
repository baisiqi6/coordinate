"""CLI contract and exit-code tests for ``coordinate workspace channel``.

Covers the plan's CLI contract: bind/resolve/release/list JSON output and
exit codes, unbound resolve as a normal success, and fail-loud exit 1 on
conflict, invalid key and unknown workspace. Registration is only through
``workspace_cli.register_workspace_commands()``; root ``cli.py`` is unchanged.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path

import coordinate.channel_binding_cli
import coordinate.cli
import coordinate.workspace_cli
from coordinate.cli import main


SRC_PATH = Path(__file__).resolve().parents[1] / "src"


class _CliCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db = str(Path(self._tmpdir.name) / "coordinator.sqlite3")
        self._run_cli(
            "workspace", "add", "ws-a", "--path", "/tmp/ws-a", "--harness-root", "/tmp/ws-a/h"
        )
        self._run_cli(
            "workspace", "add", "ws-b", "--path", "/tmp/ws-b", "--harness-root", "/tmp/ws-b/h"
        )

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def _run_cli(self, *args: str) -> tuple[int, dict, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                code = main(["--db", self.db, *args])
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else 1
        raw_out = stdout.getvalue()
        payload = json.loads(raw_out) if raw_out.strip() else {}
        return code, payload, stderr.getvalue()

    def _bind(self, *extra: str) -> tuple[int, dict, str]:
        return self._run_cli(
            "workspace", "channel", "bind", "discord", "123", "ws-a",
            "--actor", "op", "--reason", "r", "--idempotency-key", "k1", *extra,
        )


class ChannelBindCliTests(_CliCase):
    def test_bind_success(self) -> None:
        code, payload, _ = self._bind()
        self.assertEqual(code, 0)
        self.assertEqual(payload["status"], "bound")
        self.assertEqual(payload["target"], "discord:123")
        self.assertEqual(payload["workspace_id"], "ws-a")

    def test_bind_conflict_other_workspace_exit_1(self) -> None:
        self._bind()
        code, _, stderr = self._run_cli(
            "workspace", "channel", "bind", "discord", "123", "ws-b",
            "--actor", "op", "--reason", "r", "--idempotency-key", "k2",
        )
        self.assertEqual(code, 1)
        self.assertIn("already bound", stderr)

    def test_bind_invalid_platform_exit_1(self) -> None:
        code, _, stderr = self._run_cli(
            "workspace", "channel", "bind", "slack", "123", "ws-a",
            "--actor", "op", "--reason", "r", "--idempotency-key", "k1",
        )
        self.assertEqual(code, 1)
        self.assertIn("invalid platform", stderr)

    def test_bind_unknown_workspace_exit_1(self) -> None:
        code, _, stderr = self._run_cli(
            "workspace", "channel", "bind", "discord", "123", "nope",
            "--actor", "op", "--reason", "r", "--idempotency-key", "k1",
        )
        self.assertEqual(code, 1)
        self.assertIn("unknown workspace", stderr)

    def test_bind_replay_is_success(self) -> None:
        self._bind()
        code, payload, _ = self._bind()
        self.assertEqual(code, 0)
        self.assertEqual(payload["status"], "replayed")

    def test_bind_missing_required_flag_is_argparse_error(self) -> None:
        code, _, _ = self._run_cli(
            "workspace", "channel", "bind", "discord", "123", "ws-a",
            "--actor", "op", "--reason", "r",  # missing --idempotency-key
        )
        self.assertEqual(code, 2)


class ChannelResolveCliTests(_CliCase):
    def test_resolve_unbound_is_success(self) -> None:
        code, payload, _ = self._run_cli("workspace", "channel", "resolve", "discord", "999")
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"binding": None, "status": "unbound"})

    def test_resolve_bound(self) -> None:
        self._bind()
        code, payload, _ = self._run_cli("workspace", "channel", "resolve", "discord", "123")
        self.assertEqual(code, 0)
        self.assertEqual(payload["status"], "bound")
        self.assertEqual(payload["binding"]["workspace_id"], "ws-a")
        self.assertEqual(payload["binding"]["channel_id"], "123")

    def test_resolve_invalid_key_exit_1(self) -> None:
        code, _, stderr = self._run_cli("workspace", "channel", "resolve", "slack", "1")
        self.assertEqual(code, 1)
        self.assertIn("invalid platform", stderr)


class ChannelReleaseCliTests(_CliCase):
    def test_release_success(self) -> None:
        self._bind()
        code, payload, _ = self._run_cli(
            "workspace", "channel", "release", "discord", "123",
            "--expected-workspace-id", "ws-a",
            "--actor", "op", "--reason", "r", "--idempotency-key", "k2",
        )
        self.assertEqual(code, 0)
        self.assertEqual(payload["status"], "released")

    def test_release_expected_mismatch_exit_1(self) -> None:
        self._bind()
        code, _, stderr = self._run_cli(
            "workspace", "channel", "release", "discord", "123",
            "--expected-workspace-id", "ws-b",
            "--actor", "op", "--reason", "r", "--idempotency-key", "k2",
        )
        self.assertEqual(code, 1)
        self.assertIn("not expected workspace", stderr)

    def test_release_unbound_is_success_noop(self) -> None:
        code, payload, _ = self._run_cli(
            "workspace", "channel", "release", "discord", "123",
            "--expected-workspace-id", "ws-a",
            "--actor", "op", "--reason", "r", "--idempotency-key", "k2",
        )
        self.assertEqual(code, 0)
        self.assertEqual(payload["status"], "already_unbound")


class ChannelListCliTests(_CliCase):
    def test_list_empty(self) -> None:
        code, payload, _ = self._run_cli("workspace", "channel", "list")
        self.assertEqual(code, 0)
        self.assertEqual(payload, {"bindings": []})

    def test_list_with_filters(self) -> None:
        self._bind()
        self._run_cli(
            "workspace", "channel", "bind", "kook", "c2", "ws-b",
            "--actor", "op", "--reason", "r", "--idempotency-key", "k2",
        )
        code, payload, _ = self._run_cli("workspace", "channel", "list", "--platform", "kook")
        self.assertEqual(code, 0)
        self.assertEqual([b["channel_id"] for b in payload["bindings"]], ["c2"])

        code, payload, _ = self._run_cli(
            "workspace", "channel", "list", "--workspace-id", "ws-a"
        )
        self.assertEqual(code, 0)
        self.assertEqual([b["channel_id"] for b in payload["bindings"]], ["123"])

    def test_list_invalid_platform_filter_exit_1(self) -> None:
        code, _, stderr = self._run_cli("workspace", "channel", "list", "--platform", "slack")
        self.assertEqual(code, 1)
        self.assertIn("invalid platform", stderr)


class ChannelRegistrationTests(unittest.TestCase):
    """Prove the channel group is registered only via workspace_cli."""

    def test_channel_handlers_live_in_channel_binding_cli(self) -> None:
        for name in (
            "handle_workspace_channel_bind",
            "handle_workspace_channel_resolve",
            "handle_workspace_channel_release",
            "handle_workspace_channel_list",
        ):
            self.assertTrue(
                callable(getattr(coordinate.channel_binding_cli, name)),
                f"{name} missing from channel_binding_cli",
            )

    def test_channel_leaves_registered_under_workspace(self) -> None:
        parser = argparse.ArgumentParser(prog="test")
        subcommands = parser.add_subparsers(dest="command")
        coordinate.workspace_cli.register_workspace_commands(subcommands)
        workspace_parser = subcommands.choices["workspace"]
        sub_action = next(
            a for a in workspace_parser._actions
            if isinstance(a, argparse._SubParsersAction)
        )
        self.assertIn("channel", sub_action.choices)
        channel_parser = sub_action.choices["channel"]
        channel_sub = next(
            a for a in channel_parser._actions
            if isinstance(a, argparse._SubParsersAction)
        )
        self.assertEqual(
            set(channel_sub.choices.keys()), {"bind", "resolve", "release", "list"}
        )

    def test_channel_leaves_have_handlers_in_full_parser(self) -> None:
        parser = coordinate.cli.build_parser()

        def traverse(p: argparse.ArgumentParser, path: list[str]):
            sub_actions = [
                a for a in p._actions if isinstance(a, argparse._SubParsersAction)
            ]
            if not sub_actions:
                yield path, p
                return
            for name, child in sub_actions[0].choices.items():
                yield from traverse(child, path + [name])

        leaves = {" ".join(path): p for path, p in traverse(parser, [])}
        for leaf, handler in (
            ("workspace channel bind", "handle_workspace_channel_bind"),
            ("workspace channel resolve", "handle_workspace_channel_resolve"),
            ("workspace channel release", "handle_workspace_channel_release"),
            ("workspace channel list", "handle_workspace_channel_list"),
        ):
            self.assertIn(leaf, leaves, f"{leaf} not registered")
            defaults = getattr(leaves[leaf], "_defaults", {})
            self.assertIs(
                defaults.get("handler"),
                getattr(coordinate.channel_binding_cli, handler),
            )


if __name__ == "__main__":
    unittest.main()
