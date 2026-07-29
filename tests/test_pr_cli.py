import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path

from coordinate.cli import main
from coordinate.db import initialize
from tests import test_cli as cli_tests

# ---------------------------------------------------------------------------
# Phase 8.4 — `pr publish` CLI smoke
# ---------------------------------------------------------------------------

class PrPublishCliTests(cli_tests.CliTests):
    """Smoke tests for `coordinate pr publish`.

    These tests monkeypatch `coordinate.cli.publish_pr` so we can assert on
    the argv the CLI passes through and on the JSON envelope shape. They do
    not invoke real `gh` or touch GitHub.
    """

    def _setup_workspace(self, db_path, tmp):
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp, "--harness-root", tmp,
        )

    def test_pr_publish_unknown_workspace_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "publish", "missing",
                "--task-id", "task-1",
                "--repo", "a/b", "--branch", "main",
                "--head-owner", "a", "--base", "main",
                "--title", "t", "--body", "",
                "--commit", "0123456789abcdef0123456789abcdef01234567", "--pushed", "true",
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "unknown_workspace")

    def test_pr_publish_invalid_repo_emits_blocked_result_nonzero(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "publish", "demo",
                "--task-id", "task-1",
                "--repo", "BAD/REPO", "--branch", "main",
                "--head-owner", "a", "--base", "main",
                "--title", "t", "--body", "",
                "--commit", "0123456789abcdef0123456789abcdef01234567", "--pushed", "true",
            )
            # blocked/push.required must exit non-zero so CI can fail-fast.
            self.assertEqual(code, 1)
            self.assertEqual(payload["result"]["action"], "blocked")
            self.assertEqual(payload["result"]["reason"], "invalid_repo")

    def test_pr_publish_missing_required_arg_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)
            # argparse exits with SystemExit(2) on missing required --repo;
            # run_cli_raw would propagate it. Just confirm via run_cli_raw.
            try:
                code, _, _ = self.run_cli_raw(
                    "--db", db_path,
                    "pr", "publish", "demo",
                    "--task-id", "task-1",
                    "--branch", "main",
                    "--head-owner", "a", "--base", "main",
                    "--title", "t", "--body", "",
                    "--commit", "0123456789abcdef0123456789abcdef01234567", "--pushed", "true",
                )
            except SystemExit as exc:
                self.assertEqual(exc.code, 2)
            else:
                self.assertEqual(code, 2)



# ---------------------------------------------------------------------------
# Phase 8.4 review-fix round 2 — record-only sink + argv shape + exit codes
# ---------------------------------------------------------------------------

class PrPublishRecordSinkTests(unittest.TestCase):
    """CLI-level coverage for the record-only `pr publish-record` sink.

    Inherits directly from unittest.TestCase to avoid the duplicated
    execution of every PrPublishCliTests method (each parent test
    would otherwise be re-run as a child instance).
    """

    def run_cli(self, *args):
        code, stdout, _ = self.run_cli_raw(*args)
        return code, json.loads(stdout)

    def run_cli_raw(self, *args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(list(args))
        return code, stdout.getvalue(), stderr.getvalue()

    def _setup_workspace(self, db_path, tmp):
        self.run_cli(
            "--db", db_path,
            "workspace", "add", "demo",
            "--path", tmp, "--harness-root", tmp,
        )

    def _publish_result_envelope(self, action="linked", **over):
        envelope = {
            "workspace_id": "demo",
            "task_id": "task-1",
            "repo": "acme/repo",
            "branch": "agents/mac-claude/task-1",
            "head_ref": "acme:agents/mac-claude/task-1",
            "base": "main",
            "commit": "0123456789abcdef0123456789abcdef01234567",
            "reported_commit": "0123456789abcdef0123456789abcdef01234567",
            "remote_sha": "0123456789abcdef0123456789abcdef01234567",
            "pr_url": "https://github.com/acme/repo/pull/9",
            "action": action,
            "event": {
                "event_type": "pr.linked",
                "actor": "operator",
                "idempotency_key": "demo:task-1:pr.linked:acme/repo:agents/mac-claude/task-1:0123456789abcdef0123456789abcdef01234567:publish:url",
                "payload": {
                    "task_id": "task-1",
                    "pr": "https://github.com/acme/repo/pull/9",
                    "pr_url": "https://github.com/acme/repo/pull/9",
                    "branch": "agents/mac-claude/task-1",
                    "head_ref": "acme:agents/mac-claude/task-1",
                    "base": "main",
                    "repo": "acme/repo",
                    "reported_commit": "0123456789abcdef0123456789abcdef01234567",
                    "remote_sha": "0123456789abcdef0123456789abcdef01234567",
                },
            },
        }
        envelope.update(over)
        return envelope

    def test_pr_publish_record_appends_event_and_upserts_mirror(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)

            envelope = self._publish_result_envelope()
            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "publish-record", "demo",
                "--result-json", json.dumps(envelope),
            )
            self.assertEqual(code, 0)
            self.assertEqual(payload["result"]["action"], "linked")
            self.assertTrue(payload["result"]["mirror_updated"])
            self.assertTrue(payload["result"]["event_created"])

            # The task mirror must now carry the PR URL on this DB.
            from coordinate.db import connect, migrate
            conn = connect(db_path)
            migrate(conn)
            row = conn.execute(
                "SELECT pr, branch FROM tasks WHERE workspace_id=? AND task_id=?",
                ("demo", "task-1"),
            ).fetchone()
            self.assertEqual(row["pr"], "https://github.com/acme/repo/pull/9")
            self.assertEqual(row["branch"], "agents/mac-claude/task-1")
            conn.close()

    def test_pr_publish_record_idempotent_replay_no_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)
            envelope = self._publish_result_envelope()
            first = self.run_cli(
                "--db", db_path,
                "pr", "publish-record", "demo",
                "--result-json", json.dumps(envelope),
            )[1]
            second = self.run_cli(
                "--db", db_path,
                "pr", "publish-record", "demo",
                "--result-json", json.dumps(envelope),
            )[1]
            self.assertTrue(first["result"]["event_created"])
            self.assertFalse(second["result"]["event_created"])
            self.assertEqual(first["result"]["event"]["id"],
                             second["result"]["event"]["id"])

    def test_pr_publish_record_push_required_does_not_upsert_mirror(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)
            envelope = self._publish_result_envelope(
                action="push_required",
                pr_url="",
                event={
                    "event_type": "push.required",
                    "actor": "operator",
                    "idempotency_key": "demo:task-1:push.required:acme/repo:main:0000000000000000000000000000000000000000:not_pushed",
                    "payload": {
                        "task_id": "task-1",
                        "repo": "acme/repo",
                        "branch": "main",
                        "reported_commit": "0" * 40,
                        "remote": "origin",
                        "next_action": "push main to origin from worker host",
                    },
                },
            )
            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "publish-record", "demo",
                "--result-json", json.dumps(envelope),
            )
            self.assertEqual(code, 0)
            self.assertEqual(payload["result"]["action"], "push_required")
            self.assertFalse(payload["result"]["mirror_updated"])

            # No mirror row should exist.
            from coordinate.db import connect, migrate
            conn = connect(db_path)
            migrate(conn)
            row = conn.execute(
                "SELECT * FROM tasks WHERE workspace_id=? AND task_id=?",
                ("demo", "task-1"),
            ).fetchone()
            self.assertIsNone(row)
            conn.close()

    def test_pr_publish_record_invalid_json_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)
            code, stdout, stderr = self.run_cli_raw(
                "--db", db_path,
                "pr", "publish-record", "demo",
                "--result-json", "not json",
            )
            self.assertEqual(code, 1)
            self.assertIn("invalid", stdout + stderr)

    def test_pr_publish_record_mirror_conflict_returns_error(self):
        from coordinate.db import append_event, connect, migrate, upsert_task_mirror
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)
            # Seed a mirror with a conflicting branch.
            conn = connect(db_path)
            migrate(conn)
            conn.execute(
                "INSERT INTO events (id, workspace_id, task_id, event_type, "
                "actor, target, idempotency_key, payload_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
                ("evt-seed", "demo", "task-1", "branch.allocated", "op",
                 "acme/repo", "demo:task-1:branch:other/x",
                 json.dumps({"task_id": "task-1", "branch": "other/x"})),
            )
            conn.commit()
            upsert_task_mirror(
                conn, workspace_id="demo", task_id="task-1",
                phase="running", owner="mac-claude", branch="other/x",
                pr=None, payload={"metadata": {"foo": "bar"}},
                last_event_id="evt-seed",
            )
            conn.close()

            envelope = self._publish_result_envelope(
                branch="agents/mac-claude/task-1",
            )
            code, stdout, stderr = self.run_cli_raw(
                "--db", db_path,
                "pr", "publish-record", "demo",
                "--result-json", json.dumps(envelope),
            )
            self.assertEqual(code, 1)
            payload = json.loads(stdout)
            self.assertEqual(payload["error"]["reason"], "mirror_conflict")

            # Mirror branch must NOT have been overwritten.
            conn = connect(db_path)
            row = conn.execute(
                "SELECT branch FROM tasks WHERE workspace_id=? AND task_id=?",
                ("demo", "task-1"),
            ).fetchone()
            self.assertEqual(row["branch"], "other/x")
            conn.close()

    def test_pr_publish_event_cli_path_forwards_record_only_command(self):
        """--event-cli-path invokes remote `pr publish-record` with the
        host's PublishResult JSON. The remote CLI never receives a
        nested `pr publish` argv.
        """
        import subprocess
        from coordinate import cli as cli_module
        from coordinate import prs as prs_module
        from coordinate import github as github_module

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)

            captured_argv = []

            def fake_run(argv, **_kwargs):
                captured_argv.append(list(argv))
                if argv[1:3] == ["pr", "publish-preflight"]:
                    return subprocess.CompletedProcess(
                        args=argv, returncode=0,
                        stdout=json.dumps({"result": {
                            "workspace_id": "demo", "task_id": "task-1",
                            "ok": True, "reason": None, "message": "",
                        }}),
                        stderr="",
                    )
                return subprocess.CompletedProcess(
                    args=argv, returncode=0,
                    stdout=json.dumps({"result": {
                        "workspace_id": "demo", "task_id": "task-1",
                        "action": "created", "mirror_updated": True,
                        "event_created": True,
                        "event": {"id": "evt-1"},
                    }}),
                    stderr="",
                )

            # Realistic gh runner for the host path: ref found, no
            # existing PR, create succeeds.
            host_gh = _HostGhRunnerStub()

            original_run = cli_module.subprocess.run
            original_publish_runner = prs_module.github_module._run_gh
            cli_module.subprocess.run = fake_run
            prs_module.github_module._run_gh = host_gh
            try:
                code, stdout, stderr = self.run_cli_raw(
                    "--db", db_path,
                    "pr", "publish", "demo",
                    "--task-id", "task-1",
                    "--repo", "a/b", "--branch", "main",
                    "--head-owner", "a", "--base", "main",
                    "--title", "t", "--body", "",
                    "--commit", "0123456789abcdef0123456789abcdef01234567", "--pushed", "true",
                    "--event-cli-path", "/usr/local/bin/coord-ssh",
                )
            finally:
                cli_module.subprocess.run = original_run
                prs_module.github_module._run_gh = original_publish_runner

            self.assertEqual(code, 0, msg=f"stdout={stdout!r} stderr={stderr!r}")
            payload = json.loads(stdout)
            argv = captured_argv[0] if captured_argv else []
            # Preflight happens first (same path), then record-only forward.
            self.assertGreaterEqual(len(captured_argv), 2)
            self.assertEqual(
                captured_argv[0][:4],
                ["/usr/local/bin/coord-ssh", "pr", "publish-preflight", "demo"],
            )
            self.assertEqual(
                captured_argv[1][:4],
                ["/usr/local/bin/coord-ssh", "pr", "publish-record", "demo"],
            )
            # The forward carries the host's full result JSON.
            self.assertIn("--result-json", captured_argv[1])
            idx = captured_argv[1].index("--result-json")
            forwarded = json.loads(captured_argv[1][idx + 1])
            self.assertEqual(forwarded["action"], "created")
            self.assertEqual(forwarded["repo"], "a/b")
            self.assertEqual(forwarded["task_id"], "task-1")
            self.assertEqual(forwarded["pr_url"],
                             "https://github.com/a/b/pull/42")
            self.assertIn("remote", payload)
            self.assertTrue(payload["remote"]["mirror_updated"])

    def test_pr_publish_event_cli_py_path_prepends_sys_executable(self):
        """`.py` event_cli paths are auto-prepended with sys.executable
        so the Windows coding host (`coord-ssh-win.py`) spawns correctly.
        """
        import subprocess
        import sys
        from coordinate import cli as cli_module
        from coordinate import prs as prs_module

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)

            captured_argv = []

            def fake_run(argv, **_kwargs):
                captured_argv.append(list(argv))
                if argv[2:4] == ["pr", "publish-preflight"]:
                    return subprocess.CompletedProcess(
                        args=argv, returncode=0,
                        stdout=json.dumps({"result": {
                            "workspace_id": "demo", "task_id": "task-1",
                            "ok": True, "reason": None, "message": "",
                        }}),
                        stderr="",
                    )
                return subprocess.CompletedProcess(
                    args=argv, returncode=0,
                    stdout=json.dumps({"result": {
                        "workspace_id": "demo", "task_id": "task-1",
                        "action": "created", "mirror_updated": True,
                        "event_created": True, "event": {"id": "evt-1"},
                    }}),
                    stderr="",
                )

            host_gh = _HostGhRunnerStub()
            original_run = cli_module.subprocess.run
            original_publish_runner = prs_module.github_module._run_gh
            cli_module.subprocess.run = fake_run
            prs_module.github_module._run_gh = host_gh
            try:
                code, _ = self.run_cli(
                    "--db", db_path,
                    "pr", "publish", "demo",
                    "--task-id", "task-1",
                    "--repo", "a/b", "--branch", "main",
                    "--head-owner", "a", "--base", "main",
                    "--title", "t", "--body", "",
                    "--commit", "0123456789abcdef0123456789abcdef01234567", "--pushed", "true",
                    "--event-cli-path", r"C:\Users\ADMIN\coord-ssh-win.py",
                )
            finally:
                cli_module.subprocess.run = original_run
                prs_module.github_module._run_gh = original_publish_runner

            argv = captured_argv[0]
            self.assertEqual(argv[0], sys.executable)
            self.assertEqual(argv[1], r"C:\Users\ADMIN\coord-ssh-win.py")
            self.assertEqual(code, 0)

    def test_pr_publish_pushed_false_returns_nonzero(self):
        """push_required must always exit 1 so CI can fail-fast."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)
            code, payload = self.run_cli(
                "--db", db_path,
                "pr", "publish", "demo",
                "--task-id", "task-1",
                "--repo", "a/b", "--branch", "main",
                "--head-owner", "a", "--base", "main",
                "--title", "t", "--body", "",
                "--commit", "0" * 40, "--pushed", "false",
            )
            self.assertEqual(code, 1)
            self.assertEqual(payload["result"]["action"], "push_required")

    def test_pr_publish_gh_missing_returns_nonzero(self):
        """When gh is missing on the host, publish blocked (gh_missing)
        exits 1 — never a silent success."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)
            # Patch the gh runner at module level to simulate gh_missing.
            import coordinate.cli as cli_module
            original_publish = cli_module.publish_pr

            def boom(*_a, **_kw):
                from coordinate.prs import GitHubCommandError
                raise GitHubCommandError(
                    "gh CLI not available",
                    reason="gh_missing",
                )

            cli_module.publish_pr = boom
            try:
                code, stdout, stderr = self.run_cli_raw(
                    "--db", db_path,
                    "pr", "publish", "demo",
                    "--task-id", "task-1",
                    "--repo", "a/b", "--branch", "main",
                    "--head-owner", "a", "--base", "main",
                    "--title", "t", "--body", "",
                    "--commit", "0123456789abcdef0123456789abcdef01234567", "--pushed", "true",
                )
            finally:
                cli_module.publish_pr = original_publish

            self.assertEqual(code, 1)
            payload = json.loads(stdout)
            self.assertEqual(payload["error"]["reason"], "gh_missing")

    def test_pr_publish_unknown_preflight_mode_skips_gh(self):
        """A successful preflight may only select a documented mode."""
        from coordinate import cli as cli_module
        from coordinate import prs as prs_module

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)

            def fail_if_gh_runs(_cmd):
                raise AssertionError("malformed preflight must skip gh")

            original_preflight = cli_module._forward_publish_preflight
            original_gh = prs_module.github_module._run_gh
            cli_module._forward_publish_preflight = lambda *_a, **_kw: {
                "ok": True,
                "mode": "create_anyway",
            }
            prs_module.github_module._run_gh = fail_if_gh_runs
            try:
                code, payload = self.run_cli(
                    "--db", db_path,
                    "pr", "publish", "demo",
                    "--task-id", "task-1",
                    "--repo", "a/b", "--branch", "main",
                    "--head-owner", "a", "--base", "main",
                    "--title", "t", "--body", "",
                    "--commit", "0123456789abcdef0123456789abcdef01234567",
                    "--pushed", "true",
                    "--preflight-event-cli-path", "/usr/local/bin/coord-ssh",
                )
            finally:
                cli_module._forward_publish_preflight = original_preflight
                prs_module.github_module._run_gh = original_gh

            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "invalid_preflight")

    def test_pr_publish_link_existing_without_expected_url_skips_gh(self):
        """link_existing is invalid without an authoritative remote URL."""
        from coordinate import cli as cli_module
        from coordinate import prs as prs_module

        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "coordinator.sqlite3")
            self._setup_workspace(db_path, tmp)

            def fail_if_gh_runs(_cmd):
                raise AssertionError("incomplete preflight must skip gh")

            original_preflight = cli_module._forward_publish_preflight
            original_gh = prs_module.github_module._run_gh
            cli_module._forward_publish_preflight = lambda *_a, **_kw: {
                "ok": True,
                "mode": "link_existing",
            }
            prs_module.github_module._run_gh = fail_if_gh_runs
            try:
                code, payload = self.run_cli(
                    "--db", db_path,
                    "pr", "publish", "demo",
                    "--task-id", "task-1",
                    "--repo", "a/b", "--branch", "main",
                    "--head-owner", "a", "--base", "main",
                    "--title", "t", "--body", "",
                    "--commit", "0123456789abcdef0123456789abcdef01234567",
                    "--pushed", "true",
                    "--preflight-event-cli-path", "/usr/local/bin/coord-ssh",
                )
            finally:
                cli_module._forward_publish_preflight = original_preflight
                prs_module.github_module._run_gh = original_gh

            self.assertEqual(code, 1)
            self.assertEqual(payload["error"]["reason"], "invalid_preflight")


class _HostGhRunnerStub:
    """Fake `gh` runner for the host-side publish path.

    Returns:
    - ref lookup: matches SHA `0123456789abcdef0123456789abcdef01234567`
    - pr list: empty (no existing PR, forces create)
    - pr create: returns a fixed PR URL
    """

    def __init__(self):
        self.calls = []

    def __call__(self, cmd):
        import subprocess
        self.calls.append(list(cmd))
        joined = " ".join(cmd)
        if "repos/" in joined and "git/ref/heads" in joined:
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout=json.dumps({"object": {
                    "sha": "0123456789abcdef0123456789abcdef01234567",
                }}),
                stderr="",
            )
        if "pr" in joined and "list" in joined:
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout=json.dumps([]),
                stderr="",
            )
        if "pr" in joined and "create" in joined:
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout="https://github.com/a/b/pull/42\n",
                stderr="",
            )
        raise AssertionError(f"unexpected gh call: {cmd}")
