from __future__ import annotations

import json
import sqlite3
import subprocess
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from coordinate.db import (
    append_event,
    connect,
    list_events,
    migrate,
    row_to_dict,
    upsert_task_mirror,
)
from coordinate.prs import (
    LinkPrResult,
    PublishError,
    RecordPublishError,
    _discover_pr,
    link_pr,
    publish_pr,
    publish_pr_existing,
    record_publish_preflight,
    record_publish_result,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_db():
    conn = connect(":memory:")
    migrate(conn)
    conn.execute(
        "INSERT INTO workspaces (id, name, path, harness_root, base_branch, branch_namespace, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
        ("ws-1", "test-ws", "/tmp/test", "/tmp/test/docs", "main", "agents"),
    )
    conn.commit()
    return conn


def _mock_run_factory(stdout='[]', returncode=0, side_effect=None):
    """Create a mock subprocess.run function."""
    def mock_run(*args, **kwargs):
        if side_effect:
            raise side_effect
        return subprocess.CompletedProcess(
            args=args[0], returncode=returncode, stdout=stdout, stderr='',
        )
    return mock_run


def _mock_run_discover():
    """Mock that returns a single open PR."""
    return _mock_run_factory(
        stdout='[{"url": "https://github.com/example/repo/pull/1"}]',
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestLinkPr(unittest.TestCase):

    # 1. Explicit pr_url, no branch arg, no mirror
    def test_link_explicit_pr_url_no_branch(self):
        conn = _setup_db()
        result = link_pr(
            conn, "ws-1", "task-1",
            pr_url="https://github.com/example/repo/pull/42",
        )
        self.assertIsInstance(result, LinkPrResult)
        self.assertEqual(result.pr_url, "https://github.com/example/repo/pull/42")
        self.assertIsNone(result.branch)
        self.assertFalse(result.existing)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "pr.linked")

        mirror = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertIsNotNone(mirror)
        self.assertEqual(mirror["pr"], "https://github.com/example/repo/pull/42")
        self.assertIsNone(mirror["branch"])

    # 2. Explicit pr_url + branch arg
    def test_link_explicit_pr_url_with_branch_arg(self):
        conn = _setup_db()
        result = link_pr(
            conn, "ws-1", "task-1",
            pr_url="https://github.com/example/repo/pull/10",
            branch="agents/task-1",
        )
        self.assertEqual(result.pr_url, "https://github.com/example/repo/pull/10")
        self.assertEqual(result.branch, "agents/task-1")
        self.assertFalse(result.existing)

        mirror = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(mirror["pr"], "https://github.com/example/repo/pull/10")
        self.assertEqual(mirror["branch"], "agents/task-1")

    # 3. Explicit pr_url, no branch arg, mirror has branch
    def test_link_explicit_pr_url_uses_mirror_branch(self):
        conn = _setup_db()
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-1",
            phase="running",
            owner="codex",
            branch="agents/task-1",
            pr=None,
            payload={"status": "doing"},
        )
        result = link_pr(
            conn, "ws-1", "task-1",
            pr_url="https://github.com/example/repo/pull/5",
        )
        self.assertEqual(result.pr_url, "https://github.com/example/repo/pull/5")
        self.assertEqual(result.branch, "agents/task-1")

    # 4. Discovery mode with branch arg
    def test_link_discovery_mode(self):
        conn = _setup_db()
        result = link_pr(
            conn, "ws-1", "task-1",
            branch="agents/task-1",
            run=_mock_run_discover(),
        )
        self.assertEqual(
            result.pr_url, "https://github.com/example/repo/pull/1",
        )
        self.assertEqual(result.branch, "agents/task-1")
        self.assertFalse(result.existing)
        self.assertTrue(result.event_created)

    # 5. Discovery uses mirror branch when no branch arg
    def test_link_discovery_uses_mirror_branch(self):
        conn = _setup_db()
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-1",
            phase="running",
            owner="codex",
            branch="agents/task-1",
            pr=None,
            payload={"status": "doing"},
        )
        result = link_pr(
            conn, "ws-1", "task-1",
            run=_mock_run_discover(),
        )
        self.assertEqual(
            result.pr_url, "https://github.com/example/repo/pull/1",
        )
        self.assertEqual(result.branch, "agents/task-1")

    # 6. Idempotent same PR
    def test_idempotent_same_pr(self):
        conn = _setup_db()
        first = link_pr(
            conn, "ws-1", "task-1",
            pr_url="https://github.com/example/repo/pull/99",
            branch="agents/task-1",
        )
        self.assertFalse(first.existing)
        self.assertTrue(first.event_created)

        second = link_pr(
            conn, "ws-1", "task-1",
            pr_url="https://github.com/example/repo/pull/99",
            branch="agents/task-1",
        )
        self.assertTrue(second.existing)
        self.assertFalse(second.event_created)
        self.assertIsNotNone(second.event)
        self.assertEqual(second.pr_url, "https://github.com/example/repo/pull/99")

        mirror = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(mirror["pr"], "https://github.com/example/repo/pull/99")

    # 7. Crash recovery: mirror pr cleared, relink fixes it
    def test_crash_recovery_mirror_fixed(self):
        conn = _setup_db()
        link_pr(
            conn, "ws-1", "task-1",
            pr_url="https://github.com/example/repo/pull/7",
            branch="agents/task-1",
        )
        # Simulate crash: clear the mirror PR
        conn.execute(
            "UPDATE tasks SET pr = NULL WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "task-1"),
        )
        conn.commit()

        # Re-link with same PR — event is idempotent but mirror gets fixed
        result = link_pr(
            conn, "ws-1", "task-1",
            pr_url="https://github.com/example/repo/pull/7",
            branch="agents/task-1",
        )
        # The event already exists (idempotency key match) but existing=False
        # because the mirror's pr was cleared, so existing_row["pr"] != resolved_pr
        self.assertFalse(result.existing)

        mirror = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(mirror["pr"], "https://github.com/example/repo/pull/7")

    # 8. Existing PR differs from requested PR
    def test_existing_different_pr_raises(self):
        conn = _setup_db()
        link_pr(
            conn, "ws-1", "task-1",
            pr_url="https://github.com/example/repo/pull/old",
        )
        with self.assertRaises(ValueError) as ctx:
            link_pr(
                conn, "ws-1", "task-1",
                pr_url="https://github.com/example/repo/pull/new",
            )
        self.assertIn("already has pr", str(ctx.exception))
        self.assertIn("cannot relink", str(ctx.exception))

    # 9. No branch, no pr_url, no mirror branch
    def test_no_branch_no_pr_url_raises(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            link_pr(conn, "ws-1", "task-1")
        self.assertIn("no branch", str(ctx.exception))

    # 10. Explicit pr_url with no branch succeeds
    def test_explicit_pr_url_no_branch_succeeds(self):
        conn = _setup_db()
        result = link_pr(
            conn, "ws-1", "task-1",
            pr_url="https://github.com/example/repo/pull/3",
        )
        self.assertEqual(result.pr_url, "https://github.com/example/repo/pull/3")
        self.assertIsNone(result.branch)
        self.assertFalse(result.existing)

    # 11. Discovery with no open PR found
    def test_discovery_no_pr_found_raises(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            link_pr(
                conn, "ws-1", "task-1",
                branch="agents/task-1",
                run=_mock_run_factory(stdout="[]"),
            )
        self.assertIn("no open PR found", str(ctx.exception))

    # 12. Discovery: gh not installed
    def test_discovery_gh_not_installed_raises(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            link_pr(
                conn, "ws-1", "task-1",
                branch="agents/task-1",
                run=_mock_run_factory(side_effect=FileNotFoundError()),
            )
        self.assertIn("gh CLI not available", str(ctx.exception))

    # 13. Discovery: gh command fails
    def test_discovery_gh_fails_raises(self):
        conn = _setup_db()
        exc = subprocess.CalledProcessError(
            1, "gh", stderr="rate limited",
        )
        with self.assertRaises(ValueError) as ctx:
            link_pr(
                conn, "ws-1", "task-1",
                branch="agents/task-1",
                run=_mock_run_factory(side_effect=exc),
            )
        self.assertIn("gh pr list failed", str(ctx.exception))

    # 14. Discovery: invalid JSON from gh
    def test_discovery_invalid_json_raises(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            link_pr(
                conn, "ws-1", "task-1",
                branch="agents/task-1",
                run=_mock_run_factory(stdout="not json"),
            )
        self.assertIn("gh pr list returned invalid JSON", str(ctx.exception))

    # 15. Unknown workspace
    def test_unknown_workspace_raises(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            link_pr(conn, "no-such-ws", "task-1", pr_url="http://x")
        self.assertIn("unknown workspace", str(ctx.exception))

    # 16. First link creates mirror with defaults
    def test_task_mirror_not_yet_exists(self):
        conn = _setup_db()
        result = link_pr(
            conn, "ws-1", "task-new",
            pr_url="https://github.com/example/repo/pull/1",
        )
        self.assertFalse(result.existing)
        mirror = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "task-new"),
        ).fetchone()
        self.assertIsNotNone(mirror)
        self.assertEqual(mirror["pr"], "https://github.com/example/repo/pull/1")
        self.assertIsNone(mirror["phase"])
        self.assertIsNone(mirror["owner"])

    # 17. Preserves existing mirror fields
    def test_preserves_existing_mirror_fields(self):
        conn = _setup_db()
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-1",
            phase="running",
            owner="codex",
            branch="agents/task-1",
            pr=None,
            payload={"status": "doing", "title": "implement feature"},
        )
        result = link_pr(
            conn, "ws-1", "task-1",
            pr_url="https://github.com/example/repo/pull/2",
        )
        self.assertFalse(result.existing)

        mirror = conn.execute(
            "SELECT * FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("ws-1", "task-1"),
        ).fetchone()
        self.assertEqual(mirror["phase"], "running")
        self.assertEqual(mirror["owner"], "codex")
        self.assertEqual(mirror["branch"], "agents/task-1")
        self.assertEqual(mirror["pr"], "https://github.com/example/repo/pull/2")
        payload = json.loads(mirror["payload_json"])
        self.assertEqual(payload["status"], "doing")
        self.assertEqual(payload["title"], "implement feature")

    # 18. Same PR on another task raises conflict
    def test_same_pr_on_another_task_raises(self):
        conn = _setup_db()
        link_pr(
            conn, "ws-1", "task-a",
            pr_url="https://github.com/example/repo/pull/42",
            branch="agents/task-a",
        )
        with self.assertRaises(ValueError) as ctx:
            link_pr(
                conn, "ws-1", "task-b",
                pr_url="https://github.com/example/repo/pull/42",
                branch="agents/task-b",
            )
        self.assertIn("already linked to task task-a", str(ctx.exception))

    # 19. Branch arg conflicts with mirror branch
    def test_branch_arg_conflicts_with_mirror_raises(self):
        conn = _setup_db()
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-1",
            phase="running",
            owner="codex",
            branch="agents/task-1",
            pr=None,
            payload={"status": "doing"},
        )
        with self.assertRaises(ValueError) as ctx:
            link_pr(
                conn, "ws-1", "task-1",
                pr_url="https://github.com/example/repo/pull/1",
                branch="agents/different-branch",
            )
        self.assertIn("already has branch", str(ctx.exception))
        self.assertIn("cannot link PR from branch", str(ctx.exception))

    # 20. Unit tests for _discover_pr
    def test_discover_pr_unit(self):
        # Valid response
        mock = _mock_run_factory(
            stdout='[{"url": "https://github.com/example/repo/pull/1"}]',
        )
        url = _discover_pr("/tmp/test", "agents/task-1", run=mock)
        self.assertEqual(url, "https://github.com/example/repo/pull/1")

        # Empty array
        mock = _mock_run_factory(stdout="[]")
        url = _discover_pr("/tmp/test", "agents/task-1", run=mock)
        self.assertIsNone(url)

        # Missing url key
        mock = _mock_run_factory(stdout='[{"number": 1}]')
        url = _discover_pr("/tmp/test", "agents/task-1", run=mock)
        self.assertIsNone(url)

        # Invalid JSON
        mock = _mock_run_factory(stdout="not json")
        with self.assertRaises(ValueError) as ctx:
            _discover_pr("/tmp/test", "agents/task-1", run=mock)
        self.assertIn("gh pr list returned invalid JSON", str(ctx.exception))

        # FileNotFoundError
        mock = _mock_run_factory(side_effect=FileNotFoundError())
        with self.assertRaises(ValueError) as ctx:
            _discover_pr("/tmp/test", "agents/task-1", run=mock)
        self.assertIn("gh CLI not available", str(ctx.exception))

        # CalledProcessError
        exc = subprocess.CalledProcessError(1, "gh", stderr="error msg")
        mock = _mock_run_factory(side_effect=exc)
        with self.assertRaises(ValueError) as ctx:
            _discover_pr("/tmp/test", "agents/task-1", run=mock)
        self.assertIn("gh pr list failed", str(ctx.exception))
        self.assertIn("error msg", str(ctx.exception))
