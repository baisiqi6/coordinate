from __future__ import annotations

import json
import subprocess
import unittest

from coordinate.ci import (
    CheckCiResult,
    CheckResult,
    _aggregate_status,
    _query_checks,
    check_ci,
)
from coordinate.db import append_event, connect, migrate, row_to_dict, upsert_task_mirror


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

PASSED_JSON = json.dumps([
    {"name": "lint", "state": "completed", "bucket": "pass"},
    {"name": "tests", "state": "completed", "bucket": "pass"},
])

FAILED_JSON = json.dumps([
    {"name": "lint", "state": "completed", "bucket": "pass"},
    {"name": "tests", "state": "completed", "bucket": "fail"},
])

PENDING_JSON = json.dumps([
    {"name": "lint", "state": "completed", "bucket": "pass"},
    {"name": "tests", "state": "in_progress", "bucket": "pending"},
])

CANCELLED_JSON = json.dumps([
    {"name": "lint", "state": "completed", "bucket": "cancel"},
])

HEAD_SHA = "head-123"
HEAD_JSON = json.dumps({"headRefOid": HEAD_SHA})


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


def _mock_run(
    stdout="",
    returncode=0,
    stderr="",
    side_effect=None,
    *,
    head_stdout=HEAD_JSON,
    head_returncode=0,
    head_stderr="",
):
    """Create a mock subprocess.run that returns gh pr checks JSON."""

    def mock_run(*args, **kwargs):
        if side_effect:
            raise side_effect
        cmd = args[0]
        if cmd[:3] == ["gh", "pr", "view"]:
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=head_returncode,
                stdout=head_stdout,
                stderr=head_stderr,
            )
        return subprocess.CompletedProcess(
            args=cmd, returncode=returncode, stdout=stdout, stderr=stderr
        )

    return mock_run


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCheckCi(unittest.TestCase):
    """Integration tests for check_ci."""

    # 1. All checks pass -> ci.passed event written
    def test_all_passed(self):
        conn = _setup_db()
        result = check_ci(
            conn,
            "ws-1",
            "task-1",
            pr_url="https://github.com/o/r/pull/1",
            run=_mock_run(stdout=PASSED_JSON),
        )
        self.assertEqual(result.aggregated_status, "passed")
        self.assertEqual(result.event["event_type"], "ci.passed")
        self.assertTrue(result.event_created)
        self.assertFalse(result.existing)
        self.assertIsNotNone(result.event)
        self.assertEqual(result.head_sha, HEAD_SHA)
        self.assertEqual(result.event["payload"]["head_sha"], HEAD_SHA)

    # 2. One check fails -> ci.failed event written
    def test_one_failed(self):
        conn = _setup_db()
        result = check_ci(
            conn,
            "ws-1",
            "task-2",
            pr_url="https://github.com/o/r/pull/2",
            run=_mock_run(stdout=FAILED_JSON),
        )
        self.assertEqual(result.aggregated_status, "failed")
        self.assertEqual(result.event["event_type"], "ci.failed")
        self.assertTrue(result.event_created)

    # 3. Cancelled check counts as failed
    def test_cancelled_counts_as_failed(self):
        conn = _setup_db()
        result = check_ci(
            conn,
            "ws-1",
            "task-3",
            pr_url="https://github.com/o/r/pull/3",
            run=_mock_run(stdout=CANCELLED_JSON),
        )
        self.assertEqual(result.aggregated_status, "failed")

    # 4. Pending checks -> ci.pending event written
    def test_pending_writes_ci_pending_event(self):
        conn = _setup_db()
        result = check_ci(
            conn,
            "ws-1",
            "task-4",
            pr_url="https://github.com/o/r/pull/4",
            run=_mock_run(stdout=PENDING_JSON),
        )
        self.assertEqual(result.aggregated_status, "pending")
        self.assertIsNotNone(result.event)
        self.assertEqual(result.event["event_type"], "ci.pending")
        self.assertTrue(result.event_created)
        self.assertFalse(result.existing)

    # 4b. Pending idempotent
    def test_pending_idempotent(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/4b"
        run_fn = _mock_run(stdout=PENDING_JSON)

        result1 = check_ci(conn, "ws-1", "task-4b", pr_url=pr, run=run_fn)
        self.assertTrue(result1.event_created)
        self.assertFalse(result1.existing)

        result2 = check_ci(conn, "ws-1", "task-4b", pr_url=pr, run=run_fn)
        self.assertFalse(result2.event_created)
        self.assertTrue(result2.existing)
        self.assertEqual(result2.event["id"], result1.event["id"])

    # 4c. Pending -> passed transition writes new event
    def test_pending_to_passed_writes_new_event(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/4c"

        result1 = check_ci(
            conn, "ws-1", "task-4c", pr_url=pr, run=_mock_run(stdout=PENDING_JSON)
        )
        self.assertEqual(result1.aggregated_status, "pending")
        self.assertTrue(result1.event_created)

        result2 = check_ci(
            conn, "ws-1", "task-4c", pr_url=pr, run=_mock_run(stdout=PASSED_JSON)
        )
        self.assertEqual(result2.aggregated_status, "passed")
        self.assertTrue(result2.event_created)
        self.assertFalse(result2.existing)
        self.assertNotEqual(result2.event["id"], result1.event["id"])

    # 5. Empty checks list -> pending
    def test_empty_checks_pending(self):
        conn = _setup_db()
        result = check_ci(
            conn,
            "ws-1",
            "task-5",
            pr_url="https://github.com/o/r/pull/5",
            run=_mock_run(stdout="[]"),
        )
        self.assertEqual(result.aggregated_status, "pending")
        self.assertEqual(result.event["event_type"], "ci.pending")
        self.assertTrue(result.event_created)

    def test_real_gh_no_checks_message_is_pending(self):
        conn = _setup_db()
        result = check_ci(
            conn,
            "ws-1",
            "task-no-checks",
            pr_url="https://github.com/o/r/pull/5",
            run=_mock_run(
                stdout="",
                returncode=1,
                stderr="no checks reported on the 'feature' branch\n",
            ),
        )
        self.assertEqual(result.aggregated_status, "pending")
        self.assertEqual(result.checks, [])
        self.assertEqual(result.event["event_type"], "ci.pending")

    def test_no_checks_phrase_inside_command_failure_is_not_pending(self):
        failures = (
            "authentication failed: no checks reported",
            "network timeout after no checks reported",
            "HTTP 403: no checks reported because token lacks checks:read",
        )
        for index, stderr in enumerate(failures):
            with self.subTest(stderr=stderr):
                conn = _setup_db()
                with self.assertRaisesRegex(ValueError, "gh pr checks failed"):
                    check_ci(
                        conn,
                        "ws-1",
                        f"task-no-checks-failure-{index}",
                        pr_url="https://github.com/o/r/pull/5",
                        run=_mock_run(stdout="", returncode=1, stderr=stderr),
                    )
                event_count = conn.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type LIKE 'ci.%'"
                ).fetchone()[0]
                self.assertEqual(event_count, 0)

    def test_no_checks_message_with_unexpected_returncode_is_not_pending(self):
        conn = _setup_db()
        with self.assertRaisesRegex(ValueError, "gh pr checks failed"):
            check_ci(
                conn,
                "ws-1",
                "task-no-checks-rc8",
                pr_url="https://github.com/o/r/pull/5",
                run=_mock_run(
                    stdout="",
                    returncode=8,
                    stderr="no checks reported on the 'feature' branch\n",
                ),
            )
        event_count = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type LIKE 'ci.%'"
        ).fetchone()[0]
        self.assertEqual(event_count, 0)

    # 6. Idempotent: same status + same PR -> existing=True, no new event
    def test_idempotent_same_status_same_pr(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/6"
        run_fn = _mock_run(stdout=PASSED_JSON)

        result1 = check_ci(conn, "ws-1", "task-6", pr_url=pr, run=run_fn)
        self.assertTrue(result1.event_created)
        self.assertFalse(result1.existing)

        result2 = check_ci(conn, "ws-1", "task-6", pr_url=pr, run=run_fn)
        self.assertFalse(result2.event_created)
        self.assertTrue(result2.existing)
        self.assertEqual(result2.event["id"], result1.event["id"])

    def test_same_pr_same_status_new_head_writes_new_event(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/6b"

        result1 = check_ci(
            conn,
            "ws-1",
            "task-6b",
            pr_url=pr,
            run=_mock_run(
                stdout=PASSED_JSON,
                head_stdout=json.dumps({"headRefOid": "head-1"}),
            ),
        )
        self.assertTrue(result1.event_created)

        result2 = check_ci(
            conn,
            "ws-1",
            "task-6b",
            pr_url=pr,
            run=_mock_run(
                stdout=PASSED_JSON,
                head_stdout=json.dumps({"headRefOid": "head-2"}),
            ),
        )
        self.assertTrue(result2.event_created)
        self.assertFalse(result2.existing)
        self.assertNotEqual(result2.event["id"], result1.event["id"])

    # 7. Different PR with same status -> new event written
    def test_different_pr_same_status_new_event(self):
        conn = _setup_db()
        pr1 = "https://github.com/o/r/pull/7a"
        pr2 = "https://github.com/o/r/pull/7b"

        # First check with pr1
        run_fn = _mock_run(stdout=PASSED_JSON)
        result1 = check_ci(conn, "ws-1", "task-7", pr_url=pr1, run=run_fn)
        self.assertTrue(result1.event_created)

        # Manually update mirror so pr2 does not conflict
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-7",
            phase=None,
            owner=None,
            branch=None,
            pr=pr2,
            payload={},
            last_event_id=None,
        )

        # Second check with pr2 — mirror now has pr2, no conflict
        result2 = check_ci(conn, "ws-1", "task-7", pr_url=pr2, run=run_fn)
        self.assertTrue(result2.event_created)
        self.assertFalse(result2.existing)
        self.assertNotEqual(result2.event["id"], result1.event["id"])

    # 8. Status transition: failed -> passed -> new event
    def test_status_transition_failed_to_passed(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/8"

        result1 = check_ci(
            conn, "ws-1", "task-8", pr_url=pr, run=_mock_run(stdout=FAILED_JSON)
        )
        self.assertEqual(result1.aggregated_status, "failed")
        self.assertTrue(result1.event_created)

        result2 = check_ci(
            conn, "ws-1", "task-8", pr_url=pr, run=_mock_run(stdout=PASSED_JSON)
        )
        self.assertEqual(result2.aggregated_status, "passed")
        self.assertTrue(result2.event_created)
        self.assertFalse(result2.existing)
        self.assertNotEqual(result2.event["id"], result1.event["id"])

    # 9. No PR at all -> ValueError
    def test_no_pr_raises(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            check_ci(conn, "ws-1", "task-9", run=_mock_run(stdout=PASSED_JSON))
        self.assertIn("no PR", str(ctx.exception))

    # 10. Uses mirror PR when no pr_url given
    def test_uses_mirror_pr(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/10"
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-10",
            phase="working",
            owner="agent-1",
            branch="agents/task-10",
            pr=pr,
            payload={"title": "do stuff"},
            last_event_id=None,
        )
        result = check_ci(
            conn,
            "ws-1",
            "task-10",
            run=_mock_run(stdout=PASSED_JSON),
        )
        self.assertEqual(result.pr_url, pr)
        self.assertTrue(result.event_created)

    # 11. gh CLI not installed -> ValueError
    def test_gh_not_installed(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            check_ci(
                conn,
                "ws-1",
                "task-11",
                pr_url="https://github.com/o/r/pull/11",
                run=_mock_run(side_effect=FileNotFoundError()),
            )
        self.assertIn("gh CLI not available", str(ctx.exception))

    # 12. gh returns pending exit code with valid JSON -> still parses
    def test_gh_pending_returncode_with_valid_json(self):
        conn = _setup_db()
        result = check_ci(
            conn,
            "ws-1",
            "task-12",
            pr_url="https://github.com/o/r/pull/12",
            run=_mock_run(stdout=PENDING_JSON, returncode=8),
        )
        self.assertEqual(result.aggregated_status, "pending")
        self.assertTrue(result.event_created)

    def test_gh_failed_checks_returncode_with_valid_json(self):
        conn = _setup_db()
        result = check_ci(
            conn,
            "ws-1",
            "task-12b",
            pr_url="https://github.com/o/r/pull/12b",
            run=_mock_run(stdout=FAILED_JSON, returncode=1),
        )
        self.assertEqual(result.aggregated_status, "failed")
        self.assertTrue(result.event_created)

    def test_gh_unexpected_nonzero_with_valid_json_raises(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            check_ci(
                conn,
                "ws-1",
                "task-12c",
                pr_url="https://github.com/o/r/pull/12c",
                run=_mock_run(stdout=PASSED_JSON, returncode=2, stderr="auth failed"),
            )
        self.assertIn("gh pr checks failed", str(ctx.exception))

    # 13. gh returns nonzero, no JSON -> ValueError("gh pr checks failed")
    def test_gh_nonzero_no_json(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            check_ci(
                conn,
                "ws-1",
                "task-13",
                pr_url="https://github.com/o/r/pull/13",
                run=_mock_run(stdout="", returncode=1, stderr="some error"),
            )
        self.assertIn("gh pr checks failed", str(ctx.exception))

    # 14. gh returns zero, invalid JSON -> ValueError("gh pr checks returned invalid JSON")
    def test_gh_zero_invalid_json(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            check_ci(
                conn,
                "ws-1",
                "task-14",
                pr_url="https://github.com/o/r/pull/14",
                run=_mock_run(stdout="not json", returncode=0),
            )
        self.assertIn("gh pr checks returned invalid JSON", str(ctx.exception))

    # 15. Unknown workspace -> ValueError
    def test_unknown_workspace(self):
        conn = _setup_db()
        with self.assertRaises(ValueError):
            check_ci(
                conn,
                "ws-nope",
                "task-15",
                pr_url="https://github.com/o/r/pull/15",
                run=_mock_run(stdout=PASSED_JSON),
            )

    # 16. Mirror PR conflicts with arg PR -> ValueError
    def test_mirror_pr_conflict(self):
        conn = _setup_db()
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-16",
            phase=None,
            owner=None,
            branch=None,
            pr="https://github.com/o/r/pull/16a",
            payload={},
            last_event_id=None,
        )
        with self.assertRaises(ValueError) as ctx:
            check_ci(
                conn,
                "ws-1",
                "task-16",
                pr_url="https://github.com/o/r/pull/16b",
                run=_mock_run(stdout=PASSED_JSON),
            )
        self.assertIn("already has pr", str(ctx.exception))


class TestQueryChecksUnit(unittest.TestCase):
    """Unit tests for _query_checks with various bucket values."""

    # 17. Various bucket mappings
    def test_query_checks_unit(self):
        checks_json = json.dumps([
            {"name": "lint", "state": "completed", "bucket": "pass"},
            {"name": "tests", "state": "completed", "bucket": "fail"},
            {"name": "deploy", "state": "in_progress", "bucket": "pending"},
            {"name": "skipped-job", "state": "completed", "bucket": "skipping"},
            {"name": "cancelled-job", "state": "completed", "bucket": "cancel"},
        ])
        results = _query_checks(
            "/tmp/test",
            "https://github.com/o/r/pull/99",
            run=_mock_run(stdout=checks_json),
        )
        self.assertEqual(len(results), 5)
        self.assertEqual(results[0].status, "passed")
        self.assertEqual(results[1].status, "failed")
        self.assertEqual(results[2].status, "pending")
        self.assertEqual(results[3].status, "skipped")
        self.assertEqual(results[4].status, "failed")

    # 19. Unknown bucket treated as pending
    def test_unknown_bucket_treated_as_pending(self):
        checks_json = json.dumps([
            {"name": "weird-check", "state": "completed", "bucket": "unknown"},
        ])
        results = _query_checks(
            "/tmp/test",
            "https://github.com/o/r/pull/99",
            run=_mock_run(stdout=checks_json),
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "pending")

    # 20. Missing bucket field treated as pending
    def test_missing_bucket_treated_as_pending(self):
        checks_json = json.dumps([
            {"name": "no-bucket", "state": "completed"},
        ])
        results = _query_checks(
            "/tmp/test",
            "https://github.com/o/r/pull/99",
            run=_mock_run(stdout=checks_json),
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "pending")


class TestAggregateStatusUnit(unittest.TestCase):
    """Unit tests for _aggregate_status."""

    # 18. Various aggregate scenarios
    def test_aggregate_status_unit(self):
        # Empty -> pending
        self.assertEqual(_aggregate_status([]), "pending")

        # All passed
        self.assertEqual(
            _aggregate_status([
                CheckResult("a", "passed"),
                CheckResult("b", "passed"),
            ]),
            "passed",
        )

        # All skipped -> passed
        self.assertEqual(
            _aggregate_status([
                CheckResult("a", "skipped"),
                CheckResult("b", "skipped"),
            ]),
            "passed",
        )

        # Mixed passed + skipped -> passed
        self.assertEqual(
            _aggregate_status([
                CheckResult("a", "passed"),
                CheckResult("b", "skipped"),
            ]),
            "passed",
        )

        # Pending among passed -> pending
        self.assertEqual(
            _aggregate_status([
                CheckResult("a", "passed"),
                CheckResult("b", "pending"),
            ]),
            "pending",
        )

        # Failed dominates pending
        self.assertEqual(
            _aggregate_status([
                CheckResult("a", "failed"),
                CheckResult("b", "pending"),
            ]),
            "failed",
        )

        # Failed dominates passed
        self.assertEqual(
            _aggregate_status([
                CheckResult("a", "passed"),
                CheckResult("b", "failed"),
            ]),
            "failed",
        )


if __name__ == "__main__":
    unittest.main()
