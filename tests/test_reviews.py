from __future__ import annotations

import json
import subprocess
import unittest

from coordinate.db import append_event, connect, migrate, row_to_dict, upsert_task_mirror
from coordinate.reviews import (
    MergeGateResult,
    PrReviewResult,
    _query_pr_review,
    check_merge_gate,
    check_pr_review,
)


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

APPROVED_JSON = json.dumps({"reviewDecision": "APPROVED"})
CHANGES_REQUESTED_JSON = json.dumps({"reviewDecision": "CHANGES_REQUESTED"})
REVIEW_REQUIRED_JSON = json.dumps({"reviewDecision": "REVIEW_REQUIRED"})
NULL_DECISION_JSON = json.dumps({"reviewDecision": None})
NO_DECISION_JSON = json.dumps({})
UNKNOWN_DECISION_JSON = json.dumps({"reviewDecision": "DISMISSED"})
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
    """Create a mock subprocess.run that returns gh pr view JSON."""

    def mock_run(*args, **kwargs):
        if side_effect:
            raise side_effect
        cmd = args[0]
        if "--json" in cmd and cmd[cmd.index("--json") + 1] == "headRefOid":
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
# check_pr_review tests
# ---------------------------------------------------------------------------


class TestCheckPrReview(unittest.TestCase):
    """Integration tests for check_pr_review."""

    # 1. Approved review
    def test_approved(self):
        conn = _setup_db()
        result = check_pr_review(
            conn,
            "ws-1",
            "task-1",
            pr_url="https://github.com/o/r/pull/1",
            run=_mock_run(stdout=APPROVED_JSON),
        )
        self.assertEqual(result.review_decision, "approved")
        self.assertEqual(result.event["event_type"], "pr_review.approved")
        self.assertTrue(result.event_created)
        self.assertFalse(result.existing)
        self.assertIsNotNone(result.event)
        self.assertEqual(result.head_sha, HEAD_SHA)
        self.assertEqual(result.event["payload"]["head_sha"], HEAD_SHA)

    # 2. Changes requested review
    def test_changes_requested(self):
        conn = _setup_db()
        result = check_pr_review(
            conn,
            "ws-1",
            "task-2",
            pr_url="https://github.com/o/r/pull/2",
            run=_mock_run(stdout=CHANGES_REQUESTED_JSON),
        )
        self.assertEqual(result.review_decision, "changes_requested")
        self.assertEqual(result.event["event_type"], "pr_review.changes_requested")
        self.assertTrue(result.event_created)

    # 3. Review required
    def test_review_required(self):
        conn = _setup_db()
        result = check_pr_review(
            conn,
            "ws-1",
            "task-3",
            pr_url="https://github.com/o/r/pull/3",
            run=_mock_run(stdout=REVIEW_REQUIRED_JSON),
        )
        self.assertEqual(result.review_decision, "review_required")
        self.assertEqual(result.event["event_type"], "pr_review.required")
        self.assertTrue(result.event_created)

    # 4. Null reviewDecision
    def test_null_decision(self):
        conn = _setup_db()
        result = check_pr_review(
            conn,
            "ws-1",
            "task-4",
            pr_url="https://github.com/o/r/pull/4",
            run=_mock_run(stdout=NULL_DECISION_JSON),
        )
        self.assertEqual(result.review_decision, "review_required")
        self.assertEqual(result.event["event_type"], "pr_review.required")
        self.assertTrue(result.event_created)

    # 5. Unknown decision value
    def test_unknown_decision(self):
        conn = _setup_db()
        result = check_pr_review(
            conn,
            "ws-1",
            "task-5",
            pr_url="https://github.com/o/r/pull/5",
            run=_mock_run(stdout=UNKNOWN_DECISION_JSON),
        )
        self.assertEqual(result.review_decision, "review_required")
        self.assertEqual(result.event["event_type"], "pr_review.required")
        self.assertTrue(result.event_created)

    # 6. Idempotent: same decision + same PR
    def test_idempotent(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/6"
        run_fn = _mock_run(stdout=APPROVED_JSON)

        result1 = check_pr_review(conn, "ws-1", "task-6", pr_url=pr, run=run_fn)
        self.assertTrue(result1.event_created)
        self.assertFalse(result1.existing)

        result2 = check_pr_review(conn, "ws-1", "task-6", pr_url=pr, run=run_fn)
        self.assertFalse(result2.event_created)
        self.assertTrue(result2.existing)
        self.assertEqual(result2.event["id"], result1.event["id"])

    def test_same_pr_same_decision_new_head_writes_new_event(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/6b"

        result1 = check_pr_review(
            conn,
            "ws-1",
            "task-6b",
            pr_url=pr,
            run=_mock_run(
                stdout=APPROVED_JSON,
                head_stdout=json.dumps({"headRefOid": "head-1"}),
            ),
        )
        self.assertTrue(result1.event_created)

        result2 = check_pr_review(
            conn,
            "ws-1",
            "task-6b",
            pr_url=pr,
            run=_mock_run(
                stdout=APPROVED_JSON,
                head_stdout=json.dumps({"headRefOid": "head-2"}),
            ),
        )
        self.assertTrue(result2.event_created)
        self.assertFalse(result2.existing)
        self.assertNotEqual(result2.event["id"], result1.event["id"])

    # 7. Transition: changes_requested -> approved
    def test_transition_changes_requested_to_approved(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/7"

        result1 = check_pr_review(
            conn, "ws-1", "task-7", pr_url=pr, run=_mock_run(stdout=CHANGES_REQUESTED_JSON)
        )
        self.assertEqual(result1.review_decision, "changes_requested")
        self.assertTrue(result1.event_created)

        result2 = check_pr_review(
            conn, "ws-1", "task-7", pr_url=pr, run=_mock_run(stdout=APPROVED_JSON)
        )
        self.assertEqual(result2.review_decision, "approved")
        self.assertTrue(result2.event_created)
        self.assertFalse(result2.existing)
        self.assertNotEqual(result2.event["id"], result1.event["id"])

    # 8. Transition: approved -> review_required (stale approval)
    def test_transition_approved_to_review_required(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/8"

        result1 = check_pr_review(
            conn, "ws-1", "task-8", pr_url=pr, run=_mock_run(stdout=APPROVED_JSON)
        )
        self.assertEqual(result1.review_decision, "approved")
        self.assertTrue(result1.event_created)

        result2 = check_pr_review(
            conn, "ws-1", "task-8", pr_url=pr, run=_mock_run(stdout=REVIEW_REQUIRED_JSON)
        )
        self.assertEqual(result2.review_decision, "review_required")
        self.assertTrue(result2.event_created)
        self.assertFalse(result2.existing)
        self.assertNotEqual(result2.event["id"], result1.event["id"])

    # 9. Different PR with same decision -> new event
    def test_different_pr_same_decision(self):
        conn = _setup_db()
        pr1 = "https://github.com/o/r/pull/9a"
        pr2 = "https://github.com/o/r/pull/9b"

        run_fn = _mock_run(stdout=APPROVED_JSON)
        result1 = check_pr_review(conn, "ws-1", "task-9", pr_url=pr1, run=run_fn)
        self.assertTrue(result1.event_created)

        # Manually update mirror so pr2 does not conflict
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-9",
            phase=None,
            owner=None,
            branch=None,
            pr=pr2,
            payload={},
            last_event_id=None,
        )

        result2 = check_pr_review(conn, "ws-1", "task-9", pr_url=pr2, run=run_fn)
        self.assertTrue(result2.event_created)
        self.assertFalse(result2.existing)
        self.assertNotEqual(result2.event["id"], result1.event["id"])

    # 10. No PR raises ValueError
    def test_no_pr_raises(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            check_pr_review(conn, "ws-1", "task-10", run=_mock_run(stdout=APPROVED_JSON))
        self.assertIn("no PR", str(ctx.exception))

    # 11. Uses mirror PR when no pr_url given
    def test_uses_mirror_pr(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/11"
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-11",
            phase="working",
            owner="agent-1",
            branch="agents/task-11",
            pr=pr,
            payload={"title": "do stuff"},
            last_event_id=None,
        )
        result = check_pr_review(
            conn,
            "ws-1",
            "task-11",
            run=_mock_run(stdout=APPROVED_JSON),
        )
        self.assertEqual(result.pr_url, pr)
        self.assertTrue(result.event_created)

    # 12. gh CLI not installed
    def test_gh_not_installed(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            check_pr_review(
                conn,
                "ws-1",
                "task-12",
                pr_url="https://github.com/o/r/pull/12",
                run=_mock_run(side_effect=FileNotFoundError()),
            )
        self.assertIn("gh CLI not available", str(ctx.exception))

    # 13. gh returns nonzero even with valid JSON -> ValueError
    def test_gh_nonzero_with_valid_json_raises(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            check_pr_review(
                conn,
                "ws-1",
                "task-13",
                pr_url="https://github.com/o/r/pull/13",
                run=_mock_run(stdout=APPROVED_JSON, returncode=1, stderr="auth failed"),
            )
        self.assertIn("gh pr view failed", str(ctx.exception))

    # 14. gh returns nonzero, no JSON
    def test_gh_nonzero_no_json(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            check_pr_review(
                conn,
                "ws-1",
                "task-14",
                pr_url="https://github.com/o/r/pull/14",
                run=_mock_run(stdout="", returncode=1, stderr="some error"),
            )
        self.assertIn("gh pr view failed", str(ctx.exception))

    # 15. gh returns zero, invalid JSON
    def test_gh_zero_invalid_json(self):
        conn = _setup_db()
        with self.assertRaises(ValueError) as ctx:
            check_pr_review(
                conn,
                "ws-1",
                "task-15",
                pr_url="https://github.com/o/r/pull/15",
                run=_mock_run(stdout="not json", returncode=0),
            )
        self.assertIn("gh pr view returned invalid JSON", str(ctx.exception))

    # 16. Unknown workspace
    def test_unknown_workspace(self):
        conn = _setup_db()
        with self.assertRaises(ValueError):
            check_pr_review(
                conn,
                "ws-nope",
                "task-16",
                pr_url="https://github.com/o/r/pull/16",
                run=_mock_run(stdout=APPROVED_JSON),
            )

    # 17. Mirror PR conflicts with arg PR
    def test_mirror_pr_conflict(self):
        conn = _setup_db()
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-17",
            phase=None,
            owner=None,
            branch=None,
            pr="https://github.com/o/r/pull/17a",
            payload={},
            last_event_id=None,
        )
        with self.assertRaises(ValueError) as ctx:
            check_pr_review(
                conn,
                "ws-1",
                "task-17",
                pr_url="https://github.com/o/r/pull/17b",
                run=_mock_run(stdout=APPROVED_JSON),
            )
        self.assertIn("already has pr", str(ctx.exception))


class TestQueryPrReviewUnit(unittest.TestCase):
    """Unit tests for _query_pr_review directly."""

    # 18. Unit test for _query_pr_review
    def test_query_pr_review_unit(self):
        # APPROVED
        decision = _query_pr_review(
            "/tmp/test",
            "https://github.com/o/r/pull/99",
            run=_mock_run(stdout=APPROVED_JSON),
        )
        self.assertEqual(decision, "approved")

        # CHANGES_REQUESTED
        decision = _query_pr_review(
            "/tmp/test",
            "https://github.com/o/r/pull/99",
            run=_mock_run(stdout=CHANGES_REQUESTED_JSON),
        )
        self.assertEqual(decision, "changes_requested")

        # REVIEW_REQUIRED
        decision = _query_pr_review(
            "/tmp/test",
            "https://github.com/o/r/pull/99",
            run=_mock_run(stdout=REVIEW_REQUIRED_JSON),
        )
        self.assertEqual(decision, "review_required")

        # Null decision
        decision = _query_pr_review(
            "/tmp/test",
            "https://github.com/o/r/pull/99",
            run=_mock_run(stdout=NULL_DECISION_JSON),
        )
        self.assertEqual(decision, "review_required")

        # No decision key
        decision = _query_pr_review(
            "/tmp/test",
            "https://github.com/o/r/pull/99",
            run=_mock_run(stdout=NO_DECISION_JSON),
        )
        self.assertEqual(decision, "review_required")

        # Unknown value
        decision = _query_pr_review(
            "/tmp/test",
            "https://github.com/o/r/pull/99",
            run=_mock_run(stdout=UNKNOWN_DECISION_JSON),
        )
        self.assertEqual(decision, "review_required")


# ---------------------------------------------------------------------------
# check_merge_gate tests
# ---------------------------------------------------------------------------


class TestCheckMergeGate(unittest.TestCase):
    """Integration tests for check_merge_gate."""

    # 19. All checks pass
    def test_gate_all_pass(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/1"
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-1",
            phase=None,
            owner=None,
            branch=None,
            pr=pr,
            payload={},
            last_event_id=None,
        )
        append_event(
            conn,
            event_type="pr_review.approved",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-1",
            payload={"pr": pr, "head_sha": HEAD_SHA, "review_decision": "approved"},
        )
        append_event(
            conn,
            event_type="ci.passed",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-1",
            payload={"pr": pr, "head_sha": HEAD_SHA, "status": "passed"},
        )
        result = check_merge_gate(conn, "ws-1", "task-1", run=_mock_run(stdout=APPROVED_JSON))
        self.assertTrue(result.ready)
        self.assertTrue(result.human_gate_required)
        self.assertTrue(result.checks["has_pr"]["passed"])
        self.assertTrue(result.checks["current_head"]["passed"])
        self.assertTrue(result.checks["review_approved"]["passed"])
        self.assertTrue(result.checks["ci_passed"]["passed"])

    # 20. No task mirror -> no PR
    def test_gate_no_pr(self):
        conn = _setup_db()
        result = check_merge_gate(conn, "ws-1", "task-2")
        self.assertFalse(result.ready)
        self.assertFalse(result.checks["has_pr"]["passed"])
        self.assertEqual(result.checks["has_pr"]["reason"], "no task mirror")

    # 21. Latest review is changes_requested
    def test_gate_review_not_approved(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/3"
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-3",
            phase=None,
            owner=None,
            branch=None,
            pr=pr,
            payload={},
            last_event_id=None,
        )
        append_event(
            conn,
            event_type="pr_review.changes_requested",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-3",
            payload={"pr": pr, "review_decision": "changes_requested"},
        )
        append_event(
            conn,
            event_type="ci.passed",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-3",
            payload={"pr": pr, "status": "passed"},
        )
        result = check_merge_gate(conn, "ws-1", "task-3", run=_mock_run(stdout=APPROVED_JSON))
        self.assertFalse(result.ready)
        self.assertFalse(result.checks["review_approved"]["passed"])
        self.assertIn("pr_review.changes_requested", result.checks["review_approved"]["reason"])

    # 22. Review event has different PR
    def test_gate_review_pr_mismatch(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/4"
        wrong_pr = "https://github.com/o/r/pull/4-old"
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-4",
            phase=None,
            owner=None,
            branch=None,
            pr=pr,
            payload={},
            last_event_id=None,
        )
        append_event(
            conn,
            event_type="pr_review.approved",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-4",
            payload={"pr": wrong_pr, "review_decision": "approved"},
        )
        append_event(
            conn,
            event_type="ci.passed",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-4",
            payload={"pr": pr, "status": "passed"},
        )
        result = check_merge_gate(conn, "ws-1", "task-4", run=_mock_run(stdout=APPROVED_JSON))
        self.assertFalse(result.ready)
        self.assertFalse(result.checks["review_approved"]["passed"])
        self.assertIn("review event PR mismatch", result.checks["review_approved"]["reason"])

    def test_gate_review_head_mismatch(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/4b"
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-4b",
            phase=None,
            owner=None,
            branch=None,
            pr=pr,
            payload={},
            last_event_id=None,
        )
        append_event(
            conn,
            event_type="pr_review.approved",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-4b",
            payload={"pr": pr, "head_sha": "old-head", "review_decision": "approved"},
        )
        append_event(
            conn,
            event_type="ci.passed",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-4b",
            payload={"pr": pr, "head_sha": HEAD_SHA, "status": "passed"},
        )
        result = check_merge_gate(conn, "ws-1", "task-4b", run=_mock_run(stdout=APPROVED_JSON))
        self.assertFalse(result.ready)
        self.assertFalse(result.checks["review_approved"]["passed"])
        self.assertIn("review event head mismatch", result.checks["review_approved"]["reason"])

    # 23. Latest CI is ci.failed
    def test_gate_ci_not_passed(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/5"
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-5",
            phase=None,
            owner=None,
            branch=None,
            pr=pr,
            payload={},
            last_event_id=None,
        )
        append_event(
            conn,
            event_type="pr_review.approved",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-5",
            payload={"pr": pr, "review_decision": "approved"},
        )
        append_event(
            conn,
            event_type="ci.failed",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-5",
            payload={"pr": pr, "status": "failed"},
        )
        result = check_merge_gate(conn, "ws-1", "task-5", run=_mock_run(stdout=APPROVED_JSON))
        self.assertFalse(result.ready)
        self.assertFalse(result.checks["ci_passed"]["passed"])
        self.assertIn("ci.failed", result.checks["ci_passed"]["reason"])

    # 24. CI event has different PR
    def test_gate_ci_pr_mismatch(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/6"
        wrong_pr = "https://github.com/o/r/pull/6-old"
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-6",
            phase=None,
            owner=None,
            branch=None,
            pr=pr,
            payload={},
            last_event_id=None,
        )
        append_event(
            conn,
            event_type="pr_review.approved",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-6",
            payload={"pr": pr, "review_decision": "approved"},
        )
        append_event(
            conn,
            event_type="ci.passed",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-6",
            payload={"pr": wrong_pr, "status": "passed"},
        )
        result = check_merge_gate(conn, "ws-1", "task-6", run=_mock_run(stdout=APPROVED_JSON))
        self.assertFalse(result.ready)
        self.assertFalse(result.checks["ci_passed"]["passed"])
        self.assertIn("ci event PR mismatch", result.checks["ci_passed"]["reason"])

    def test_gate_ci_head_mismatch(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/6b"
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-6b",
            phase=None,
            owner=None,
            branch=None,
            pr=pr,
            payload={},
            last_event_id=None,
        )
        append_event(
            conn,
            event_type="pr_review.approved",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-6b",
            payload={"pr": pr, "head_sha": HEAD_SHA, "review_decision": "approved"},
        )
        append_event(
            conn,
            event_type="ci.passed",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-6b",
            payload={"pr": pr, "head_sha": "old-head", "status": "passed"},
        )
        result = check_merge_gate(conn, "ws-1", "task-6b", run=_mock_run(stdout=APPROVED_JSON))
        self.assertFalse(result.ready)
        self.assertFalse(result.checks["ci_passed"]["passed"])
        self.assertIn("ci event head mismatch", result.checks["ci_passed"]["reason"])

    # 25. No CI events
    def test_gate_no_ci_event(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/7"
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-7",
            phase=None,
            owner=None,
            branch=None,
            pr=pr,
            payload={},
            last_event_id=None,
        )
        append_event(
            conn,
            event_type="pr_review.approved",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-7",
            payload={"pr": pr, "review_decision": "approved"},
        )
        result = check_merge_gate(conn, "ws-1", "task-7", run=_mock_run(stdout=APPROVED_JSON))
        self.assertFalse(result.ready)
        self.assertFalse(result.checks["ci_passed"]["passed"])
        self.assertEqual(result.checks["ci_passed"]["reason"], "no ci event")

    # 26. No review events
    def test_gate_no_review_event(self):
        conn = _setup_db()
        pr = "https://github.com/o/r/pull/8"
        upsert_task_mirror(
            conn,
            workspace_id="ws-1",
            task_id="task-8",
            phase=None,
            owner=None,
            branch=None,
            pr=pr,
            payload={},
            last_event_id=None,
        )
        append_event(
            conn,
            event_type="ci.passed",
            actor="operator",
            workspace_id="ws-1",
            task_id="task-8",
            payload={"pr": pr, "status": "passed"},
        )
        result = check_merge_gate(conn, "ws-1", "task-8", run=_mock_run(stdout=APPROVED_JSON))
        self.assertFalse(result.ready)
        self.assertFalse(result.checks["review_approved"]["passed"])
        self.assertEqual(result.checks["review_approved"]["reason"], "no pr_review event")

    # 27. Unknown workspace
    def test_gate_unknown_workspace(self):
        conn = _setup_db()
        with self.assertRaises(ValueError):
            check_merge_gate(conn, "ws-nope", "task-9")


if __name__ == "__main__":
    unittest.main()
