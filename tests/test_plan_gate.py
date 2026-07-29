import unittest

from coordinate.db import (
    append_event,
    initialize,
    list_events,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
)
from coordinate.plan_gate import (
    approve_plan,
    reject_plan,
    review_request_plan,
)


class PlanGateServiceTests(unittest.TestCase):
    def _make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        return conn

    def _setup_workspace_with_task(self, conn, task_id="t1"):
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/tmp/demo",
            harness_root="/tmp/demo",
        )
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id=task_id,
            phase="ready",
            owner=None,
            branch=None,
            pr=None,
            payload={"task_id": task_id, "title": "Test"},
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.ready",
            actor="operator",
            target="worker",
            task_id=task_id,
            idempotency_key=f"demo:{task_id}:plan.ready",
            payload={"task_id": task_id, "title": "Test", "plan_doc": "docs/plan.md"},
        )

    # --- review_request_plan ---

    def test_review_request_creates_event(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        result = review_request_plan(conn, workspace_id="demo", task_id="t1")

        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "plan.review_requested")
        self.assertEqual(result.event["workspace_id"], "demo")
        self.assertEqual(result.event["task_id"], "t1")
        self.assertEqual(result.event["actor"], "operator")
        self.assertEqual(result.event["target"], "reviewer")

    def test_review_request_idempotent(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        first = review_request_plan(conn, workspace_id="demo", task_id="t1")
        second = review_request_plan(conn, workspace_id="demo", task_id="t1")

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])

        events = list(list_events(conn, "demo"))
        review_events = [e for e in events if row_to_dict(e)["event_type"] == "plan.review_requested"]
        self.assertEqual(len(review_events), 1)

    def test_review_request_raises_on_unknown_workspace(self):
        conn = self._make_conn()

        with self.assertRaises(ValueError):
            review_request_plan(conn, workspace_id="nonexistent", task_id="t1")

    def test_review_request_raises_on_missing_task(self):
        conn = self._make_conn()
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/tmp/demo",
            harness_root="/tmp/demo",
        )

        with self.assertRaises(ValueError):
            review_request_plan(conn, workspace_id="demo", task_id="t1")

    # --- approve_plan ---

    def test_approve_creates_event_without_overwriting_harness_phase(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        result = approve_plan(conn, workspace_id="demo", task_id="t1", scope="implementation plan")

        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "plan.approved")
        self.assertEqual(result.task["phase"], "ready")
        self.assertEqual(result.event["payload"]["scope"], "implementation plan")

    def test_approve_includes_reviewer_and_notes(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        result = approve_plan(
            conn,
            workspace_id="demo",
            task_id="t1",
            scope="implementation plan",
            reviewer="alice",
            notes="Looks good",
        )

        payload = result.event["payload"]
        self.assertEqual(payload["reviewer"], "alice")
        self.assertEqual(payload["notes"], "Looks good")
        self.assertEqual(payload["scope"], "implementation plan")

    def test_approve_includes_source_plan_from_plan_ready(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        result = approve_plan(conn, workspace_id="demo", task_id="t1", scope="implementation plan")

        payload = result.event["payload"]
        self.assertEqual(payload["source_plan"], "docs/plan.md")
        self.assertIn("plan_ready_event_id", payload)
        self.assertIsNotNone(payload["plan_ready_event_id"])

    def test_approve_idempotent(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        first = approve_plan(conn, workspace_id="demo", task_id="t1", scope="implementation plan")
        second = approve_plan(conn, workspace_id="demo", task_id="t1", scope="implementation plan")

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])

    def test_approve_idempotency_key_includes_scope(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        result = approve_plan(conn, workspace_id="demo", task_id="t1", scope="implementation plan")

        self.assertIn("implementation plan", result.event["idempotency_key"])

    # --- reject_plan ---

    def test_reject_creates_event_without_overwriting_harness_phase(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        result = reject_plan(conn, workspace_id="demo", task_id="t1", scope="implementation plan")

        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "plan.rejected")
        self.assertEqual(result.task["phase"], "ready")
        self.assertEqual(result.event["payload"]["scope"], "implementation plan")

    def test_reject_includes_reason(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        result = reject_plan(
            conn,
            workspace_id="demo",
            task_id="t1",
            scope="implementation plan",
            reason="Scope too broad",
        )

        payload = result.event["payload"]
        self.assertEqual(payload["reason"], "Scope too broad")

    def test_reject_then_approve_creates_new_events(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        rejected = reject_plan(conn, workspace_id="demo", task_id="t1", scope="implementation plan")
        approved = approve_plan(conn, workspace_id="demo", task_id="t1", scope="implementation plan")

        self.assertTrue(rejected.event_created)
        self.assertTrue(approved.event_created)
        self.assertNotEqual(rejected.event["id"], approved.event["id"])

    def test_approve_then_reject_then_approve_cycle(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        first_approve = approve_plan(conn, workspace_id="demo", task_id="t1", scope="implementation plan")
        reject = reject_plan(conn, workspace_id="demo", task_id="t1", scope="implementation plan")
        second_approve = approve_plan(conn, workspace_id="demo", task_id="t1", scope="implementation plan")

        self.assertTrue(first_approve.event_created)
        self.assertTrue(reject.event_created)
        self.assertTrue(second_approve.event_created)

        ids = {
            first_approve.event["id"],
            reject.event["id"],
            second_approve.event["id"],
        }
        self.assertEqual(len(ids), 3, "all three events should have distinct ids")

    def test_different_scopes_produce_different_events(self):
        conn = self._make_conn()
        self._setup_workspace_with_task(conn)

        scope_a = approve_plan(conn, workspace_id="demo", task_id="t1", scope="scope-a")
        scope_b = approve_plan(conn, workspace_id="demo", task_id="t1", scope="scope-b")

        self.assertTrue(scope_a.event_created)
        self.assertTrue(scope_b.event_created)
        self.assertNotEqual(scope_a.event["id"], scope_b.event["id"])


if __name__ == "__main__":
    unittest.main()
