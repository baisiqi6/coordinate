"""Phase 8.6: operator pending actions tests."""

import json
import unittest

from coordinate.db import (
    append_event,
    initialize,
    list_task_mirrors,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
)
from coordinate.operator import list_pending_actions


class OperatorPendingTests(unittest.TestCase):
    def setUp(self):
        self.conn = initialize(":memory:")
        upsert_workspace(
            self.conn,
            workspace_id="demo",
            name="Demo",
            path="/tmp/demo",
            harness_root="/tmp/demo",
        )

    def _create_task(self, task_id="t1", phase="ready", owner=None):
        upsert_task_mirror(
            self.conn,
            workspace_id="demo",
            task_id=task_id,
            phase=phase,
            owner=owner,
            branch=None,
            pr=None,
            payload={},
        )

    def _add_event(self, event_type, task_id="t1", payload=None, actor="mac-omp"):
        return append_event(
            self.conn,
            workspace_id="demo",
            event_type=event_type,
            actor=actor,
            task_id=task_id,
            idempotency_key=f"test:{event_type}:{task_id}:{actor}",
            payload=payload or {},
        )

    # -- event-derived attention and legacy compatibility --

    def test_legacy_awaiting_operator_with_agent_reported_returns_review_code(self):
        self._create_task("t1", phase="awaiting_operator", owner="mac-omp")
        self._add_event("agent.reported", "t1", {"action": "done", "summary": "Done!"})

        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].task_id, "t1")
        self.assertEqual(actions[0].action, "review_code")
        self.assertEqual(actions[0].phase, "awaiting_operator")
        self.assertEqual(actions[0].latest_event_type, "agent.reported")

    def test_legacy_awaiting_operator_without_event_returns_nothing(self):
        self._create_task("t1", phase="awaiting_operator", owner="mac-omp")

        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(actions, [])

    def test_task_done_suppresses_pending_action(self):
        self._create_task("t1", phase="running", owner="mac-omp")
        self._add_event("agent.reported", "t1", {"action": "done"})
        self._add_event("task.done", "t1", {"action": "done"}, actor="operator")

        actions = list_pending_actions(self.conn, "demo")

        self.assertEqual(actions, [])

    def test_late_agent_report_does_not_reopen_done_task(self):
        self._create_task("t1", phase="running", owner="mac-omp")
        self._add_event("task.done", "t1", {"action": "done"}, actor="operator")
        self._add_event("agent.reported", "t1", {"action": "done"})

        actions = list_pending_actions(self.conn, "demo")

        self.assertEqual(actions, [])

    # -- inference rule: planned + plan.review_requested --

    def test_planned_with_review_requested_returns_approve_plan(self):
        self._create_task("t1", phase="planned", owner="mac-claude")
        self._add_event("plan.review_requested", "t1", {"scope": "implementation plan"})

        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].action, "approve_plan")

    def test_planned_without_review_requested_returns_nothing(self):
        self._create_task("t1", phase="planned", owner="mac-claude")

        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(len(actions), 0)

    def test_same_second_plan_approval_supersedes_review_request(self):
        self._create_task("t1", phase="planned", owner=None)
        self._add_event("plan.review_requested", "t1", {"scope": "implementation plan"})
        self._add_event("plan.approved", "t1", {"decision": "approved"})
        self.conn.execute(
            "UPDATE events SET created_at = ? WHERE workspace_id = ? AND task_id = ?",
            ("2026-07-10T00:00:00Z", "demo", "t1"),
        )
        self.conn.commit()

        actions = list_pending_actions(self.conn, "demo")

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].action, "handoff")
        self.assertEqual(actions[0].latest_event_type, "plan.approved")

    # -- inference rule: implementing + agent.reported done --

    def test_implementing_with_agent_reported_returns_review_code(self):
        self._create_task("t1", phase="implementing", owner="mac-omp")
        self._add_event("agent.reported", "t1", {"action": "done", "summary": "Done!"})

        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].action, "review_code")

    def test_accepted_with_agent_reported_returns_review_code(self):
        self._create_task("t1", phase="accepted", owner="mac-omp")
        self._add_event("agent.reported", "t1", {"action": "done", "summary": "Done!"})

        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].action, "review_code")

    def test_approved_review_returns_mark_done(self):
        self._create_task("t1", phase="review_approved", owner="mac-omp")
        self._add_event(
            "review.completed",
            "t1",
            {"decision": "approved", "summary": "looks good"},
            actor="reviewer",
        )

        actions = list_pending_actions(self.conn, "demo")

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].action, "mark_done")

    def test_rejected_review_does_not_return_mark_done(self):
        self._create_task("t1", phase="changes_requested", owner="mac-omp")
        self._add_event(
            "review.completed",
            "t1",
            {"decision": "rejected", "summary": "needs changes"},
            actor="reviewer",
        )

        actions = list_pending_actions(self.conn, "demo")

        self.assertEqual(actions, [])

    # -- inference rule: ready + no owner --

    def test_ready_without_owner_returns_handoff(self):
        self._create_task("t1", phase="ready", owner=None)

        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].action, "handoff")

    def test_ready_with_owner_returns_nothing(self):
        self._create_task("t1", phase="ready", owner="mac-omp")

        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(len(actions), 0)

    # -- multiple tasks --

    def test_multiple_pending_tasks(self):
        self._create_task("t1", phase="awaiting_operator", owner="mac-omp")
        self._add_event("agent.reported", "t1", {"action": "done"})

        self._create_task("t2", phase="planned", owner="mac-claude")
        self._add_event("plan.review_requested", "t2", payload={"scope": "impl"})

        self._create_task("t3", phase="ready", owner=None)

        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(len(actions), 3)
        action_types = {a.action for a in actions}
        self.assertEqual(action_types, {"review_code", "approve_plan", "handoff"})

    # -- empty workspace --

    def test_empty_workspace_returns_empty(self):
        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(len(actions), 0)

    # -- result_summary extraction --

    def test_result_summary_from_agent_reported(self):
        self._create_task("t1", phase="running", owner="mac-omp")
        self._add_event("agent.reported", "t1", {
            "action": "done",
            "result_summary": "Implemented the feature",
        })

        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(actions[0].latest_event_summary, "Implemented the feature")

    def test_summary_fallback(self):
        self._create_task("t1", phase="running", owner="mac-omp")
        self._add_event("agent.reported", "t1", {
            "action": "done",
            "summary": "Fallback summary",
        })

        actions = list_pending_actions(self.conn, "demo")
        self.assertEqual(actions[0].latest_event_summary, "Fallback summary")
