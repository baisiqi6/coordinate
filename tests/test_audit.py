import json
import unittest

from coordinate.audit import AuditDrift, AuditReport, audit_workspace
from coordinate.db import (
    append_event,
    initialize,
    list_events,
    list_task_mirrors,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
)
from coordinate.harness import HarnessError


class _FakeAuditAdapter:
    """Minimal fake adapter for audit tests."""

    def __init__(
        self,
        *,
        refresh_state_result=None,
        refresh_state_error=None,
        checklist_result=None,
        checklist_error=None,
    ):
        self._refresh_state_result = refresh_state_result
        self._refresh_state_error = refresh_state_error
        self._checklist_result = checklist_result
        self._checklist_error = checklist_error

    def refresh_state(self):
        if self._refresh_state_error is not None:
            raise self._refresh_state_error
        return self._refresh_state_result or {}

    def read_checklist(self):
        if self._checklist_error is not None:
            raise self._checklist_error
        return self._checklist_result or {"items": []}


class AuditTestBase(unittest.TestCase):
    def _make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )
        return conn

    def _make_matching_adapter(self, mirrors):
        """Build a fake adapter whose checklist items match the given mirror dicts."""
        items = []
        for m in mirrors:
            item = {
                "id": m["task_id"],
                "status": m.get("phase", "todo"),
                "owner": m.get("owner"),
            }
            if m.get("phase"):
                item["workflow"] = {"status": m["phase"]}
            items.append(item)
        return _FakeAuditAdapter(
            refresh_state_result={"current_item": items[0] if items else None},
            checklist_result={"items": items},
        )


class NoDriftsTest(AuditTestBase):
    """1. Task mirrors match harness state: empty drifts list."""

    def test_no_drifts_when_all_match(self):
        conn = self._make_conn()
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="doing",
            owner="codex",
            branch=None,
            pr=None,
            payload={"id": "mvp-001", "status": "doing", "owner": "codex"},
        )
        adapter = _FakeAuditAdapter(
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "doing"},
                    "status": "doing",
                    "owner": "codex",
                }
            },
            checklist_result={
                "items": [
                    {
                        "id": "mvp-001",
                        "workflow": {"status": "doing"},
                        "status": "doing",
                        "owner": "codex",
                    }
                ]
            },
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertTrue(report.harness_available)
        self.assertIsNone(report.harness_error)
        self.assertEqual(report.drifts, [])
        self.assertEqual(report.summary["drifts"], 0)
        self.assertEqual(report.summary["mirrors"], 1)
        self.assertEqual(report.summary["harness_tasks"], 1)


class MirrorMissingTest(AuditTestBase):
    """2. Mutation event exists but no task mirror: drift detected."""

    def test_mutation_event_without_mirror(self):
        conn = self._make_conn()
        append_event(
            conn,
            workspace_id="demo",
            event_type="assignment.requested",
            actor="operator",
            task_id="mvp-001",
            idempotency_key="demo:assign:mvp-001",
            payload={"task_id": "mvp-001", "owner": "codex"},
        )
        # No task mirror for mvp-001
        adapter = _FakeAuditAdapter(
            refresh_state_result={"current_item": None},
            checklist_result={"items": []},
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertEqual(len(report.drifts), 1)
        drift = report.drifts[0]
        self.assertEqual(drift.kind, "mirror_missing")
        self.assertEqual(drift.task_id, "mvp-001")
        self.assertIn("assignment.requested", drift.detail)
        self.assertIsNotNone(drift.coordinator)
        self.assertIsNone(drift.harness)

    def test_duplicate_mutation_events_for_same_task_produce_one_drift(self):
        conn = self._make_conn()
        append_event(
            conn,
            workspace_id="demo",
            event_type="assignment.requested",
            actor="operator",
            task_id="mvp-001",
            idempotency_key="demo:assign:mvp-001:v1",
            payload={"task_id": "mvp-001"},
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="assignment.accepted",
            actor="codex",
            task_id="mvp-001",
            idempotency_key="demo:accept:mvp-001:v1",
            payload={"task_id": "mvp-001"},
        )

        adapter = _FakeAuditAdapter(
            refresh_state_result={"current_item": None},
            checklist_result={"items": []},
        )
        report = audit_workspace(conn, "demo", adapter=adapter)

        # Only one drift per unique task_id
        mirror_missing = [d for d in report.drifts if d.kind == "mirror_missing"]
        self.assertEqual(len(mirror_missing), 1)


class StatusMismatchTest(AuditTestBase):
    """3. Mirror phase != harness workflow status: drift detected."""

    def test_status_mismatch_drift(self):
        conn = self._make_conn()
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="doing",
            owner="codex",
            branch=None,
            pr=None,
            payload={"id": "mvp-001", "status": "doing", "owner": "codex"},
        )
        adapter = _FakeAuditAdapter(
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "done"},
                    "status": "done",
                    "owner": "codex",
                }
            },
            checklist_result={
                "items": [
                    {
                        "id": "mvp-001",
                        "workflow": {"status": "done"},
                        "status": "done",
                        "owner": "codex",
                    }
                ]
            },
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertEqual(len(report.drifts), 1)
        drift = report.drifts[0]
        self.assertEqual(drift.kind, "status_mismatch")
        self.assertEqual(drift.task_id, "mvp-001")
        self.assertIn("doing", drift.detail)
        self.assertIn("done", drift.detail)
        self.assertEqual(drift.coordinator, {"phase": "doing"})
        self.assertEqual(drift.harness, {"workflow_status": "done", "status": "done"})

    def test_no_status_drift_when_phase_matches_coarse_status(self):
        """If mirror.phase matches harness status (coarse), no drift."""
        conn = self._make_conn()
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="done",
            owner="codex",
            branch=None,
            pr=None,
            payload={"id": "mvp-001"},
        )
        adapter = _FakeAuditAdapter(
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "closed"},
                    "status": "done",
                    "owner": "codex",
                }
            },
            checklist_result={
                "items": [
                    {
                        "id": "mvp-001",
                        "workflow": {"status": "closed"},
                        "status": "done",
                        "owner": "codex",
                    }
                ]
            },
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        status_drifts = [d for d in report.drifts if d.kind == "status_mismatch"]
        self.assertEqual(len(status_drifts), 0)

    def test_no_status_drift_when_ready_maps_to_harness_todo(self):
        """Coordinator ready means planned/ready, while harness checklist uses todo."""
        conn = self._make_conn()
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="ready",
            owner=None,
            branch=None,
            pr=None,
            payload={"id": "mvp-001"},
        )
        adapter = _FakeAuditAdapter(
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "todo"},
                    "status": "todo",
                    "owner": None,
                }
            },
            checklist_result={
                "items": [
                    {
                        "id": "mvp-001",
                        "workflow": {"status": "todo"},
                        "status": "todo",
                        "owner": None,
                    }
                ]
            },
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        status_drifts = [d for d in report.drifts if d.kind == "status_mismatch"]
        self.assertEqual(len(status_drifts), 0)


class OwnerMismatchTest(AuditTestBase):
    """4. Mirror owner != harness owner: drift detected."""

    def test_owner_mismatch_drift(self):
        conn = self._make_conn()
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="doing",
            owner="codex",
            branch=None,
            pr=None,
            payload={"id": "mvp-001", "status": "doing", "owner": "codex"},
        )
        adapter = _FakeAuditAdapter(
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "doing"},
                    "status": "doing",
                    "owner": "claude",
                }
            },
            checklist_result={
                "items": [
                    {
                        "id": "mvp-001",
                        "workflow": {"status": "doing"},
                        "status": "doing",
                        "owner": "claude",
                    }
                ]
            },
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertEqual(len(report.drifts), 1)
        drift = report.drifts[0]
        self.assertEqual(drift.kind, "owner_mismatch")
        self.assertEqual(drift.task_id, "mvp-001")
        self.assertIn("codex", drift.detail)
        self.assertIn("claude", drift.detail)
        self.assertEqual(drift.coordinator, {"owner": "codex"})
        self.assertEqual(drift.harness, {"owner": "claude"})

    def test_no_owner_drift_when_one_side_is_none(self):
        """If either owner is None, no drift (nothing to compare)."""
        conn = self._make_conn()
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="doing",
            owner=None,
            branch=None,
            pr=None,
            payload={"id": "mvp-001", "status": "doing"},
        )
        adapter = _FakeAuditAdapter(
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "doing"},
                    "status": "doing",
                    "owner": "codex",
                }
            },
            checklist_result={
                "items": [
                    {
                        "id": "mvp-001",
                        "workflow": {"status": "doing"},
                        "status": "doing",
                        "owner": "codex",
                    }
                ]
            },
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        owner_drifts = [d for d in report.drifts if d.kind == "owner_mismatch"]
        self.assertEqual(len(owner_drifts), 0)


class HarnessTaskUntrackedTest(AuditTestBase):
    """5. Harness has task but no mirror: drift detected."""

    def test_harness_task_untracked(self):
        conn = self._make_conn()
        # No task mirror registered
        adapter = _FakeAuditAdapter(
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "todo"},
                    "status": "todo",
                    "owner": None,
                }
            },
            checklist_result={
                "items": [
                    {
                        "id": "mvp-001",
                        "workflow": {"status": "todo"},
                        "status": "todo",
                        "owner": None,
                    }
                ]
            },
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertEqual(len(report.drifts), 1)
        drift = report.drifts[0]
        self.assertEqual(drift.kind, "harness_task_untracked")
        self.assertEqual(drift.task_id, "mvp-001")
        self.assertIn("no coordinator task mirror", drift.detail)
        self.assertIsNone(drift.coordinator)
        self.assertIsNotNone(drift.harness)


class MutationFailuresTest(AuditTestBase):
    """6. mutation_failed events appear in report."""

    def test_mutation_failures_collected(self):
        conn = self._make_conn()
        append_event(
            conn,
            workspace_id="demo",
            event_type="harness.mutation_failed",
            actor="operator",
            task_id="mvp-001",
            idempotency_key="demo:assign:mvp-001:failed",
            payload={"operation": "assign", "task_id": "mvp-001", "exit_code": 1},
        )
        adapter = _FakeAuditAdapter(
            refresh_state_result={"current_item": None},
            checklist_result={"items": []},
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertEqual(len(report.mutation_failures), 1)
        failure = report.mutation_failures[0]
        self.assertEqual(failure["task_id"], "mvp-001")
        self.assertEqual(failure["payload"]["operation"], "assign")
        self.assertIn("event_id", failure)
        self.assertIn("created_at", failure)
        self.assertEqual(report.summary["mutation_failures"], 1)

    def test_multiple_mutation_failures(self):
        conn = self._make_conn()
        append_event(
            conn,
            workspace_id="demo",
            event_type="harness.mutation_failed",
            actor="operator",
            task_id="mvp-001",
            idempotency_key="demo:assign:mvp-001:failed",
            payload={"operation": "assign"},
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="harness.mutation_failed",
            actor="operator",
            task_id="mvp-002",
            idempotency_key="demo:accept:mvp-002:failed",
            payload={"operation": "accept"},
        )

        adapter = _FakeAuditAdapter(
            refresh_state_result={"current_item": None},
            checklist_result={"items": []},
        )
        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertEqual(len(report.mutation_failures), 2)
        self.assertEqual(report.summary["mutation_failures"], 2)

    def test_later_successful_same_mutation_resolves_failure(self):
        conn = self._make_conn()
        failed = append_event(
            conn,
            workspace_id="demo",
            event_type="harness.mutation_failed",
            actor="mac-codex",
            task_id="mvp-001",
            idempotency_key="demo:accept:mvp-001:failed",
            payload={"operation": "accept", "owner": "mac-codex", "exit_code": 1},
        )
        conn.execute(
            "UPDATE events SET created_at = ? WHERE id = ?",
            ("2026-05-31T04:00:00Z", failed.row["id"]),
        )
        accepted = append_event(
            conn,
            workspace_id="demo",
            event_type="assignment.accepted",
            actor="mac-codex",
            target="mac-codex",
            task_id="mvp-001",
            idempotency_key="demo:accept:mvp-001:success",
            payload={"operation": "accept", "owner": "mac-codex"},
        )
        conn.execute(
            "UPDATE events SET created_at = ? WHERE id = ?",
            ("2026-05-31T04:00:01Z", accepted.row["id"]),
        )
        adapter = _FakeAuditAdapter(
            refresh_state_result={"current_item": None},
            checklist_result={"items": []},
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertEqual(report.mutation_failures, [])
        self.assertEqual(report.summary["mutation_failures"], 0)

    def test_different_later_mutation_does_not_resolve_failure(self):
        conn = self._make_conn()
        failed = append_event(
            conn,
            workspace_id="demo",
            event_type="harness.mutation_failed",
            actor="mac-codex",
            task_id="mvp-001",
            idempotency_key="demo:accept:mvp-001:failed",
            payload={"operation": "accept", "owner": "mac-codex", "exit_code": 1},
        )
        conn.execute(
            "UPDATE events SET created_at = ? WHERE id = ?",
            ("2026-05-31T04:00:00Z", failed.row["id"]),
        )
        review = append_event(
            conn,
            workspace_id="demo",
            event_type="review.completed",
            actor="mac-codex",
            target="mac-codex",
            task_id="mvp-001",
            idempotency_key="demo:review:mvp-001:success",
            payload={"operation": "review-result", "owner": "mac-codex"},
        )
        conn.execute(
            "UPDATE events SET created_at = ? WHERE id = ?",
            ("2026-05-31T04:00:01Z", review.row["id"]),
        )
        adapter = _FakeAuditAdapter(
            refresh_state_result={"current_item": None},
            checklist_result={"items": []},
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertEqual(len(report.mutation_failures), 1)
        self.assertEqual(report.summary["mutation_failures"], 1)

    def test_later_task_done_resolves_prior_mutation_failure(self):
        conn = self._make_conn()
        failed = append_event(
            conn,
            workspace_id="demo",
            event_type="harness.mutation_failed",
            actor="codex",
            task_id="mvp-001",
            idempotency_key="demo:accept:mvp-001:failed",
            payload={"operation": "accept", "owner": "codex", "exit_code": 1},
        )
        conn.execute(
            "UPDATE events SET created_at = ? WHERE id = ?",
            ("2026-05-31T04:00:00Z", failed.row["id"]),
        )
        done = append_event(
            conn,
            workspace_id="demo",
            event_type="task.done",
            actor="reviewer",
            target="mvp-001",
            task_id="mvp-001",
            idempotency_key="demo:done:mvp-001:success",
            payload={"operation": "mark-done"},
        )
        conn.execute(
            "UPDATE events SET created_at = ? WHERE id = ?",
            ("2026-05-31T04:00:01Z", done.row["id"]),
        )
        adapter = _FakeAuditAdapter(
            refresh_state_result={"current_item": None},
            checklist_result={"items": []},
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertEqual(report.mutation_failures, [])
        self.assertEqual(report.summary["mutation_failures"], 0)


class HarnessUnavailableTest(AuditTestBase):
    """7. Adapter fails: report has harness_available=False, no harness-side drifts."""

    def test_harness_unavailable(self):
        conn = self._make_conn()
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="doing",
            owner="codex",
            branch=None,
            pr=None,
            payload={"id": "mvp-001"},
        )
        adapter = _FakeAuditAdapter(
            refresh_state_error=HarnessError("harness-state.json not found"),
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertFalse(report.harness_available)
        self.assertIsNotNone(report.harness_error)
        self.assertIn("harness-state.json not found", report.harness_error)
        self.assertEqual(report.summary["harness_tasks"], 0)
        # Only coordinator-side drifts possible
        harness_side_drifts = [
            d for d in report.drifts
            if d.kind in ("status_mismatch", "owner_mismatch", "harness_task_untracked")
        ]
        self.assertEqual(len(harness_side_drifts), 0)

    def test_harness_unavailable_still_detects_mirror_missing(self):
        conn = self._make_conn()
        append_event(
            conn,
            workspace_id="demo",
            event_type="assignment.requested",
            actor="operator",
            task_id="mvp-001",
            idempotency_key="demo:assign:mvp-001",
            payload={"task_id": "mvp-001"},
        )
        adapter = _FakeAuditAdapter(
            refresh_state_error=HarnessError("unavailable"),
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertFalse(report.harness_available)
        mirror_missing = [d for d in report.drifts if d.kind == "mirror_missing"]
        self.assertEqual(len(mirror_missing), 1)


class UnknownWorkspaceTest(unittest.TestCase):
    """8. Unknown workspace raises ValueError."""

    def test_unknown_workspace_raises(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)

        with self.assertRaises(ValueError) as ctx:
            audit_workspace(conn, "nonexistent")
        self.assertIn("unknown workspace", str(ctx.exception))


class EmptyWorkspaceTest(AuditTestBase):
    """9. No mirrors, no events, no harness items: clean report."""

    def test_empty_workspace(self):
        conn = self._make_conn()
        adapter = _FakeAuditAdapter(
            refresh_state_result={"current_item": None},
            checklist_result={"items": []},
        )

        report = audit_workspace(conn, "demo", adapter=adapter)

        self.assertTrue(report.harness_available)
        self.assertEqual(report.drifts, [])
        self.assertEqual(report.mutation_failures, [])
        self.assertEqual(report.summary["mirrors"], 0)
        self.assertEqual(report.summary["harness_tasks"], 0)
        self.assertEqual(report.summary["drifts"], 0)
        self.assertEqual(report.summary["mutation_failures"], 0)


class ToDictTest(AuditTestBase):
    """10. to_dict() serializes cleanly to JSON."""

    def test_to_dict_is_json_serializable(self):
        conn = self._make_conn()
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="doing",
            owner="codex",
            branch=None,
            pr=None,
            payload={"id": "mvp-001"},
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="harness.mutation_failed",
            actor="operator",
            task_id="mvp-002",
            idempotency_key="demo:assign:mvp-002:failed",
            payload={"operation": "assign", "exit_code": 1},
        )
        adapter = _FakeAuditAdapter(
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "done"},
                    "status": "done",
                    "owner": "claude",
                }
            },
            checklist_result={
                "items": [
                    {
                        "id": "mvp-001",
                        "workflow": {"status": "done"},
                        "status": "done",
                        "owner": "claude",
                    }
                ]
            },
        )

        report = audit_workspace(conn, "demo", adapter=adapter)
        d = report.to_dict()

        # Must be JSON-serializable without error
        serialized = json.dumps(d)
        self.assertIsInstance(serialized, str)

        # Structure checks
        self.assertEqual(d["workspace_id"], "demo")
        self.assertTrue(d["harness_available"])
        self.assertIn("drifts", d)
        self.assertIn("mutation_failures", d)
        self.assertIn("summary", d)
        self.assertIsInstance(d["drifts"], list)
        self.assertIsInstance(d["mutation_failures"], list)
        self.assertIsInstance(d["summary"], dict)

        # Verify drift structure
        for drift_dict in d["drifts"]:
            self.assertIn("task_id", drift_dict)
            self.assertIn("kind", drift_dict)
            self.assertIn("detail", drift_dict)
            self.assertIn("coordinator", drift_dict)
            self.assertIn("harness", drift_dict)

    def test_to_dict_with_harness_error(self):
        conn = self._make_conn()
        adapter = _FakeAuditAdapter(
            refresh_state_error=HarnessError("something broke"),
        )

        report = audit_workspace(conn, "demo", adapter=adapter)
        d = report.to_dict()

        serialized = json.dumps(d)
        self.assertIsInstance(serialized, str)
        self.assertFalse(d["harness_available"])
        self.assertIn("something broke", d["harness_error"])


if __name__ == "__main__":
    unittest.main()
