import tempfile
import unittest
import unittest.mock
from datetime import datetime, timezone
from pathlib import Path

from coordinate.assignments import AssignmentRequestResult, request_assignment
from coordinate.db import (
    get_workspace,
    initialize,
    list_deliveries,
    list_events,
    row_to_dict,
    upsert_workspace,
)
from coordinate.harness import HarnessMutationResult


class _FakeAdapter:
    def __init__(self, workspace, *, success=True, stdout="ok", stderr=""):
        self.workspace = workspace
        self._success = success
        self._stdout = stdout
        self._stderr = stderr
        self.calls: list[dict] = []

    def run_mutation(self, operation, task_id, actor, args=None, idempotency_hint=None):
        self.calls.append({
            "operation": operation,
            "task_id": task_id,
            "actor": actor,
            "args": list(args) if args else [],
            "idempotency_hint": idempotency_hint,
        })
        return HarnessMutationResult(
            operation=operation,
            task_id=task_id,
            actor=actor,
            idempotency_hint=idempotency_hint or f"{self.workspace.id}:{operation}:{task_id}:{actor}",
            started_at=datetime.now(timezone.utc).isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            command=["harnessctl", operation, task_id] + (list(args) if args else []),
            exit_code=0 if self._success else 1,
            stdout=self._stdout,
            stderr=self._stderr,
            success=self._success,
        )


class AssignmentServiceTests(unittest.TestCase):
    def _make_conn(self, **ws_kwargs):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
            **ws_kwargs,
        )
        return conn

    def _make_adapter(self, conn, **kwargs):
        workspace = get_workspace(conn, "demo")
        return _FakeAdapter(workspace, **kwargs)

    # --- success path ---

    def test_success_creates_assignment_requested_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertIsInstance(result, AssignmentRequestResult)
        self.assertIsNotNone(result.mutation)
        self.assertTrue(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "assignment.requested")
        self.assertEqual(result.event["target"], "codex")
        self.assertEqual(result.event["actor"], "operator")
        self.assertEqual(result.event["task_id"], "mvp-001")
        self.assertEqual(result.event["workspace_id"], "demo")

    def test_success_passes_correct_args_to_run_mutation(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1",
            actor="operator", branch="agent/codex/mvp-001", adapter=adapter,
        )

        self.assertEqual(len(adapter.calls), 1)
        call = adapter.calls[0]
        self.assertEqual(call["operation"], "assign")
        self.assertEqual(call["task_id"], "mvp-001")
        self.assertEqual(call["actor"], "operator")
        self.assertEqual(
            call["args"],
            ["codex", "sess-1", "--actor", "operator", "--branch", "agent/codex/mvp-001"],
        )

    def test_success_without_branch_omits_branch_arg(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        request_assignment(conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter)

        self.assertEqual(
            adapter.calls[0]["args"],
            ["codex", "sess-1", "--actor", "operator"],
        )

    def test_success_payload_contains_required_fields(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1",
            branch="agent/codex/mvp-001", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["task_id"], "mvp-001")
        self.assertEqual(payload["owner"], "codex")
        self.assertEqual(payload["session"], "sess-1")
        self.assertEqual(payload["branch"], "agent/codex/mvp-001")
        self.assertIn("mutation", payload)
        self.assertEqual(payload["mutation"]["operation"], "assign")
        self.assertTrue(payload["mutation"]["success"])

    # --- idempotency ---

    def test_default_idempotency_key_format(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertEqual(
            result.event["idempotency_key"],
            "demo:assign:mvp-001:codex:sess-1:assignment.requested",
        )

    def test_custom_idempotency_hint_used_in_key(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1",
            idempotency_hint="custom:hint", adapter=adapter,
        )

        self.assertTrue(result.event_created)
        self.assertEqual(
            result.event["idempotency_key"],
            "custom:hint:assignment.requested",
        )

    def test_idempotent_repeat_skips_mutation_and_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        first = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        self.assertTrue(first.event_created)
        self.assertEqual(len(adapter.calls), 1)

        second = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        self.assertFalse(second.event_created)
        self.assertIsNone(second.mutation)
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(first.event["id"], second.event["id"])

        events = list(list_events(conn, "demo"))
        self.assertEqual(len(events), 1)

    # --- delivery ---

    def test_default_bus_destination_creates_delivery(self):
        conn = self._make_conn(default_bus="stdout", default_destination="local")
        adapter = self._make_adapter(conn)

        result = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertIsNotNone(result.delivery)
        self.assertTrue(result.delivery_created)
        self.assertEqual(result.delivery["platform"], "stdout")
        self.assertEqual(result.delivery["destination"], "local")
        self.assertEqual(result.delivery["status"], "pending")

    def test_explicit_platform_destination_creates_delivery(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1",
            platform="discord", destination="channel-1", adapter=adapter,
        )

        self.assertIsNotNone(result.delivery)
        self.assertTrue(result.delivery_created)
        self.assertEqual(result.delivery["platform"], "discord")
        self.assertEqual(result.delivery["destination"], "channel-1")

    def test_explicit_platform_overrides_default(self):
        conn = self._make_conn(default_bus="stdout", default_destination="local")
        adapter = self._make_adapter(conn)

        result = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1",
            platform="discord", destination="ch-1", adapter=adapter,
        )

        self.assertEqual(result.delivery["platform"], "discord")
        self.assertEqual(result.delivery["destination"], "ch-1")

    def test_no_delivery_without_platform_or_destination(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertIsNone(result.delivery)
        self.assertIsNone(result.delivery_created)
        self.assertEqual(list(list_deliveries(conn)), [])

    def test_idempotent_delivery_not_duplicated(self):
        conn = self._make_conn(default_bus="stdout", default_destination="local")
        adapter = self._make_adapter(conn)

        first = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        self.assertTrue(first.delivery_created)

        second = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        self.assertFalse(second.delivery_created)
        self.assertIsNotNone(second.delivery)
        self.assertEqual(first.delivery["id"], second.delivery["id"])

        deliveries = [row_to_dict(d) for d in list_deliveries(conn)]
        self.assertEqual(len(deliveries), 1)

    def test_idempotent_retry_creates_missing_delivery(self):
        conn = self._make_conn(default_bus="stdout", default_destination="local")
        adapter = self._make_adapter(conn)

        first = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        self.assertTrue(first.delivery_created)

        # Delete the delivery to simulate a failed delivery creation
        conn.execute("DELETE FROM deliveries WHERE id = ?", (first.delivery["id"],))
        conn.commit()

        second = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        self.assertFalse(second.event_created)
        self.assertTrue(second.delivery_created)
        self.assertIsNotNone(second.delivery)
        self.assertEqual(len(list(list_deliveries(conn))), 1)

    # --- failure path ---

    def test_failure_writes_mutation_failed_event(self):
        conn = self._make_conn(default_bus="stdout", default_destination="local")
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = request_assignment(
            conn, "demo", "mvp-999", "codex", "sess-1", adapter=adapter,
        )

        self.assertIsNotNone(result.mutation)
        self.assertFalse(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "harness.mutation_failed")
        self.assertEqual(result.event["task_id"], "mvp-999")
        self.assertEqual(result.event["actor"], "operator")

    def test_failure_payload_contains_required_fields(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = request_assignment(
            conn, "demo", "mvp-999", "codex", "sess-1", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["operation"], "assign")
        self.assertEqual(payload["task_id"], "mvp-999")
        self.assertEqual(payload["owner"], "codex")
        self.assertEqual(payload["session"], "sess-1")
        self.assertEqual(payload["stderr"], "item not found")
        self.assertEqual(payload["exit_code"], 1)
        self.assertIn("mutation", payload)

    def test_failure_creates_blocker_delivery(self):
        conn = self._make_conn(default_bus="stdout", default_destination="local")
        adapter = self._make_adapter(conn, success=False, stderr="error")

        result = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertIsNotNone(result.delivery)
        self.assertTrue(result.delivery_created)
        self.assertEqual(result.delivery["platform"], "stdout")
        self.assertEqual(result.delivery["destination"], "local")
        self.assertEqual(result.delivery["status"], "pending")
        self.assertEqual(result.delivery["payload"]["visible_header"], "[BLOCKER]")
        self.assertIn("状态：harness mutation 执行失败", result.delivery["payload"]["text"])
        self.assertIn("操作：assign", result.delivery["payload"]["text"])
        self.assertIn("目标：codex", result.delivery["payload"]["text"])

    def test_failure_event_is_idempotent(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        first = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        second = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertIsNone(second.mutation)
        self.assertEqual(first.event["id"], second.event["id"])
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(len(list(list_events(conn, "demo"))), 1)

    def test_failure_idempotent_retry_creates_missing_delivery(self):
        conn = self._make_conn(default_bus="stdout", default_destination="local")
        adapter = self._make_adapter(conn, success=False, stderr="error")

        first = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        self.assertTrue(first.delivery_created)
        conn.execute("DELETE FROM deliveries WHERE id = ?", (first.delivery["id"],))
        conn.commit()

        second = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertFalse(second.event_created)
        self.assertIsNone(second.mutation)
        self.assertTrue(second.delivery_created)
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(len(list(list_deliveries(conn))), 1)

    # --- adapter=None path ---

    def test_unknown_workspace_without_adapter_raises(self):
        conn = self._make_conn()

        with self.assertRaises(ValueError):
            request_assignment(
                conn, "nonexistent", "mvp-001", "codex", "sess-1",
            )

    def test_missing_harnessctl_without_adapter_writes_failure_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = initialize(":memory:")
            self.addCleanup(conn.close)
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=tmp,
                harness_root=tmp,
                harnessctl_path=Path(tmp) / "missing-harnessctl",
                default_bus="stdout",
                default_destination="local",
            )

            result = request_assignment(
                conn, "demo", "mvp-001", "codex", "sess-1",
            )

            self.assertIsNotNone(result.mutation)
            self.assertFalse(result.mutation.success)
            self.assertTrue(result.event_created)
            self.assertEqual(result.event["event_type"], "harness.mutation_failed")
            self.assertIn("configured harnessctl_path not found", result.event["payload"]["stderr"])
            self.assertIsNotNone(result.delivery)
            self.assertEqual(result.delivery["payload"]["visible_header"], "[BLOCKER]")


class PostMutationReconcileTests(unittest.TestCase):
    def _make_conn(self, **ws_kwargs):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
            **ws_kwargs,
        )
        return conn

    def _make_adapter(self, conn, **kwargs):
        workspace = get_workspace(conn, "demo")
        return _FakeAdapter(workspace, **kwargs)

    @unittest.mock.patch("coordinate.assignments.reconcile_workspace")
    def test_request_assignment_calls_reconcile_on_fresh_success(self, mock_reconcile):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertTrue(result.event_created)
        mock_reconcile.assert_called_once()
        call_args = mock_reconcile.call_args
        self.assertEqual(call_args[0][1].id, "demo")
        self.assertTrue(call_args[1]["refresh"])

    @unittest.mock.patch("coordinate.assignments.reconcile_workspace")
    def test_request_assignment_skips_reconcile_on_idempotent_retry(self, mock_reconcile):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        request_assignment(conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter)
        self.assertEqual(mock_reconcile.call_count, 1)

        second = request_assignment(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        self.assertFalse(second.event_created)
        self.assertEqual(mock_reconcile.call_count, 1)

    @unittest.mock.patch("coordinate.assignments.reconcile_workspace")
    def test_reconcile_failure_does_not_block_request_assignment(self, mock_reconcile):
        mock_reconcile.side_effect = RuntimeError("checklist missing")
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        with self.assertLogs("coordinate.assignments", level="WARNING") as cm:
            result = request_assignment(
                conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
            )

        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "assignment.requested")
        self.assertIsNotNone(result.mutation)
        self.assertTrue(result.mutation.success)
        self.assertTrue(any("reconcile failed" in msg for msg in cm.output))
        self.assertTrue(any("demo" in msg for msg in cm.output))


if __name__ == "__main__":
    unittest.main()
