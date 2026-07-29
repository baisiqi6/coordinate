import json
import tempfile
import unittest
import unittest.mock
from datetime import datetime, timezone
from pathlib import Path

from coordinate.db import (
    append_event,
    get_workspace,
    initialize,
    list_events,
    row_to_dict,
    upsert_workspace,
)
from coordinate.harness import HarnessError, HarnessMutationResult
from coordinate.transitions import (
    AcceptTaskResult,
    BlockerTaskResult,
    CloseoutTaskResult,
    HandoffTaskResult,
    MarkDoneFilesResult,
    MarkDoneGateResult,
    MarkDoneRecordResult,
    MarkDoneTaskResult,
    ReviewResultTaskResult,
    UnblockTaskResult,
    accept_task,
    blocker_task,
    closeout_task,
    handoff_task,
    mark_done_files,
    mark_done_record,
    mark_done_task,
    review_result_task,
    unblock_task,
)


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

    def refresh_state(self):
        raise HarnessError("refresh_state not configured on _FakeAdapter")

    def read_state(self):
        # No harness-state.json available in test; gate will skip
        raise HarnessError("harness-state.json not found in test")

    def read_checklist(self):
        raise HarnessError("read_checklist not configured on _FakeAdapter")


class AcceptTaskTests(unittest.TestCase):
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

    # --- mutation args ---

    def test_success_passes_correct_args_to_run_mutation(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1",
            branch="agent/codex/mvp-001", adapter=adapter,
        )

        self.assertEqual(len(adapter.calls), 1)
        call = adapter.calls[0]
        self.assertEqual(call["operation"], "accept")
        self.assertEqual(call["task_id"], "mvp-001")
        self.assertEqual(call["actor"], "codex")
        self.assertEqual(
            call["args"],
            ["codex", "sess-1", "--branch", "agent/codex/mvp-001"],
        )

    def test_success_without_branch_omits_branch_arg(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        accept_task(conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter)

        self.assertEqual(adapter.calls[0]["args"], ["codex", "sess-1"])

    # --- success event ---

    def test_success_creates_assignment_accepted_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertIsInstance(result, AcceptTaskResult)
        self.assertIsNotNone(result.mutation)
        self.assertTrue(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "assignment.accepted")
        self.assertEqual(result.event["actor"], "codex")
        self.assertEqual(result.event["target"], "codex")
        self.assertEqual(result.event["task_id"], "mvp-001")
        self.assertEqual(result.event["workspace_id"], "demo")

    def test_success_default_actor_is_owner(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertEqual(result.event["actor"], "codex")

    def test_explicit_actor_overrides_default(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1",
            actor="operator", adapter=adapter,
        )

        self.assertEqual(result.event["actor"], "operator")
        self.assertEqual(adapter.calls[0]["actor"], "operator")

    def test_success_payload_contains_required_fields(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1",
            branch="agent/codex/mvp-001", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["task_id"], "mvp-001")
        self.assertEqual(payload["owner"], "codex")
        self.assertEqual(payload["session"], "sess-1")
        self.assertEqual(payload["branch"], "agent/codex/mvp-001")
        self.assertIn("mutation", payload)
        self.assertEqual(payload["mutation"]["operation"], "accept")
        self.assertTrue(payload["mutation"]["success"])

    # --- idempotency ---

    def test_idempotent_repeat_skips_mutation_and_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        first = accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        self.assertTrue(first.event_created)
        self.assertEqual(len(adapter.calls), 1)

        second = accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        self.assertFalse(second.event_created)
        self.assertIsNone(second.mutation)
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(first.event["id"], second.event["id"])

        events = list(list_events(conn, "demo"))
        self.assertEqual(len(events), 1)

    def test_default_idempotency_key_format(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertEqual(
            result.event["idempotency_key"],
            "demo:accept:mvp-001:codex:sess-1:assignment.accepted",
        )

    # --- failure path ---

    def test_failure_writes_mutation_failed_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = accept_task(
            conn, "demo", "mvp-999", "codex", "sess-1", adapter=adapter,
        )

        self.assertIsNotNone(result.mutation)
        self.assertFalse(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "harness.mutation_failed")
        self.assertEqual(result.event["task_id"], "mvp-999")
        self.assertEqual(result.event["actor"], "codex")

    def test_failure_payload_operation_is_accept(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = accept_task(
            conn, "demo", "mvp-999", "codex", "sess-1", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["operation"], "accept")
        self.assertEqual(payload["task_id"], "mvp-999")
        self.assertEqual(payload["owner"], "codex")
        self.assertEqual(payload["session"], "sess-1")
        self.assertEqual(payload["stderr"], "item not found")
        self.assertEqual(payload["exit_code"], 1)
        self.assertIn("mutation", payload)

    def test_failure_event_is_idempotent(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        first = accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        second = accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])
        self.assertEqual(len(list(list_events(conn, "demo"))), 1)

    def test_failure_idempotent_does_not_repeat_mutation_call(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        accept_task(conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter)
        accept_task(conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter)

        self.assertEqual(len(adapter.calls), 1)

    # --- adapter=None path ---

    def test_unknown_workspace_without_adapter_raises(self):
        conn = self._make_conn()

        with self.assertRaises(ValueError):
            accept_task(
                conn, "nonexistent", "mvp-001", "codex", "sess-1",
            )

    def test_missing_harnessctl_writes_mutation_failed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = initialize(":memory:")
            self.addCleanup(conn.close)
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=tmp,
                harness_root=tmp,
                harnessctl_path=missing_harnessctl,
            )

            result = accept_task(
                conn, "demo", "mvp-001", "codex", "sess-1",
            )

            self.assertIsNotNone(result.mutation)
            self.assertFalse(result.mutation.success)
            self.assertTrue(result.event_created)
            self.assertEqual(result.event["event_type"], "harness.mutation_failed")
            self.assertEqual(result.event["task_id"], "mvp-001")
            self.assertIn("not found", result.event["payload"]["stderr"])


class HandoffTaskTests(unittest.TestCase):
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

    # --- mutation args ---

    def test_success_passes_correct_args_to_run_mutation(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        handoff_task(
            conn, "demo", "mvp-001", "claude",
            reason="codex busy", adapter=adapter,
        )

        self.assertEqual(len(adapter.calls), 1)
        call = adapter.calls[0]
        self.assertEqual(call["operation"], "handoff")
        self.assertEqual(call["task_id"], "mvp-001")
        self.assertEqual(call["actor"], "operator")
        self.assertEqual(
            call["args"],
            ["claude", "--actor", "operator", "--reason", "codex busy"],
        )

    def test_success_without_reason_omits_reason_arg(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        handoff_task(conn, "demo", "mvp-001", "claude", adapter=adapter)

        self.assertEqual(
            adapter.calls[0]["args"],
            ["claude", "--actor", "operator"],
        )

    # --- success event ---

    def test_success_creates_handoff_requested_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = handoff_task(
            conn, "demo", "mvp-001", "claude", adapter=adapter,
        )

        self.assertIsInstance(result, HandoffTaskResult)
        self.assertIsNotNone(result.mutation)
        self.assertTrue(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "handoff.requested")
        self.assertEqual(result.event["actor"], "operator")
        self.assertEqual(result.event["target"], "claude")
        self.assertEqual(result.event["task_id"], "mvp-001")
        self.assertEqual(result.event["workspace_id"], "demo")

    def test_success_default_actor_is_operator(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = handoff_task(conn, "demo", "mvp-001", "claude", adapter=adapter)

        self.assertEqual(result.event["actor"], "operator")

    def test_explicit_actor_overrides_default(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = handoff_task(
            conn, "demo", "mvp-001", "claude",
            actor="codex", adapter=adapter,
        )

        self.assertEqual(result.event["actor"], "codex")
        self.assertEqual(adapter.calls[0]["actor"], "codex")
        self.assertEqual(adapter.calls[0]["args"], ["claude", "--actor", "codex"])

    def test_success_payload_contains_required_fields(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = handoff_task(
            conn, "demo", "mvp-001", "claude",
            reason="codex busy", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["task_id"], "mvp-001")
        self.assertEqual(payload["target"], "claude")
        self.assertEqual(payload["reason"], "codex busy")
        self.assertIn("mutation", payload)
        self.assertEqual(payload["mutation"]["operation"], "handoff")
        self.assertTrue(payload["mutation"]["success"])

    # --- idempotency ---

    def test_idempotent_repeat_skips_mutation_and_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        first = handoff_task(
            conn, "demo", "mvp-001", "claude", adapter=adapter,
        )
        self.assertTrue(first.event_created)
        self.assertEqual(len(adapter.calls), 1)

        second = handoff_task(
            conn, "demo", "mvp-001", "claude", adapter=adapter,
        )
        self.assertFalse(second.event_created)
        self.assertIsNone(second.mutation)
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(first.event["id"], second.event["id"])

        events = list(list_events(conn, "demo"))
        self.assertEqual(len(events), 1)

    def test_default_idempotency_key_format(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = handoff_task(
            conn, "demo", "mvp-001", "claude", adapter=adapter,
        )

        self.assertEqual(
            result.event["idempotency_key"],
            "demo:handoff:mvp-001:claude:operator:handoff.requested",
        )

    # --- failure path ---

    def test_failure_writes_mutation_failed_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = handoff_task(
            conn, "demo", "mvp-999", "claude", adapter=adapter,
        )

        self.assertIsNotNone(result.mutation)
        self.assertFalse(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "harness.mutation_failed")
        self.assertEqual(result.event["task_id"], "mvp-999")
        self.assertEqual(result.event["actor"], "operator")
        self.assertEqual(result.event["target"], "claude")

    def test_failure_payload_operation_is_handoff(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = handoff_task(
            conn, "demo", "mvp-999", "claude",
            reason="need help", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["operation"], "handoff")
        self.assertEqual(payload["task_id"], "mvp-999")
        self.assertEqual(payload["target"], "claude")
        self.assertEqual(payload["reason"], "need help")
        self.assertEqual(payload["stderr"], "item not found")
        self.assertEqual(payload["exit_code"], 1)
        self.assertIn("mutation", payload)

    def test_failure_event_is_idempotent(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        first = handoff_task(
            conn, "demo", "mvp-001", "claude", adapter=adapter,
        )
        second = handoff_task(
            conn, "demo", "mvp-001", "claude", adapter=adapter,
        )

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])
        self.assertEqual(len(list(list_events(conn, "demo"))), 1)

    def test_failure_idempotent_does_not_repeat_mutation_call(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        handoff_task(conn, "demo", "mvp-001", "claude", adapter=adapter)
        handoff_task(conn, "demo", "mvp-001", "claude", adapter=adapter)

        self.assertEqual(len(adapter.calls), 1)

    # --- adapter=None path ---

    def test_unknown_workspace_without_adapter_raises(self):
        conn = self._make_conn()

        with self.assertRaises(ValueError):
            handoff_task(
                conn, "nonexistent", "mvp-001", "claude",
            )

    def test_missing_harnessctl_writes_mutation_failed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = initialize(":memory:")
            self.addCleanup(conn.close)
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=tmp,
                harness_root=tmp,
                harnessctl_path=missing_harnessctl,
            )

            result = handoff_task(
                conn, "demo", "mvp-001", "claude",
            )

            self.assertIsNotNone(result.mutation)
            self.assertFalse(result.mutation.success)
            self.assertTrue(result.event_created)
            self.assertEqual(result.event["event_type"], "harness.mutation_failed")
            self.assertEqual(result.event["task_id"], "mvp-001")
            self.assertEqual(result.event["payload"]["operation"], "handoff")
            self.assertIn("not found", result.event["payload"]["stderr"])

    def test_invalid_workspace_path_writes_mutation_failed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = initialize(":memory:")
            self.addCleanup(conn.close)
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            missing_workspace = Path(tmp) / "missing-workspace"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=missing_workspace,
                harness_root=tmp,
                harnessctl_path=harnessctl_path,
            )

            result = handoff_task(
                conn, "demo", "mvp-001", "claude",
            )

            self.assertIsNotNone(result.mutation)
            self.assertFalse(result.mutation.success)
            self.assertTrue(result.event_created)
            self.assertEqual(result.event["event_type"], "harness.mutation_failed")
            self.assertEqual(result.event["payload"]["operation"], "handoff")
            self.assertIn("missing-workspace", result.event["payload"]["stderr"])


class BlockerTaskTests(unittest.TestCase):
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

    # --- mutation args ---

    def test_success_passes_correct_args_to_run_mutation(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        blocker_task(
            conn, "demo", "mvp-001",
            reason="stuck on dependency", adapter=adapter,
        )

        self.assertEqual(len(adapter.calls), 1)
        call = adapter.calls[0]
        self.assertEqual(call["operation"], "blocker")
        self.assertEqual(call["task_id"], "mvp-001")
        self.assertEqual(call["actor"], "operator")
        self.assertEqual(
            call["args"],
            ["--actor", "operator", "--reason", "stuck on dependency"],
        )

    def test_success_without_reason_omits_reason_arg(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        blocker_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertEqual(
            adapter.calls[0]["args"],
            ["--actor", "operator"],
        )

    # --- success event ---

    def test_success_creates_blocker_raised_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = blocker_task(
            conn, "demo", "mvp-001", adapter=adapter,
        )

        self.assertIsInstance(result, BlockerTaskResult)
        self.assertIsNotNone(result.mutation)
        self.assertTrue(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "blocker.raised")
        self.assertEqual(result.event["actor"], "operator")
        self.assertEqual(result.event["target"], "mvp-001")
        self.assertEqual(result.event["task_id"], "mvp-001")
        self.assertEqual(result.event["workspace_id"], "demo")

    def test_success_default_actor_is_operator(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = blocker_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertEqual(result.event["actor"], "operator")

    def test_explicit_actor_overrides_default(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = blocker_task(
            conn, "demo", "mvp-001",
            actor="codex", adapter=adapter,
        )

        self.assertEqual(result.event["actor"], "codex")
        self.assertEqual(adapter.calls[0]["actor"], "codex")
        self.assertEqual(adapter.calls[0]["args"], ["--actor", "codex"])

    def test_success_payload_contains_required_fields(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = blocker_task(
            conn, "demo", "mvp-001",
            reason="stuck on dependency", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["task_id"], "mvp-001")
        self.assertEqual(payload["reason"], "stuck on dependency")
        self.assertIn("mutation", payload)
        self.assertEqual(payload["mutation"]["operation"], "blocker")
        self.assertTrue(payload["mutation"]["success"])

    # --- idempotency ---

    def test_idempotent_repeat_skips_mutation_and_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        first = blocker_task(
            conn, "demo", "mvp-001", adapter=adapter,
        )
        self.assertTrue(first.event_created)
        self.assertEqual(len(adapter.calls), 1)

        second = blocker_task(
            conn, "demo", "mvp-001", adapter=adapter,
        )
        self.assertFalse(second.event_created)
        self.assertIsNone(second.mutation)
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(first.event["id"], second.event["id"])

        events = list(list_events(conn, "demo"))
        self.assertEqual(len(events), 1)

    def test_default_idempotency_key_format(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = blocker_task(
            conn, "demo", "mvp-001", adapter=adapter,
        )

        self.assertEqual(
            result.event["idempotency_key"],
            "demo:blocker:mvp-001:operator:blocker.raised",
        )

    # --- failure path ---

    def test_failure_writes_mutation_failed_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = blocker_task(
            conn, "demo", "mvp-999", adapter=adapter,
        )

        self.assertIsNotNone(result.mutation)
        self.assertFalse(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "harness.mutation_failed")
        self.assertEqual(result.event["task_id"], "mvp-999")
        self.assertEqual(result.event["actor"], "operator")

    def test_failure_payload_operation_is_blocker(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = blocker_task(
            conn, "demo", "mvp-999",
            reason="stuck", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["operation"], "blocker")
        self.assertEqual(payload["task_id"], "mvp-999")
        self.assertEqual(payload["reason"], "stuck")
        self.assertEqual(payload["stderr"], "item not found")
        self.assertEqual(payload["exit_code"], 1)
        self.assertIn("mutation", payload)

    def test_failure_event_is_idempotent(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        first = blocker_task(
            conn, "demo", "mvp-001", adapter=adapter,
        )
        second = blocker_task(
            conn, "demo", "mvp-001", adapter=adapter,
        )

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])
        self.assertEqual(len(list(list_events(conn, "demo"))), 1)

    def test_failure_idempotent_does_not_repeat_mutation_call(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        blocker_task(conn, "demo", "mvp-001", adapter=adapter)
        blocker_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertEqual(len(adapter.calls), 1)

    # --- adapter=None path ---

    def test_unknown_workspace_without_adapter_raises(self):
        conn = self._make_conn()

        with self.assertRaises(ValueError):
            blocker_task(
                conn, "nonexistent", "mvp-001",
            )

    def test_missing_harnessctl_writes_mutation_failed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = initialize(":memory:")
            self.addCleanup(conn.close)
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=tmp,
                harness_root=tmp,
                harnessctl_path=missing_harnessctl,
            )

            result = blocker_task(
                conn, "demo", "mvp-001",
            )

            self.assertIsNotNone(result.mutation)
            self.assertFalse(result.mutation.success)
            self.assertTrue(result.event_created)
            self.assertEqual(result.event["event_type"], "harness.mutation_failed")
            self.assertEqual(result.event["task_id"], "mvp-001")
            self.assertEqual(result.event["payload"]["operation"], "blocker")
            self.assertIn("not found", result.event["payload"]["stderr"])

    def test_invalid_workspace_path_writes_mutation_failed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = initialize(":memory:")
            self.addCleanup(conn.close)
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            missing_workspace = Path(tmp) / "missing-workspace"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=missing_workspace,
                harness_root=tmp,
                harnessctl_path=harnessctl_path,
            )

            result = blocker_task(
                conn, "demo", "mvp-001",
            )

            self.assertIsNotNone(result.mutation)
            self.assertFalse(result.mutation.success)
            self.assertTrue(result.event_created)
            self.assertEqual(result.event["event_type"], "harness.mutation_failed")
            self.assertEqual(result.event["payload"]["operation"], "blocker")
            self.assertIn("missing-workspace", result.event["payload"]["stderr"])


class UnblockTaskTests(unittest.TestCase):
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

    # --- mutation args ---

    def test_success_passes_correct_args_without_force_or_reason(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            adapter=adapter,
        )

        self.assertEqual(len(adapter.calls), 1)
        call = adapter.calls[0]
        self.assertEqual(call["operation"], "unblock")
        self.assertEqual(call["task_id"], "mvp-001")
        self.assertEqual(call["actor"], "codex")
        self.assertEqual(call["args"], ["codex", "--decision", "resolved"])

    def test_success_passes_force_flag(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            force=True, adapter=adapter,
        )

        self.assertEqual(
            adapter.calls[0]["args"],
            ["codex", "--decision", "resolved", "--force"],
        )

    def test_success_passes_reason(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            reason="dependency fixed", adapter=adapter,
        )

        self.assertEqual(
            adapter.calls[0]["args"],
            ["codex", "--decision", "resolved", "--reason", "dependency fixed"],
        )

    def test_success_passes_force_and_reason(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            force=True, reason="override", adapter=adapter,
        )

        self.assertEqual(
            adapter.calls[0]["args"],
            ["codex", "--decision", "resolved", "--force", "--reason", "override"],
        )

    # --- success event ---

    def test_success_creates_blocker_resolved_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            adapter=adapter,
        )

        self.assertIsInstance(result, UnblockTaskResult)
        self.assertIsNotNone(result.mutation)
        self.assertTrue(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "blocker.resolved")
        self.assertEqual(result.event["actor"], "codex")
        self.assertEqual(result.event["target"], "mvp-001")
        self.assertEqual(result.event["task_id"], "mvp-001")
        self.assertEqual(result.event["workspace_id"], "demo")

    def test_explicit_actor_appears_in_event_and_mutation(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            adapter=adapter,
        )

        self.assertEqual(result.event["actor"], "codex")
        self.assertEqual(adapter.calls[0]["actor"], "codex")

    def test_success_payload_contains_required_fields(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            force=True, reason="dependency fixed", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["task_id"], "mvp-001")
        self.assertEqual(payload["decision"], "resolved")
        self.assertTrue(payload["force"])
        self.assertEqual(payload["reason"], "dependency fixed")
        self.assertIn("mutation", payload)
        self.assertEqual(payload["mutation"]["operation"], "unblock")
        self.assertTrue(payload["mutation"]["success"])

    def test_success_payload_without_force_or_reason(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertFalse(payload["force"])
        self.assertIsNone(payload["reason"])

    # --- idempotency ---

    def test_idempotent_repeat_skips_mutation_and_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        first = unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            adapter=adapter,
        )
        self.assertTrue(first.event_created)
        self.assertEqual(len(adapter.calls), 1)

        second = unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            adapter=adapter,
        )
        self.assertFalse(second.event_created)
        self.assertIsNone(second.mutation)
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(first.event["id"], second.event["id"])

        events = list(list_events(conn, "demo"))
        self.assertEqual(len(events), 1)

    def test_default_idempotency_key_format(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            adapter=adapter,
        )

        self.assertEqual(
            result.event["idempotency_key"],
            "demo:unblock:mvp-001:codex:resolved:blocker.resolved",
        )

    # --- failure path ---

    def test_failure_writes_mutation_failed_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = unblock_task(
            conn, "demo", "mvp-999", "codex", "resolved",
            adapter=adapter,
        )

        self.assertIsNotNone(result.mutation)
        self.assertFalse(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "harness.mutation_failed")
        self.assertEqual(result.event["task_id"], "mvp-999")
        self.assertEqual(result.event["actor"], "codex")

    def test_failure_payload_operation_is_unblock(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = unblock_task(
            conn, "demo", "mvp-999", "codex", "resolved",
            force=True, reason="override", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["operation"], "unblock")
        self.assertEqual(payload["task_id"], "mvp-999")
        self.assertEqual(payload["decision"], "resolved")
        self.assertTrue(payload["force"])
        self.assertEqual(payload["reason"], "override")
        self.assertEqual(payload["stderr"], "item not found")
        self.assertEqual(payload["exit_code"], 1)
        self.assertIn("mutation", payload)

    def test_failure_event_is_idempotent(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        first = unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            adapter=adapter,
        )
        second = unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved",
            adapter=adapter,
        )

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])
        self.assertEqual(len(list(list_events(conn, "demo"))), 1)

    def test_failure_idempotent_does_not_repeat_mutation_call(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved", adapter=adapter,
        )
        unblock_task(
            conn, "demo", "mvp-001", "codex", "resolved", adapter=adapter,
        )

        self.assertEqual(len(adapter.calls), 1)

    # --- adapter=None path ---

    def test_unknown_workspace_without_adapter_raises(self):
        conn = self._make_conn()

        with self.assertRaises(ValueError):
            unblock_task(
                conn, "nonexistent", "mvp-001", "codex", "resolved",
            )

    def test_missing_harnessctl_writes_mutation_failed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = initialize(":memory:")
            self.addCleanup(conn.close)
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=tmp,
                harness_root=tmp,
                harnessctl_path=missing_harnessctl,
            )

            result = unblock_task(
                conn, "demo", "mvp-001", "codex", "resolved",
            )

            self.assertIsNotNone(result.mutation)
            self.assertFalse(result.mutation.success)
            self.assertTrue(result.event_created)
            self.assertEqual(result.event["event_type"], "harness.mutation_failed")
            self.assertEqual(result.event["task_id"], "mvp-001")
            self.assertEqual(result.event["payload"]["operation"], "unblock")
            self.assertIn("not found", result.event["payload"]["stderr"])

    def test_invalid_workspace_path_writes_mutation_failed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = initialize(":memory:")
            self.addCleanup(conn.close)
            harnessctl_path = Path(tmp) / "fake-harnessctl"
            missing_workspace = Path(tmp) / "missing-workspace"
            harnessctl_path.write_text("#!/bin/bash\necho 'ok'\nexit 0\n")
            harnessctl_path.chmod(0o755)
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=missing_workspace,
                harness_root=tmp,
                harnessctl_path=harnessctl_path,
            )

            result = unblock_task(
                conn, "demo", "mvp-001", "codex", "resolved",
            )

            self.assertIsNotNone(result.mutation)
            self.assertFalse(result.mutation.success)
            self.assertTrue(result.event_created)
            self.assertEqual(result.event["event_type"], "harness.mutation_failed")
            self.assertEqual(result.event["payload"]["operation"], "unblock")
            self.assertIn("missing-workspace", result.event["payload"]["stderr"])


class CloseoutTaskTests(unittest.TestCase):
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

    # --- mutation args ---

    def test_success_passes_self_test_evidence_to_run_mutation(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        closeout_task(
            conn, "demo", "mvp-001", "reviewer-a",
            self_test_evidence="Deploy SHA: abc123; E2E: passed; Bugs: none",
            adapter=adapter,
        )

        self.assertEqual(len(adapter.calls), 1)
        call = adapter.calls[0]
        self.assertEqual(
            call["args"],
            [
                "reviewer-a",
                "--self-test-evidence",
                "Deploy SHA: abc123; E2E: passed; Bugs: none",
            ],
        )

    def test_success_payload_contains_self_test_evidence(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a",
            self_test_evidence="Deploy SHA: abc123; E2E: passed; Bugs: none",
            adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(
            payload["self_test_evidence"],
            "Deploy SHA: abc123; E2E: passed; Bugs: none",
        )

    def test_success_payload_self_test_evidence_defaults_to_empty_string(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["self_test_evidence"], "")


    # --- success event ---

    def test_success_creates_closeout_requested_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a", adapter=adapter,
        )

        self.assertIsInstance(result, CloseoutTaskResult)
        self.assertIsNotNone(result.mutation)
        self.assertTrue(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "closeout.requested")
        self.assertEqual(result.event["actor"], "operator")
        self.assertEqual(result.event["target"], "reviewer-a")
        self.assertEqual(result.event["task_id"], "mvp-001")
        self.assertEqual(result.event["workspace_id"], "demo")

    def test_success_default_actor_is_operator(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a", adapter=adapter,
        )

        self.assertEqual(result.event["actor"], "operator")

    def test_explicit_actor_overrides_default(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a",
            actor="codex", adapter=adapter,
        )

        self.assertEqual(result.event["actor"], "codex")
        self.assertEqual(adapter.calls[0]["actor"], "codex")
        self.assertEqual(adapter.calls[0]["args"], ["reviewer-a"])

    def test_success_payload_contains_required_fields(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["task_id"], "mvp-001")
        self.assertEqual(payload["reviewer"], "reviewer-a")
        self.assertIn("mutation", payload)
        self.assertEqual(payload["mutation"]["operation"], "closeout")
        self.assertTrue(payload["mutation"]["success"])

    # --- idempotency ---

    def test_idempotent_repeat_skips_mutation_and_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        first = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a", adapter=adapter,
        )
        self.assertTrue(first.event_created)
        self.assertEqual(len(adapter.calls), 1)

        second = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a", adapter=adapter,
        )
        self.assertFalse(second.event_created)
        self.assertIsNone(second.mutation)
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(first.event["id"], second.event["id"])

        events = list(list_events(conn, "demo"))
        self.assertEqual(len(events), 1)

    def test_default_idempotency_key_format(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a", adapter=adapter,
        )

        self.assertEqual(
            result.event["idempotency_key"],
            "demo:closeout:mvp-001:reviewer-a:operator:closeout.requested",
        )

    # --- failure path ---

    def test_failure_writes_mutation_failed_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = closeout_task(
            conn, "demo", "mvp-999", "reviewer-a", adapter=adapter,
        )

        self.assertIsNotNone(result.mutation)
        self.assertFalse(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "harness.mutation_failed")
        self.assertEqual(result.event["task_id"], "mvp-999")
        self.assertEqual(result.event["actor"], "operator")

    def test_failure_payload_operation_is_closeout(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = closeout_task(
            conn, "demo", "mvp-999", "reviewer-a", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["operation"], "closeout")
        self.assertEqual(payload["task_id"], "mvp-999")
        self.assertEqual(payload["reviewer"], "reviewer-a")
        self.assertEqual(payload["stderr"], "item not found")
        self.assertEqual(payload["exit_code"], 1)
        self.assertIn("mutation", payload)

    def test_failure_event_is_idempotent(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        first = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a", adapter=adapter,
        )
        second = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a", adapter=adapter,
        )

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])
        self.assertEqual(len(list(list_events(conn, "demo"))), 1)

    def test_failure_idempotent_does_not_repeat_mutation_call(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        closeout_task(conn, "demo", "mvp-001", "reviewer-a", adapter=adapter)
        closeout_task(conn, "demo", "mvp-001", "reviewer-a", adapter=adapter)

        self.assertEqual(len(adapter.calls), 1)

    # --- adapter=None path ---

    def test_missing_harnessctl_writes_mutation_failed_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = initialize(":memory:")
            self.addCleanup(conn.close)
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=tmp,
                harness_root=tmp,
                harnessctl_path=missing_harnessctl,
            )

            result = closeout_task(
                conn, "demo", "mvp-001", "reviewer-a",
            )

            self.assertIsNotNone(result.mutation)
            self.assertFalse(result.mutation.success)
            self.assertTrue(result.event_created)
            self.assertEqual(result.event["event_type"], "harness.mutation_failed")
            self.assertEqual(result.event["task_id"], "mvp-001")
            self.assertEqual(result.event["payload"]["operation"], "closeout")
            self.assertIn("not found", result.event["payload"]["stderr"])


class ReviewResultTaskTests(unittest.TestCase):
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

    # --- mutation args ---

    def test_success_passes_correct_args_without_summary(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved", adapter=adapter,
        )

        self.assertEqual(len(adapter.calls), 1)
        call = adapter.calls[0]
        self.assertEqual(call["operation"], "review-result")
        self.assertEqual(call["task_id"], "mvp-001")
        self.assertEqual(call["actor"], "operator")
        self.assertEqual(call["args"], ["reviewer-a", "approved"])

    def test_success_passes_summary_arg(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved",
            summary="looks good", adapter=adapter,
        )

        self.assertEqual(len(adapter.calls), 1)
        call = adapter.calls[0]
        self.assertEqual(
            call["args"],
            ["reviewer-a", "approved", "--summary", "looks good"],
        )

    # --- success event ---

    def test_success_creates_review_completed_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved", adapter=adapter,
        )

        self.assertIsInstance(result, ReviewResultTaskResult)
        self.assertIsNotNone(result.mutation)
        self.assertTrue(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "review.completed")
        self.assertEqual(result.event["actor"], "operator")
        self.assertEqual(result.event["target"], "reviewer-a")
        self.assertEqual(result.event["task_id"], "mvp-001")
        self.assertEqual(result.event["workspace_id"], "demo")

    def test_success_default_actor_is_operator(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved", adapter=adapter,
        )

        self.assertEqual(result.event["actor"], "operator")

    def test_explicit_actor_overrides_default(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved",
            actor="codex", adapter=adapter,
        )

        self.assertEqual(result.event["actor"], "codex")
        self.assertEqual(adapter.calls[0]["actor"], "codex")

    def test_success_payload_contains_required_fields(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["task_id"], "mvp-001")
        self.assertEqual(payload["reviewer"], "reviewer-a")
        self.assertEqual(payload["decision"], "approved")
        self.assertIsNone(payload["summary"])
        self.assertIn("mutation", payload)
        self.assertEqual(payload["mutation"]["operation"], "review-result")
        self.assertTrue(payload["mutation"]["success"])

    # --- idempotency ---

    def test_idempotent_repeat_skips_mutation_and_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        first = review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved", adapter=adapter,
        )
        self.assertTrue(first.event_created)
        self.assertEqual(len(adapter.calls), 1)

        second = review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved", adapter=adapter,
        )
        self.assertFalse(second.event_created)
        self.assertIsNone(second.mutation)
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(first.event["id"], second.event["id"])

        events = list(list_events(conn, "demo"))
        self.assertEqual(len(events), 1)

    def test_default_idempotency_key_format(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved", adapter=adapter,
        )

        self.assertEqual(
            result.event["idempotency_key"],
            "demo:review-result:mvp-001:reviewer-a:approved:operator:review.completed",
        )

    # --- failure path ---

    def test_failure_writes_mutation_failed_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = review_result_task(
            conn, "demo", "mvp-999", "reviewer-a", "approved", adapter=adapter,
        )

        self.assertIsNotNone(result.mutation)
        self.assertFalse(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "harness.mutation_failed")
        self.assertEqual(result.event["task_id"], "mvp-999")
        self.assertEqual(result.event["actor"], "operator")

    def test_failure_payload_operation_is_review_result(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="item not found")

        result = review_result_task(
            conn, "demo", "mvp-999", "reviewer-a", "approved", adapter=adapter,
        )

        payload = result.event["payload"]
        self.assertEqual(payload["operation"], "review-result")
        self.assertEqual(payload["task_id"], "mvp-999")
        self.assertEqual(payload["reviewer"], "reviewer-a")
        self.assertEqual(payload["decision"], "approved")
        self.assertEqual(payload["stderr"], "item not found")
        self.assertEqual(payload["exit_code"], 1)
        self.assertIn("mutation", payload)

    def test_failure_event_is_idempotent(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        first = review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved", adapter=adapter,
        )
        second = review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved", adapter=adapter,
        )

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])
        self.assertEqual(len(list(list_events(conn, "demo"))), 1)

    def test_failure_idempotent_does_not_repeat_mutation_call(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved", adapter=adapter,
        )
        review_result_task(
            conn, "demo", "mvp-001", "reviewer-a", "approved", adapter=adapter,
        )

        self.assertEqual(len(adapter.calls), 1)


class MarkDoneTaskTests(unittest.TestCase):
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

    def _make_adapter(self, conn, task_id="mvp-001", **kwargs):
        workspace = get_workspace(conn, "demo")
        return _GateFakeAdapter(
            workspace,
            refresh_state_result={
                "current_item": {
                    "id": task_id,
                    "workflow": {"status": "review_approved"},
                    "status": "doing",
                }
            },
            **kwargs,
        )

    # --- mutation args ---

    def test_success_passes_correct_args(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        mark_done_task(conn, "demo", "mvp-001", actor="codex", adapter=adapter)

        self.assertEqual(len(adapter.calls), 1)
        call = adapter.calls[0]
        self.assertEqual(call["operation"], "mark-done")
        self.assertEqual(call["task_id"], "mvp-001")
        self.assertEqual(call["actor"], "codex")
        self.assertEqual(call["args"], ["codex"])

    # --- success event ---

    def test_success_creates_task_done_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertIsInstance(result, MarkDoneTaskResult)
        self.assertIsNotNone(result.mutation)
        self.assertTrue(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "task.done")
        self.assertEqual(result.event["actor"], "operator")
        self.assertEqual(result.event["target"], "mvp-001")
        self.assertEqual(result.event["task_id"], "mvp-001")
        self.assertEqual(result.event["workspace_id"], "demo")

    def test_success_default_actor_is_operator(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertEqual(result.event["actor"], "operator")

    def test_success_payload_contains_required_fields(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = mark_done_task(conn, "demo", "mvp-001", actor="codex", adapter=adapter)

        payload = result.event["payload"]
        self.assertEqual(payload["task_id"], "mvp-001")
        self.assertIn("mutation", payload)
        self.assertEqual(payload["mutation"]["operation"], "mark-done")
        self.assertTrue(payload["mutation"]["success"])

    # --- idempotency ---

    def test_idempotent_repeat_skips_mutation_and_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        first = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)
        self.assertTrue(first.event_created)
        self.assertEqual(len(adapter.calls), 1)

        second = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)
        self.assertFalse(second.event_created)
        self.assertIsNone(second.mutation)
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(first.event["id"], second.event["id"])

        events = list(list_events(conn, "demo"))
        self.assertEqual(len(events), 1)

    def test_default_idempotency_key_format(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = mark_done_task(conn, "demo", "mvp-001", actor="codex", adapter=adapter)

        self.assertEqual(
            result.event["idempotency_key"],
            "demo:mark-done:mvp-001:codex:task.done",
        )

    # --- failure path ---

    def test_failure_writes_mutation_failed_event(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, task_id="mvp-999", success=False, stderr="item not found")

        result = mark_done_task(conn, "demo", "mvp-999", adapter=adapter)

        self.assertIsNotNone(result.mutation)
        self.assertFalse(result.mutation.success)
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "harness.mutation_failed")
        self.assertEqual(result.event["task_id"], "mvp-999")
        self.assertEqual(result.event["actor"], "operator")

    def test_failure_payload_operation_is_mark_done(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, task_id="mvp-999", success=False, stderr="item not found")

        result = mark_done_task(conn, "demo", "mvp-999", adapter=adapter)

        payload = result.event["payload"]
        self.assertEqual(payload["operation"], "mark-done")
        self.assertEqual(payload["task_id"], "mvp-999")
        self.assertEqual(payload["stderr"], "item not found")
        self.assertEqual(payload["exit_code"], 1)
        self.assertIn("mutation", payload)

    def test_failure_event_is_idempotent(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        first = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)
        second = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])
        self.assertEqual(len(list(list_events(conn, "demo"))), 1)

    def test_failure_idempotent_does_not_repeat_mutation_call(self):
        conn = self._make_conn()
        adapter = self._make_adapter(conn, success=False, stderr="error")

        mark_done_task(conn, "demo", "mvp-001", adapter=adapter)
        mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertEqual(len(adapter.calls), 1)

    # --- adapter=None path ---

    def test_missing_harnessctl_gate_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = initialize(":memory:")
            self.addCleanup(conn.close)
            missing_harnessctl = str(Path(tmp) / "missing-harnessctl")
            upsert_workspace(
                conn,
                workspace_id="demo",
                name="Demo",
                path=tmp,
                harness_root=tmp,
                harnessctl_path=missing_harnessctl,
            )

            result = mark_done_task(conn, "demo", "mvp-001")

            # Fail-closed: missing harnessctl → gate fails, no mutation attempted
            self.assertIsNone(result.mutation)
            self.assertFalse(result.event_created)
            self.assertEqual(result.event, {})
            self.assertIsNotNone(result.gate)
            self.assertFalse(result.gate.passed)
            self.assertIn("harness state unavailable", result.gate.reason)


class _GateFakeAdapter:
    """Fake adapter for gate tests. Supports run_mutation, refresh_state, and read_checklist."""

    def __init__(
        self,
        workspace,
        *,
        success=True,
        stdout="ok",
        stderr="",
        refresh_state_result=None,
        refresh_state_error=None,
        checklist_result=None,
        checklist_error=None,
    ):
        self.workspace = workspace
        self._success = success
        self._stdout = stdout
        self._stderr = stderr
        self._refresh_state_result = refresh_state_result
        self._refresh_state_error = refresh_state_error
        self._checklist_result = checklist_result
        self._checklist_error = checklist_error
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

    def refresh_state(self):
        if self._refresh_state_error is not None:
            raise self._refresh_state_error
        return self._refresh_state_result or {}

    def read_state(self):
        return self.refresh_state()

    def read_checklist(self):
        if self._checklist_error is not None:
            raise self._checklist_error
        return self._checklist_result or {"items": []}


class MarkDoneGateTests(unittest.TestCase):
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

    def _make_gate_adapter(self, conn, **kwargs):
        workspace = get_workspace(conn, "demo")
        return _GateFakeAdapter(workspace, **kwargs)

    # --- 1. Gate passed (current_item matches, workflow status in allowed set) ---

    def test_gate_passed_current_item_review_approved(self):
        conn = self._make_conn()
        adapter = self._make_gate_adapter(
            conn,
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "review_approved"},
                    "status": "doing",
                }
            },
        )

        result = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "task.done")
        self.assertIsNone(result.gate)
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(adapter.calls[0]["operation"], "mark-done")

    # --- 2. Gate passed (checklist fallback) ---

    def test_gate_passed_checklist_fallback_closed(self):
        conn = self._make_conn()
        adapter = self._make_gate_adapter(
            conn,
            refresh_state_result={
                "current_item": {
                    "id": "mvp-002",
                    "workflow": {"status": "running"},
                    "status": "doing",
                }
            },
            checklist_result={
                "items": [
                    {
                        "id": "mvp-001",
                        "workflow": {"status": "closed"},
                        "status": "doing",
                    }
                ]
            },
        )

        result = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "task.done")
        self.assertIsNone(result.gate)
        self.assertEqual(len(adapter.calls), 1)

    # --- 3. Gate failed (wrong status) ---

    def test_gate_failed_wrong_status_running(self):
        conn = self._make_conn()
        adapter = self._make_gate_adapter(
            conn,
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "running"},
                    "status": "doing",
                }
            },
        )

        result = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertFalse(result.event_created)
        self.assertEqual(result.event, {})
        self.assertIsNotNone(result.gate)
        self.assertFalse(result.gate.passed)
        self.assertIn("running", result.gate.reason)
        self.assertEqual(result.gate.task_status, "running")
        # harnessctl mark-done should NOT have been called
        self.assertEqual(len(adapter.calls), 0)

    # --- 4. Gate failed (task not found) ---

    def test_gate_failed_task_not_found(self):
        conn = self._make_conn()
        adapter = self._make_gate_adapter(
            conn,
            refresh_state_result={"current_item": None},
            checklist_result={"items": []},
        )

        result = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertFalse(result.event_created)
        self.assertIsNotNone(result.gate)
        self.assertFalse(result.gate.passed)
        self.assertIn("task not found", result.gate.reason)
        self.assertEqual(len(adapter.calls), 0)

    # --- 5. Gate passes when harness state unavailable (graceful skip) ---

    def test_gate_fails_when_harness_state_unavailable(self):
        conn = self._make_conn()
        adapter = self._make_gate_adapter(
            conn,
            refresh_state_error=HarnessError("harnessctl state failed: file missing"),
        )

        result = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        # Fail-closed: harness state unavailable → gate failure
        self.assertFalse(result.event_created)
        self.assertEqual(result.event, {})
        self.assertIsNotNone(result.gate)
        self.assertFalse(result.gate.passed)
        self.assertIn("harness state unavailable", result.gate.reason)
        self.assertEqual(len(adapter.calls), 0)

    # --- 6. Gate fails when checklist unavailable (fail-closed) ---

    def test_gate_fails_when_checklist_unavailable(self):
        conn = self._make_conn()
        adapter = self._make_gate_adapter(
            conn,
            refresh_state_result={"current_item": None},
            checklist_error=HarnessError("mvp-checklist.json not found"),
        )

        result = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        # Fail-closed: checklist unavailable → gate failure
        self.assertFalse(result.event_created)
        self.assertEqual(result.event, {})
        self.assertIsNotNone(result.gate)
        self.assertFalse(result.gate.passed)
        self.assertIn("checklist unavailable", result.gate.reason)
        self.assertEqual(len(adapter.calls), 0)

    # --- 7. Idempotent retry skips gate (success event) ---

    def test_idempotent_retry_skips_gate_success_event(self):
        conn = self._make_conn()
        # First call: adapter that passes gate and runs mutation
        adapter_pass = self._make_gate_adapter(
            conn,
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "review_approved"},
                    "status": "doing",
                }
            },
        )
        first = mark_done_task(conn, "demo", "mvp-001", adapter=adapter_pass)
        self.assertTrue(first.event_created)
        self.assertEqual(len(adapter_pass.calls), 1)

        # Second call with a different adapter whose refresh_state would fail
        # This proves the gate is NOT checked on idempotent retry
        adapter_would_fail = self._make_gate_adapter(
            conn,
            refresh_state_error=HarnessError("should not be called"),
        )
        second = mark_done_task(conn, "demo", "mvp-001", adapter=adapter_would_fail)

        self.assertFalse(second.event_created)
        self.assertIsNone(second.gate)
        self.assertEqual(first.event["id"], second.event["id"])
        # refresh_state should not have been called since idempotent path returns early
        self.assertEqual(len(adapter_would_fail.calls), 0)

    # --- 8. Idempotent retry (failed event) skips gate ---

    def test_idempotent_retry_skips_gate_failed_event(self):
        conn = self._make_conn()
        # First call: mutation fails
        adapter_fail = self._make_gate_adapter(
            conn,
            success=False,
            stderr="error",
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "review_approved"},
                    "status": "doing",
                }
            },
        )
        first = mark_done_task(conn, "demo", "mvp-001", adapter=adapter_fail)
        self.assertTrue(first.event_created)
        self.assertEqual(first.event["event_type"], "harness.mutation_failed")

        # Second call with adapter whose refresh_state would fail
        adapter_would_fail = self._make_gate_adapter(
            conn,
            refresh_state_error=HarnessError("should not be called"),
        )
        second = mark_done_task(conn, "demo", "mvp-001", adapter=adapter_would_fail)

        self.assertFalse(second.event_created)
        self.assertIsNone(second.gate)
        self.assertEqual(first.event["id"], second.event["id"])
        self.assertEqual(len(adapter_would_fail.calls), 0)

    # --- 9. Coarse status "done" passes gate ---

    def test_gate_passed_coarse_status_done(self):
        conn = self._make_conn()
        adapter = self._make_gate_adapter(
            conn,
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "closed"},
                    "status": "done",
                }
            },
        )

        result = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "task.done")
        self.assertIsNone(result.gate)
        self.assertEqual(len(adapter.calls), 1)

    # --- 10. Coarse status "done" with workflow "todo" is REJECTED (trust workflow) ---

    def test_gate_rejected_coarse_done_workflow_todo(self):
        conn = self._make_conn()
        adapter = self._make_gate_adapter(
            conn,
            refresh_state_result={
                "current_item": {
                    "id": "mvp-001",
                    "workflow": {"status": "todo"},
                    "status": "done",
                }
            },
        )

        result = mark_done_task(conn, "demo", "mvp-001", adapter=adapter)

        self.assertFalse(result.event_created)
        self.assertIsNotNone(result.gate)
        self.assertEqual(result.gate.task_status, "todo")


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

    # --- reconcile called on fresh success ---

    @unittest.mock.patch("coordinate.transitions.reconcile_workspace")
    def test_accept_task_calls_reconcile_on_fresh_success(self, mock_reconcile):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )

        self.assertTrue(result.event_created)
        mock_reconcile.assert_called_once()
        call_args = mock_reconcile.call_args
        self.assertEqual(call_args[0][1].id, "demo")
        self.assertTrue(call_args[1]["refresh"])

    @unittest.mock.patch("coordinate.transitions.reconcile_workspace")
    def test_closeout_task_calls_reconcile_on_fresh_success(self, mock_reconcile):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        result = closeout_task(
            conn, "demo", "mvp-001", "reviewer-a", adapter=adapter,
        )

        self.assertTrue(result.event_created)
        mock_reconcile.assert_called_once()
        call_args = mock_reconcile.call_args
        self.assertEqual(call_args[0][1].id, "demo")
        self.assertTrue(call_args[1]["refresh"])

    # --- reconcile NOT called on idempotent retry ---

    @unittest.mock.patch("coordinate.transitions.reconcile_workspace")
    def test_accept_task_skips_reconcile_on_idempotent_retry(self, mock_reconcile):
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        accept_task(conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter)
        self.assertEqual(mock_reconcile.call_count, 1)

        second = accept_task(
            conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
        )
        self.assertFalse(second.event_created)
        # Should still be 1 — not called again for idempotent retry
        self.assertEqual(mock_reconcile.call_count, 1)

    # --- reconcile failure doesn't affect the result ---

    @unittest.mock.patch("coordinate.transitions.reconcile_workspace")
    def test_reconcile_failure_does_not_block_accept_result(self, mock_reconcile):
        mock_reconcile.side_effect = RuntimeError("checklist missing")
        conn = self._make_conn()
        adapter = self._make_adapter(conn)

        with self.assertLogs("coordinate.transitions", level="WARNING") as cm:
            result = accept_task(
                conn, "demo", "mvp-001", "codex", "sess-1", adapter=adapter,
            )

        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "assignment.accepted")
        self.assertIsNotNone(result.mutation)
        self.assertTrue(result.mutation.success)
        self.assertTrue(any("reconcile failed" in msg for msg in cm.output))
        self.assertTrue(any("demo" in msg for msg in cm.output))


class MarkDoneFilesTests(unittest.TestCase):
    """mark-done-files: canonical local write, service-layer authorized."""

    def _make_checklist_dir(self, items=None):
        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        checklist = {"updated_at": "2026-01-01", "items": items or []}
        checklist_path = Path(tmp) / "mvp-checklist.json"
        checklist_path.write_text(
            json.dumps(checklist, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return tmp

    def _make_item(self, task_id="mvp-001", status="todo", workflow_status="todo"):
        return {
            "id": task_id,
            "title": f"Task {task_id}",
            "status": status,
            "priority": "p1",
            "owner": None,
            "verification": "",
            "updated_at": "2026-01-01T00:00:00Z",
            "workflow": {"status": workflow_status, "branch": None,
                         "updated_at": "2026-01-01T00:00:00Z"},
        }

    # --- Service-layer authorization (P1-7) ---

    def test_requires_receipt_or_repair_reason(self):
        tmp = self._make_checklist_dir(items=[self._make_item()])
        with self.assertRaises(ValueError) as ctx:
            mark_done_files(workspace_path=tmp, harness_root=tmp, task_id="mvp-001")
        self.assertIn("receipt", str(ctx.exception).lower())
        item = json.loads(Path(tmp, "mvp-checklist.json").read_text())["items"][0]
        self.assertEqual(item["status"], "todo")

    def test_repair_path_mutates_and_stamps_repair_only(self):
        tmp = self._make_checklist_dir(items=[self._make_item()])
        result = mark_done_files(
            workspace_path=tmp, harness_root=tmp, task_id="mvp-001",
            repair_reason="drift fix",
        )
        self.assertTrue(result.checklist_changed)
        self.assertTrue(result.repair_only)
        self.assertEqual(result.repair_reason, "drift fix")
        self.assertIsNone(result.receipt_id)

    # --- Normal path writes structured metadata (P1-6) ---

    def test_normal_path_writes_completion_receipt_metadata(self):
        from coordinate.completion import ReceiptEvidence, compute_mark_done_fingerprints
        tmp = self._make_checklist_dir(items=[self._make_item()])
        fps = compute_mark_done_fingerprints(harness_root=tmp, task_id="mvp-001")
        evidence = ReceiptEvidence("rec-1", fps.before_fingerprint, fps.after_fingerprint)
        result = mark_done_files(
            workspace_path=tmp, harness_root=tmp, task_id="mvp-001", receipt=evidence,
        )
        self.assertTrue(result.checklist_changed)
        self.assertEqual(result.receipt_id, "rec-1")
        self.assertEqual(result.after_fingerprint, fps.after_fingerprint)
        item = json.loads(Path(tmp, "mvp-checklist.json").read_text())["items"][0]
        self.assertEqual(item["completion_receipt"]["receipt_id"], "rec-1")
        self.assertEqual(item["completion_receipt"]["after_fingerprint"],
                         fps.after_fingerprint)
        self.assertIn("applied_at", item["completion_receipt"])

    def test_normal_path_idempotent_retry_validates_metadata(self):
        from coordinate.completion import ReceiptEvidence, compute_mark_done_fingerprints
        tmp = self._make_checklist_dir(items=[self._make_item()])
        fps = compute_mark_done_fingerprints(harness_root=tmp, task_id="mvp-001")
        evidence = ReceiptEvidence("rec-1", fps.before_fingerprint, fps.after_fingerprint)
        first = mark_done_files(workspace_path=tmp, harness_root=tmp,
                                task_id="mvp-001", receipt=evidence)
        self.assertTrue(first.checklist_changed)
        second = mark_done_files(workspace_path=tmp, harness_root=tmp,
                                 task_id="mvp-001", receipt=evidence)
        self.assertFalse(second.checklist_changed)

    def test_normal_path_rejects_cross_receipt_on_done_item(self):
        from coordinate.completion import ReceiptEvidence, compute_mark_done_fingerprints
        tmp = self._make_checklist_dir(items=[self._make_item()])
        fps = compute_mark_done_fingerprints(harness_root=tmp, task_id="mvp-001")
        mark_done_files(workspace_path=tmp, harness_root=tmp, task_id="mvp-001",
                        receipt=ReceiptEvidence("rec-1", fps.before_fingerprint,
                                                fps.after_fingerprint))
        with self.assertRaises(ValueError):
            mark_done_files(workspace_path=tmp, harness_root=tmp, task_id="mvp-001",
                            receipt=ReceiptEvidence("rec-OTHER",
                                                    fps.before_fingerprint,
                                                    fps.after_fingerprint))

    def test_normal_path_rejects_before_fingerprint_drift_before_write(self):
        """TOCTOU: if the on-disk item drifts between reserve and apply so that
        its current lifecycle fingerprint != receipt.before_fingerprint, the
        fresh path must reject BEFORE any item/checklist mutation. The file
        must be byte-identical to the drifted state and carry no
        completion_receipt metadata."""
        from coordinate.completion import ReceiptEvidence, compute_mark_done_fingerprints
        tmp = self._make_checklist_dir(items=[self._make_item()])
        fps = compute_mark_done_fingerprints(harness_root=tmp, task_id="mvp-001")
        # Drift the on-disk item's branch after the receipt was reserved.
        drifted = json.loads(Path(tmp, "mvp-checklist.json").read_text())
        drifted["items"][0]["workflow"]["branch"] = "feat-b"
        drifted_text = json.dumps(drifted, ensure_ascii=False, indent=2) + "\n"
        Path(tmp, "mvp-checklist.json").write_text(drifted_text, encoding="utf-8")

        evidence = ReceiptEvidence("rec-1", fps.before_fingerprint, fps.after_fingerprint)
        with self.assertRaises(ValueError) as ctx:
            mark_done_files(workspace_path=tmp, harness_root=tmp, task_id="mvp-001",
                            receipt=evidence)
        self.assertIn("before-fingerprint", str(ctx.exception).lower())
        # Zero mutation: file bytes unchanged, no completion_receipt metadata,
        # task still in its pre-done lifecycle state.
        self.assertEqual(Path(tmp, "mvp-checklist.json").read_text(), drifted_text)
        item = json.loads(Path(tmp, "mvp-checklist.json").read_text())["items"][0]
        self.assertNotIn("completion_receipt", item)
        self.assertNotEqual(item.get("status"), "done")

    def test_normal_path_idempotent_rejects_after_fingerprint_drift(self):
        """TOCTOU on the idempotent already-done path: even if the on-disk
        completion_receipt metadata appears to match the receipt, the actual
        recomputed lifecycle fingerprint must equal receipt.after_fingerprint.
        On mismatch (e.g. the done item's branch drifted) reject and leave the
        file byte-identical."""
        from coordinate.completion import ReceiptEvidence, compute_mark_done_fingerprints
        tmp = self._make_checklist_dir(items=[self._make_item()])
        fps = compute_mark_done_fingerprints(harness_root=tmp, task_id="mvp-001")
        # Legitimately complete under rec-1 (writes done/closed + metadata).
        mark_done_files(workspace_path=tmp, harness_root=tmp, task_id="mvp-001",
                        receipt=ReceiptEvidence("rec-1", fps.before_fingerprint,
                                                fps.after_fingerprint))
        # Drift the done item's branch, leaving metadata intact.
        drifted = json.loads(Path(tmp, "mvp-checklist.json").read_text())
        drifted["items"][0]["workflow"]["branch"] = "feat-b"
        drifted_text = json.dumps(drifted, ensure_ascii=False, indent=2) + "\n"
        Path(tmp, "mvp-checklist.json").write_text(drifted_text, encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            mark_done_files(workspace_path=tmp, harness_root=tmp, task_id="mvp-001",
                            receipt=ReceiptEvidence("rec-1", fps.before_fingerprint,
                                                    fps.after_fingerprint))
        self.assertIn("after-fingerprint", str(ctx.exception).lower())
        # File unchanged from drifted state.
        self.assertEqual(Path(tmp, "mvp-checklist.json").read_text(), drifted_text)

    # --- /opt guard (applies on both paths) ---

    def test_opt_path_refused_without_allow_flag(self):
        with self.assertRaises(ValueError) as ctx:
            mark_done_files(
                workspace_path="/opt/multinexus",
                harness_root="/opt/multinexus/docs/project-harness",
                task_id="mvp-001", repair_reason="x",
            )
        self.assertIn("/opt/", str(ctx.exception))

    def test_opt_path_allowed_with_flag(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        opt_dir = Path(tmp) / "opt" / "multinexus" / "docs" / "project-harness"
        opt_dir.mkdir(parents=True)
        (opt_dir / "mvp-checklist.json").write_text(
            json.dumps({"updated_at": "2026-01-01",
                        "items": [self._make_item()]}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        result = mark_done_files(
            workspace_path=str(Path(tmp) / "opt" / "multinexus"),
            harness_root=str(opt_dir), task_id="mvp-001",
            allow_runtime_copy=True, repair_reason="x",
        )
        self.assertTrue(result.checklist_changed)

    # --- Error paths ---

    def test_task_not_in_checklist_raises_error(self):
        tmp = self._make_checklist_dir(items=[self._make_item(task_id="other")])
        with self.assertRaises(ValueError) as ctx:
            mark_done_files(workspace_path=tmp, harness_root=tmp,
                            task_id="mvp-001", repair_reason="x")
        self.assertIn("mvp-001", str(ctx.exception))
        self.assertIn("not found", str(ctx.exception))

    def test_missing_checklist_file_raises_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError) as ctx:
                mark_done_files(workspace_path=tmp, harness_root=tmp,
                                task_id="mvp-001", repair_reason="x")
            self.assertIn("not found", str(ctx.exception))

    def test_repair_idempotent_already_done_closed_noop(self):
        item = self._make_item(status="done", workflow_status="closed")
        item["verification"] = "already reconciled"
        tmp = self._make_checklist_dir(items=[item])
        result = mark_done_files(
            workspace_path=tmp, harness_root=tmp, task_id="mvp-001",
            repair_reason="drift",
        )
        self.assertFalse(result.checklist_changed)
        self.assertEqual(result.verification, "already reconciled")


class MarkDoneRecordTests(unittest.TestCase):
    """mark-done-record is repair-only at the service layer (P1-7)."""

    def _make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        upsert_workspace(
            conn, workspace_id="demo", name="Demo",
            path="/tmp/demo", harness_root="/tmp/demo/harness",
        )
        return conn

    def test_requires_repair_reason(self):
        conn = self._make_conn()
        with self.assertRaises(ValueError) as ctx:
            mark_done_record(conn, workspace_id="demo", task_id="mvp-001")
        self.assertIn("repair_reason", str(ctx.exception))

    def test_new_task_creates_task_done_event_stamped_repair(self):
        conn = self._make_conn()
        result = mark_done_record(
            conn, workspace_id="demo", task_id="mvp-001",
            actor="operator", repair_reason="drift reconciliation",
        )
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "task.done")
        payload = result.event["payload"]
        self.assertTrue(payload["repair_only"])
        self.assertEqual(payload["repair_reason"], "drift reconciliation")

    def test_payload_includes_verification_when_provided(self):
        conn = self._make_conn()
        result = mark_done_record(
            conn, workspace_id="demo", task_id="mvp-001",
            verification="reconciled drift", repair_reason="drift",
        )
        self.assertEqual(result.event["payload"]["verification"], "reconciled drift")

    def test_wide_match_idempotent_prior_done_by_different_actor(self):
        conn = self._make_conn()
        first = mark_done_record(
            conn, workspace_id="demo", task_id="mvp-001",
            actor="omp", repair_reason="drift",
        )
        self.assertTrue(first.event_created)
        second = mark_done_record(
            conn, workspace_id="demo", task_id="mvp-001",
            actor="operator", repair_reason="drift",
        )
        self.assertFalse(second.event_created)
        self.assertEqual(second.event["id"], first.event["id"])
        task_done = [row_to_dict(e) for e in list_events(conn, "demo")
                     if row_to_dict(e)["event_type"] == "task.done"]
        self.assertEqual(len(task_done), 1)

    def test_wide_match_ignores_idempotency_hint(self):
        conn = self._make_conn()
        first = mark_done_record(
            conn, workspace_id="demo", task_id="mvp-001", actor="omp",
            idempotency_hint="custom-hint-1", repair_reason="drift",
        )
        self.assertTrue(first.event_created)
        second = mark_done_record(
            conn, workspace_id="demo", task_id="mvp-001", actor="operator",
            idempotency_hint="different-hint", repair_reason="drift",
        )
        self.assertFalse(second.event_created)
        self.assertEqual(second.event["id"], first.event["id"])

    def test_unknown_workspace_raises_error(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        with self.assertRaises(ValueError) as ctx:
            mark_done_record(
                conn, workspace_id="nonexistent", task_id="mvp-001",
                repair_reason="drift",
            )
        self.assertIn("nonexistent", str(ctx.exception))

    def test_local_checklist_files_zero_change(self):
        import inspect
        params = list(inspect.signature(mark_done_record).parameters.keys())
        self.assertNotIn("harness_root", params)
        self.assertNotIn("workspace_path", params)


if __name__ == "__main__":
    unittest.main()
