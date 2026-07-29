import json
import tempfile
import unittest
from pathlib import Path

from coordinate.db import (
    append_event,
    get_workspace,
    initialize,
    set_workspace_agent as _set_workspace_agent,
    upsert_workspace_host_profile,
    upsert_task_mirror,
    upsert_workspace,
)
from coordinate.handoff import (
    _ReviewerBootstrapContext,
    _WorkerBootstrapContext,
    _build_reviewer_bootstrap,
    _build_worker_bootstrap,
    _render_reviewer_assignment,
    _render_reviewer_constraints_block,
    _render_reviewer_focus,
    _render_reviewer_output_format,
    _render_reviewer_self_test,
    _render_reviewer_session_startup,
    _render_worker_assignment,
    _render_worker_constraints,
    _render_worker_coordinator_cli,
    _render_worker_implementation_protocol,
    _render_worker_self_test,
    _render_worker_session_end,
    _render_worker_session_startup,
    _render_worker_visible_discord,
    _require_harness_task,
    _resolve_reviewer_bootstrap_context,
    prepare_handoff,
)
from coordinate.issues import materialize_issue, triage_issue
from coordinate.runtime import register_agent


def set_workspace_agent(conn, **kwargs):
    """Create an explicit fixture override without leaking setup audit events."""
    result = _set_workspace_agent(
        conn, actor="test-fixture", reason="handoff test fixture", **kwargs
    )
    conn.execute("DELETE FROM events WHERE event_type = 'workspace.agent_override.set'")
    conn.commit()
    return result


class HandoffServiceTests(unittest.TestCase):
    def _make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        return conn

    def _setup_workspace_with_approved_plan(
        self,
        conn,
        task_id="t1",
        scope="implementation plan",
        plan_payload=None,
    ):
        plan_payload = plan_payload or {
            "task_id": task_id,
            "title": "Test Phase",
            "plan_doc": "docs/plan.md",
        }
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        workspace_path = Path(tmp.name)
        harness_root = workspace_path / "docs" / "project-harness"
        harness_root.mkdir(parents=True)
        (harness_root / "harness-state.json").write_text(
            json.dumps(
                {
                    "project": "demo",
                    "current_item": {"id": task_id, "status": "ready"},
                    "items": [{"id": task_id, "status": "ready"}],
                }
            ),
            encoding="utf-8",
        )
        (harness_root / "mvp-checklist.json").write_text(
            json.dumps(
                {
                    "project": "demo",
                    "items": [{"id": task_id, "status": "pending"}],
                }
            ),
            encoding="utf-8",
        )
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=str(workspace_path),
            harness_root=str(harness_root),
            base_branch="main",
        )
        plan_ready = append_event(
            conn,
            workspace_id="demo",
            event_type="plan.ready",
            actor="operator",
            target="worker",
            task_id=task_id,
            idempotency_key=f"demo:{task_id}:plan.ready",
            payload=plan_payload,
        )
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id=task_id,
            phase="ready",
            owner=None,
            branch="agents/worker/t1",
            pr=None,
            payload={"task_id": task_id, "title": plan_payload.get("title", "Test Phase")},
        )
        from coordinate.db import row_to_dict
        plan_ready_id = row_to_dict(plan_ready.row)["id"]
        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.approved",
            actor="operator",
            target="worker",
            task_id=task_id,
            idempotency_key=f"demo:{task_id}:plan.approved:{scope}",
            payload={
                "task_id": task_id,
                "decision": "approved",
                "scope": scope,
                "plan_ready_event_id": plan_ready_id,
            },
        )
        return workspace_path, harness_root

    # --- prepare_handoff success ---

    def test_prepare_handoff_creates_event_with_handoff_text(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "worker.handoff.prepared")
        self.assertIn("Workspace:", result.handoff_text)
        self.assertIn("agents/worker/t1", result.handoff_text)

    def test_prepare_handoff_fails_when_task_missing_from_checklist(self):
        conn = self._make_conn()
        _, harness_root = self._setup_workspace_with_approved_plan(conn)
        (harness_root / "mvp-checklist.json").write_text(
            json.dumps({"project": "demo", "items": [{"id": "other-task"}]}),
            encoding="utf-8",
        )

        with self.assertRaises(ValueError) as ctx:
            prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("checklist does not contain task 't1'", str(ctx.exception))
        events = conn.execute(
            "SELECT * FROM events WHERE event_type = 'worker.handoff.prepared'"
        ).fetchall()
        self.assertEqual(events, [])

    def test_prepare_handoff_fails_when_harness_state_missing(self):
        conn = self._make_conn()
        _, harness_root = self._setup_workspace_with_approved_plan(conn)
        (harness_root / "harness-state.json").unlink()

        with self.assertRaises(ValueError) as ctx:
            prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("workspace harness preflight failed", str(ctx.exception))
        events = conn.execute(
            "SELECT * FROM events WHERE event_type = 'worker.handoff.prepared'"
        ).fetchall()
        self.assertEqual(events, [])

    def test_prepare_handoff_allows_state_summary_without_task_id(self):
        conn = self._make_conn()
        _, harness_root = self._setup_workspace_with_approved_plan(conn)
        (harness_root / "harness-state.json").write_text(
            json.dumps({"project": "demo", "current_item": None, "recent_events": []}),
            encoding="utf-8",
        )

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "worker.handoff.prepared")

    def test_prepare_handoff_requires_matching_scope(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn, scope="harness initialization only")

        with self.assertRaises(ValueError) as ctx:
            prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker", required_scope="implementation plan")

        self.assertIn("implementation plan", str(ctx.exception))

    def test_prepare_handoff_wrong_scope_rejected_blocks_even_if_other_scope_approved(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn, scope="harness initialization only")

        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.rejected",
            actor="operator",
            target="worker",
            task_id="t1",
            idempotency_key="demo:t1:plan.rejected:implementation plan",
            payload={"task_id": "t1", "decision": "rejected", "scope": "implementation plan"},
        )

        with self.assertRaises(ValueError) as ctx:
            prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker", required_scope="implementation plan")

        self.assertIn("plan.rejected", str(ctx.exception))

    def test_prepare_handoff_no_gate_event_fails(self):
        conn = self._make_conn()
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
            task_id="t1",
            phase="ready",
            owner=None,
            branch=None,
            pr=None,
            payload={"task_id": "t1", "title": "Test"},
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.ready",
            actor="operator",
            target="worker",
            task_id="t1",
            idempotency_key="demo:t1:plan.ready",
            payload={"task_id": "t1", "title": "Test"},
        )

        with self.assertRaises(ValueError) as ctx:
            prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("no plan gate event", str(ctx.exception))

    def test_prepare_handoff_rejected_after_approved_blocks(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn, scope="implementation plan")

        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.rejected",
            actor="operator",
            target="worker",
            task_id="t1",
            idempotency_key="demo:t1:plan.rejected:implementation plan",
            payload={"task_id": "t1", "decision": "rejected", "scope": "implementation plan"},
        )

        with self.assertRaises(ValueError) as ctx:
            prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("plan.rejected", str(ctx.exception))

    def test_prepare_handoff_reapproved_after_reject_works(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn, scope="implementation plan")

        # Get the original plan.ready event id for re-approval binding
        from coordinate.db import row_to_dict as _rtd
        plan_ready_row = conn.execute(
            "SELECT id FROM events WHERE workspace_id = 'demo' AND task_id = 't1' AND event_type = 'plan.ready' ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        plan_ready_id = _rtd(plan_ready_row)["id"] if plan_ready_row else None

        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.rejected",
            actor="operator",
            target="worker",
            task_id="t1",
            idempotency_key="demo:t1:plan.rejected:implementation plan",
            payload={"task_id": "t1", "decision": "rejected", "scope": "implementation plan"},
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.approved",
            actor="operator",
            target="worker",
            task_id="t1",
            idempotency_key="demo:t1:plan.approved:implementation plan:after_1_rejects",
            payload={
                "task_id": "t1",
                "decision": "approved",
                "scope": "implementation plan",
                "plan_ready_event_id": plan_ready_id,
            },
        )

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "worker.handoff.prepared")

    def test_prepare_handoff_blocks_when_plan_ready_updated_after_approval(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn, scope="implementation plan")

        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.ready",
            actor="operator",
            target="worker",
            task_id="t1",
            idempotency_key="demo:t1:plan.ready:v2",
            payload={"task_id": "t1", "title": "Test Phase v2", "plan_doc": "docs/plan-v2.md"},
        )

        with self.assertRaises(ValueError) as ctx:
            prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("plan.ready was updated after approval", str(ctx.exception))

    def test_prepare_handoff_idempotent(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        first = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")
        second = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertTrue(first.event_created)
        self.assertFalse(second.event_created)
        self.assertEqual(first.event["id"], second.event["id"])

    def test_prepare_handoff_idempotency_key_includes_gate_event_id(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("gate_", result.event["idempotency_key"])

    def test_reviewer_handoff_idempotency_key_distinguishes_plan_and_code(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        plan_review = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="reviewer", review_type="plan")
        code_review = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="reviewer", review_type="code")

        # Both create events (different idempotency keys), not de-duped
        self.assertTrue(plan_review.event_created)
        self.assertTrue(code_review.event_created)
        self.assertNotEqual(plan_review.event["id"], code_review.event["id"])
        self.assertIn("_plan", plan_review.event["idempotency_key"])
        self.assertIn("_code", code_review.event["idempotency_key"])

    def test_prepare_handoff_after_reapprove_produces_new_handoff(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn, scope="implementation plan")

        first = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")
        self.assertTrue(first.event_created)

        # Get the original plan.ready event id for re-approval binding
        from coordinate.db import row_to_dict as _rtd
        plan_ready_row = conn.execute(
            "SELECT id FROM events WHERE workspace_id = 'demo' AND task_id = 't1' AND event_type = 'plan.ready' ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        plan_ready_id = _rtd(plan_ready_row)["id"] if plan_ready_row else None

        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.rejected",
            actor="operator",
            target="worker",
            task_id="t1",
            idempotency_key="demo:t1:plan.rejected:implementation plan",
            payload={"task_id": "t1", "decision": "rejected", "scope": "implementation plan"},
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.approved",
            actor="operator",
            target="worker",
            task_id="t1",
            idempotency_key="demo:t1:plan.approved:implementation plan:after_1_rejects",
            payload={
                "task_id": "t1",
                "decision": "approved",
                "scope": "implementation plan",
                "plan_ready_event_id": plan_ready_id,
            },
        )

        second = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertTrue(second.event_created)
        self.assertNotEqual(first.event["id"], second.event["id"])

    def test_prepare_handoff_raises_on_unknown_workspace(self):
        conn = self._make_conn()

        with self.assertRaises(ValueError):
            prepare_handoff(conn, workspace_id="nonexistent", task_id="t1", role="worker")

    def test_prepare_handoff_raises_on_missing_task(self):
        conn = self._make_conn()
        upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path="/tmp/demo",
            harness_root="/tmp/demo",
        )

        with self.assertRaises(ValueError):
            prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

    # --- handoff_text structure ---

    def test_handoff_text_includes_all_six_sections(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        for section in [
            "Context Recovery",
            "Implementation Scope",
            "Non-Goals",
            "Validation Commands",
            "Return Format",
            "Constraints",
        ]:
            self.assertIn(section, result.handoff_text, f"missing section: {section}")

    def test_handoff_text_includes_workspace_path(self):
        conn = self._make_conn()
        workspace_path, _ = self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn(str(workspace_path), result.handoff_text)

    def test_handoff_text_includes_constraints(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("Human gate required", result.handoff_text)
        self.assertIn("no merge", result.handoff_text)
        self.assertIn("No deploy", result.handoff_text)

    def test_handoff_text_includes_recovery_commands(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        for cmd in [
            "git status --short",
            "git branch --show-current",
            "git log --oneline -8",
            "harness-state.json",
            "mvp-checklist.json",
        ]:
            self.assertIn(cmd, result.handoff_text, f"missing recovery command: {cmd}")

    def test_handoff_text_uses_dynamic_harness_root(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("docs/project-harness/harness-state.json", result.handoff_text)
        self.assertIn("docs/project-harness/mvp-checklist.json", result.handoff_text)

    def test_handoff_text_includes_source_plan_in_recovery(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("cat docs/plan.md", result.handoff_text)

    def test_handoff_text_fallback_for_missing_non_goals(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(
            conn,
            plan_payload={"task_id": "t1", "title": "Test"},
        )

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker", required_scope="implementation plan")

        self.assertIn("No non-goals specified", result.handoff_text)

    def test_handoff_text_fallback_for_missing_test_baseline(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(
            conn,
            plan_payload={"task_id": "t1", "title": "Test"},
        )

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker", required_scope="implementation plan")

        self.assertIn("No validation baseline recorded", result.handoff_text)

    def test_payload_includes_approved_gate_event_id(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("approved_gate_event_id", result.event["payload"])
        self.assertTrue(result.event["payload"]["approved_gate_event_id"])

    def test_legacy_approval_without_plan_ready_event_id_fails_closed(self):
        conn = self._make_conn()
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
            task_id="t1",
            phase="ready",
            owner=None,
            branch=None,
            pr=None,
            payload={"task_id": "t1", "title": "Test"},
        )
        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.ready",
            actor="operator",
            target="worker",
            task_id="t1",
            idempotency_key="demo:t1:plan.ready",
            payload={"task_id": "t1", "title": "Test"},
        )
        # Legacy approval without plan_ready_event_id
        append_event(
            conn,
            workspace_id="demo",
            event_type="plan.approved",
            actor="operator",
            target="worker",
            task_id="t1",
            idempotency_key="demo:t1:plan.approved:implementation plan",
            payload={"task_id": "t1", "decision": "approved", "scope": "implementation plan"},
        )

        with self.assertRaises(ValueError) as ctx:
            prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("legacy approval", str(ctx.exception))

    def test_handoff_uses_approved_plan_payload_not_latest(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(
            conn,
            plan_payload={
                "task_id": "t1",
                "title": "Phase v1",
                "plan_doc": "docs/plan-v1.md",
            },
        )

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("Phase v1", result.handoff_text)
        self.assertIn("docs/plan-v1.md", result.handoff_text)

    # --- worker bootstrap ---

    def test_prepare_handoff_includes_bootstrap_text(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertTrue(result.bootstrap_text)
        for section in ["Session Startup", "Your Assignment", "Coordinator CLI", "Implementation Protocol", "Session End Protocol", "Constraints"]:
            self.assertIn(section, result.bootstrap_text, f"missing section: {section}")

    def test_bootstrap_contains_task_context(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("t1", result.bootstrap_text)
        self.assertIn("docs/plan.md", result.bootstrap_text)
        self.assertIn("agents/worker/t1", result.bootstrap_text)
        self.assertIn("demo", result.bootstrap_text)

    def test_bootstrap_contains_coordinator_cli(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn, task_id="my-task")

        result = prepare_handoff(
            conn, workspace_id="demo", task_id="my-task", role="worker",
            db_path="/path/to/db.sqlite3",
            coordinator_path="/path/to/coordinator",
        )

        self.assertIn("coordinate", result.bootstrap_text)
        self.assertIn("/path/to/db.sqlite3", result.bootstrap_text)
        self.assertIn("/path/to/coordinator", result.bootstrap_text)
        self.assertIn("assignment accept demo", result.bootstrap_text)

    def test_bootstrap_contains_session_protocol(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        for step in ["Step 1", "Step 2", "Step 3", "Step 4", "Step 5"]:
            self.assertIn(step, result.bootstrap_text, f"missing step: {step}")
        self.assertIn("git status --short", result.bootstrap_text)
        self.assertIn("git log --oneline -10", result.bootstrap_text)
        self.assertIn("harness-state.json", result.bootstrap_text)
        self.assertIn("progress.md", result.bootstrap_text)
        self.assertIn("scope.md", result.bootstrap_text)
        self.assertIn("architecture.md", result.bootstrap_text)
        self.assertIn("Visible Discord Updates", result.bootstrap_text)
        self.assertIn("[agent-report]\naction=progress", result.bootstrap_text)
        self.assertIn("[agent-report]\naction=blocker", result.bootstrap_text)
        self.assertIn("[agent-report]\naction=done", result.bootstrap_text)
        self.assertIn("@Coordinator", result.bootstrap_text)
        self.assertIn("@Codex", result.bootstrap_text)

    def test_bootstrap_contains_shared_worktree_guard(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("Shared-worktree guard", result.bootstrap_text)
        self.assertIn("stop and report a blocker instead of switching branches", result.bootstrap_text)
        self.assertIn("git reset", result.bootstrap_text)
        self.assertIn("git switch", result.bootstrap_text)
        self.assertIn("git push --force", result.bootstrap_text)

    def test_bootstrap_contains_self_test_before_closeout_section(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("Self-Test Before Closeout", result.bootstrap_text)
        self.assertIn("explicit deployment authority", result.bootstrap_text)
        self.assertIn("Never infer production-write authority", result.bootstrap_text)
        self.assertIn("--self-test-evidence", result.bootstrap_text)
        self.assertIn("phase-8.5", result.bootstrap_text)
        self.assertIn("phase-8.6", result.bootstrap_text)

    def test_reviewer_bootstrap_contains_verify_self_test_evidence(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="reviewer")

        self.assertIn("Verify Worker Self-Test Evidence", result.bootstrap_text)
        self.assertIn("self_test_evidence", result.bootstrap_text)
        self.assertIn("Reject", result.bootstrap_text)

    def test_reviewer_bootstrap_code_review_has_worktree_guard_by_default(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="reviewer")

        # code review (default): pinned to worktree, branch guard, self_test check
        self.assertIn("Shared-worktree guard", result.bootstrap_text)
        self.assertIn("git branch --show-current", result.bootstrap_text)
        self.assertIn("**Branch**:", result.bootstrap_text)

    def test_reviewer_bootstrap_plan_review_skips_worktree_and_self_test(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(
            conn, workspace_id="demo", task_id="t1", role="reviewer", review_type="plan"
        )

        # plan review: read-only, no worktree/branch guard, no self_test requirement
        self.assertIn("plan review", result.bootstrap_text)
        self.assertIn("read-only", result.bootstrap_text)
        self.assertNotIn("Shared-worktree guard", result.bootstrap_text)
        self.assertNotIn("git branch --show-current", result.bootstrap_text)
        self.assertNotIn("**Branch**:", result.bootstrap_text)
        self.assertNotIn("Verify Worker Self-Test Evidence", result.bootstrap_text)
        self.assertNotIn("self_test_evidence", result.bootstrap_text)
        # host-safe: plan doc path is relative (any host's checkout), not a stale worktree abs path
        self.assertIn(f"openspec/changes/{ 't1' }/proposal.md", result.bootstrap_text)
        self.assertIn("read your LOCAL repo", result.bootstrap_text)
        # still has the plan-review focus + output format
        self.assertIn("Plan completeness", result.bootstrap_text)
        self.assertIn("decision=approve", result.bootstrap_text)

    def test_plan_review_self_test_section_is_empty(self):
        """Plan review must not emit self_test_evidence verification — there
        is no implementation or closeout packet at plan review stage."""
        ctx = _ReviewerBootstrapContext(
            task_id="t1", title="T", branch="main", ws_id="demo",
            is_plan_review=True,
            execution_workspace_path="/tmp/demo",
            execution_harness="docs/project-harness",
            execution_source_plan="plan.md",
            harness_rel="docs/project-harness",
            acceptance_criteria="criteria",
        )
        self.assertEqual(_render_reviewer_self_test(ctx), "")

    def test_code_review_self_test_section_present(self):
        """Code review must emit self_test_evidence verification."""
        ctx = _ReviewerBootstrapContext(
            task_id="t1", title="T", branch="main", ws_id="demo",
            is_plan_review=False,
            execution_workspace_path="/tmp/demo",
            execution_harness="docs/project-harness",
            execution_source_plan="plan.md",
            harness_rel="docs/project-harness",
            acceptance_criteria="criteria",
        )
        section = _render_reviewer_self_test(ctx)
        self.assertIn("Verify Worker Self-Test Evidence", section)
        self.assertIn("self_test_evidence", section)
        self.assertIn("Reject", section)

    def test_plan_review_session_startup_no_worktree_guard(self):
        """Plan review session startup must not pin to worktree or branch."""
        ctx = _ReviewerBootstrapContext(
            task_id="t1", title="T", branch="main", ws_id="demo",
            is_plan_review=True,
            execution_workspace_path="/tmp/demo",
            execution_harness="docs/project-harness",
            execution_source_plan="plan.md",
            harness_rel="docs/project-harness",
            acceptance_criteria="criteria",
        )
        startup = _render_reviewer_session_startup(ctx)
        self.assertIn("plan review", startup)
        self.assertIn("read-only", startup)
        self.assertNotIn("Shared-worktree guard", startup)
        self.assertNotIn("git branch --show-current", startup)

    def test_code_review_session_startup_has_worktree_guard(self):
        """Code review session startup must pin to worktree with branch guard."""
        ctx = _ReviewerBootstrapContext(
            task_id="t1", title="T", branch="feature-x", ws_id="demo",
            is_plan_review=False,
            execution_workspace_path="/opt/demo",
            execution_harness="docs/project-harness",
            execution_source_plan="plan.md",
            harness_rel="docs/project-harness",
            acceptance_criteria="criteria",
        )
        startup = _render_reviewer_session_startup(ctx)
        self.assertIn("Shared-worktree guard", startup)
        self.assertIn("feature-x", startup)
        self.assertIn("/opt/demo", startup)

    def test_plan_review_assignment_omits_branch(self):
        """Plan review assignment block must not include Branch line."""
        ctx = _ReviewerBootstrapContext(
            task_id="t1", title="T", branch="main", ws_id="demo",
            is_plan_review=True,
            execution_workspace_path="/tmp/demo",
            execution_harness="docs/project-harness",
            execution_source_plan="plan.md",
            harness_rel="docs/project-harness",
            acceptance_criteria="criteria",
        )
        assignment = _render_reviewer_assignment(ctx)
        self.assertNotIn("**Branch**:", assignment)
        self.assertIn("openspec/changes/t1/proposal.md", assignment)

    def test_code_review_assignment_includes_branch(self):
        """Code review assignment block must include Branch line."""
        ctx = _ReviewerBootstrapContext(
            task_id="t1", title="T", branch="feature-x", ws_id="demo",
            is_plan_review=False,
            execution_workspace_path="/tmp/demo",
            execution_harness="docs/project-harness",
            execution_source_plan="plan.md",
            harness_rel="docs/project-harness",
            acceptance_criteria="criteria",
        )
        assignment = _render_reviewer_assignment(ctx)
        self.assertIn("**Branch**: feature-x", assignment)

    def test_code_review_focus_blank_line_format(self):
        """Code review ## Review Focus must have two blank lines before bullets
        (byte-for-byte identical to the pre-#12.5 monolith output)."""
        ctx = _ReviewerBootstrapContext(
            task_id="t1", title="T", branch="main", ws_id="demo",
            is_plan_review=False,
            execution_workspace_path="/tmp/demo",
            execution_harness="docs/project-harness",
            execution_source_plan="plan.md",
            harness_rel="docs/project-harness",
            acceptance_criteria="criteria",
        )
        focus = _render_reviewer_focus(ctx)
        # "## Review Focus\n\n\n- Plan completeness..."
        self.assertIn("## Review Focus\n\n\n- Plan completeness", focus)

    def test_reviewer_output_format_parseable(self):
        """[agent-report] decision block must be present and parseable."""
        ctx = _ReviewerBootstrapContext(
            task_id="t1", title="T", branch="main", ws_id="demo",
            is_plan_review=False,
            execution_workspace_path="/tmp/demo",
            execution_harness="docs/project-harness",
            execution_source_plan="plan.md",
            harness_rel="docs/project-harness",
            acceptance_criteria="criteria",
        )
        output = _render_reviewer_output_format(ctx)
        self.assertIn("[agent-report]", output)
        self.assertIn("decision=approve", output)
        self.assertIn("decision=reject", output)
        self.assertIn("workspace_id=demo", output)
        self.assertIn("task_id=t1", output)

    def test_reviewer_bootstrap_host_aware_path_flows(self):
        """execution_profile.workspace_path must flow into code review startup."""
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)
        set_workspace_agent(conn, workspace_id="demo", agent_name="mac-claude", discord_user_id="123")
        register_agent(conn, agent_id="mac-claude", host_id="remote-host")
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="remote-host",
            workspace_path="/opt/remote-demo",
            harness_root="/opt/remote-demo/docs/project-harness",
            coordinator_cli_path="/opt/remote-bin/coordinate",
            coordinator_db_path="/opt/remote-data/coordinator.sqlite3",
        )
        conn.commit()
        result = prepare_handoff(
            conn, workspace_id="demo", task_id="t1", role="reviewer",
            review_type="code", target_agent="mac-claude",
        )
        self.assertIn("/opt/remote-demo", result.bootstrap_text)

    def test_bootstrap_requires_structured_done_closeout(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIn("assignment closeout demo --task-id t1", result.bootstrap_text)
        self.assertIn("MUST include exactly one parseable `[agent-report]` block", result.bootstrap_text)
        self.assertIn("action=done", result.bootstrap_text)
        self.assertIn("natural-language completion alone is not enough", result.bootstrap_text)

    def test_bootstrap_path_is_task_scoped(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn, task_id="my-task")

        result = prepare_handoff(conn, workspace_id="demo", task_id="my-task", role="worker")

        self.assertEqual(
            result.bootstrap_recommended_path,
            "docs/project-harness/tasks/my-task/worker-bootstrap.md",
        )

    def test_bootstrap_no_executable_harnessctl(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        import re
        executable_patterns = [
            r"harnessctl\s+session-init",
            r"bash\s+.*harnessctl",
            r"/harnessctl",
        ]
        for pattern in executable_patterns:
            self.assertIsNone(
                re.search(pattern, result.bootstrap_text),
                f"found executable harnessctl pattern '{pattern}' in bootstrap",
            )

    def test_db_path_placeholder_when_none(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker", db_path=None)

        self.assertIn("<db-path>", result.bootstrap_text)
        self.assertIn("<coordinator-path>", result.bootstrap_text)

    # --- target_agent / bootstrap_path in payload ---

    def test_handoff_payload_includes_target_agent(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)
        set_workspace_agent(conn, workspace_id="demo", agent_name="mac-codex", discord_user_id="123")

        result = prepare_handoff(
            conn, workspace_id="demo", task_id="t1", role="worker",
            target_agent="mac-codex",
        )

        self.assertEqual(result.event["payload"]["target_agent"], "mac-codex")

    def test_target_agent_handoff_uses_host_execution_profile(self):
        conn = self._make_conn()
        workspace_path, _ = self._setup_workspace_with_approved_plan(conn)
        set_workspace_agent(conn, workspace_id="demo", agent_name="mac-claude", discord_user_id="123")
        register_agent(conn, agent_id="mac-claude", host_id="macbook-local")
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="macbook-local",
            workspace_path="/home/synthetic-user/projects/multinexus",
            coordinator_cli_path="/home/synthetic-user/.local/bin/coord-ssh",
            shell="bash",
        )

        result = prepare_handoff(
            conn,
            workspace_id="demo",
            task_id="t1",
            role="worker",
            target_agent="mac-claude",
        )

        self.assertIn("/home/synthetic-user/projects/multinexus", result.bootstrap_text)
        self.assertIn("/home/synthetic-user/projects/multinexus", result.handoff_text)
        self.assertIn("/home/synthetic-user/.local/bin/coord-ssh <command> demo", result.bootstrap_text)
        self.assertNotIn(f"cd {workspace_path}", result.bootstrap_text)
        self.assertNotIn(str(workspace_path), result.handoff_text)
        self.assertEqual(
            result.event["payload"]["execution_profile"]["workspace_path"],
            "/home/synthetic-user/projects/multinexus",
        )
        self.assertIn("bootstrap_text", result.event["payload"])

    def test_prepare_handoff_materializes_harness_root_fallback(self):
        # R2-3: when the host profile omits harness_root, prepare_handoff must
        # materialize a complete canonical harness path under the host workspace.
        conn = self._make_conn()
        workspace_path, harness_root = self._setup_workspace_with_approved_plan(conn)
        set_workspace_agent(conn, workspace_id="demo", agent_name="mac-claude", discord_user_id="123")
        register_agent(conn, agent_id="mac-claude", host_id="macbook-local")
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="macbook-local",
            workspace_path="/home/synthetic-user/projects/multinexus",
            harness_root=None,
        )

        result = prepare_handoff(
            conn,
            workspace_id="demo",
            task_id="t1",
            role="worker",
            target_agent="mac-claude",
        )

        profile = result.event["payload"]["execution_profile"]
        self.assertEqual(profile["workspace_path"], "/home/synthetic-user/projects/multinexus")
        self.assertTrue(profile["harness_root"])
        self.assertIn("/home/synthetic-user/projects/multinexus", profile["harness_root"])
        self.assertIn(profile["harness_root"], result.handoff_text)

    def test_target_agent_handoff_preserves_full_execution_profile(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)
        set_workspace_agent(conn, workspace_id="demo", agent_name="mac-claude", discord_user_id="123")
        register_agent(conn, agent_id="mac-claude", host_id="macbook-local")
        upsert_workspace_host_profile(
            conn,
            workspace_id="demo",
            host_id="macbook-local",
            workspace_path="/home/synthetic-user/projects/multinexus",
            harness_root="/home/synthetic-user/projects/multinexus/docs/project-harness",
            harnessctl_path="/home/synthetic-user/.local/bin/harnessctl",
            coordinator_cli_path="/home/synthetic-user/.local/bin/coord-ssh",
            coordinator_db_path="/home/synthetic-user/.local/share/coordinate/coordinator.sqlite3",
            shell="bash",
            metadata={"ssh_host": "macbook-local", "executor": "local"},
        )

        result = prepare_handoff(
            conn,
            workspace_id="demo",
            task_id="t1",
            role="worker",
            target_agent="mac-claude",
        )

        profile = result.event["payload"]["execution_profile"]
        self.assertIsNotNone(profile)
        self.assertEqual(profile["workspace_id"], "demo")
        self.assertEqual(profile["host_id"], "macbook-local")
        self.assertEqual(profile["workspace_path"], "/home/synthetic-user/projects/multinexus")
        self.assertEqual(profile["harness_root"], "/home/synthetic-user/projects/multinexus/docs/project-harness")
        self.assertEqual(profile["harnessctl_path"], "/home/synthetic-user/.local/bin/harnessctl")
        self.assertEqual(profile["coordinator_cli_path"], "/home/synthetic-user/.local/bin/coord-ssh")
        self.assertEqual(profile["coordinator_db_path"], "/home/synthetic-user/.local/share/coordinate/coordinator.sqlite3")
        self.assertEqual(profile["shell"], "bash")
        self.assertEqual(profile["metadata"], {"ssh_host": "macbook-local", "executor": "local"})

    def test_untargeted_handoff_stores_null_execution_profile(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIsNone(result.event["payload"]["execution_profile"])

    def test_target_agent_with_registered_host_requires_execution_profile(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)
        set_workspace_agent(conn, workspace_id="demo", agent_name="mac-claude", discord_user_id="123")
        register_agent(conn, agent_id="mac-claude", host_id="macbook-local")

        with self.assertRaises(ValueError) as ctx:
            prepare_handoff(
                conn,
                workspace_id="demo",
                task_id="t1",
                role="worker",
                target_agent="mac-claude",
            )

        self.assertIn("no execution profile", str(ctx.exception))
        self.assertIn("workspace host-profile set", str(ctx.exception))

    def test_handoff_payload_includes_bootstrap_path(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertEqual(
            result.event["payload"]["bootstrap_path"],
            result.bootstrap_recommended_path,
        )

    def test_handoff_without_target_agent_has_none(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        result = prepare_handoff(conn, workspace_id="demo", task_id="t1", role="worker")

        self.assertIsNone(result.event["payload"]["target_agent"])

    def test_handoff_idempotency_key_includes_target(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)
        set_workspace_agent(conn, workspace_id="demo", agent_name="mac-codex", discord_user_id="123")

        result = prepare_handoff(
            conn, workspace_id="demo", task_id="t1", role="worker",
            target_agent="mac-codex",
        )

        self.assertIn(":target_mac-codex", result.event["idempotency_key"])

    def test_handoff_different_target_creates_new_event(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)
        set_workspace_agent(conn, workspace_id="demo", agent_name="mac-codex", discord_user_id="123")
        set_workspace_agent(conn, workspace_id="demo", agent_name="mac-claude", discord_user_id="456")

        first = prepare_handoff(
            conn, workspace_id="demo", task_id="t1", role="worker",
            target_agent="mac-codex",
        )
        second = prepare_handoff(
            conn, workspace_id="demo", task_id="t1", role="worker",
            target_agent="mac-claude",
        )

        self.assertTrue(first.event_created)
        self.assertTrue(second.event_created)
        self.assertNotEqual(first.event["id"], second.event["id"])

    def test_handoff_unregistered_target_agent_fails(self):
        conn = self._make_conn()
        self._setup_workspace_with_approved_plan(conn)

        with self.assertRaises(ValueError) as ctx:
            prepare_handoff(
                conn, workspace_id="demo", task_id="t1", role="worker",
                target_agent="unknown-agent",
            )
        self.assertIn("not registered", str(ctx.exception))
        self.assertIn("workspace agent add", str(ctx.exception))


class IssueMaterializeHandoffTests(unittest.TestCase):
    """Phase 8.3: prove materialize is what unblocks the harness-checklist gate.

    Direct _require_harness_task checks isolate the checklist gate from
    prepare_handoff's plan-ready/plan-approved ordering, and the end-to-end
    test proves materialize + plan approve lets prepare_handoff succeed.
    """

    def _make_conn(self):
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        return conn

    def _setup_issue_workspace(self, conn, *, task_id="bug-1"):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        workspace_path = Path(tmp.name)
        harness_root = workspace_path / "docs" / "project-harness"
        harness_root.mkdir(parents=True)
        (harness_root / "mvp-checklist.json").write_text(
            json.dumps({"project": "demo", "items": []}), encoding="utf-8"
        )
        (harness_root / "harness-state.json").write_text(
            json.dumps({
                "project": "demo",
                "current_item": {"id": task_id, "status": "ready"},
                "items": [{"id": task_id, "status": "ready"}],
            }), encoding="utf-8"
        )
        plan_abs = workspace_path / "docs" / "plan.md"
        plan_abs.parent.mkdir(parents=True, exist_ok=True)
        plan_abs.write_text("# Plan\nacceptance: ...\n", encoding="utf-8")
        upsert_workspace(
            conn, workspace_id="demo", name="Demo",
            path=str(workspace_path), harness_root=str(harness_root), base_branch="main",
        )
        spotted = append_event(
            conn, workspace_id="demo", event_type="issue.spotted", actor="github",
            target="acme/repo", idempotency_key="demo:github_issue:acme/repo:1:t",
            payload={
                "repo": "acme/repo", "number": 1,
                "url": "https://github.com/acme/repo/issues/1",
                "title": "Bug", "content_trust": "untrusted",
            },
        )
        triage = triage_issue(
            conn, workspace_id="demo", event_id=spotted.row["id"],
            decision="accept", task_id=task_id,
        )
        return triage.event["id"], "docs/plan.md", task_id

    def _approve_plan(self, conn, task_id, scope="implementation plan"):
        plan_ready = conn.execute(
            "SELECT id FROM events WHERE workspace_id = ? AND task_id = ? "
            "AND event_type = 'plan.ready' ORDER BY rowid DESC LIMIT 1",
            ("demo", task_id),
        ).fetchone()
        plan_ready_id = plan_ready["id"] if plan_ready else None
        append_event(
            conn, workspace_id="demo", event_type="plan.approved", actor="operator",
            target="worker", task_id=task_id,
            idempotency_key=f"demo:{task_id}:plan.approved:{scope}",
            payload={
                "task_id": task_id, "decision": "approved", "scope": scope,
                "plan_ready_event_id": plan_ready_id,
            },
        )

    def test_require_harness_task_fails_before_materialize(self):
        # After triage accept, the DB task mirror exists but the harness
        # checklist does not contain the task yet.
        conn = self._make_conn()
        triage_id, plan_doc, task_id = self._setup_issue_workspace(conn)
        workspace = get_workspace(conn, "demo")
        with self.assertRaises(ValueError):
            _require_harness_task(workspace, task_id)

    def test_require_harness_task_passes_after_materialize(self):
        # materialize syncs the task into mvp-checklist.json.
        conn = self._make_conn()
        triage_id, plan_doc, task_id = self._setup_issue_workspace(conn)
        materialize_issue(
            conn, workspace_id="demo", event_id=triage_id, plan_doc=plan_doc
        )
        workspace = get_workspace(conn, "demo")
        _require_harness_task(workspace, task_id)  # must not raise

    def test_prepare_handoff_passes_after_materialize_and_approve(self):
        # End-to-end: triage accept -> materialize -> plan approve -> handoff ok.
        conn = self._make_conn()
        triage_id, plan_doc, task_id = self._setup_issue_workspace(conn)
        materialize_issue(
            conn, workspace_id="demo", event_id=triage_id, plan_doc=plan_doc
        )
        self._approve_plan(conn, task_id)
        result = prepare_handoff(conn, workspace_id="demo", task_id=task_id, role="worker")
        self.assertTrue(result.event_created)
        self.assertEqual(result.event["event_type"], "worker.handoff.prepared")


class WorkerBootstrapSectionTests(unittest.TestCase):
    """#12.4 — verify the _build_worker_bootstrap section split.

    The refactor is byte-for-byte equivalent (verified separately against a
    pre-refactor golden capture). These tests lock the section-renderer
    *structure*: dispatch hits, section presence/order, and the key worker-
    visible anchors — guarding against future section loss/reorder and the
    closeout --reviewer regression that nearly slipped in during the split.
    """

    def _ctx(self, **overrides):
        defaults = dict(
            task_id="phase-001",
            title="Implement phase 001",
            branch="agents/x/phase-001",
            ws_id="demo",
            execution_workspace_path="/host/ws",
            execution_harness="docs",
            execution_source_plan="docs/plans/p001.md",
            coordinator_cli="coord-local <command> demo [options]",
        )
        defaults.update(overrides)
        return _WorkerBootstrapContext(**defaults)

    def _workspace(self):
        from types import SimpleNamespace

        return SimpleNamespace(
            id="demo", path="/tmp/ws", base_branch="main", harness_root="/tmp/ws/docs"
        )

    def test_section_renderers_are_all_callables(self):
        # Locks the #12.4 dispatch: each section is an independently callable renderer.
        for renderer in (
            _render_worker_session_startup,
            _render_worker_assignment,
            _render_worker_coordinator_cli,
            _render_worker_implementation_protocol,
            _render_worker_visible_discord,
            _render_worker_self_test,
            _render_worker_session_end,
            _render_worker_constraints,
        ):
            self.assertTrue(callable(renderer), f"{renderer!r} is not callable")

    def test_bootstrap_renders_all_sections_in_order(self):
        text = _build_worker_bootstrap(
            workspace=self._workspace(),
            task={"task_id": "phase-001", "branch": "agents/x/phase-001"},
            plan_payload={"title": "Implement phase 001", "plan_doc": "/tmp/ws/docs/plans/p001.md"},
            db_path="/tmp/db.sqlite",
            coordinator_path="/tmp/coord",
        )
        expected_sections = [
            "## Session Startup",
            "## Your Assignment",
            "## Coordinator CLI",
            "## Implementation Protocol",
            "## Visible Discord Updates",
            "## Self-Test Before Closeout",
            "## Session End Protocol",
            "## Constraints",
        ]
        positions = [text.index(s) for s in expected_sections]
        self.assertEqual(positions, sorted(positions), "sections missing or out of order")
        self.assertTrue(text.startswith("# Worker Bootstrap: phase-001\n"))

    def test_coordinator_cli_closeout_keeps_reviewer_arg(self):
        # Regression guard: the Coordinator-CLI closeout command must carry
        # --reviewer <name> (nearly dropped during the #12.4 split).
        text = _render_worker_coordinator_cli(self._ctx())
        self.assertIn("assignment closeout demo --task-id <id> --reviewer <name>", text)
        # mark-done is the last command and the only one trailed by a blank line
        self.assertIn("- `assignment mark-done demo --task-id <id>`\n\n", text)

    def test_visible_discord_carries_three_agent_report_blocks(self):
        text = _render_worker_visible_discord(self._ctx())
        # 3 report blocks + 1 prose mention ("The `[agent-report]` marker...")
        self.assertEqual(text.count("[agent-report]"), 4)
        for action in ("action=progress", "action=blocker", "action=done"):
            self.assertIn(action, text)
        self.assertIn("workspace_id=demo", text)
        self.assertIn("task_id=phase-001", text)

    def test_execution_profile_workspace_path_flows_into_startup(self):
        # A host execution_profile workspace_path must reach Step 1 (cd target).
        text = _render_worker_session_startup(
            self._ctx(execution_workspace_path="/host/path-xyz")
        )
        self.assertIn("`/host/path-xyz`", text)
        self.assertIn("`cd /host/path-xyz`", text)


if __name__ == "__main__":
    unittest.main()
