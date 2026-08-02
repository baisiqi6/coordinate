import unittest
from unittest import mock

from coordinate.db import (
    append_event,
    initialize,
    list_events,
    list_task_mirrors,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
)
import coordinate.reconcile
from coordinate.reconcile import (
    ReconcileConflictError,
    ReconcileTaskNotFoundError,
    reconcile_workspace,
)


class FakeHarnessAdapter:
    def __init__(self, state, checklist):
        self.state = state
        self.checklist = checklist
        self.refresh_count = 0

    def refresh_state(self):
        self.refresh_count += 1
        return self.state

    def read_state(self):
        return self.state

    def read_checklist(self):
        return self.checklist


class ReconcileTests(unittest.TestCase):
    def test_reconcile_creates_task_mirrors_and_events(self):
        conn = initialize(":memory:")
        workspace = upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )
        adapter = FakeHarnessAdapter(
            state={"project": "demo", "generated_at": "2026-05-17T00:00:00Z"},
            checklist={
                "project": "demo",
                "items": [
                    {
                        "id": "mvp-001",
                        "title": "Build core",
                        "status": "doing",
                        "owner": "codex",
                        "workflow": {"status": "running", "branch": "agents/mvp-001"},
                        "artifacts": {"pr": "https://github.example/pr/1"},
                    },
                    {
                        "id": "mvp-002",
                        "title": "Review core",
                        "status": "todo",
                    },
                ],
            },
        )

        result = reconcile_workspace(conn, workspace, adapter=adapter)

        mirrors = [row_to_dict(row) for row in list_task_mirrors(conn, "demo")]
        events = [row_to_dict(row) for row in list_events(conn, "demo")]
        self.assertEqual(result.created, 2)
        self.assertEqual(result.updated, 0)
        self.assertEqual(result.unchanged, 0)
        self.assertEqual(adapter.refresh_count, 1)
        self.assertEqual(mirrors[0]["phase"], "running")
        self.assertEqual(mirrors[0]["branch"], "agents/mvp-001")
        self.assertEqual(mirrors[0]["pr"], "https://github.example/pr/1")
        event_types = [event["event_type"] for event in events]
        self.assertEqual(event_types.count("task_mirror.created"), 2)
        self.assertEqual(event_types.count("reconciliation.completed"), 1)

    def test_reconcile_second_run_is_unchanged(self):
        conn = initialize(":memory:")
        workspace = upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )
        adapter = FakeHarnessAdapter(
            state={"project": "demo", "generated_at": "2026-05-17T00:00:00Z"},
            checklist={
                "project": "demo",
                "items": [
                    {"id": "mvp-001", "title": "Build core", "status": "todo"},
                ],
            },
        )

        reconcile_workspace(conn, workspace, adapter=adapter)
        result = reconcile_workspace(conn, workspace, adapter=adapter)

        events = [row_to_dict(row) for row in list_events(conn, "demo")]
        self.assertEqual(result.created, 0)
        self.assertEqual(result.updated, 0)
        self.assertEqual(result.unchanged, 1)
        self.assertEqual([event["event_type"] for event in events].count("reconciliation.completed"), 1)

    def test_reconcile_preserves_coordinator_owned_publish_state(self):
        conn = initialize(":memory:")
        workspace = upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )
        event = append_event(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            event_type="pr.linked",
            actor="operator",
            payload={"pr_url": "https://github.com/o/r/pull/1"},
        )
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="running",
            owner="codex",
            branch="agents/mvp-001",
            pr="https://github.com/o/r/pull/1",
            payload={
                "id": "mvp-001",
                "status": "doing",
                "publish_metadata": {"reported_commit": "a" * 40},
            },
            last_event_id=event.row["id"],
        )
        adapter = FakeHarnessAdapter(
            state={"project": "demo"},
            checklist={
                "project": "demo",
                "items": [
                    {
                        "id": "mvp-001",
                        "title": "Build core",
                        "status": "done",
                        "workflow": {"status": "closed"},
                    }
                ],
            },
        )

        reconcile_workspace(conn, workspace, adapter=adapter)

        mirror = row_to_dict(list_task_mirrors(conn, "demo")[0])
        self.assertEqual(mirror["phase"], "closed")
        self.assertEqual(mirror["branch"], "agents/mvp-001")
        self.assertEqual(mirror["pr"], "https://github.com/o/r/pull/1")
        self.assertEqual(mirror["last_event_id"], event.row["id"])
        self.assertEqual(
            mirror["payload"]["publish_metadata"]["reported_commit"],
            "a" * 40,
        )
        self.assertEqual(mirror["payload"]["status"], "done")

    def test_reconcile_rejects_coordinator_identity_rebind(self):
        for conflicting_item in (
            {
                "id": "mvp-001",
                "status": "doing",
                "workflow": {"status": "running", "branch": "agents/other"},
            },
            {
                "id": "mvp-001",
                "status": "doing",
                "artifacts": {"pr": "https://github.com/o/r/pull/2"},
            },
            {
                "id": "mvp-001",
                "status": "doing",
                "publish_metadata": {"reported_commit": "b" * 40},
            },
        ):
            with self.subTest(item=conflicting_item):
                conn = initialize(":memory:")
                workspace = upsert_workspace(
                    conn,
                    workspace_id="demo",
                    name="Demo",
                    path=".",
                    harness_root=".",
                )
                upsert_task_mirror(
                    conn,
                    workspace_id="demo",
                    task_id="mvp-001",
                    phase="running",
                    owner="codex",
                    branch="agents/mvp-001",
                    pr="https://github.com/o/r/pull/1",
                    payload={
                        "id": "mvp-001",
                        "status": "doing",
                        "publish_metadata": {"reported_commit": "a" * 40},
                    },
                )
                adapter = FakeHarnessAdapter(
                    state={"project": "demo"},
                    checklist={"project": "demo", "items": [conflicting_item]},
                )

                with self.assertRaises(ReconcileConflictError):
                    reconcile_workspace(conn, workspace, adapter=adapter)

                mirror = row_to_dict(list_task_mirrors(conn, "demo")[0])
                self.assertEqual(mirror["branch"], "agents/mvp-001")
                self.assertEqual(mirror["pr"], "https://github.com/o/r/pull/1")
                self.assertEqual(
                    mirror["payload"]["publish_metadata"]["reported_commit"],
                    "a" * 40,
                )


    # -- Harness phase remains authoritative during reconcile --

    def test_reconcile_replaces_legacy_awaiting_operator_with_harness_phase(self):
        """Legacy runtime overlays are replaced by the current harness phase."""
        conn = initialize(":memory:")
        workspace = upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )
        # Pre-create task with awaiting_operator phase
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="phase-8.6",
            phase="awaiting_operator",
            owner="mac-omp",
            branch=None,
            pr=None,
            payload={},
        )

        # Harness says running → legacy awaiting_operator is removed.
        adapter_doing = FakeHarnessAdapter(
            state={"project": "demo"},
            checklist={
                "project": "demo",
                "items": [
                    {
                        "id": "phase-8.6",
                        "title": "Phase 8.6",
                        "status": "doing",
                        "owner": "mac-omp",
                        "workflow": {"status": "running"},
                    },
                ],
            },
        )
        reconcile_workspace(conn, workspace, adapter=adapter_doing, refresh=False)
        tasks = list_task_mirrors(conn, workspace_id="demo")
        self.assertEqual(tasks[0]["phase"], "running")

        # Harness later says done → mirror follows it as well.
        adapter_done = FakeHarnessAdapter(
            state={"project": "demo"},
            checklist={
                "project": "demo",
                "items": [
                    {
                        "id": "phase-8.6",
                        "title": "Phase 8.6",
                        "status": "done",
                        "owner": "mac-omp",
                        "workflow": {"status": "done"},
                    },
                ],
            },
        )
        reconcile_workspace(conn, workspace, adapter=adapter_done, refresh=False)
        tasks = list_task_mirrors(conn, workspace_id="demo")
        self.assertEqual(tasks[0]["phase"], "done",
                         "awaiting_operator should be cleared when harness says done")

    def test_reconcile_awaiting_operator_cleared_when_harness_closed(self):
        """Harness closed → awaiting_operator cleared."""
        conn = initialize(":memory:")
        workspace = upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="phase-8.6",
            phase="awaiting_operator",
            owner="mac-omp",
            branch=None,
            pr=None,
            payload={},
        )

        adapter = FakeHarnessAdapter(
            state={"project": "demo"},
            checklist={
                "project": "demo",
                "items": [
                    {
                        "id": "phase-8.6",
                        "title": "Phase 8.6",
                        "status": "done",
                        "owner": "mac-omp",
                        "workflow": {"status": "closed"},
                    },
                ],
            },
        )
        reconcile_workspace(conn, workspace, adapter=adapter, refresh=False)
        tasks = list_task_mirrors(conn, workspace_id="demo")
        self.assertEqual(tasks[0]["phase"], "closed",
                         "awaiting_operator should be cleared when harness says closed")

class TargetedReconcileTests(unittest.TestCase):
    """Completion 后单任务 mirror 定向收敛（plan §6）。"""

    def _workspace(self, conn):
        return upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )

    def _make_target_reconcile_env(self):
        """Fresh env for a targeted reconcile of a brand-new mvp-001 mirror."""
        conn = initialize(":memory:")
        workspace = self._workspace(conn)
        adapter = FakeHarnessAdapter(
            state={"project": "demo"},
            checklist={
                "project": "demo",
                "items": [
                    {
                        "id": "mvp-001",
                        "title": "Build core",
                        "status": "doing",
                        "workflow": {"status": "running"},
                    },
                ],
            },
        )
        return conn, workspace, adapter

    def test_targeted_updates_only_target_mirror(self):
        conn = initialize(":memory:")
        workspace = self._workspace(conn)
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="todo",
            owner="codex",
            branch=None,
            pr=None,
            payload={"id": "mvp-001", "status": "todo"},
        )
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-002",
            phase="todo",
            owner="codex",
            branch=None,
            pr=None,
            payload={"id": "mvp-002", "status": "todo"},
        )
        before_002 = row_to_dict(list_task_mirrors(conn, "demo")[1])
        adapter = FakeHarnessAdapter(
            state={"project": "demo", "generated_at": "2026-05-17T00:00:00Z"},
            checklist={
                "project": "demo",
                "items": [
                    {
                        "id": "mvp-001",
                        "title": "Build core",
                        "status": "doing",
                        "owner": "codex",
                        "workflow": {"status": "running"},
                    },
                    {
                        "id": "mvp-002",
                        "title": "Review core",
                        "status": "doing",
                        "owner": "codex",
                        "workflow": {"status": "running"},
                    },
                ],
            },
        )

        result = reconcile_workspace(conn, workspace, adapter=adapter, task_id="mvp-001")

        mirrors = {
            m["task_id"]: m
            for m in (row_to_dict(row) for row in list_task_mirrors(conn, "demo"))
        }
        self.assertEqual(mirrors["mvp-001"]["phase"], "running")
        # 无关 item 保持字节不变。
        self.assertEqual(mirrors["mvp-002"], before_002)
        # 输出只反映目标 item。
        self.assertEqual(result.created, 0)
        self.assertEqual(result.updated, 1)
        self.assertEqual(result.unchanged, 0)
        self.assertEqual(len(result.tasks), 1)
        self.assertEqual(result.tasks[0]["task_id"], "mvp-001")
        self.assertEqual(result.scope, {"kind": "task", "task_id": "mvp-001"})
        self.assertEqual(result.to_dict()["scope"], {"kind": "task", "task_id": "mvp-001"})

    def test_targeted_ignores_unrelated_conflict(self):
        conn = initialize(":memory:")
        workspace = self._workspace(conn)
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="todo",
            owner="codex",
            branch=None,
            pr=None,
            payload={},
        )
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-002",
            phase="doing",
            owner="codex",
            branch="agents/keep",
            pr=None,
            payload={},
        )
        adapter = FakeHarnessAdapter(
            state={"project": "demo"},
            checklist={
                "project": "demo",
                "items": [
                    {
                        "id": "mvp-001",
                        "title": "Build core",
                        "status": "doing",
                        "workflow": {"status": "running"},
                    },
                    {
                        "id": "mvp-002",
                        "title": "Review core",
                        "status": "doing",
                        "workflow": {
                            "status": "running",
                            "branch": "agents/other",
                        },
                    },
                ],
            },
        )

        # 无关 item 的 branch conflict 不阻塞目标。
        result = reconcile_workspace(conn, workspace, adapter=adapter, refresh=False, task_id="mvp-001")
        self.assertEqual(result.updated, 1)
        self.assertEqual(result.tasks[0]["task_id"], "mvp-001")
        mirror_002 = row_to_dict(list_task_mirrors(conn, "demo")[1])
        self.assertEqual(mirror_002["branch"], "agents/keep")

        # full reconcile 仍 fail closed。
        with self.assertRaises(ReconcileConflictError):
            reconcile_workspace(conn, workspace, adapter=adapter, refresh=False)

    def test_targeted_target_conflict_fails_closed(self):
        conn = initialize(":memory:")
        workspace = self._workspace(conn)
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="running",
            owner="codex",
            branch="agents/mvp-001",
            pr=None,
            payload={},
        )
        adapter = FakeHarnessAdapter(
            state={"project": "demo"},
            checklist={
                "project": "demo",
                "items": [
                    {
                        "id": "mvp-001",
                        "title": "Build core",
                        "status": "doing",
                        "workflow": {
                            "status": "running",
                            "branch": "agents/other",
                        },
                    },
                ],
            },
        )

        with self.assertRaises(ReconcileConflictError):
            reconcile_workspace(conn, workspace, adapter=adapter, refresh=False, task_id="mvp-001")

        # 零 mutation：mirror 原样，零事件。
        mirror = row_to_dict(list_task_mirrors(conn, "demo")[0])
        self.assertEqual(mirror["branch"], "agents/mvp-001")
        self.assertEqual(list(list_events(conn, "demo")), [])

    def test_targeted_missing_id_zero_mutation(self):
        conn = initialize(":memory:")
        workspace = self._workspace(conn)
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="todo",
            owner="codex",
            branch=None,
            pr=None,
            payload={},
        )
        adapter = FakeHarnessAdapter(
            state={"project": "demo"},
            checklist={
                "project": "demo",
                "items": [{"id": "mvp-001", "title": "Build core", "status": "todo"}],
            },
        )

        with self.assertRaises(ReconcileTaskNotFoundError):
            reconcile_workspace(conn, workspace, adapter=adapter, refresh=False, task_id="mvp-999")

        self.assertEqual(list(list_events(conn, "demo")), [])
        self.assertEqual(list_task_mirrors(conn, "demo")[0]["task_id"], "mvp-001")

    def test_targeted_event_key_scoped_and_idempotent(self):
        conn = initialize(":memory:")
        workspace = self._workspace(conn)
        state = {"project": "demo", "generated_at": "2026-05-17T00:00:00Z"}
        checklist_a = {
            "project": "demo",
            "items": [
                {"id": "mvp-001", "title": "Build core", "status": "todo"},
                {"id": "mvp-002", "title": "Review core", "status": "todo"},
            ],
        }
        checklist_b = {
            "project": "demo",
            "items": [
                {
                    "id": "mvp-001",
                    "title": "Build core",
                    "status": "doing",
                    "workflow": {"status": "running"},
                },
                {"id": "mvp-002", "title": "Review core", "status": "todo"},
            ],
        }
        checklist_c = {
            "project": "demo",
            "items": [
                {
                    "id": "mvp-001",
                    "title": "Build core",
                    "status": "doing",
                    "workflow": {"status": "running"},
                },
                {
                    "id": "mvp-002",
                    "title": "Review core",
                    "status": "doing",
                    "workflow": {"status": "running"},
                },
            ],
        }

        full = reconcile_workspace(
            conn, workspace, adapter=FakeHarnessAdapter(state, checklist_a), refresh=False
        )
        self.assertNotIn("scope", full.to_dict())

        # 目标自身变化 → targeted 只更新目标。
        adapter_b = FakeHarnessAdapter(state, checklist_b)
        first = reconcile_workspace(conn, workspace, adapter=adapter_b, refresh=False, task_id="mvp-001")
        self.assertEqual(first.updated, 1)
        self.assertEqual(first.unchanged, 0)

        # 重放（同 state + 同目标 item）幂等，不新增 summary 事件。
        second = reconcile_workspace(conn, workspace, adapter=adapter_b, refresh=False, task_id="mvp-001")
        self.assertEqual(second.updated, 0)
        self.assertEqual(second.unchanged, 1)
        events = [row_to_dict(e) for e in list_events(conn, "demo")]
        summaries = [e for e in events if e["event_type"] == "reconciliation.completed"]
        self.assertEqual(len(summaries), 2)
        full_key = next(e["idempotency_key"] for e in summaries if "mvp-001" not in e["idempotency_key"])
        scoped_key = next(e["idempotency_key"] for e in summaries if "mvp-001" in e["idempotency_key"])
        self.assertNotEqual(full_key, scoped_key)
        self.assertTrue(scoped_key.startswith("demo:reconcile:mvp-001:"))

        # fingerprint 只覆盖 state + 目标 item：无关 item 变化后重放，key 不变、不新增事件。
        adapter_c = FakeHarnessAdapter(state, checklist_c)
        third = reconcile_workspace(conn, workspace, adapter=adapter_c, refresh=False, task_id="mvp-001")
        self.assertEqual(third.unchanged, 1)
        events = [row_to_dict(e) for e in list_events(conn, "demo")]
        summaries = [e for e in events if e["event_type"] == "reconciliation.completed"]
        self.assertEqual(len(summaries), 2)
        self.assertIn(scoped_key, [e["idempotency_key"] for e in summaries])

    def test_targeted_rolls_back_mirror_when_event_fails(self):
        conn, workspace, adapter = self._make_target_reconcile_env()

        with mock.patch(
            "coordinate.reconcile.append_event",
            side_effect=RuntimeError("event write failed"),
        ):
            with self.assertRaises(RuntimeError):
                reconcile_workspace(conn, workspace, adapter=adapter, refresh=False, task_id="mvp-001")

        # 事件写入失败 → 目标 mirror 与本轮 events 均回滚。
        self.assertEqual(list(list_task_mirrors(conn, "demo")), [])
        self.assertEqual(list(list_events(conn, "demo")), [])

    def test_targeted_rolls_back_mirror_when_summary_event_fails(self):
        conn, workspace, adapter = self._make_target_reconcile_env()
        real_append = coordinate.reconcile.append_event

        def flaky(*args, **kwargs):
            if kwargs.get("event_type") == "reconciliation.completed":
                raise RuntimeError("summary write failed")
            return real_append(*args, **kwargs)

        with mock.patch("coordinate.reconcile.append_event", side_effect=flaky):
            with self.assertRaises(RuntimeError):
                reconcile_workspace(conn, workspace, adapter=adapter, refresh=False, task_id="mvp-001")

        # mirror 已 upsert 但 summary 失败 → 整体回滚，可观察状态不变。
        self.assertEqual(list(list_task_mirrors(conn, "demo")), [])
        self.assertEqual(list(list_events(conn, "demo")), [])

    def test_full_output_keeps_key_set(self):
        conn = initialize(":memory:")
        workspace = self._workspace(conn)
        adapter = FakeHarnessAdapter(
            state={"project": "demo"},
            checklist={
                "project": "demo",
                "items": [{"id": "mvp-001", "title": "Build core", "status": "todo"}],
            },
        )
        result = reconcile_workspace(conn, workspace, adapter=adapter)
        self.assertEqual(
            set(result.to_dict().keys()),
            {
                "workspace_id",
                "project",
                "created",
                "updated",
                "unchanged",
                "events_created",
                "tasks",
            },
        )


if __name__ == "__main__":
    unittest.main()
