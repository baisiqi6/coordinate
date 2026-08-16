import json
import tempfile
import unittest
from pathlib import Path
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
from coordinate.onboarding import create_plan_task_record
import coordinate.reconcile
from coordinate.reconcile import (
    ReconcileConflictError,
    ReconcileTaskNotFoundError,
    reconcile_workspace,
)
from coordinate.split_operations import (
    CONTRACT_VERSION,
    OPERATION_KIND_TASK_CREATE,
    apply_task_create_files,
    apply_task_create_record,
    build_task_create_envelope,
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


class SplitOperationProjectionTests(unittest.TestCase):
    """G0: checklist split-operation envelope must be projected to the
    six-key reduced mirror metadata, never copied verbatim; conflicts fail
    closed with zero mutation."""

    OPERATION_ID = "12345678-1234-1234-1234-123456789abc"

    def _workspace(self, conn):
        return upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=".",
            harness_root=".",
        )

    def _meta(self, **overrides):
        meta = {
            "contract_version": CONTRACT_VERSION,
            "operation_id": self.OPERATION_ID,
            "operation_kind": OPERATION_KIND_TASK_CREATE,
            "input_fingerprint": "a" * 64,
            "before_fingerprint": "b" * 64,
            "after_fingerprint": "c" * 64,
        }
        meta.update(overrides)
        return meta

    def _envelope(self, **overrides):
        envelope = build_task_create_envelope(
            operation_id=self.OPERATION_ID,
            workspace_id="demo",
            task_id="mvp-001",
            input_fingerprint="a" * 64,
            before_fingerprint="b" * 64,
            after_fingerprint="c" * 64,
            files_applied_at="2026-07-13T12:00:00Z",
        )
        envelope.update(overrides)
        return envelope

    def _item(self, **overrides):
        item = {
            "id": "mvp-001",
            "title": "Build core",
            "status": "doing",
            "workflow": {"status": "running"},
        }
        item["split_operation"] = self._envelope(**overrides)
        return item

    def _adapter(self, items):
        return FakeHarnessAdapter(
            state={"project": "demo", "generated_at": "2026-05-17T00:00:00Z"},
            checklist={"project": "demo", "items": items},
        )

    def _payload(self, conn):
        return row_to_dict(list_task_mirrors(conn, "demo")[0])["payload"]

    def _raw(self, conn, task_id="mvp-001"):
        return conn.execute(
            "SELECT payload_json FROM tasks WHERE workspace_id = ? AND task_id = ?",
            ("demo", task_id),
        ).fetchone()["payload_json"]

    def test_new_mirror_stores_only_six_field_projection(self):
        """full reconcile：新 mirror 只存六字段投影，envelope 字段不入库。"""
        conn = initialize(":memory:")
        workspace = self._workspace(conn)

        result = reconcile_workspace(conn, workspace, adapter=self._adapter([self._item()]))

        self.assertEqual(result.created, 1)
        meta = self._payload(conn)["split_operation"]
        self.assertEqual(meta, self._meta())
        self.assertEqual(set(meta), set(self._meta()))
        for key in (
            "workspace_id",
            "target_kind",
            "target_id",
            "source_kind",
            "source_id",
            "files_applied_at",
        ):
            self.assertNotIn(key, meta, key)

    def test_existing_reduced_metadata_preserved_idempotently(self):
        """既有六字段 metadata：targeted reconcile 精确保留、重放幂等。"""
        conn = initialize(":memory:")
        workspace = self._workspace(conn)
        item = self._item()
        seeded = {k: v for k, v in item.items() if k != "split_operation"}
        seeded["split_operation"] = self._meta()
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="running",
            owner=None,
            branch=None,
            pr=None,
            payload=seeded,
        )
        raw_before = self._raw(conn)
        adapter = self._adapter([item])

        first = reconcile_workspace(
            conn, workspace, adapter=adapter, refresh=False, task_id="mvp-001"
        )
        self.assertEqual(first.unchanged, 1)
        second = reconcile_workspace(
            conn, workspace, adapter=adapter, refresh=False, task_id="mvp-001"
        )
        self.assertEqual(second.unchanged, 1)
        self.assertEqual(self._raw(conn), raw_before)
        self.assertEqual(self._payload(conn)["split_operation"], self._meta())

    def test_known_full_envelope_pollution_normalized(self):
        """G0 已知污染形态：stored 是 checklist envelope 的完整拷贝 →
        targeted reconcile 有界归一化为六字段。"""
        conn = initialize(":memory:")
        workspace = self._workspace(conn)
        item = self._item()
        # 模拟旧 bug：旧 targeted reconcile 曾把完整 envelope 原样复制进 mirror。
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="running",
            owner=None,
            branch=None,
            pr=None,
            payload={
                "id": "mvp-001",
                "status": "doing",
                "split_operation": item["split_operation"],
            },
        )

        result = reconcile_workspace(
            conn, workspace, adapter=self._adapter([item]), refresh=False, task_id="mvp-001"
        )

        self.assertEqual(result.updated, 1)
        meta = self._payload(conn)["split_operation"]
        self.assertEqual(meta, self._meta())
        self.assertNotIn("files_applied_at", meta)

    def test_stored_mismatch_or_unknown_extra_fails_closed(self):
        """identity/fingerprint 不同、六字段+未知 extra、畸形 stored →
        fail closed，mirror/events 字节级不变。"""
        cases = (
            # 六字段 identity 与 checklist 不同
            self._meta(operation_id="11111111-1111-1111-1111-111111111111"),
            # 六字段 + 未知 extra key：不得泛化接受
            {**self._meta(), "files_applied_at": "2026-07-13T12:00:00Z"},
            # 完整 envelope 但与 checklist 不完全相同
            {**self._envelope(), "after_fingerprint": "d" * 64},
            # 缺必要字段
            {k: v for k, v in self._meta().items() if k != "before_fingerprint"},
            # 类型非法
            None,
        )
        for stored in cases:
            with self.subTest(stored=stored):
                conn = initialize(":memory:")
                workspace = self._workspace(conn)
                upsert_task_mirror(
                    conn,
                    workspace_id="demo",
                    task_id="mvp-001",
                    phase="running",
                    owner=None,
                    branch=None,
                    pr=None,
                    payload={
                        "id": "mvp-001",
                        "status": "doing",
                        "split_operation": stored,
                    },
                )
                raw_before = self._raw(conn)

                with self.assertRaises(ReconcileConflictError):
                    reconcile_workspace(
                        conn,
                        workspace,
                        adapter=self._adapter([self._item()]),
                        refresh=False,
                        task_id="mvp-001",
                    )

                self.assertEqual(self._raw(conn), raw_before)
                self.assertEqual(list(list_events(conn, "demo")), [])

    def test_malformed_checklist_envelope_fails_closed(self):
        """checklist envelope 畸形（null/缺字段/类型非法）→ fail closed，
        零 mirror、零事件（full reconcile，新 mirror 场景）。"""
        bad_envelopes = (
            None,
            {k: v for k, v in self._envelope().items() if k != "input_fingerprint"},
            {**self._envelope(), "after_fingerprint": "x" * 64},
            # unknown extra key：投影必须 fail closed，不得静默忽略
            {**self._envelope(), "unknown_extra": 1},
        )
        for envelope in bad_envelopes:
            with self.subTest(envelope=envelope):
                conn = initialize(":memory:")
                workspace = self._workspace(conn)
                item = self._item()
                item["split_operation"] = envelope

                with self.assertRaises(ReconcileConflictError):
                    reconcile_workspace(conn, workspace, adapter=self._adapter([item]))

                self.assertEqual(list_task_mirrors(conn, "demo"), [])
                self.assertEqual(list(list_events(conn, "demo")), [])

    def test_legacy_item_without_envelope_preserves_stored_metadata(self):
        """checklist 无 split_operation：legacy 行为，stored 六字段不被抹除。"""
        conn = initialize(":memory:")
        workspace = self._workspace(conn)
        seeded = self._meta()
        upsert_task_mirror(
            conn,
            workspace_id="demo",
            task_id="mvp-001",
            phase="todo",
            owner=None,
            branch=None,
            pr=None,
            payload={
                "id": "mvp-001",
                "title": "Build core",
                "status": "todo",
                "split_operation": seeded,
            },
        )
        raw_before = self._raw(conn)
        adapter = FakeHarnessAdapter(
            state={"project": "demo"},
            checklist={
                "project": "demo",
                "items": [
                    {"id": "mvp-001", "title": "Build core", "status": "todo"},
                ],
            },
        )

        result = reconcile_workspace(conn, workspace, adapter=adapter, refresh=False)

        self.assertEqual(result.unchanged, 1)
        self.assertEqual(self._raw(conn), raw_before)
        self.assertEqual(self._payload(conn)["split_operation"], seeded)

    def test_task_create_reconcile_revision_chain_closes(self):
        """G0 根因链：task create → targeted reconcile（含已知 12→6 归一化）
        → plan revision 闭合，六字段值全程不漂移。"""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        plan_path = Path(tmp.name) / "plan.md"
        plan_path.write_text("# Plan v1\n", encoding="utf-8")
        (Path(tmp.name) / "mvp-checklist.json").write_text(
            json.dumps(
                {
                    "project": "demo",
                    "harness_root": ".",
                    "version": 1,
                    "updated_at": "2026-07-13",
                    "items": [],
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        conn = initialize(":memory:")
        self.addCleanup(conn.close)
        workspace = upsert_workspace(
            conn,
            workspace_id="demo",
            name="Demo",
            path=tmp.name,
            harness_root=tmp.name,
        )

        files = apply_task_create_files(
            workspace_path=tmp.name,
            harness_root=tmp.name,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Task 1",
            phase="ready",
            priority="p1",
            operation_id=self.OPERATION_ID,
        )
        apply_task_create_record(
            conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Task 1",
            phase="ready",
            owner=None,
            branch=None,
            actor="operator",
            target=None,
            payload=None,
            operation_id=self.OPERATION_ID,
            input_fingerprint=files.input_fingerprint,
            before_fingerprint=files.before_fingerprint,
            after_fingerprint=files.after_fingerprint,
        )
        record_meta = json.loads(self._raw(conn, "task-1"))["split_operation"]
        self.assertEqual(set(record_meta), set(self._meta()))
        checklist = json.loads(
            (Path(tmp.name) / "mvp-checklist.json").read_text(encoding="utf-8")
        )
        envelope = checklist["items"][0]["split_operation"]
        self.assertEqual(set(envelope), set(self._envelope()))

        # 已知旧 bug：旧 targeted reconcile 曾把完整 envelope 原样复制进 mirror。
        polluted = json.loads(self._raw(conn, "task-1"))
        polluted["split_operation"] = envelope
        conn.execute(
            "UPDATE tasks SET payload_json = ? WHERE workspace_id = ? AND task_id = ?",
            (json.dumps(polluted), "demo", "task-1"),
        )

        result = reconcile_workspace(
            conn,
            workspace,
            adapter=FakeHarnessAdapter(
                state={"project": "demo", "generated_at": "2026-05-17T00:00:00Z"},
                checklist=checklist,
            ),
            refresh=False,
            task_id="task-1",
        )
        self.assertEqual(result.scope, {"kind": "task", "task_id": "task-1"})
        self.assertEqual(result.updated, 1)
        after_reconcile = json.loads(self._raw(conn, "task-1"))
        self.assertEqual(after_reconcile["split_operation"], record_meta)
        self.assertNotIn("files_applied_at", after_reconcile["split_operation"])

        # plan revision：修复前 mirror 被 12 字段 envelope 污染时这里 fail closed。
        plan_path.write_text("# Plan v1 revised\n", encoding="utf-8")
        revised = create_plan_task_record(
            conn,
            workspace_id="demo",
            task_id="task-1",
            plan_doc="plan.md",
            title="Task 1",
            phase="ready",
            actor="operator",
        )
        self.assertTrue(revised.event_created)
        after_revise = json.loads(self._raw(conn, "task-1"))
        self.assertEqual(after_revise["split_operation"], record_meta)
        self.assertNotIn("files_applied_at", after_revise["split_operation"])


if __name__ == "__main__":
    unittest.main()
