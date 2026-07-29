import unittest

from coordinate.db import (
    append_event,
    initialize,
    list_events,
    list_task_mirrors,
    row_to_dict,
    upsert_task_mirror,
    upsert_workspace,
)
from coordinate.reconcile import ReconcileConflictError, reconcile_workspace


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

if __name__ == "__main__":
    unittest.main()
